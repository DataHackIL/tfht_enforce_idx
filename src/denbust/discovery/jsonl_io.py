"""Gzip-aware JSONL I/O for the durable discovery state files.

The big rewrite-per-run and append-only discovery state files (the candidate
store, retry/backfill queues, provenance and scrape-attempt logs, backfill
batches and executed-query logs) are stored gzip-compressed (``*.jsonl.gz``) to
keep the git state repo bounded: each is rewritten or extended on every run, and
gzip shrinks each blob roughly 8x.

All I/O here is *suffix-driven* — a path ending in ``.gz`` is read/written as
gzip, any other path as plain text — so the storage format is chosen entirely by
the resolved state path. Reads transparently fall back to a legacy uncompressed
sibling (``foo.jsonl`` for ``foo.jsonl.gz``) so pre-existing state keeps working
without a separate migration step; writing a ``.gz`` file removes that legacy
sibling, and the first append folds any legacy content into the gz file so data
is never split across the two.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import IO, Any, cast

from pydantic import BaseModel

_GZ_SUFFIX = ".gz"


def legacy_sibling(path: Path) -> Path | None:
    """Return the uncompressed sibling of a ``*.gz`` path, or ``None`` otherwise."""
    if path.suffix == _GZ_SUFFIX:
        return path.with_suffix("")
    return None


def resolve_read_path(path: Path) -> Path | None:
    """Return the physical file to read for *path*.

    Prefers *path* as given; for a ``.gz`` path that does not exist yet, falls
    back to a legacy uncompressed sibling. Returns ``None`` when neither exists.
    """
    if path.exists():
        return path
    legacy = legacy_sibling(path)
    if legacy is not None and legacy.exists():
        return legacy
    return None


def state_file_exists(path: Path) -> bool:
    """True if *path* or its legacy uncompressed sibling exists."""
    return resolve_read_path(path) is not None


def iter_jsonl_lines(path: Path) -> Iterator[str]:
    """Yield non-blank, stripped lines from *path* (gz or legacy, transparently)."""
    physical = resolve_read_path(path)
    if physical is None:
        return
    with _open_text(physical, append=False, read=True) as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def read_models(path: Path, model: type[BaseModel]) -> list[Any]:
    """Read and validate every JSONL row in *path* into *model* instances."""
    return [model.model_validate_json(line) for line in iter_jsonl_lines(path)]


def write_jsonl(path: Path, lines: Iterable[str]) -> Path:
    """Rewrite *path* with *lines* (gzip when ``.gz``); drop any legacy sibling."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text(path, append=False, read=False) as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")
    _drop_legacy_sibling(path)
    return path


def write_models(path: Path, rows: Iterable[BaseModel]) -> Path:
    """Rewrite *path* with the JSON serialization of *rows*."""
    return write_jsonl(path, (row.model_dump_json() for row in rows))


def append_jsonl(path: Path, lines: Sequence[str]) -> Path:
    """Append *lines* to *path*.

    For a ``.gz`` path this writes a new gzip member (gzip transparently reads
    concatenated members on the way back). If only a legacy uncompressed sibling
    exists, its contents are folded into a fresh gz file first so data is never
    split across the two files.
    """
    if not lines:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_into_gz(path)
    with _open_text(path, append=True, read=False) as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")
    return path


def append_models(path: Path, rows: Sequence[BaseModel]) -> Path:
    """Append the JSON serialization of *rows* to *path*."""
    return append_jsonl(path, [row.model_dump_json() for row in rows])


def _open_text(path: Path, *, append: bool, read: bool) -> IO[str]:
    """Open *path* as a UTF-8 text stream, gzip-aware by suffix."""
    mode = "rt" if read else "at" if append else "wt"
    if path.suffix == _GZ_SUFFIX:
        # gzip.open's return type is a union for a non-literal mode; in text mode
        # ("rt"/"wt"/"at") it is always a text stream.
        return cast("IO[str]", gzip.open(path, mode, encoding="utf-8"))
    return path.open(mode[0], encoding="utf-8")


def _drop_legacy_sibling(path: Path) -> None:
    """Remove the legacy uncompressed sibling once a ``.gz`` file is authoritative."""
    legacy = legacy_sibling(path)
    if legacy is not None and legacy != path and legacy.exists():
        legacy.unlink()


def _migrate_legacy_into_gz(path: Path) -> None:
    """Fold a legacy uncompressed sibling into a fresh gz file before appending.

    No-op unless *path* is a ``.gz`` file that does not exist yet while its legacy
    sibling does — that is exactly the first append after the format switch.
    """
    if path.suffix != _GZ_SUFFIX or path.exists():
        return
    legacy = legacy_sibling(path)
    if legacy is None or not legacy.exists():
        return
    write_jsonl(path, list(iter_jsonl_lines(legacy)))
