"""Unit tests for the gzip-aware JSONL state I/O helpers."""

from __future__ import annotations

import gzip
from pathlib import Path

from pydantic import BaseModel

from denbust.discovery import jsonl_io


class _Row(BaseModel):
    id: str
    n: int


def test_write_jsonl_gz_roundtrips_and_compresses(tmp_path: Path) -> None:
    """A .gz path is written as real gzip and reads back identically."""
    path = tmp_path / "rows.jsonl.gz"
    jsonl_io.write_jsonl(path, ["a", "b", "c"])

    # The physical file is gzip, not plain text.
    with path.open("rb") as raw:
        assert raw.read(2) == b"\x1f\x8b"  # gzip magic
    assert list(jsonl_io.iter_jsonl_lines(path)) == ["a", "b", "c"]


def test_plain_jsonl_path_stays_uncompressed(tmp_path: Path) -> None:
    """A non-.gz path is written as plain text (suffix-driven)."""
    path = tmp_path / "rows.jsonl"
    jsonl_io.write_jsonl(path, ["x", "y"])

    assert path.read_text(encoding="utf-8") == "x\ny\n"
    assert list(jsonl_io.iter_jsonl_lines(path)) == ["x", "y"]


def test_read_falls_back_to_legacy_uncompressed_sibling(tmp_path: Path) -> None:
    """Reading a .gz path finds a pre-existing uncompressed .jsonl sibling."""
    gz_path = tmp_path / "rows.jsonl.gz"
    legacy = tmp_path / "rows.jsonl"
    legacy.write_text("old1\n\nold2\n", encoding="utf-8")  # includes a blank line

    assert jsonl_io.state_file_exists(gz_path) is True
    assert list(jsonl_io.iter_jsonl_lines(gz_path)) == ["old1", "old2"]


def test_write_gz_drops_stale_legacy_sibling(tmp_path: Path) -> None:
    """Once the .gz file is authoritative the legacy sibling is removed."""
    gz_path = tmp_path / "rows.jsonl.gz"
    legacy = tmp_path / "rows.jsonl"
    legacy.write_text("stale\n", encoding="utf-8")

    jsonl_io.write_jsonl(gz_path, ["fresh"])

    assert not legacy.exists()
    assert list(jsonl_io.iter_jsonl_lines(gz_path)) == ["fresh"]


def test_append_concatenates_gzip_members(tmp_path: Path) -> None:
    """Appending to a .gz file writes additional members that read back in order."""
    path = tmp_path / "log.jsonl.gz"
    jsonl_io.append_jsonl(path, ["one"])
    jsonl_io.append_jsonl(path, ["two", "three"])

    # Reads transparently across the concatenated gzip members.
    assert list(jsonl_io.iter_jsonl_lines(path)) == ["one", "two", "three"]
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        assert handle.read() == "one\ntwo\nthree\n"


def test_append_migrates_legacy_then_extends_single_gz(tmp_path: Path) -> None:
    """First append folds a legacy .jsonl into the gz file; data is not split."""
    gz_path = tmp_path / "log.jsonl.gz"
    legacy = tmp_path / "log.jsonl"
    legacy.write_text("legacy1\nlegacy2\n", encoding="utf-8")

    jsonl_io.append_jsonl(gz_path, ["new1"])

    assert not legacy.exists()  # migrated, not left behind
    assert list(jsonl_io.iter_jsonl_lines(gz_path)) == ["legacy1", "legacy2", "new1"]


def test_models_roundtrip(tmp_path: Path) -> None:
    """write_models / append_models / read_models round-trip pydantic rows."""
    path = tmp_path / "models.jsonl.gz"
    jsonl_io.write_models(path, [_Row(id="a", n=1)])
    jsonl_io.append_models(path, [_Row(id="b", n=2)])

    rows = jsonl_io.read_models(path, _Row)
    assert [(r.id, r.n) for r in rows] == [("a", 1), ("b", 2)]


def test_missing_file_reads_empty(tmp_path: Path) -> None:
    """A missing path (and missing legacy sibling) reads as empty, not an error."""
    path = tmp_path / "absent.jsonl.gz"
    assert jsonl_io.state_file_exists(path) is False
    assert list(jsonl_io.iter_jsonl_lines(path)) == []
    assert jsonl_io.read_models(path, _Row) == []


def test_append_empty_is_noop(tmp_path: Path) -> None:
    """Appending nothing does not create a file."""
    path = tmp_path / "log.jsonl.gz"
    jsonl_io.append_jsonl(path, [])
    assert not path.exists()
