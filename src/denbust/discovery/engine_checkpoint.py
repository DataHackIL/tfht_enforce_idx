"""Per-query result checkpoint for crash-resilient search-engine discovery.

After every successful API call the raw DiscoveredCandidate objects for that
query are written to a small JSONL file under ``engine_query_cache_dir``.  On
the next run (e.g. after a crash or forced kill) those files are read back and
re-used instead of re-issuing the API call, as long as they are younger than
``_CACHE_TTL_HOURS``.

Cache layout::

    <engine_query_cache_dir>/
        brave/
            <16-char-hash>.jsonl
        exa/
            <16-char-hash>.jsonl
        ...

The hash is a deterministic SHA-256 prefix of the query fingerprint:
engine name + query_text + query_kind + source_hint + date window +
preferred_domains.  Query-execution order and count parameters are
intentionally excluded so the cache is reusable across runs with different
pagination settings.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from denbust.discovery.models import DiscoveredCandidate, DiscoveryQuery

logger = logging.getLogger(__name__)

_CACHE_TTL_HOURS: int = 24
_HASH_PREFIX_LEN: int = 16


def _query_cache_key(engine: str, query: DiscoveryQuery) -> str:
    """Return a short deterministic hex string that identifies this query."""
    parts: list[str] = [
        str(engine),
        str(query.query_text),
        str(query.query_kind.value),
        str(query.source_hint or ""),
        str(query.date_from.date().isoformat()) if query.date_from is not None else "",
        str(query.date_to.date().isoformat()) if query.date_to is not None else "",
        ",".join(sorted(str(d) for d in query.preferred_domains)),
    ]
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return digest[:_HASH_PREFIX_LEN]


def cache_path(cache_dir: Path, engine: str, query: DiscoveryQuery) -> Path:
    """Return the checkpoint file path for one (engine, query) pair."""
    return cache_dir / engine / f"{_query_cache_key(engine, query)}.jsonl"


def query_last_run_at(cache_dir: Path, engine: str, query: DiscoveryQuery) -> datetime | None:
    """Return when *query* was last issued live on *engine*, or None if never.

    The checkpoint file's mtime is written each time the query hits the API, so
    it doubles as a per-query last-run timestamp for cross-run rotation.
    """
    path = cache_path(cache_dir, engine, query)
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, UTC)


def load_cached_candidates(
    path: Path,
    *,
    max_age_hours: int = _CACHE_TTL_HOURS,
) -> list[DiscoveredCandidate] | None:
    """Load candidates from a checkpoint file if it exists and is fresh.

    Returns *None* when the file does not exist or has expired; the caller
    should then issue a live API call and save the result via
    :func:`save_cached_candidates`.
    """
    if not path.exists():
        return None
    age = datetime.now(UTC) - datetime.fromtimestamp(path.stat().st_mtime, UTC)
    if age > timedelta(hours=max_age_hours):
        logger.debug("checkpoint expired (age=%s): %s", age, path)
        return None
    candidates: list[DiscoveredCandidate] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                candidates.append(DiscoveredCandidate.model_validate_json(line))
    logger.debug("checkpoint hit (%d candidates): %s", len(candidates), path)
    return candidates


def save_cached_candidates(path: Path, candidates: list[DiscoveredCandidate]) -> None:
    """Write candidates to a checkpoint file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for candidate in candidates:
            handle.write(candidate.model_dump_json())
            handle.write("\n")
    logger.debug("checkpoint saved (%d candidates): %s", len(candidates), path)
