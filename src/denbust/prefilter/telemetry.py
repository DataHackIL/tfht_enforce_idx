"""Telemetry writer for pre-classification filter decisions."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path

from denbust.prefilter.models import PrefilterDecision


class PrefilterDecisionWriter:
    """Append-only JSONL writer for ``PrefilterDecision`` records.

    Decisions are written to ``<decisions_dir>/<utc_date>.jsonl``, one
    decision per line.  Multiple writer instances pointing at the same
    directory safely append to existing files — there is no truncation on
    construction.

    Parameters
    ----------
    decisions_dir:
        Directory under which per-day JSONL files are created.
        The directory is created on first write if it does not exist.
    """

    def __init__(self, decisions_dir: Path) -> None:
        self._decisions_dir = decisions_dir

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def append(self, decision: PrefilterDecision) -> None:
        """Serialize *decision* and append one line to today's JSONL file."""
        path = self._path_for(decision.decided_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = dataclasses.asdict(decision)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        """No-op: each ``append`` call writes and closes immediately."""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _path_for(self, decided_at: str) -> Path:
        """Return the JSONL path for the date embedded in *decided_at*.

        Falls back to today's UTC date if parsing fails.
        """
        try:
            date_str = decided_at[:10]  # "YYYY-MM-DD"
        except (IndexError, TypeError):
            date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        return self._decisions_dir / f"{date_str}.jsonl"
