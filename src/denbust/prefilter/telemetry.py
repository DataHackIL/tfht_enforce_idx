"""Telemetry writer for pre-classification filter decisions."""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime
from pathlib import Path

from denbust.prefilter.models import PrefilterDecision


class PrefilterDecisionWriter:
    """Append-only JSONL writer for :class:`PrefilterDecision` records.

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

    def append(self, decision: PrefilterDecision) -> None:
        """Serialize *decision* and append one line to the day's JSONL file."""
        path = self._path_for(decision.decided_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = dataclasses.asdict(decision)
        # datetime is not JSON-serialisable by default; convert explicitly.
        record["decided_at"] = decision.decided_at.isoformat()
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _path_for(self, decided_at: datetime) -> Path:
        """Return the JSONL path for the UTC date of *decided_at*."""
        return self._decisions_dir / f"{decided_at.strftime('%Y-%m-%d')}.jsonl"
