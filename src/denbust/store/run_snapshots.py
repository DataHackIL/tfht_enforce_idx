"""Persist per-run pipeline snapshots."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from denbust.models.runs import RunSnapshot


def snapshot_filename(run_timestamp: datetime) -> str:
    """Build a git-safe filename for a run snapshot."""
    safe_timestamp = run_timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    return f"{safe_timestamp}.json"


def write_run_snapshot(runs_dir: Path, snapshot: RunSnapshot) -> Path:
    """Write a pipeline run snapshot to disk."""
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / snapshot_filename(snapshot.run_timestamp)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot.model_dump(mode="json"), f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path
