"""Persist per-run pipeline snapshots."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from denbust.data_models import UnifiedItem


class RunSnapshot(BaseModel):
    """Summary of a single denbust pipeline run."""

    run_timestamp: datetime
    config_name: str
    days_searched: int
    output_formats: list[str]
    raw_article_count: int = 0
    unseen_article_count: int = 0
    relevant_article_count: int = 0
    unified_item_count: int = 0
    seen_count_before: int = 0
    seen_count_after: int = 0
    items: list[UnifiedItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


def snapshot_filename(run_timestamp: datetime) -> str:
    """Build a git-safe filename for a run snapshot."""
    safe_timestamp = run_timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"{safe_timestamp}.json"


def write_run_snapshot(runs_dir: Path, snapshot: RunSnapshot) -> Path:
    """Write a pipeline run snapshot to disk."""
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / snapshot_filename(snapshot.run_timestamp)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot.model_dump(mode="json"), f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path
