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


def write_run_debug_log(logs_dir: Path, snapshot: RunSnapshot, payload: dict[str, object]) -> Path:
    """Write a detailed diagnostic artifact for a run."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / snapshot_filename(snapshot.run_timestamp)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def write_run_debug_summary(
    logs_dir: Path,
    snapshot: RunSnapshot,
    payload: dict[str, object],
) -> Path:
    """Write a compact machine-oriented diagnostic summary for a run."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / snapshot_filename(snapshot.run_timestamp).replace(".json", ".summary.json")
    summary_payload = {
        "schema_version": payload.get("schema_version"),
        "run_timestamp": payload.get("run_timestamp"),
        "dataset_name": payload.get("dataset_name"),
        "job_name": payload.get("job_name"),
        "config_name": payload.get("config_name"),
        "result_summary": payload.get("result_summary"),
        "counts": payload.get("counts", {}),
        "workflow": payload.get("workflow", {}),
        "source_summaries": payload.get("source_summaries", []),
        "classifier_summary": payload.get("classifier_summary", {}),
        "problems": payload.get("problems", {}),
        "suspicions": payload.get("suspicions", []),
        "warnings": payload.get("warnings", []),
        "errors": payload.get("errors", []),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path
