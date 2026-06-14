"""State-repo path helpers for the durable discovery layer."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from denbust.discovery.models import PersistentCandidate
from denbust.discovery.redaction import redact_secrets
from denbust.models.common import DatasetName, JobName
from denbust.store.run_snapshots import snapshot_filename


class DiscoveryStatePaths(BaseModel):
    """Resolved candidate-layer paths under the state root."""

    state_root: Path
    dataset_name: DatasetName
    job_name: JobName
    namespace_dir: Path
    runs_dir: Path
    candidates_dir: Path
    metrics_dir: Path
    backfill_batches_dir: Path
    latest_candidates_path: Path
    latest_backfill_batches_path: Path
    retry_queue_path: Path
    backfill_queue_path: Path
    candidate_provenance_path: Path
    scrape_attempts_path: Path
    engine_overlap_latest_path: Path
    discovery_diagnostics_latest_path: Path
    source_suggestions_latest_path: Path
    backfill_executed_queries_path: Path
    engine_query_cache_dir: Path
    domain_verdicts_path: Path
    search_budget_path: Path
    query_yield_path: Path


def resolve_discovery_state_paths(
    *,
    state_root: Path,
    dataset_name: DatasetName,
    job_name: JobName = JobName.DISCOVER,
) -> DiscoveryStatePaths:
    """Resolve the candidate-layer state layout for a dataset."""
    namespace_dir = state_root / dataset_name / job_name
    runs_dir = namespace_dir / "runs"
    candidates_dir = namespace_dir / "candidates"
    metrics_dir = namespace_dir / "metrics"
    backfill_batches_dir = namespace_dir / "backfill_batches"
    return DiscoveryStatePaths(
        state_root=state_root,
        dataset_name=dataset_name,
        job_name=job_name,
        namespace_dir=namespace_dir,
        runs_dir=runs_dir,
        candidates_dir=candidates_dir,
        metrics_dir=metrics_dir,
        backfill_batches_dir=backfill_batches_dir,
        engine_query_cache_dir=candidates_dir / "engine_query_cache",
        domain_verdicts_path=candidates_dir / "domain_verdicts.jsonl",
        search_budget_path=candidates_dir / "search_budget.jsonl",
        query_yield_path=candidates_dir / "query_yield.json",
        latest_candidates_path=candidates_dir / "latest_candidates.jsonl",
        latest_backfill_batches_path=backfill_batches_dir / "latest_backfill_batches.jsonl",
        retry_queue_path=candidates_dir / "retry_queue.jsonl",
        backfill_queue_path=candidates_dir / "backfill_queue.jsonl",
        candidate_provenance_path=candidates_dir / "candidate_provenance.jsonl",
        scrape_attempts_path=candidates_dir / "scrape_attempts.jsonl",
        engine_overlap_latest_path=metrics_dir / "engine_overlap_latest.json",
        discovery_diagnostics_latest_path=metrics_dir / "discovery_diagnostics_latest.json",
        source_suggestions_latest_path=metrics_dir / "source_suggestions_latest.json",
        backfill_executed_queries_path=backfill_batches_dir / "executed_queries.jsonl",
    )


def discovery_snapshot_filename(run_timestamp: datetime) -> str:
    """Build a discovery-layer snapshot filename using the shared run format."""
    return snapshot_filename(run_timestamp)


def write_discovery_run_snapshot(
    runs_dir: Path, payload: dict[str, Any], *, run_timestamp: datetime
) -> Path:
    """Write a per-run discovery artifact to disk.

    The serialized snapshot is passed through ``redact_secrets`` as a safety net:
    run snapshots hold error strings (and other run metadata), which can carry a
    credential echoed in an API error URL. Snapshots never contain candidate
    bodies, so blanket redaction here is safe.
    """
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / discovery_snapshot_filename(run_timestamp)
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(redact_secrets(serialized))
        handle.write("\n")
    return path


def write_candidate_jsonl(path: Path, candidates: list[PersistentCandidate]) -> Path:
    """Write candidate rows to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for candidate in candidates:
            handle.write(candidate.model_dump_json())
            handle.write("\n")
    return path


def write_model_jsonl(path: Path, rows: Sequence[BaseModel]) -> Path:
    """Write generic Pydantic rows to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(row.model_dump_json())
            handle.write("\n")
    return path


def write_json_snapshot(path: Path, payload: dict[str, Any]) -> Path:
    """Write one JSON snapshot file (e.g. a backfill batch record).

    Redacted before writing as a safety net: these snapshots hold run/batch
    metadata (including error strings) but no candidate bodies.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(redact_secrets(serialized))
        handle.write("\n")
    return path


def write_metrics_snapshot(path: Path, payload: dict[str, Any]) -> Path:
    """Write a JSON metrics artifact for discovery diagnostics (redacted)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(redact_secrets(serialized))
        handle.write("\n")
    return path
