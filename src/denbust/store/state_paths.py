"""State path resolution for dataset/job-namespaced storage."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from denbust.models.common import DatasetName, JobName


class DatasetStatePaths(BaseModel):
    """Resolved state paths for a specific dataset/job namespace."""

    state_root: Path
    dataset_name: DatasetName
    job_name: JobName
    namespace_dir: Path
    seen_path: Path
    runs_dir: Path
    logs_dir: Path
    publication_dir: Path


def resolve_dataset_state_paths(
    *,
    state_root: Path,
    dataset_name: DatasetName,
    job_name: JobName,
    seen_path: Path | None = None,
    runs_dir: Path | None = None,
    logs_dir: Path | None = None,
    publication_dir: Path | None = None,
) -> DatasetStatePaths:
    """Resolve dataset/job-aware state paths from root and explicit overrides."""
    namespace_dir = state_root / dataset_name / job_name
    resolved_seen_path = seen_path or (namespace_dir / "seen.json")
    resolved_runs_dir = runs_dir or (namespace_dir / "runs")
    resolved_logs_dir = logs_dir or (namespace_dir / "logs")
    resolved_publication_dir = publication_dir or (namespace_dir / "publication")
    return DatasetStatePaths(
        state_root=state_root,
        dataset_name=dataset_name,
        job_name=job_name,
        namespace_dir=namespace_dir,
        seen_path=resolved_seen_path,
        runs_dir=resolved_runs_dir,
        logs_dir=resolved_logs_dir,
        publication_dir=resolved_publication_dir,
    )
