"""State-repo path helpers for the pre-classification filter cascade."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.models.common import DatasetName, JobName


class PrefilterStatePaths(BaseModel):
    """Resolved artifact paths for the pre-filter cascade.

    All paths live under ``<state_root>/<dataset>/<job>/prefilter/``.
    No directories are created on resolve; callers are responsible for
    ``mkdir(parents=True, exist_ok=True)`` before writing.
    """

    root: Path
    labels_path: Path
    models_dir: Path
    decisions_dir: Path
    calibration_dir: Path
    reports_dir: Path


def resolve_prefilter_state_paths(
    *,
    state_root: Path,
    dataset_name: DatasetName,
    job_name: JobName = JobName.DISCOVER,
) -> PrefilterStatePaths:
    """Resolve the pre-filter artifact layout for a dataset/job pair.

    Anchors the layout under the discovery namespace directory so prefilter
    artifacts co-locate with the candidate files they operate on.

    Parameters
    ----------
    state_root:
        Repository-relative or absolute root for all state files
        (e.g. ``Path("data")``).
    dataset_name:
        Dataset identifier (e.g. ``DatasetName.NEWS_ITEMS``).
    job_name:
        Job identifier; defaults to ``JobName.DISCOVER`` since prefilter
        artifacts are logically owned by the discover namespace.
    """
    discovery_paths = resolve_discovery_state_paths(
        state_root=state_root,
        dataset_name=dataset_name,
        job_name=job_name,
    )
    root = discovery_paths.namespace_dir / "prefilter"
    return PrefilterStatePaths(
        root=root,
        labels_path=root / "labels.parquet",
        models_dir=root / "models",
        decisions_dir=root / "decisions",
        calibration_dir=root / "calibration",
        reports_dir=root / "reports",
    )
