"""Unit tests for dataset/job state path resolution."""

from pathlib import Path

from denbust.models.common import DatasetName, JobName
from denbust.store.state_paths import resolve_dataset_state_paths


def test_resolve_state_paths_derives_namespaced_defaults() -> None:
    """Derived state paths should be scoped by dataset and job."""
    paths = resolve_dataset_state_paths(
        state_root=Path("data"),
        dataset_name=DatasetName.NEWS_ITEMS,
        job_name=JobName.INGEST,
    )

    assert paths.namespace_dir == Path("data/news_items/ingest")
    assert paths.seen_path == Path("data/news_items/ingest/seen.json")
    assert paths.runs_dir == Path("data/news_items/ingest/runs")
    assert paths.publication_dir == Path("data/news_items/ingest/publication")


def test_resolve_state_paths_honors_explicit_overrides() -> None:
    """Explicit file and directory overrides should bypass derivation."""
    paths = resolve_dataset_state_paths(
        state_root=Path("data"),
        dataset_name=DatasetName.EVENTS,
        job_name=JobName.BACKUP,
        seen_path=Path("/tmp/custom-seen.json"),
        runs_dir=Path("/tmp/custom-runs"),
        publication_dir=Path("/tmp/custom-publication"),
    )

    assert paths.namespace_dir == Path("data/events/backup")
    assert paths.seen_path == Path("/tmp/custom-seen.json")
    assert paths.runs_dir == Path("/tmp/custom-runs")
    assert paths.publication_dir == Path("/tmp/custom-publication")
