"""Unit tests for prefilter.state_paths — path resolution."""

from __future__ import annotations

from pathlib import Path

from denbust.models.common import DatasetName, JobName
from denbust.prefilter.state_paths import PrefilterStatePaths, resolve_prefilter_state_paths


def test_paths_live_under_prefilter_namespace() -> None:
    paths = resolve_prefilter_state_paths(
        state_root=Path("state_repo"),
        dataset_name=DatasetName.NEWS_ITEMS,
    )
    assert paths.root == Path("state_repo/news_items/discover/prefilter")
    assert paths.labels_path == Path("state_repo/news_items/discover/prefilter/labels.parquet")
    assert paths.models_dir == Path("state_repo/news_items/discover/prefilter/models")
    assert paths.decisions_dir == Path("state_repo/news_items/discover/prefilter/decisions")
    assert paths.calibration_dir == Path("state_repo/news_items/discover/prefilter/calibration")
    assert paths.reports_dir == Path("state_repo/news_items/discover/prefilter/reports")


def test_job_name_propagates() -> None:
    paths = resolve_prefilter_state_paths(
        state_root=Path("data"),
        dataset_name=DatasetName.NEWS_ITEMS,
        job_name=JobName.DISCOVER,
    )
    assert "discover" in str(paths.root)


def test_resolve_does_not_create_directories(tmp_path: Path) -> None:
    paths = resolve_prefilter_state_paths(
        state_root=tmp_path,
        dataset_name=DatasetName.NEWS_ITEMS,
    )
    # None of the directories should be auto-created by resolve alone
    assert not paths.root.exists()
    assert not paths.models_dir.exists()
    assert not paths.decisions_dir.exists()
    assert not paths.calibration_dir.exists()


def test_paths_model_is_pydantic() -> None:
    paths = resolve_prefilter_state_paths(
        state_root=Path("data"),
        dataset_name=DatasetName.NEWS_ITEMS,
    )
    assert isinstance(paths, PrefilterStatePaths)
    # Pydantic model — should be dumpable
    dumped = paths.model_dump()
    assert "root" in dumped
    assert "decisions_dir" in dumped
