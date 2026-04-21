"""Unit tests for discovery-layer state path helpers."""

import json
from datetime import UTC, datetime
from pathlib import Path

from denbust.discovery.models import BackfillBatch, PersistentCandidate
from denbust.discovery.state_paths import (
    discovery_snapshot_filename,
    resolve_discovery_state_paths,
    write_candidate_jsonl,
    write_discovery_run_snapshot,
    write_json_snapshot,
    write_metrics_snapshot,
    write_model_jsonl,
)
from denbust.models.common import DatasetName


def test_resolve_discovery_state_paths_uses_discover_namespace() -> None:
    """Discovery-layer state files should live under dataset/discover."""
    paths = resolve_discovery_state_paths(
        state_root=Path("state_repo"),
        dataset_name=DatasetName.NEWS_ITEMS,
    )

    assert paths.namespace_dir == Path("state_repo/news_items/discover")
    assert paths.runs_dir == Path("state_repo/news_items/discover/runs")
    assert paths.latest_candidates_path == Path(
        "state_repo/news_items/discover/candidates/latest_candidates.jsonl"
    )
    assert paths.latest_backfill_batches_path == Path(
        "state_repo/news_items/discover/backfill_batches/latest_backfill_batches.jsonl"
    )
    assert paths.scrape_attempts_path == Path(
        "state_repo/news_items/discover/candidates/scrape_attempts.jsonl"
    )
    assert paths.engine_overlap_latest_path == Path(
        "state_repo/news_items/discover/metrics/engine_overlap_latest.json"
    )
    assert paths.discovery_diagnostics_latest_path == Path(
        "state_repo/news_items/discover/metrics/discovery_diagnostics_latest.json"
    )


def test_discovery_snapshot_filename_is_git_safe() -> None:
    """Discovery snapshots should use the shared timestamp-safe filename format."""
    filename = discovery_snapshot_filename(datetime(2026, 4, 10, 12, 0, 1, tzinfo=UTC))

    assert filename == "2026-04-10T12-00-01-000000Z.json"


def test_write_discovery_layer_artifacts(tmp_path: Path) -> None:
    """Helpers should write JSON and JSONL candidate artifacts."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    candidate = PersistentCandidate(
        current_url="https://www.ynet.co.il/item",
        titles=["Example candidate"],
        discovered_via=["brave"],
    )

    run_path = write_discovery_run_snapshot(
        paths.runs_dir,
        {"run_id": "run-1", "status": "running"},
        run_timestamp=datetime(2026, 4, 10, 12, 0, 1, tzinfo=UTC),
    )
    candidates_path = write_candidate_jsonl(paths.latest_candidates_path, [candidate])
    batches_path = write_model_jsonl(
        paths.latest_backfill_batches_path,
        [
            BackfillBatch(
                batch_id="batch-1",
                requested_date_from=datetime(2026, 1, 1, tzinfo=UTC),
                requested_date_to=datetime(2026, 1, 7, tzinfo=UTC),
            )
        ],
    )
    metrics_path = write_metrics_snapshot(paths.engine_overlap_latest_path, {"brave": 3})
    snapshot_path = write_json_snapshot(
        paths.backfill_batches_dir / "batch-1.json",
        {"batch_id": "batch-1"},
    )

    assert run_path.exists()
    assert candidates_path.exists()
    assert batches_path.exists()
    assert metrics_path.exists()
    assert snapshot_path.exists()
    assert json.loads(metrics_path.read_text(encoding="utf-8")) == {"brave": 3}
    candidate_lines = candidates_path.read_text(encoding="utf-8").splitlines()
    assert len(candidate_lines) == 1
    candidate_record = json.loads(candidate_lines[0])
    assert candidate_record["current_url"] == "https://www.ynet.co.il/item"
