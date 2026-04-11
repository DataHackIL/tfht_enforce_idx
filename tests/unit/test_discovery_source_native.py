"""Unit tests for source-native discovery candidacy persistence."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.discovery.models import DiscoveryRun
from denbust.discovery.source_native import (
    persist_discovered_candidates,
    raw_article_to_discovered_candidate,
)
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName


def build_raw_article(
    url: str,
    *,
    source_name: str = "ynet",
    title: str = "פשיטה על בית בושת",
    discovered_at: datetime | None = None,
) -> RawArticle:
    """Build a raw article fixture for source-native discovery tests."""
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet="המשטרה ביצעה פשיטה.",
        date=discovered_at or datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        source_name=source_name,
    )


def test_raw_article_to_discovered_candidate_uses_source_native_defaults() -> None:
    """Raw source articles should normalize into source-native discovered candidates."""
    article = build_raw_article("https://www.ynet.co.il/news/article/abc?utm_source=test")

    candidate = raw_article_to_discovered_candidate(article)

    assert candidate.producer_kind.value == "source_native"
    assert str(candidate.canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidate.source_hint == "ynet"
    assert candidate.metadata["source_name"] == "ynet"


def test_persist_discovered_candidates_merges_repeat_discovery_by_canonical_url(
    tmp_path: Path,
) -> None:
    """Repeated discovery of the same article should upsert a single durable candidate."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    first_seen = datetime(2026, 4, 11, 8, 0, tzinfo=UTC)
    second_seen = first_seen + timedelta(hours=2)

    first_discovery = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://www.ynet.co.il/news/article/abc?utm_source=test",
            title="פשיטה ראשונה",
            discovered_at=first_seen,
        ),
        discovered_at=first_seen,
    )
    second_discovery = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://ynet.co.il/news/article/abc?Partner=searchResults",
            title="פשיטה ראשונה עודכן",
            discovered_at=second_seen,
        ),
        discovered_at=second_seen,
    )

    persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-1"),
        discovered_candidates=[first_discovery],
        persistence=persistence,
    )
    persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-2"),
        discovered_candidates=[second_discovery],
        persistence=persistence,
    )

    candidates = persistence.list_candidates()
    assert len(candidates) == 1
    candidate = candidates[0]
    assert str(candidate.canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidate.titles == ["פשיטה ראשונה", "פשיטה ראשונה עודכן"]
    assert candidate.source_discovery_only is True
    assert candidate.first_seen_at == first_seen
    assert candidate.last_seen_at == second_seen

    provenance = persistence.list_provenance(candidate.candidate_id)
    assert len(provenance) == 2
    run_snapshots = sorted(paths.runs_dir.glob("*.json"))
    assert len(run_snapshots) == 2
    latest_snapshot = json.loads(run_snapshots[-1].read_text(encoding="utf-8"))
    assert latest_snapshot["merged_candidate_count"] == 1
    assert latest_snapshot["finished_at"] is not None


def test_persist_discovered_candidates_marks_runs_finished_on_success(tmp_path: Path) -> None:
    """Successful persistence should stamp a terminal finished_at value on the run."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = raw_article_to_discovered_candidate(
        build_raw_article("https://www.ynet.co.il/news/article/xyz")
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-finished"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    assert persisted.run.status.value == "succeeded"
    assert persisted.run.finished_at is not None


def test_persist_discovered_candidates_writes_failed_run_before_reraising() -> None:
    """Persistence failures should update the run record to failed before propagating."""

    class FailingPersistence(StateRepoDiscoveryPersistence):
        def __init__(self, paths: object) -> None:
            super().__init__(paths)
            self.written_runs: list[DiscoveryRun] = []

        def write_run(self, run: DiscoveryRun) -> None:
            self.written_runs.append(run.model_copy(deep=True))

        def upsert_candidates(self, candidates: object) -> None:
            del candidates
            raise RuntimeError("candidate write failed")

    paths = resolve_discovery_state_paths(
        state_root=Path("/tmp/discovery-source-native-failure"),
        dataset_name=DatasetName.NEWS_ITEMS,
    )
    persistence = FailingPersistence(paths)
    discovery = raw_article_to_discovered_candidate(
        build_raw_article("https://www.ynet.co.il/news/article/failure")
    )

    try:
        persist_discovered_candidates(
            run=DiscoveryRun(run_id="run-failure"),
            discovered_candidates=[discovery],
            persistence=persistence,
        )
    except RuntimeError as exc:
        assert str(exc) == "candidate write failed"
    else:
        raise AssertionError("expected RuntimeError")

    assert len(persistence.written_runs) == 1
    failed_run = persistence.written_runs[0]
    assert failed_run.status.value == "failed"
    assert failed_run.finished_at is not None
    assert failed_run.errors == ["persistence: RuntimeError: candidate write failed"]
