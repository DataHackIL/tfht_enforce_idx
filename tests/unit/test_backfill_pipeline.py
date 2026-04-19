"""Unit tests for backfill pipeline jobs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import HttpUrl

from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import (
    CandidateStatus,
    DiscoveryRun,
    DiscoveryRunStatus,
    PersistentCandidate,
)
from denbust.discovery.source_native import PersistedSourceDiscovery
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName
from denbust.pipeline import run_news_backfill_discover_job, run_news_backfill_scrape_job


class FakeSource:
    """Minimal source stub for backfill pipeline tests."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        return []


def build_candidate(batch_id: str) -> PersistentCandidate:
    """Create one batch-linked candidate fixture."""
    return PersistentCandidate(
        candidate_id="candidate-1",
        current_url=HttpUrl("https://example.com/article"),
        canonical_url=HttpUrl("https://example.com/article"),
        titles=["title"],
        snippets=["snippet"],
        discovered_via=["brave"],
        discovery_queries=["בית בושת"],
        source_hints=["ynet"],
        first_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
        last_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
        backfill_batch_id=batch_id,
        metadata={"backfill_window_start": datetime(2026, 1, 1, tzinfo=UTC).isoformat()},
    )


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_succeeds_without_operational_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill discover should succeed with discovery persistence only."""
    monkeypatch.setenv("DENBUST_BACKFILL_DATE_FROM", "2026-01-01T00:00:00+00:00")
    monkeypatch.setenv("DENBUST_BACKFILL_DATE_TO", "2026-01-03T00:00:00+00:00")
    store = StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    )

    async def fake_engine(
        *,
        config: Config,
        run_id: str,
        batch_id: str,
        window,
        engine_name: str,
    ) -> PersistedSourceDiscovery:
        del config, run_id, window, engine_name
        candidate = build_candidate(batch_id)
        store.upsert_candidates([candidate])
        return PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-1",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.BACKFILL_DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                query_count=1,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[candidate],
            provenance=[],
            warnings=[],
        )

    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
    monkeypatch.setattr("denbust.pipeline._run_backfill_engine_discovery", fake_engine)

    result = await run_news_backfill_discover_job(
        Config(
            job_name=JobName.BACKFILL_DISCOVER,
            store={"state_root": tmp_path},
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {"brave": {"enabled": True}},
            },
        )
    )

    assert result.fatal is False
    assert "backfill discovery persisted 1 candidate(s)" in (result.result_summary or "")
    assert store.list_backfill_batches(limit=1)[0].merged_candidate_count == 1


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_finishes_when_queue_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill scrape should finish cleanly when no historical candidates are eligible."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [FakeSource("ynet")])
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))
    monkeypatch.setattr(
        "denbust.pipeline.create_discovery_persistence",
        lambda _config: StateRepoDiscoveryPersistence(
            resolve_discovery_state_paths(
                state_root=tmp_path,
                dataset_name=DatasetName.NEWS_ITEMS,
            )
        ),
    )

    result = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )

    assert result.fatal is False
    assert result.result_summary == "no queued backfill candidates eligible for scrape"
