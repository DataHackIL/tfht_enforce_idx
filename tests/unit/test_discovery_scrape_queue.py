"""Unit tests for candidate-driven scrape orchestration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import HttpUrl

from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import CandidateStatus, FetchStatus, PersistentCandidate
from denbust.discovery.scrape_queue import (
    SCRAPEABLE_CANDIDATE_STATUSES,
    scrape_candidates,
    select_candidates_for_scrape,
)
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName


def build_candidate(
    candidate_id: str,
    *,
    status: CandidateStatus,
    source_hint: str = "ynet",
    current_url: str = "https://www.ynet.co.il/news/article/abc?utm_source=test",
    canonical_url: str = "https://www.ynet.co.il/news/article/abc",
    next_scrape_attempt_at: datetime | None = None,
    scrape_attempt_count: int = 0,
) -> PersistentCandidate:
    """Build a persistent candidate fixture."""
    return PersistentCandidate(
        candidate_id=candidate_id,
        canonical_url=HttpUrl(canonical_url),
        current_url=HttpUrl(current_url),
        titles=["title"],
        snippets=["snippet"],
        discovered_via=["source_native"],
        discovery_queries=["בית בושת"],
        source_hints=[source_hint],
        first_seen_at=datetime(2026, 4, 10, 8, 0, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        candidate_status=status,
        next_scrape_attempt_at=next_scrape_attempt_at,
        scrape_attempt_count=scrape_attempt_count,
    )


def build_raw_article(
    url: str = "https://www.ynet.co.il/news/article/abc?utm_source=test",
    *,
    source_name: str = "ynet",
) -> RawArticle:
    """Build a raw article fixture."""
    return RawArticle(
        url=HttpUrl(url),
        title="פשיטה על בית בושת",
        snippet="המשטרה ביצעה פשיטה.",
        date=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        source_name=source_name,
    )


class FakeSource:
    """Simple source stub used by the scrape queue tests."""

    def __init__(self, name: str, articles: list[RawArticle]) -> None:
        self.name = name
        self.articles = articles
        self.calls = 0

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        self.calls += 1
        return self.articles


def build_store(tmp_path: Path) -> StateRepoDiscoveryPersistence:
    """Create a state-repo persistence backend rooted in pytest temp storage."""
    return StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(
            state_root=tmp_path,
            dataset_name=DatasetName.NEWS_ITEMS,
        )
    )


def test_select_candidates_for_scrape_filters_retryable_due_candidates(tmp_path: Path) -> None:
    """Only retryable candidates whose retry window has elapsed should be selected."""
    store = build_store(tmp_path)
    due_time = datetime(2026, 4, 11, 10, 0, tzinfo=UTC)
    store.upsert_candidates(
        [
            build_candidate("new", status=CandidateStatus.NEW),
            build_candidate(
                "failed_due",
                status=CandidateStatus.SCRAPE_FAILED,
                next_scrape_attempt_at=due_time - timedelta(hours=1),
            ),
            build_candidate(
                "failed_future",
                status=CandidateStatus.SCRAPE_FAILED,
                next_scrape_attempt_at=due_time + timedelta(hours=1),
            ),
            build_candidate("closed", status=CandidateStatus.CLOSED),
        ]
    )

    selected = select_candidates_for_scrape(store, limit=10, now=due_time)

    assert {candidate.candidate_id for candidate in selected} == {"new", "failed_due"}
    assert all(candidate.candidate_status in SCRAPEABLE_CANDIDATE_STATUSES for candidate in selected)


@pytest.mark.asyncio
async def test_scrape_candidates_records_success_and_updates_candidate(tmp_path: Path) -> None:
    """Successful source-adapter scrapes should yield raw articles and succeeded candidates."""
    store = build_store(tmp_path)
    candidate = build_candidate("candidate-1", status=CandidateStatus.NEW)
    store.upsert_candidates([candidate])
    source = FakeSource("ynet", [build_raw_article()])

    batch = await scrape_candidates(
        config=Config(store={"state_root": tmp_path}),
        persistence=store,
        candidates=[candidate],
        sources=[source],
        preloaded_source_articles={"ynet": [build_raw_article()]},
    )

    stored = store.get_candidate("candidate-1")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.SCRAPE_SUCCEEDED
    assert stored.scrape_attempt_count == 1
    assert len(batch.raw_articles) == 1
    assert len(batch.attempts) == 1
    assert batch.attempts[0].fetch_status is FetchStatus.SUCCESS
    assert source.calls == 0


@pytest.mark.asyncio
async def test_scrape_candidates_retains_failed_candidate_for_retry(tmp_path: Path) -> None:
    """Missing source matches should keep the candidate retryable with attempt history."""
    store = build_store(tmp_path)
    candidate = build_candidate("candidate-2", status=CandidateStatus.SCRAPE_FAILED)
    store.upsert_candidates([candidate])
    source = FakeSource("ynet", [])

    batch = await scrape_candidates(
        config=Config(store={"state_root": tmp_path}),
        persistence=store,
        candidates=[candidate],
        sources=[source],
    )

    stored = store.get_candidate("candidate-2")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.SCRAPE_FAILED
    assert stored.scrape_attempt_count == 2
    assert stored.next_scrape_attempt_at is not None
    assert [attempt.fetch_status for attempt in batch.attempts] == [
        FetchStatus.FAILED,
        FetchStatus.UNSUPPORTED,
    ]
    assert batch.raw_articles == []
    assert len(store.list_attempts("candidate-2")) == 2


@pytest.mark.asyncio
async def test_scrape_candidates_accumulates_attempt_count_across_retries(tmp_path: Path) -> None:
    """Repeated scrape passes should append attempts rather than resetting candidate history."""
    store = build_store(tmp_path)
    candidate = build_candidate(
        "candidate-3",
        status=CandidateStatus.SCRAPE_FAILED,
        scrape_attempt_count=2,
        next_scrape_attempt_at=datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
    )
    store.upsert_candidates([candidate])
    source = FakeSource("ynet", [])

    batch = await scrape_candidates(
        config=Config(store={"state_root": tmp_path}),
        persistence=store,
        candidates=[candidate],
        sources=[source],
    )

    stored = store.get_candidate("candidate-3")
    assert stored is not None
    assert stored.scrape_attempt_count == 4
    assert len(batch.attempts) == 2
    assert len(store.list_attempts("candidate-3")) == 2


@pytest.mark.asyncio
async def test_scrape_candidates_marks_unknown_sources_as_unsupported(tmp_path: Path) -> None:
    """Candidates without a matching source adapter should become unsupported."""
    store = build_store(tmp_path)
    candidate = build_candidate(
        "candidate-4",
        status=CandidateStatus.NEW,
        source_hint="unknown-source",
        current_url="https://unknown.example.com/article",
        canonical_url="https://unknown.example.com/article",
    )

    batch = await scrape_candidates(
        config=Config(store={"state_root": tmp_path}),
        persistence=store,
        candidates=[candidate],
        sources=[],
    )

    stored = store.get_candidate("candidate-4")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.UNSUPPORTED_SOURCE
    assert stored.next_scrape_attempt_at is None
    assert len(batch.attempts) == 2
    assert batch.attempts[0].fetch_status is FetchStatus.UNSUPPORTED
