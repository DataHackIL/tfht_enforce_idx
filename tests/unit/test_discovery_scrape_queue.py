"""Unit tests for candidate-driven scrape orchestration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from bs4 import BeautifulSoup
from pydantic import HttpUrl

import denbust.discovery.scrape_queue as scrape_queue_module
from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import (
    CandidateStatus,
    ContentBasis,
    FetchStatus,
    PersistentCandidate,
)
from denbust.discovery.scrape_queue import (
    SCRAPEABLE_CANDIDATE_STATUSES,
    GenericFetchResult,
    _metadata_datetime,
    scrape_candidates,
    select_backfill_candidates_for_scrape,
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


class FailingSource(FakeSource):
    """Source stub that raises from fetch."""

    def __init__(self, name: str, error: Exception) -> None:
        super().__init__(name, [])
        self.error = error

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        self.calls += 1
        raise self.error


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
    assert all(
        candidate.candidate_status in SCRAPEABLE_CANDIDATE_STATUSES for candidate in selected
    )


def test_select_candidates_for_scrape_prioritizes_none_and_earlier_retry_times(
    tmp_path: Path,
) -> None:
    """Selection should prefer immediate candidates and earlier due retries."""
    store = build_store(tmp_path)
    due_time = datetime(2026, 4, 11, 10, 0, tzinfo=UTC)
    store.upsert_candidates(
        [
            build_candidate(
                "later_due",
                status=CandidateStatus.SCRAPE_FAILED,
                next_scrape_attempt_at=due_time - timedelta(minutes=5),
            ),
            build_candidate(
                "immediate",
                status=CandidateStatus.NEW,
                next_scrape_attempt_at=None,
            ),
            build_candidate(
                "earlier_due",
                status=CandidateStatus.SCRAPE_FAILED,
                next_scrape_attempt_at=due_time - timedelta(hours=1),
            ),
        ]
    )

    selected = select_candidates_for_scrape(store, limit=10, now=due_time)

    assert [candidate.candidate_id for candidate in selected] == [
        "immediate",
        "earlier_due",
        "later_due",
    ]


def test_metadata_datetime_handles_invalid_and_naive_values() -> None:
    """Backfill metadata parsing should reject invalid strings and normalize naive ones to UTC."""
    invalid_candidate = build_candidate("invalid", status=CandidateStatus.NEW).model_copy(
        update={"metadata": {"backfill_window_start": "not-a-date"}}
    )
    naive_candidate = build_candidate("naive", status=CandidateStatus.NEW).model_copy(
        update={"metadata": {"backfill_window_start": "2026-01-01T12:00:00"}}
    )

    assert _metadata_datetime(invalid_candidate, "backfill_window_start") is None
    assert _metadata_datetime(naive_candidate, "backfill_window_start") == datetime(
        2026, 1, 1, 12, 0, tzinfo=UTC
    )


def test_select_backfill_candidates_for_scrape_respects_explicit_batch_id(tmp_path: Path) -> None:
    """Explicit backfill batch selection should only return candidates from that batch."""
    store = build_store(tmp_path)
    first = build_candidate("first", status=CandidateStatus.NEW).model_copy(
        update={
            "backfill_batch_id": "batch-1",
            "metadata": {"backfill_window_start": "2026-01-01T00:00:00+00:00"},
        }
    )
    second = build_candidate("second", status=CandidateStatus.NEW).model_copy(
        update={
            "backfill_batch_id": "batch-2",
            "metadata": {"backfill_window_start": "2025-12-01T00:00:00+00:00"},
        }
    )
    store.upsert_candidates([first, second])

    selected = select_backfill_candidates_for_scrape(store, limit=10, batch_id="batch-1")

    assert [candidate.candidate_id for candidate in selected] == ["first"]


def test_select_backfill_candidates_for_scrape_uses_batch_filter_when_provided(
    tmp_path: Path,
) -> None:
    """Explicit batch selection should push the batch filter into persistence reads."""
    store = build_store(tmp_path)
    batch_candidate = build_candidate("first", status=CandidateStatus.NEW).model_copy(
        update={"backfill_batch_id": "batch-1"}
    )
    other_candidate = build_candidate("second", status=CandidateStatus.NEW).model_copy(
        update={"backfill_batch_id": "batch-2"}
    )
    store.upsert_candidates([batch_candidate, other_candidate])

    selected = select_backfill_candidates_for_scrape(store, limit=10, batch_id="batch-1")

    assert [candidate.candidate_id for candidate in selected] == ["first"]


@pytest.mark.asyncio
async def test_scrape_candidates_returns_empty_batch_for_no_candidates(tmp_path: Path) -> None:
    """An empty scrape pass should return an empty batch without persistence writes."""
    store = build_store(tmp_path)

    batch = await scrape_candidates(
        config=Config(store={"state_root": tmp_path}),
        persistence=store,
        candidates=[],
        sources=[],
    )

    assert batch.selected_candidates == []
    assert batch.updated_candidates == []
    assert batch.fallback_candidates == []
    assert batch.attempts == []
    assert batch.raw_articles == []
    assert batch.errors == []


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
    assert stored.content_basis is ContentBasis.FULL_ARTICLE_PAGE
    assert stored.scrape_attempt_count == 1
    assert batch.fallback_candidates == []
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
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_failed",
            error_message="Generic fetch failed",
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[source],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-2")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.SCRAPE_FAILED
    assert stored.content_basis is ContentBasis.SEARCH_RESULT_ONLY
    assert stored.scrape_attempt_count == 2
    assert stored.next_scrape_attempt_at is not None
    assert stored.needs_review is True
    assert stored.self_heal_eligible is True
    assert [attempt.fetch_status for attempt in batch.attempts] == [
        FetchStatus.FAILED,
        FetchStatus.FAILED,
    ]
    assert batch.raw_articles == []
    assert [candidate.candidate_id for candidate in batch.fallback_candidates] == ["candidate-2"]
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
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_failed",
            error_message="Generic fetch failed",
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[source],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-3")
    assert stored is not None
    assert stored.scrape_attempt_count == 4
    assert len(batch.attempts) == 2
    assert len(store.list_attempts("candidate-3")) == 2


@pytest.mark.asyncio
async def test_scrape_candidates_retains_unknown_sources_as_search_results(tmp_path: Path) -> None:
    """Candidates without a matching source adapter should retain search-result-only metadata."""
    store = build_store(tmp_path)
    candidate = build_candidate(
        "candidate-4",
        status=CandidateStatus.NEW,
        source_hint="unknown-source",
        current_url="https://unknown.example.com/article",
        canonical_url="https://unknown.example.com/article",
    )
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_failed",
            error_message="Generic fetch failed",
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-4")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.SCRAPE_FAILED
    assert stored.content_basis is ContentBasis.SEARCH_RESULT_ONLY
    assert stored.next_scrape_attempt_at is not None
    assert len(batch.attempts) == 1
    assert batch.attempts[0].fetch_status is FetchStatus.FAILED


@pytest.mark.asyncio
async def test_scrape_candidates_retains_partial_page_metadata(tmp_path: Path) -> None:
    """Generic fetches with limited metadata should become partial-page fallback rows."""
    store = build_store(tmp_path)
    candidate = build_candidate("candidate-5", status=CandidateStatus.NEW)
    store.upsert_candidates([candidate])
    source = FakeSource("ynet", [])
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.PARTIAL,
            title="כותרת חלקית",
            snippet="תיאור חלקי",
            publication_datetime=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[source],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-5")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.PARTIALLY_SCRAPED
    assert stored.content_basis is ContentBasis.PARTIAL_PAGE
    assert stored.next_scrape_attempt_at is None
    assert stored.needs_review is True
    assert stored.self_heal_eligible is False
    assert stored.scrape_attempt_count == 2
    assert stored.metadata["fallback_publication_datetime"] == "2026-04-11T12:00:00+00:00"
    assert [candidate.candidate_id for candidate in batch.fallback_candidates] == ["candidate-5"]


@pytest.mark.asyncio
async def test_scrape_candidates_records_adapter_exceptions_and_continues(
    tmp_path: Path,
) -> None:
    """Adapter exceptions should still fall back to generic retention and continue the batch."""
    store = build_store(tmp_path)
    failing_candidate = build_candidate("candidate-fail", status=CandidateStatus.NEW)
    succeeding_candidate = build_candidate(
        "candidate-ok",
        status=CandidateStatus.NEW,
        current_url="https://www.mako.co.il/news/article/ok?utm_source=test",
        canonical_url="https://www.mako.co.il/news/article/ok",
        source_hint="mako",
    )
    store.upsert_candidates([failing_candidate, succeeding_candidate])
    sources = [
        FailingSource("ynet", RuntimeError("adapter boom")),
        FakeSource(
            "mako",
            [
                build_raw_article(
                    "https://www.mako.co.il/news/article/ok?utm_source=test", source_name="mako"
                )
            ],
        ),
    ]
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_failed",
            error_message="Generic fetch failed",
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[failing_candidate, succeeding_candidate],
            sources=sources,
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    failed = store.get_candidate("candidate-fail")
    succeeded = store.get_candidate("candidate-ok")
    assert failed is not None
    assert failed.candidate_status is CandidateStatus.SCRAPE_FAILED
    assert failed.content_basis is ContentBasis.SEARCH_RESULT_ONLY
    assert failed.next_scrape_attempt_at is not None
    assert failed.last_scrape_error_code == "generic_fetch_failed"
    assert failed.last_scrape_error_message == "Generic fetch failed"
    assert failed.needs_review is True
    assert failed.metadata["fallback_source_name"] == "ynet"
    assert succeeded is not None
    assert succeeded.candidate_status is CandidateStatus.SCRAPE_SUCCEEDED
    assert len(batch.raw_articles) == 1
    assert any(
        error == "candidate-fail: ynet adapter failed: RuntimeError: adapter boom"
        for error in batch.errors
    )
    assert [attempt.fetch_status for attempt in batch.attempts] == [
        FetchStatus.FAILED,
        FetchStatus.FAILED,
        FetchStatus.SUCCESS,
    ]


@pytest.mark.asyncio
async def test_scrape_candidates_retains_partial_fallback_after_adapter_exception(
    tmp_path: Path,
) -> None:
    """Adapter exceptions should still allow partial-page fallback retention."""
    store = build_store(tmp_path)
    candidate = build_candidate("candidate-partial", status=CandidateStatus.NEW)
    store.upsert_candidates([candidate])
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.PARTIAL,
            title="כותרת חלקית",
            snippet="תיאור חלקי",
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        batch = await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[FailingSource("ynet", RuntimeError("adapter boom"))],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-partial")
    assert stored is not None
    assert stored.candidate_status is CandidateStatus.PARTIALLY_SCRAPED
    assert stored.content_basis is ContentBasis.PARTIAL_PAGE
    assert stored.scrape_attempt_count == 2
    assert stored.metadata["fallback_source_name"] == "ynet"
    assert [attempt.fetch_status for attempt in batch.attempts] == [
        FetchStatus.FAILED,
        FetchStatus.PARTIAL,
    ]


@pytest.mark.asyncio
async def test_scrape_candidates_records_fallback_metadata_for_final_url_and_diagnostics(
    tmp_path: Path,
) -> None:
    """Search-result fallbacks should persist generic-fetch provenance for review."""
    store = build_store(tmp_path)
    candidate = build_candidate("candidate-6", status=CandidateStatus.NEW)
    store.upsert_candidates([candidate])
    source = FakeSource("ynet", [])
    original_fetch = scrape_candidates.__globals__["_fetch_partial_page"]

    async def fake_fetch_partial_page(
        _candidate: PersistentCandidate, *, client: object
    ) -> GenericFetchResult:
        del client
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_no_metadata",
            error_message="Generic fetch returned a page without usable metadata",
            title="כותרת מגוגל",
            snippet="תקציר מגוגל",
            final_url="https://www.ynet.co.il/news/article/abc?ref=final",
            diagnostics={"content_type": "text/html"},
        )

    scrape_candidates.__globals__["_fetch_partial_page"] = fake_fetch_partial_page

    try:
        await scrape_candidates(
            config=Config(store={"state_root": tmp_path}),
            persistence=store,
            candidates=[candidate],
            sources=[source],
        )
    finally:
        scrape_candidates.__globals__["_fetch_partial_page"] = original_fetch

    stored = store.get_candidate("candidate-6")
    assert stored is not None
    assert (
        stored.metadata["fallback_final_url"] == "https://www.ynet.co.il/news/article/abc?ref=final"
    )
    assert stored.metadata["fallback_diagnostics"] == {"content_type": "text/html"}


@pytest.mark.asyncio
async def test_fetch_partial_page_reuses_passed_client_and_extracts_metadata() -> None:
    """Generic fetch helper should parse title, description, and publication metadata."""
    html = """
    <html>
      <head>
        <title>  Page Title  </title>
        <meta property="og:description" content="  Page description  ">
        <meta name="article:published_time" content="2026-04-11T12:00:00Z">
      </head>
    </html>
    """
    request = httpx.Request("GET", "https://example.com/article")
    response = httpx.Response(
        200,
        request=request,
        text=html,
        headers={"content-type": "text/html; charset=utf-8"},
    )

    class FakeClient:
        async def get(self, url: str) -> httpx.Response:
            assert url == "https://www.ynet.co.il/news/article/abc?utm_source=test"
            return response

    result = await scrape_queue_module._fetch_partial_page(
        build_candidate("candidate-7", status=CandidateStatus.NEW),
        client=FakeClient(),
    )

    assert result.fetch_status is FetchStatus.PARTIAL
    assert result.title == "Page Title"
    assert result.snippet == "Page description"
    assert result.final_url == "https://example.com/article"
    assert result.diagnostics == {"content_type": "text/html; charset=utf-8"}
    assert result.publication_datetime == datetime(2026, 4, 11, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_fetch_partial_page_returns_timeout_blocked_http_error_and_no_metadata() -> None:
    """Generic fetch helper should classify timeout, blocked, transport, and no-metadata outcomes."""
    candidate = build_candidate("candidate-8", status=CandidateStatus.NEW)
    request = httpx.Request("GET", "https://example.com/article")

    class TimeoutClient:
        async def get(self, url: str) -> httpx.Response:
            raise httpx.ReadTimeout("boom", request=httpx.Request("GET", url))

    timeout_result = await scrape_queue_module._fetch_partial_page(
        candidate, client=TimeoutClient()
    )
    assert timeout_result.fetch_status is FetchStatus.TIMEOUT
    assert timeout_result.error_code == "generic_fetch_timeout"

    blocked_response = httpx.Response(403, request=request)

    class BlockedClient:
        async def get(self, url: str) -> httpx.Response:
            del url
            raise httpx.HTTPStatusError("blocked", request=request, response=blocked_response)

    blocked_result = await scrape_queue_module._fetch_partial_page(
        candidate, client=BlockedClient()
    )
    assert blocked_result.fetch_status is FetchStatus.BLOCKED
    assert blocked_result.error_code == "generic_fetch_http_403"

    class ErrorClient:
        async def get(self, url: str) -> httpx.Response:
            raise httpx.RequestError("network", request=httpx.Request("GET", url))

    error_result = await scrape_queue_module._fetch_partial_page(candidate, client=ErrorClient())
    assert error_result.fetch_status is FetchStatus.FAILED
    assert error_result.error_code == "generic_fetch_error"

    no_meta_response = httpx.Response(
        200,
        request=request,
        text="<html><head></head><body>hello</body></html>",
        headers={"content-type": "text/html"},
    )

    class NoMetadataClient:
        async def get(self, url: str) -> httpx.Response:
            del url
            return no_meta_response

    no_meta_result = await scrape_queue_module._fetch_partial_page(
        candidate, client=NoMetadataClient()
    )
    assert no_meta_result.fetch_status is FetchStatus.FAILED
    assert no_meta_result.error_code == "generic_fetch_no_metadata"
    assert no_meta_result.final_url == "https://example.com/article"
    assert no_meta_result.diagnostics == {"content_type": "text/html"}


def test_extract_partial_page_helpers_cover_meta_parsing_edge_cases() -> None:
    """Metadata helpers should normalize values and accept ISO/RFC-2822 publication dates."""
    html = """
    <html>
      <head>
        <meta name="twitter:title" content="  Twitter title  ">
        <meta name="description" content="  Summary text  ">
        <meta name="pubdate" content="Tue, 11 Apr 2026 10:00:00 +0000">
      </head>
    </html>
    """

    parsed = scrape_queue_module._extract_partial_page_metadata(html)

    assert parsed["title"] == "Twitter title"
    assert parsed["snippet"] == "Summary text"
    assert parsed["publication_datetime"] == datetime(2026, 4, 11, 10, 0, tzinfo=UTC)
    assert scrape_queue_module._first_non_empty(None, "  ", " value ") == "value"
    assert scrape_queue_module._first_non_empty(None, "  ") is None
    assert scrape_queue_module._extract_partial_page_metadata(
        '<meta name="description" content><meta name="date" content="2026-04-11T10:00:00">'
    )["publication_datetime"] == datetime(2026, 4, 11, 10, 0, tzinfo=UTC)
    assert (
        scrape_queue_module._extract_partial_page_metadata(
            '<meta name="description" content><meta name="date" content="2026-04-11T10:00:00">'
        )["snippet"]
        is None
    )
    assert (
        scrape_queue_module._meta_content(
            BeautifulSoup('<meta name="description">', "html.parser"),
            name="description",
        )
        is None
    )
    assert scrape_queue_module._parse_publication_datetime("bad date") is None
