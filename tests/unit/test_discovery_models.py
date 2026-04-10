"""Unit tests for persistent discovery-layer models."""

from datetime import UTC, datetime, timedelta

import pytest

from denbust.discovery.models import (
    CandidateProvenance,
    CandidateStatus,
    ContentBasis,
    DiscoveredCandidate,
    DiscoveryQuery,
    DiscoveryQueryKind,
    DiscoveryRun,
    DiscoveryRunStatus,
    FetchStatus,
    PersistentCandidate,
    ProducerKind,
    ScrapeAttempt,
    ScrapeAttemptKind,
    _domain_from_url,
)
from denbust.models.common import JobName


def test_discovery_query_validates_date_window() -> None:
    """Discovery queries should reject inverted date windows."""
    query = DiscoveryQuery(
        query_text='site:example.com "בית בושת"',
        date_from=datetime(2026, 4, 1, tzinfo=UTC),
        date_to=datetime(2026, 4, 2, tzinfo=UTC),
        query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
    )

    assert query.query_kind == DiscoveryQueryKind.SOURCE_TARGETED

    with pytest.raises(ValueError):
        DiscoveryQuery(
            query_text="bad",
            date_from=datetime(2026, 4, 3, tzinfo=UTC),
            date_to=datetime(2026, 4, 2, tzinfo=UTC),
        )


def test_discovered_candidate_serializes_with_inferred_domain() -> None:
    """Discovered candidates should infer domains from their URL."""
    candidate = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        candidate_url="https://news.example.com/item/123",
        title="Candidate title",
    )

    assert candidate.domain == "news.example.com"
    payload = candidate.model_dump(mode="json")
    assert payload["candidate_url"] == "https://news.example.com/item/123"
    assert payload["producer_kind"] == "search_engine"


def test_domain_from_url_none_passthrough() -> None:
    """The internal domain helper should preserve a missing URL as None."""
    assert _domain_from_url(None) is None


def test_persistent_candidate_validates_scrape_timestamps() -> None:
    """Persistent candidates should enforce coherent retry timestamps."""
    first_seen = datetime(2026, 4, 10, 8, 0, tzinfo=UTC)
    last_attempt = first_seen + timedelta(hours=1)
    next_attempt = last_attempt + timedelta(hours=12)
    candidate = PersistentCandidate(
        current_url="https://mako.co.il/news/123",
        canonical_url="https://mako.co.il/news/123",
        titles=["Example"],
        discovered_via=["source_native"],
        first_seen_at=first_seen,
        last_seen_at=first_seen,
        last_scrape_attempt_at=last_attempt,
        next_scrape_attempt_at=next_attempt,
        candidate_status=CandidateStatus.SCRAPE_FAILED,
        content_basis=ContentBasis.PARTIAL_PAGE,
        scrape_attempt_count=1,
    )

    assert candidate.domain == "mako.co.il"
    assert candidate.candidate_status == CandidateStatus.SCRAPE_FAILED

    with pytest.raises(ValueError):
        PersistentCandidate(
            current_url="https://mako.co.il/news/123",
            first_seen_at=first_seen,
            last_seen_at=first_seen - timedelta(minutes=1),
        )

    with pytest.raises(ValueError):
        PersistentCandidate(
            current_url="https://mako.co.il/news/123",
            first_seen_at=first_seen,
            last_seen_at=first_seen,
            last_scrape_attempt_at=first_seen - timedelta(minutes=1),
        )

    with pytest.raises(ValueError):
        PersistentCandidate(
            current_url="https://mako.co.il/news/123",
            first_seen_at=first_seen,
            last_seen_at=first_seen,
            last_scrape_attempt_at=last_attempt,
            next_scrape_attempt_at=last_attempt - timedelta(minutes=1),
        )


def test_candidate_provenance_uses_normalized_url_for_domain() -> None:
    """Provenance should prefer normalized URLs when inferring domains."""
    provenance = CandidateProvenance(
        run_id="run-1",
        candidate_id="candidate-1",
        producer_name="google_cse",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        raw_url="https://google.com/url?q=https://www.ynet.co.il/item",
        normalized_url="https://www.ynet.co.il/item",
    )

    assert provenance.domain == "www.ynet.co.il"


def test_scrape_attempt_validates_finished_at() -> None:
    """Scrape attempts should reject finish times before the start."""
    started_at = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)
    attempt = ScrapeAttempt(
        candidate_id="candidate-1",
        started_at=started_at,
        finished_at=started_at + timedelta(minutes=1),
        attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
        fetch_status=FetchStatus.SUCCESS,
    )

    assert attempt.fetch_status == FetchStatus.SUCCESS

    with pytest.raises(ValueError):
        ScrapeAttempt(
            candidate_id="candidate-1",
            started_at=started_at,
            finished_at=started_at - timedelta(minutes=1),
            attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
            fetch_status=FetchStatus.FAILED,
        )


def test_discovery_run_defaults_to_discover_job() -> None:
    """Discovery runs should default to the new discover job identity."""
    run = DiscoveryRun(status=DiscoveryRunStatus.RUNNING)

    assert run.job_name == JobName.DISCOVER
    assert run.status == DiscoveryRunStatus.RUNNING


def test_discovery_run_rejects_finished_at_before_started_at() -> None:
    """Discovery runs should reject inverted timestamps."""
    started_at = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)

    with pytest.raises(ValueError):
        DiscoveryRun(
            started_at=started_at,
            finished_at=started_at - timedelta(minutes=1),
        )
