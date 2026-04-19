"""Unit tests for historical backfill planning helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl

from denbust.config import Config
from denbust.discovery.backfill import (
    BACKFILL_DATE_FROM_ENV,
    BACKFILL_DATE_TO_ENV,
    build_backfill_queries,
    plan_backfill_windows,
    resolve_backfill_request_window,
)
from denbust.discovery.models import (
    BackfillBatch,
    BackfillBatchStatus,
    CandidateStatus,
    PersistentCandidate,
)
from denbust.discovery.scrape_queue import select_backfill_candidates_for_scrape
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName


def build_candidate(
    candidate_id: str,
    *,
    batch_id: str,
    window_start: datetime,
    first_seen_at: datetime,
    publication_hint: datetime | None = None,
) -> PersistentCandidate:
    metadata: dict[str, object] = {
        "backfill_window_start": window_start.isoformat(),
    }
    if publication_hint is not None:
        metadata["latest_publication_datetime_hint"] = publication_hint.isoformat()
    return PersistentCandidate(
        candidate_id=candidate_id,
        current_url=HttpUrl(f"https://example.com/{candidate_id}"),
        canonical_url=HttpUrl(f"https://example.com/{candidate_id}"),
        titles=["title"],
        snippets=["snippet"],
        discovered_via=["brave"],
        discovery_queries=["בית בושת"],
        source_hints=["ynet"],
        first_seen_at=first_seen_at,
        last_seen_at=first_seen_at,
        candidate_status=CandidateStatus.NEW,
        backfill_batch_id=batch_id,
        metadata=metadata,
    )


def test_backfill_batch_validates_date_window() -> None:
    """Backfill batches should reject inverted requested windows."""
    batch = BackfillBatch(
        batch_id="batch-1",
        dataset_name=DatasetName.NEWS_ITEMS,
        job_name=JobName.BACKFILL_DISCOVER,
        status=BackfillBatchStatus.RUNNING,
        requested_date_from=datetime(2026, 1, 1, tzinfo=UTC),
        requested_date_to=datetime(2026, 1, 31, tzinfo=UTC),
    )

    assert batch.status == BackfillBatchStatus.RUNNING

    with pytest.raises(ValueError):
        BackfillBatch(
            batch_id="batch-2",
            requested_date_from=datetime(2026, 2, 1, tzinfo=UTC),
            requested_date_to=datetime(2026, 1, 31, tzinfo=UTC),
        )


def test_plan_backfill_windows_splits_range() -> None:
    """A historical range should be partitioned into contiguous windows."""
    windows = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 15, tzinfo=UTC),
        batch_window_days=7,
    )

    assert len(windows) == 3
    assert windows[0].date_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert windows[-1].date_to == datetime(2026, 1, 15, tzinfo=UTC)


def test_build_backfill_queries_uses_explicit_window() -> None:
    """Historical backfill queries should use the requested date range."""
    config = Config(
        discovery={"default_query_kinds": ["broad"]},
        keywords=["בית בושת"],
    )
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 3, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    assert len(queries) == 1
    assert queries[0].date_from == window.date_from
    assert queries[0].date_to == window.date_to
    assert "backfill" in queries[0].tags


def test_resolve_backfill_request_window_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill runs should read the requested window from environment variables."""
    monkeypatch.setenv(BACKFILL_DATE_FROM_ENV, "2026-01-01T00:00:00+00:00")
    monkeypatch.setenv(BACKFILL_DATE_TO_ENV, "2026-01-15T00:00:00+00:00")

    date_from, date_to = resolve_backfill_request_window()

    assert date_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert date_to == datetime(2026, 1, 15, tzinfo=UTC)


def test_select_backfill_candidates_prefers_oldest_window_then_oldest_publication(
    tmp_path,
) -> None:
    """Backfill scraping should choose the oldest active window first."""
    store = StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    )
    store.upsert_candidates(
        [
            build_candidate(
                "older-window",
                batch_id="batch-a",
                window_start=datetime(2026, 1, 1, tzinfo=UTC),
                first_seen_at=datetime(2026, 4, 1, tzinfo=UTC),
            ),
            build_candidate(
                "older-publication",
                batch_id="batch-a",
                window_start=datetime(2026, 1, 1, tzinfo=UTC),
                first_seen_at=datetime(2026, 4, 3, tzinfo=UTC),
                publication_hint=datetime(2026, 1, 2, tzinfo=UTC),
            ),
            build_candidate(
                "newer-window",
                batch_id="batch-b",
                window_start=datetime(2026, 2, 1, tzinfo=UTC),
                first_seen_at=datetime(2026, 4, 2, tzinfo=UTC),
            ),
        ]
    )

    selected = select_backfill_candidates_for_scrape(store, limit=2)

    assert [candidate.candidate_id for candidate in selected] == [
        "older-publication",
        "older-window",
    ]
