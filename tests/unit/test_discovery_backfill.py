"""Unit tests for historical backfill planning helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl

from denbust.config import Config
from denbust.discovery.backfill import (
    BACKFILL_DATE_FROM_ENV,
    BACKFILL_DATE_TO_ENV,
    backfill_metadata,
    build_backfill_queries,
    parse_backfill_datetime,
    plan_backfill_windows,
    resolve_backfill_request_window,
)
from denbust.discovery.models import (
    BackfillBatch,
    BackfillBatchStatus,
    CandidateStatus,
    DiscoveryQueryKind,
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


def test_parse_backfill_datetime_rejects_empty_and_normalizes_values() -> None:
    """Backfill datetime parsing should reject blanks and normalize Z/naive timestamps to UTC."""
    with pytest.raises(ValueError, match="TEST_ENV must not be empty"):
        parse_backfill_datetime("   ", env_name="TEST_ENV")

    assert parse_backfill_datetime("2026-01-01T00:00:00Z", env_name="TEST_ENV") == datetime(
        2026, 1, 1, tzinfo=UTC
    )
    assert parse_backfill_datetime("2026-01-01T12:00:00", env_name="TEST_ENV") == datetime(
        2026, 1, 1, 12, 0, tzinfo=UTC
    )


def test_resolve_backfill_request_window_rejects_missing_or_inverted_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill request window resolution should reject missing and inverted ranges."""
    monkeypatch.delenv(BACKFILL_DATE_FROM_ENV, raising=False)
    monkeypatch.delenv(BACKFILL_DATE_TO_ENV, raising=False)
    with pytest.raises(ValueError, match="Missing required backfill window environment variable"):
        resolve_backfill_request_window()

    monkeypatch.setenv(BACKFILL_DATE_FROM_ENV, "2026-01-02T00:00:00+00:00")
    monkeypatch.setenv(BACKFILL_DATE_TO_ENV, "2026-01-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="must be earlier than or equal to"):
        resolve_backfill_request_window()


def test_build_backfill_queries_normalizes_keywords_and_emits_source_targeted() -> None:
    """Backfill queries should ignore blank/duplicate keywords and emit all enabled query kinds."""
    config = Config(
        keywords=["", "בית בושת", "בית בושת", "  סחר  "],
        sources=[
            {"name": "ynet", "type": "rss", "enabled": True, "url": "https://example.com/feed.xml"},
            {"name": "ynet", "type": "rss", "enabled": True, "url": "https://example.com/feed.xml"},
            {
                "name": "walla",
                "type": "rss",
                "enabled": True,
                "url": "https://news.walla.co.il/feed",
            },
        ],
        discovery={
            "default_query_kinds": [
                "broad",
                "source_targeted",
                "taxonomy_targeted",
                "social_targeted",
            ]
        },
    )
    window = BackfillBatch(
        requested_date_from=datetime(2026, 1, 1, tzinfo=UTC),
        requested_date_to=datetime(2026, 1, 2, tzinfo=UTC),
    )
    queries = build_backfill_queries(
        config,
        window=plan_backfill_windows(
            date_from=window.requested_date_from,
            date_to=window.requested_date_to,
            batch_window_days=7,
        )[0],
    )

    broad_queries = [query for query in queries if query.query_kind is DiscoveryQueryKind.BROAD]
    source_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]
    taxonomy_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.TAXONOMY_TARGETED
    ]
    social_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOCIAL_TARGETED
    ]

    assert [query.query_text for query in broad_queries] == ["בית בושת", "סחר"]
    assert len(source_queries) == 4
    assert taxonomy_queries
    assert len(social_queries) == 2
    assert {query.source_hint for query in source_queries} == {"ynet", "walla"}
    assert all(query.preferred_domains for query in source_queries)
    assert all(not query.preferred_domains for query in taxonomy_queries)
    assert any(
        query.query_text == "צו הגבלת שימוש"
        and {
            "backfill",
            "taxonomy",
            "category:brothels",
            "subcategory:administrative_closure",
            "window:0",
        }.issubset(set(query.tags))
        for query in taxonomy_queries
    )
    assert {tuple(query.preferred_domains) for query in social_queries} == {
        ("www.facebook.com",),
    }


def test_build_backfill_queries_returns_empty_when_keywords_normalize_away() -> None:
    """Backfill query generation should short-circuit when no usable keyword-driven queries remain."""
    config = Config(keywords=["", "   "], discovery={"default_query_kinds": ["source_targeted"]})
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    assert build_backfill_queries(config, window=window) == []


def test_build_backfill_queries_allows_taxonomy_only_generation() -> None:
    """Backfill should still emit taxonomy-targeted queries when operator keywords are blank."""
    config = Config(keywords=["", "   "], discovery={"default_query_kinds": ["taxonomy_targeted"]})
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    assert queries
    assert all(query.query_kind is DiscoveryQueryKind.TAXONOMY_TARGETED for query in queries)


def test_build_backfill_queries_does_not_load_taxonomy_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill should avoid taxonomy helper work when taxonomy-targeted discovery is disabled."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: (_ for _ in ()).throw(AssertionError("should not load taxonomy specs")),
    )

    config = Config(
        keywords=["זנות"],
        discovery={"default_query_kinds": ["broad", "source_targeted", "social_targeted"]},
    )
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    assert queries
    assert all(query.query_kind is not DiscoveryQueryKind.TAXONOMY_TARGETED for query in queries)


def test_build_backfill_queries_deduplicates_social_targeted_entries() -> None:
    """Duplicate normalized keywords should not emit duplicate social-targeted backfill queries."""
    config = Config(
        keywords=["זנות", "  זנות  "],
        discovery={"default_query_kinds": ["social_targeted"]},
    )
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    social_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOCIAL_TARGETED
    ]
    assert len(social_queries) == 1
    assert social_queries[0].preferred_domains == ["www.facebook.com"]


def test_build_backfill_queries_deduplicates_taxonomy_terms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate taxonomy terms should collapse into one backfill query with merged tags."""

    class _FakeTaxonomy:
        def discovery_terms(self) -> list[tuple[str, str, str]]:
            return [
                ("cat_a", "leaf_a", "מונח משותף"),
                ("cat_b", "leaf_b", "מונח משותף"),
            ]

    monkeypatch.setattr("denbust.discovery.queries.default_taxonomy", lambda: _FakeTaxonomy())

    config = Config(
        keywords=["זנות"],
        discovery={"default_query_kinds": ["taxonomy_targeted"]},
    )
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    assert len(queries) == 1
    assert queries[0].query_text == "מונח משותף"
    assert "category:cat_a" in queries[0].tags
    assert "category:cat_b" in queries[0].tags
    assert "window:0" in queries[0].tags


def test_build_backfill_queries_skips_duplicate_taxonomy_specs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated taxonomy specs should hit the backfill seen-key guard only once."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: [
            ("מונח משותף", ["taxonomy", "category:cat_a"]),
            ("מונח משותף", ["taxonomy", "category:cat_b"]),
        ],
    )

    config = Config(
        keywords=[],
        discovery={"default_query_kinds": ["taxonomy_targeted"]},
    )
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 1, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    queries = build_backfill_queries(config, window=window)

    assert len(queries) == 1
    assert queries[0].query_text == "מונח משותף"


def test_backfill_metadata_serializes_batch_and_window() -> None:
    """Backfill metadata should preserve stable batch/window identifiers."""
    window = plan_backfill_windows(
        date_from=datetime(2026, 1, 1, tzinfo=UTC),
        date_to=datetime(2026, 1, 3, tzinfo=UTC),
        batch_window_days=7,
    )[0]

    metadata = backfill_metadata(batch_id="batch-1", window=window)

    assert metadata == {
        "backfill_batch_id": "batch-1",
        "backfill_window_index": 0,
        "backfill_window_start": "2026-01-01T00:00:00+00:00",
        "backfill_window_end": "2026-01-03T00:00:00+00:00",
    }


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
