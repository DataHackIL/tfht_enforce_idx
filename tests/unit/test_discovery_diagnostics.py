"""Unit tests for discovery-layer diagnostics reporting."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import HttpUrl

from denbust.config import Config, OperationalProvider
from denbust.diagnostics.discovery import (
    DiscoveryDiagnosticReport,
    _build_failure_summaries,
    _build_scrape_failure_diagnostics,
    _candidate_source,
    _load_operational_record_urls,
    _normalize_domain,
    _partial_attempt_source_label,
    _read_jsonl,
    _record_content_basis,
    _record_has_valid_taxonomy_pair,
    build_discovery_diagnostic_report,
    persist_discovery_diagnostic_artifacts,
    render_discovery_diagnostic_report,
    run_discovery_diagnostics,
)
from denbust.discovery.models import (
    CandidateProvenance,
    CandidateStatus,
    ContentBasis,
    FetchStatus,
    PersistentCandidate,
    ProducerKind,
    ScrapeAttempt,
    ScrapeAttemptKind,
)
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName
from denbust.ops.storage import LocalJsonOperationalStore


def _candidate(
    candidate_id: str,
    *,
    url: str,
    discovered_via: list[str],
    status: CandidateStatus,
    first_seen_at: datetime,
    last_seen_at: datetime,
    next_scrape_attempt_at: datetime | None = None,
    content_basis: ContentBasis = ContentBasis.CANDIDATE_ONLY,
    self_heal_eligible: bool = False,
    metadata: dict[str, object] | None = None,
) -> PersistentCandidate:
    return PersistentCandidate(
        candidate_id=candidate_id,
        current_url=HttpUrl(url),
        canonical_url=HttpUrl(url),
        discovered_via=discovered_via,
        source_hints=discovered_via,
        titles=[candidate_id],
        snippets=[candidate_id],
        candidate_status=status,
        content_basis=content_basis,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        next_scrape_attempt_at=next_scrape_attempt_at,
        self_heal_eligible=self_heal_eligible,
        metadata=metadata or {},
    )


def test_build_discovery_diagnostic_report_summarizes_state(tmp_path: Path) -> None:
    """The report should summarize overlap, queue health, failures, and conversion."""
    now = datetime.now(UTC)
    config = Config(
        store={"state_root": tmp_path},
        operational={
            "provider": OperationalProvider.LOCAL_JSON,
            "root_dir": tmp_path / "operational",
        },
    )
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    persistence.upsert_candidates(
        [
            _candidate(
                "candidate-shared",
                url="https://www.ynet.co.il/news/article/shared",
                discovered_via=["ynet", "brave", "exa", "google_cse"],
                status=CandidateStatus.SCRAPE_SUCCEEDED,
                first_seen_at=now - timedelta(days=2),
                last_seen_at=now - timedelta(hours=2),
                content_basis=ContentBasis.FULL_ARTICLE_PAGE,
                self_heal_eligible=True,
            ),
            _candidate(
                "candidate-source-only",
                url="https://www.walla.co.il/news/article/source-only",
                discovered_via=["walla"],
                status=CandidateStatus.NEW,
                first_seen_at=now - timedelta(days=10),
                last_seen_at=now - timedelta(days=8),
            ),
            _candidate(
                "candidate-brave-failed",
                url="https://www.mako.co.il/news/article/brave-failed",
                discovered_via=["brave"],
                status=CandidateStatus.SCRAPE_FAILED,
                first_seen_at=now - timedelta(days=3),
                last_seen_at=now - timedelta(days=2),
                next_scrape_attempt_at=now - timedelta(hours=1),
                content_basis=ContentBasis.SEARCH_RESULT_ONLY,
                self_heal_eligible=True,
            ),
            _candidate(
                "candidate-google-partial",
                url="https://www.maariv.co.il/news/article/google-partial",
                discovered_via=["google_cse"],
                status=CandidateStatus.PARTIALLY_SCRAPED,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now - timedelta(hours=3),
                next_scrape_attempt_at=now + timedelta(hours=12),
                content_basis=ContentBasis.PARTIAL_PAGE,
            ),
        ]
    )
    persistence.append_attempts(
        [
            ScrapeAttempt(
                candidate_id="candidate-brave-failed",
                started_at=now - timedelta(hours=2),
                finished_at=now - timedelta(hours=2),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.FAILED,
                source_adapter_name="mako",
                error_code="candidate_not_found",
                error_message="mako adapter did not return the candidate URL",
            ),
            ScrapeAttempt(
                candidate_id="candidate-google-partial",
                started_at=now - timedelta(hours=1),
                finished_at=now - timedelta(hours=1),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.UNSUPPORTED,
                error_code="generic_fetch_not_implemented",
                error_message="generic fetch fallback not implemented yet",
            ),
        ]
    )
    store = LocalJsonOperationalStore(tmp_path / "operational")
    store.upsert_records(
        DatasetName.NEWS_ITEMS.value,
        [
            {
                "id": "news-item-1",
                "canonical_url": "https://www.ynet.co.il/news/article/shared",
                "publication_datetime": now.isoformat(),
            }
        ],
    )

    report = build_discovery_diagnostic_report(
        config=config,
        config_path=Path("agents/news/local.yaml"),
        stale_after_days=7,
    )

    assert report.engine_overlap.source_native == 2
    assert report.engine_overlap.brave == 2
    assert report.engine_overlap.exa == 1
    assert report.engine_overlap.google_cse == 2
    assert report.engine_overlap.source_native_brave_shared == 1
    assert report.source_search_coverage.total_candidates == 4
    assert report.source_search_coverage.source_native_only_candidates == 1
    assert report.source_search_coverage.search_engine_only_candidates == 2
    assert report.source_search_coverage.shared_candidates == 1
    assert report.queue_health.new_candidates == 1
    assert report.queue_health.scrape_failed_candidates == 1
    assert report.queue_health.partially_scraped_candidates == 1
    assert report.queue_health.search_result_only_candidates == 1
    assert report.queue_health.partial_page_candidates == 1
    assert report.queue_health.full_article_page_candidates == 1
    assert report.queue_health.stale_candidates == 1
    assert report.queue_health.retry_backlog_candidates == 1
    assert report.queue_health.self_heal_eligible_candidates == 1
    assert report.candidate_conversion.scrape_succeeded_candidates == 1
    assert report.candidate_conversion.search_result_only_candidates == 1
    assert report.candidate_conversion.operational_record_matches == 1
    assert report.candidate_conversion.per_producer[0].candidate_count >= 1
    assert report.top_failure_sources[0].name == "mako"
    assert report.top_failure_domains[0].name in {"www.mako.co.il", "www.maariv.co.il"}
    assert report.scrape_failure_diagnostics[0].error_code == "candidate_not_found"
    assert report.scrape_failure_diagnostics[0].self_heal_eligible_count == 1
    assert "Overlap" in render_discovery_diagnostic_report(report)
    assert "Structured scrape failures" in render_discovery_diagnostic_report(report)


def test_build_discovery_diagnostic_report_counts_search_noise_reasons(tmp_path: Path) -> None:
    """Diagnostics should explain why unsupported search-result noise was filtered."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    persistence.upsert_candidates(
        [
            _candidate(
                "candidate-x",
                url="https://x.com/example_profile",
                discovered_via=["brave"],
                status=CandidateStatus.UNSUPPORTED_SOURCE,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now,
                metadata={
                    "unsupported_source_filter": "search_noise",
                    "unsupported_source_reason": "social_profile",
                    "unsupported_source_domain": "x.com",
                    "latest_discovery_metadata": {
                        "search_noise_filter_reason": "social_profile",
                        "search_noise_filter_domain": "x.com",
                    },
                },
            ),
            _candidate(
                "candidate-instagram",
                url="https://www.instagram.com/example_profile/",
                discovered_via=["exa"],
                status=CandidateStatus.UNSUPPORTED_SOURCE,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now,
                metadata={
                    "unsupported_source_filter": "search_noise",
                    "unsupported_source_reason": "social_profile",
                    "unsupported_source_domain": "instagram.com",
                    "latest_discovery_metadata": {
                        "search_noise_filter_reason": "social_profile",
                        "search_noise_filter_domain": "instagram.com",
                    },
                },
            ),
            _candidate(
                "candidate-facebook",
                url="https://facebook.com/story.php?story_fbid=3&id=4",
                discovered_via=["brave"],
                status=CandidateStatus.UNSUPPORTED_SOURCE,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now,
            ),
        ]
    )

    report = build_discovery_diagnostic_report(config=config)
    rendered = render_discovery_diagnostic_report(report)

    assert report.queue_health.unsupported_source_candidates == 3
    assert report.queue_health.search_noise_filter_reason_counts == {
        "social_profile": 2,
    }
    assert "search_noise_filters social_profile=2" in rendered


def test_build_discovery_diagnostic_report_breaks_down_partial_pages(
    tmp_path: Path,
) -> None:
    """Partial-page diagnostics should distinguish retained rows from metadata-only partials."""
    now = datetime.now(UTC)
    config = Config(
        store={"state_root": tmp_path},
        operational={
            "provider": OperationalProvider.LOCAL_JSON,
            "root_dir": tmp_path / "operational",
        },
    )
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    retained = _candidate(
        "partial-retained",
        url="https://www.globes.co.il/news/article-retained",
        discovered_via=["brave"],
        status=CandidateStatus.PARTIALLY_SCRAPED,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
        content_basis=ContentBasis.PARTIAL_PAGE,
    )
    metadata_only = _candidate(
        "partial-metadata-only",
        url="https://www.themarker.com/news/article-metadata-only",
        discovered_via=["exa"],
        status=CandidateStatus.PARTIALLY_SCRAPED,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
        content_basis=ContentBasis.PARTIAL_PAGE,
    )
    blocked = _candidate(
        "blocked-search-result-only",
        url="https://www.globes.co.il/news/article-blocked",
        discovered_via=["brave"],
        status=CandidateStatus.SCRAPE_FAILED,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
        content_basis=ContentBasis.SEARCH_RESULT_ONLY,
    )
    persistence.upsert_candidates([retained, metadata_only, blocked])
    persistence.append_attempts(
        [
            ScrapeAttempt(
                candidate_id="partial-retained",
                started_at=now,
                finished_at=now,
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.FAILED,
                source_adapter_name="globes",
                error_code="candidate_not_found",
            ),
            ScrapeAttempt(
                candidate_id="partial-retained",
                started_at=now + timedelta(seconds=1),
                finished_at=now + timedelta(seconds=1),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.PARTIAL,
            ),
            ScrapeAttempt(
                candidate_id="partial-retained",
                started_at=now + timedelta(seconds=2),
                finished_at=now + timedelta(seconds=2),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.PARTIAL,
            ),
            ScrapeAttempt(
                candidate_id="partial-metadata-only",
                started_at=now + timedelta(seconds=3),
                finished_at=now + timedelta(seconds=3),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.PARTIAL,
            ),
            ScrapeAttempt(
                candidate_id="blocked-search-result-only",
                started_at=now + timedelta(seconds=4),
                finished_at=now + timedelta(seconds=4),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.BLOCKED,
                error_code="generic_fetch_http_403",
            ),
        ]
    )
    store = LocalJsonOperationalStore(tmp_path / "operational")
    store.upsert_records(
        DatasetName.NEWS_ITEMS.value,
        [
            {
                "id": "fallback-retained",
                "source_name": "globes",
                "source_domain": "globes.co.il",
                "url": "https://www.globes.co.il/news/article-retained",
                "canonical_url": "https://www.globes.co.il/news/article-retained",
                "publication_datetime": now.isoformat(),
                "retrieval_datetime": now.isoformat(),
                "title": "retained",
                "category": "not_relevant",
                "sub_category": None,
                "summary_one_sentence": "retained",
                "annotation_source": "candidate_fallback",
                "event_candidate_ids": ["partial-retained"],
                "content_basis": "partial_page",
                "record_confidence": "low",
                "classification_confidence": "low",
            },
            {
                "id": "full-row-same-url",
                "source_name": "themarker",
                "source_domain": "themarker.com",
                "url": "https://www.themarker.com/news/article-metadata-only",
                "canonical_url": "https://www.themarker.com/news/article-metadata-only",
                "publication_datetime": now.isoformat(),
                "retrieval_datetime": now.isoformat(),
                "title": "full row should not count as fallback retention",
                "category": "not_relevant",
                "sub_category": None,
                "summary_one_sentence": "full row",
                "annotation_source": None,
                "event_candidate_ids": [],
                "content_basis": "full_article_page",
                "record_confidence": "high",
            },
            {
                "id": "wrong-basis-fallback",
                "source_name": "themarker",
                "source_domain": "themarker.com",
                "publication_datetime": now.isoformat(),
                "retrieval_datetime": now.isoformat(),
                "title": "wrong-basis fallback should not count as partial retention",
                "category": "not_relevant",
                "sub_category": None,
                "summary_one_sentence": "wrong basis",
                "annotation_source": "candidate_fallback",
                "event_candidate_ids": ["partial-metadata-only"],
                "content_basis": "search_result_only",
                "record_confidence": "high",
            },
            {
                "id": "unrelated-fallback",
                "source_name": "globes",
                "source_domain": "globes.co.il",
                "url": "https://www.globes.co.il/news/unrelated",
                "canonical_url": "https://www.globes.co.il/news/unrelated",
                "publication_datetime": now.isoformat(),
                "retrieval_datetime": now.isoformat(),
                "title": "unrelated fallback should not pollute current diagnostics",
                "category": "not_relevant",
                "sub_category": None,
                "summary_one_sentence": "unrelated",
                "annotation_source": "candidate_fallback",
                "event_candidate_ids": ["unrelated-candidate"],
                "content_basis": "partial_page",
                "record_confidence": "low",
            },
            {
                "id": "search-result-fallback",
                "source_name": "globes",
                "source_domain": "globes.co.il",
                "url": "https://www.globes.co.il/news/article-blocked",
                "canonical_url": "https://www.globes.co.il/news/article-blocked",
                "publication_datetime": now.isoformat(),
                "retrieval_datetime": now.isoformat(),
                "title": "search-result fallback should count classifier warning signals",
                "category": "not_relevant",
                "sub_category": None,
                "summary_one_sentence": "search-result fallback",
                "annotation_source": "candidate_fallback",
                "event_candidate_ids": ["blocked-search-result-only"],
                "content_basis": "search_result_only",
                "record_confidence": "low",
                "taxonomy_category_id": "invalid_category",
                "taxonomy_subcategory_id": "invalid_subcategory",
            },
        ],
    )

    report = build_discovery_diagnostic_report(config=config)
    partials = report.partial_page_diagnostics

    assert partials.partial_candidate_count == 2
    assert partials.operational_matching_enabled is True
    assert partials.operational_records_available is True
    assert partials.retained_operational_record_candidate_count == 1
    assert partials.retained_operational_record_count == 1
    assert partials.metadata_only_partial_candidate_count == 1
    assert partials.search_result_only_candidate_count == 1
    assert partials.generic_fetch_partial_candidate_count == 2
    assert partials.source_adapter_partial_candidate_count == 1
    assert partials.partial_after_source_adapter_attempt_count == 1
    assert partials.partial_without_source_adapter_attempt_count == 1
    assert partials.blocked_generic_fetch_candidate_count == 1
    assert partials.generic_fetch_error_code_counts[0].model_dump(mode="json") == {
        "name": "generic_fetch_http_403",
        "count": 1,
    }
    assert partials.classifier_warning_signals.candidate_fallback_record_count == 3
    assert partials.classifier_warning_signals.partial_page_fallback_record_count == 1
    assert partials.classifier_warning_signals.search_result_only_fallback_record_count == 2
    assert partials.classifier_warning_signals.low_confidence_fallback_record_count == 2
    assert partials.classifier_warning_signals.invalid_taxonomy_pair_record_count == 1
    assert partials.classifier_warning_signals.partial_page_fallback_without_taxonomy_count == 1
    rendered = render_discovery_diagnostic_report(report)
    assert "Partial pages" in rendered
    assert "operational_matching=enabled" in rendered
    assert "metadata_only_partial_candidates=1" in rendered
    assert "partial_after_source_adapter_attempts=1" in rendered
    assert "partial_attempt_kinds: generic_fetch=2, source_adapter=1" in rendered
    assert "partial_attempt_sources: generic_fetch=2, unknown_source_adapter=1" in rendered
    assert "classifier_signals candidate_fallback_records=3" in rendered


def test_partial_page_diagnostics_mark_operational_counts_as_unmeasured_when_skipped(
    tmp_path: Path,
) -> None:
    """Skipped operational matching should not masquerade as metadata-only evidence."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    candidate = _candidate(
        "partial-skipped",
        url="https://www.globes.co.il/news/article-skipped",
        discovered_via=["brave"],
        status=CandidateStatus.PARTIALLY_SCRAPED,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
        content_basis=ContentBasis.PARTIAL_PAGE,
    )

    report = build_discovery_diagnostic_report(
        config=config,
        candidates_override=[candidate],
        attempts_override=[],
        include_operational_matches=False,
    )
    partials = report.partial_page_diagnostics

    assert partials.operational_matching_enabled is False
    assert partials.retained_operational_record_candidate_count == 0
    assert partials.retained_operational_record_count == 0
    assert partials.metadata_only_partial_candidate_count == 0
    assert "operational_matching=skipped" in render_discovery_diagnostic_report(report)


def test_discovery_diagnostic_report_treats_event_id_only_operational_rows_as_available(
    tmp_path: Path,
) -> None:
    """Operational availability should not depend on canonical_url presence."""
    now = datetime.now(UTC)
    config = Config(
        store={"state_root": tmp_path},
        operational={
            "provider": OperationalProvider.LOCAL_JSON,
            "root_dir": tmp_path / "operational",
        },
    )
    candidate = _candidate(
        "event-id-only",
        url="https://www.globes.co.il/news/event-id-only",
        discovered_via=["brave"],
        status=CandidateStatus.PARTIALLY_SCRAPED,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
        content_basis=ContentBasis.PARTIAL_PAGE,
    )
    LocalJsonOperationalStore(tmp_path / "operational").upsert_records(
        DatasetName.NEWS_ITEMS.value,
        [
            {
                "id": "event-id-only-row",
                "publication_datetime": now.isoformat(),
                "event_candidate_ids": ["event-id-only"],
                "annotation_source": "candidate_fallback",
                "content_basis": "partial_page",
            }
        ],
    )

    report = build_discovery_diagnostic_report(
        config=config,
        candidates_override=[candidate],
        attempts_override=[],
    )

    assert report.operational_records_available is True
    assert report.candidate_conversion.operational_record_matches == 1
    assert report.partial_page_diagnostics.operational_records_available is True
    assert report.partial_page_diagnostics.retained_operational_record_candidate_count == 1
    assert (
        "No operational records were available for candidate-to-news-item matching."
        not in report.notes
    )


def test_record_helpers_handle_enum_blank_and_missing_taxonomy_values() -> None:
    """Operational helper predicates should handle typed and incomplete rows."""
    now = datetime.now(UTC)

    assert _record_content_basis({"content_basis": ContentBasis.PARTIAL_PAGE}) == "partial_page"
    assert _record_content_basis({"content_basis": ""}) == ""
    assert _record_content_basis({}) == ""
    assert (
        _record_has_valid_taxonomy_pair(
            {"taxonomy_category_id": None, "taxonomy_subcategory_id": "missing"}
        )
        is False
    )
    assert (
        _partial_attempt_source_label(
            ScrapeAttempt(
                candidate_id="source-named",
                started_at=now,
                finished_at=now,
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.PARTIAL,
                source_adapter_name="mako",
            )
        )
        == "mako"
    )


def test_build_discovery_diagnostic_report_explains_queue_drain_budget_cap(
    tmp_path: Path,
) -> None:
    """Queue-drain diagnostics should expose selection order, source mix, and cap stops."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    config = Config(store={"state_root": tmp_path}, max_articles=2)
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    first = _candidate(
        "selected-ice-1",
        url="https://www.ice.co.il/article/1",
        discovered_via=["ice"],
        status=CandidateStatus.SCRAPE_SUCCEEDED,
        first_seen_at=now - timedelta(hours=3),
        last_seen_at=now - timedelta(minutes=20),
        content_basis=ContentBasis.FULL_ARTICLE_PAGE,
    )
    second = _candidate(
        "selected-ice-2",
        url="https://www.ice.co.il/article/2",
        discovered_via=["ice"],
        status=CandidateStatus.SCRAPE_SUCCEEDED,
        first_seen_at=now - timedelta(hours=3),
        last_seen_at=now - timedelta(minutes=10),
        content_basis=ContentBasis.FULL_ARTICLE_PAGE,
    )
    pending_mako = _candidate(
        "pending-mako",
        url="https://www.mako.co.il/news/article/pending",
        discovered_via=["mako"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(hours=2),
        last_seen_at=now - timedelta(minutes=5),
    )
    pending_haaretz = _candidate(
        "pending-haaretz",
        url="https://www.haaretz.co.il/news/article/pending",
        discovered_via=["haaretz"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(hours=2),
        last_seen_at=now - timedelta(minutes=15),
    )
    persistence.upsert_candidates([first, second, pending_mako, pending_haaretz])
    attempts = [
        ScrapeAttempt(
            candidate_id="selected-ice-1",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.SUCCESS,
            source_adapter_name="ice",
        ),
        ScrapeAttempt(
            candidate_id="selected-ice-2",
            started_at=now + timedelta(seconds=1),
            finished_at=now + timedelta(seconds=1),
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.SUCCESS,
            source_adapter_name="ice",
        ),
    ]
    persistence.append_attempts(attempts)

    report = build_discovery_diagnostic_report(
        config=config,
        attempts_override=attempts,
        candidates_override=persistence.list_candidates(),
        include_operational_matches=False,
    )

    assert report.queue_drain.max_candidate_budget == 2
    assert report.queue_drain.persisted_attempted_candidate_count == 2
    assert report.queue_drain.persisted_scrape_attempt_count == 2
    assert report.queue_drain.remaining_eligible_candidate_count == 2
    assert report.queue_drain.inferred_stop_reason == "budget_cap_reached"
    assert [
        item.candidate_id for item in report.queue_drain.persisted_attempted_candidate_order
    ] == ["selected-ice-1", "selected-ice-2"]
    assert [
        item.candidate_id for item in report.queue_drain.remaining_eligible_candidate_order
    ] == ["pending-mako", "pending-haaretz"]
    assert [item.model_dump(mode="json") for item in report.queue_drain.attempted_source_mix] == [
        {"source": "ice", "candidate_count": 2}
    ]
    assert [
        item.model_dump(mode="json") for item in report.queue_drain.remaining_eligible_source_mix
    ] == [
        {"source": "haaretz", "candidate_count": 1},
        {"source": "mako", "candidate_count": 1},
    ]
    rendered = render_discovery_diagnostic_report(report)
    assert "Queue drain" in rendered
    assert "inferred_stop_reason=budget_cap_reached" in rendered
    assert "attempted_source_mix: ice=2" in rendered


def test_queue_drain_attempted_source_mix_uses_attempt_adapter_names(
    tmp_path: Path,
) -> None:
    """Attempted source mix should prefer the source adapter that actually ran."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    config = Config(store={"state_root": tmp_path}, max_articles=5)
    candidate = _candidate(
        "search-produced",
        url="https://www.mako.co.il/news/article/search-produced",
        discovered_via=["brave"],
        status=CandidateStatus.SCRAPE_FAILED,
        first_seen_at=now - timedelta(hours=1),
        last_seen_at=now,
    ).model_copy(update={"source_hints": ["mako"]})
    attempts = [
        ScrapeAttempt(
            candidate_id="search-produced",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.FAILED,
            source_adapter_name="ynet",
        ),
        ScrapeAttempt(
            candidate_id="search-produced",
            started_at=now + timedelta(seconds=1),
            finished_at=now + timedelta(seconds=1),
            attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
            fetch_status=FetchStatus.FAILED,
        ),
    ]

    report = build_discovery_diagnostic_report(
        config=config,
        candidates_override=[candidate],
        attempts_override=attempts,
        include_operational_matches=False,
    )

    assert [item.model_dump(mode="json") for item in report.queue_drain.attempted_source_mix] == [
        {"source": "ynet", "candidate_count": 1}
    ]


def test_queue_drain_attempted_candidates_skip_duplicates_and_missing_candidates(
    tmp_path: Path,
) -> None:
    """Persisted attempted order should count each known candidate once."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    config = Config(store={"state_root": tmp_path}, max_articles=5)
    candidate = _candidate(
        "known",
        url="https://www.walla.co.il/news/article/known",
        discovered_via=["walla"],
        status=CandidateStatus.SCRAPE_SUCCEEDED,
        first_seen_at=now - timedelta(hours=1),
        last_seen_at=now,
    )
    attempts = [
        ScrapeAttempt(
            candidate_id="known",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.SUCCESS,
            source_adapter_name="walla",
        ),
        ScrapeAttempt(
            candidate_id="known",
            started_at=now + timedelta(seconds=1),
            finished_at=now + timedelta(seconds=1),
            attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
            fetch_status=FetchStatus.SUCCESS,
        ),
        ScrapeAttempt(
            candidate_id="missing",
            started_at=now + timedelta(seconds=2),
            finished_at=now + timedelta(seconds=2),
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.SUCCESS,
            source_adapter_name="mako",
        ),
    ]

    report = build_discovery_diagnostic_report(
        config=config,
        candidates_override=[candidate],
        attempts_override=attempts,
        include_operational_matches=False,
    )

    assert report.queue_drain.persisted_attempted_candidate_count == 1
    assert [
        item.candidate_id for item in report.queue_drain.persisted_attempted_candidate_order
    ] == ["known"]


def test_candidate_source_falls_back_to_non_search_producer_then_domain() -> None:
    """Candidate-source inference should cover non-search producers and domain fallback."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    source_native = _candidate(
        "source-native",
        url="https://example.com/source-native",
        discovered_via=["source_native"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})
    domain_only = _candidate(
        "domain-only",
        url="https://example.com/domain-only",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})

    assert _candidate_source(source_native) == "source_native"
    assert _candidate_source(domain_only) == "example.com"


def test_candidate_source_maps_supported_generic_source_family_domains() -> None:
    """Search-only generic-fetch candidates should be grouped by source family."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    globes = _candidate(
        "globes",
        url="https://www.globes.co.il/news/article.aspx?did=1001531007",
        discovered_via=["exa"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": ["exa"]})
    themarker = _candidate(
        "themarker",
        url="https://www.themarker.com/news/2026-01-04/ty-article/abc",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})
    kan = _candidate(
        "kan",
        url="https://www.kan.org.il/content/kan-news/local/296141/",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})
    news1 = _candidate(
        "news1",
        url="https://www.news1.co.il/Archive/001-D-512703-00.html",
        discovered_via=["exa"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})
    news1_non_archive = _candidate(
        "news1-non-archive",
        url="https://www.news1.co.il/Home/",
        discovered_via=["exa"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})
    kan_non_article = _candidate(
        "kan-non-article",
        url="https://www.kan.org.il/live/",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": []})

    assert _candidate_source(globes) == "globes"
    assert _candidate_source(themarker) == "themarker"
    assert _candidate_source(kan) == "kan"
    assert _candidate_source(news1) == "news1"
    assert _candidate_source(news1_non_archive) == "www.news1.co.il"
    assert _candidate_source(kan_non_article) == "www.kan.org.il"


def test_candidate_source_scans_all_source_hints_before_domain_fallback() -> None:
    """Search-engine hints should not hide later concrete source hints."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    candidate = _candidate(
        "multi-hint",
        url="https://www.mako.co.il/news/article/multi-hint",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now,
        last_seen_at=now,
    ).model_copy(update={"source_hints": ["exa", "mako"]})

    assert _candidate_source(candidate) == "mako"


def test_render_discovery_diagnostic_report_caps_queue_order_text(tmp_path: Path) -> None:
    """Text diagnostics should summarize long orders without dumping the whole queue."""
    now = datetime(2026, 5, 3, 15, 31, tzinfo=UTC)
    config = Config(store={"state_root": tmp_path}, max_articles=5)
    candidates = [
        _candidate(
            f"pending-{index:02d}",
            url=f"https://www.mako.co.il/news/article/pending-{index:02d}",
            discovered_via=["mako"],
            status=CandidateStatus.NEW,
            first_seen_at=now - timedelta(hours=1),
            last_seen_at=now - timedelta(minutes=index),
        )
        for index in range(12)
    ]

    report = build_discovery_diagnostic_report(
        config=config,
        candidates_override=candidates,
        attempts_override=[],
        include_operational_matches=False,
    )
    rendered = render_discovery_diagnostic_report(report)

    assert "remaining_eligible_order: showing first 10 of 12" in rendered
    assert "pending-00" in rendered
    assert "pending-10" not in rendered


def test_persist_discovery_diagnostic_artifacts_writes_latest_files(tmp_path: Path) -> None:
    """The diagnostics artifact writer should persist both JSON reports."""
    config = Config(store={"state_root": tmp_path})
    report = build_discovery_diagnostic_report(config=config, stale_after_days=7)

    persist_discovery_diagnostic_artifacts(config=config, report=report)

    overlap_payload = json.loads(
        config.discovery_state_paths.engine_overlap_latest_path.read_text(encoding="utf-8")
    )
    combined_payload = json.loads(
        config.discovery_state_paths.discovery_diagnostics_latest_path.read_text(encoding="utf-8")
    )

    assert overlap_payload["source_native"] == 0
    assert combined_payload["dataset_name"] == "news_items"
    suggestions_payload = json.loads(
        config.discovery_state_paths.source_suggestions_latest_path.read_text(encoding="utf-8")
    )
    assert suggestions_payload["suggestions"] == []


def test_run_discovery_diagnostics_loads_config_and_forwards_flags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The CLI-facing wrapper should load config and forward optional flags."""
    config = Config(store={"state_root": tmp_path})
    captured: dict[str, object] = {}

    def fake_build_discovery_diagnostic_report(**kwargs: object) -> DiscoveryDiagnosticReport:
        captured.update(kwargs)
        return DiscoveryDiagnosticReport(
            config_path=str(tmp_path / "agents/news/local.yaml"),
            dataset_name="news_items",
            stale_after_days=11,
            latest_candidates_path="candidates.jsonl",
            scrape_attempts_path="attempts.jsonl",
            candidate_provenance_path="candidate_provenance.jsonl",
            operational_records_available=False,
        )

    monkeypatch.setattr("denbust.diagnostics.discovery.load_config", lambda _path: config)
    monkeypatch.setattr(
        "denbust.diagnostics.discovery.build_discovery_diagnostic_report",
        fake_build_discovery_diagnostic_report,
    )

    report = run_discovery_diagnostics(
        config_path=tmp_path / "agents/news/local.yaml",
        stale_after_days=11,
        include_operational_matches=False,
    )

    assert report.stale_after_days == 11
    assert captured["config"] == config
    assert captured["config_path"] == tmp_path / "agents/news/local.yaml"
    assert captured["stale_after_days"] == 11
    assert captured["include_operational_matches"] is False


def test_build_discovery_diagnostic_report_notes_when_operational_records_missing(
    tmp_path: Path,
) -> None:
    """Operational providers with no matching records should emit a note."""
    config = Config(
        store={"state_root": tmp_path},
        operational={
            "provider": OperationalProvider.LOCAL_JSON,
            "root_dir": tmp_path / "operational",
        },
    )

    report = build_discovery_diagnostic_report(config=config)

    assert (
        "No operational records were available for candidate-to-news-item matching." in report.notes
    )


def test_build_discovery_diagnostic_report_respects_empty_overlap_override(
    tmp_path: Path,
) -> None:
    """An explicit empty overlap override should not fall back to durable candidates."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    StateRepoDiscoveryPersistence(config.discovery_state_paths).upsert_candidates(
        [
            _candidate(
                "candidate-1",
                url="https://www.ynet.co.il/news/article/1",
                discovered_via=["ynet", "brave"],
                status=CandidateStatus.NEW,
                first_seen_at=now,
                last_seen_at=now,
            )
        ]
    )

    report = build_discovery_diagnostic_report(
        config=config,
        overlap_candidates_override=[],
    )

    assert report.engine_overlap.source_native == 0
    assert report.engine_overlap.brave == 0
    assert report.source_search_coverage.total_candidates == 1


def test_build_discovery_diagnostic_report_skipped_operational_matching_omits_missing_note(
    tmp_path: Path,
) -> None:
    """Skip-mode should not also claim that no operational records were available."""
    config = Config(
        store={"state_root": tmp_path},
        operational={
            "provider": OperationalProvider.LOCAL_JSON,
            "root_dir": tmp_path / "operational",
        },
    )

    report = build_discovery_diagnostic_report(
        config=config,
        include_operational_matches=False,
    )

    assert "Operational record matching was skipped for this diagnostics artifact." in report.notes
    assert (
        "No operational records were available for candidate-to-news-item matching."
        not in report.notes
    )


def test_render_discovery_diagnostic_report_includes_notes(tmp_path: Path) -> None:
    """Rendered reports should include notes when present."""
    config = Config(store={"state_root": tmp_path})
    report = build_discovery_diagnostic_report(config=config)
    report.notes.append("Operational record matching was skipped for this diagnostics artifact.")

    rendered = render_discovery_diagnostic_report(report)

    assert "Notes" in rendered
    assert "Operational record matching was skipped" in rendered


def test_build_discovery_diagnostic_report_builds_source_suggestions(tmp_path: Path) -> None:
    """Repeated unseen non-social domains should surface in source suggestions."""
    now = datetime.now(UTC)
    config = Config(
        store={"state_root": tmp_path},
        sources=[
            {
                "name": "ynet",
                "type": "rss",
                "url": "https://www.ynet.co.il/feed.xml",
                "enabled": True,
            }
        ],
    )
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    suggested_a = _candidate(
        "candidate-example-1",
        url="https://example.com/news/1",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(days=4),
        last_seen_at=now - timedelta(days=1),
    )
    suggested_b = _candidate(
        "candidate-example-2",
        url="https://example.com/news/2",
        discovered_via=["exa"],
        status=CandidateStatus.SCRAPE_SUCCEEDED,
        first_seen_at=now - timedelta(days=3),
        last_seen_at=now - timedelta(hours=6),
    ).model_copy(update={"content_basis": ContentBasis.SEARCH_RESULT_ONLY})
    social = _candidate(
        "candidate-facebook",
        url="https://facebook.com/story.php?story_fbid=3&id=4",
        discovered_via=["brave"],
        status=CandidateStatus.UNSUPPORTED_SOURCE,
        first_seen_at=now - timedelta(days=2),
        last_seen_at=now - timedelta(hours=5),
    )
    known_source = _candidate(
        "candidate-known-source",
        url="https://ynet.co.il/news/article/3",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(days=2),
        last_seen_at=now - timedelta(hours=3),
    )
    no_domain = _candidate(
        "candidate-no-domain",
        url="https://example.org/news/4",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now - timedelta(hours=2),
    ).model_copy(update={"domain": None})
    persistence.upsert_candidates([suggested_a, suggested_b, social, known_source, no_domain])
    persistence.append_provenance(
        [
            CandidateProvenance(
                run_id="run-1",
                candidate_id="candidate-example-1",
                producer_name="brave",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://example.com/news/1"),
                normalized_url=HttpUrl("https://example.com/news/1"),
                discovered_at=now - timedelta(days=4),
                metadata={"query_kind": "broad"},
            ),
            CandidateProvenance(
                run_id="run-2",
                candidate_id="candidate-example-2",
                producer_name="exa",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://example.com/news/2"),
                normalized_url=HttpUrl("https://example.com/news/2"),
                discovered_at=now - timedelta(days=3),
                metadata={"query_kind": "broad"},
            ),
            CandidateProvenance(
                run_id="run-3",
                candidate_id="candidate-facebook",
                producer_name="brave",
                producer_kind=ProducerKind.SOCIAL_SEARCH,
                raw_url=HttpUrl("https://facebook.com/story.php?story_fbid=3&id=4"),
                normalized_url=HttpUrl("https://facebook.com/story.php?story_fbid=3&id=4"),
                discovered_at=now - timedelta(days=2),
                metadata={"query_kind": "social_targeted"},
            ),
            CandidateProvenance(
                run_id="run-4",
                candidate_id="candidate-known-source",
                producer_name="brave",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://ynet.co.il/news/article/3"),
                normalized_url=HttpUrl("https://ynet.co.il/news/article/3"),
                discovered_at=now - timedelta(days=2),
                metadata={"query_kind": "broad"},
            ),
            CandidateProvenance(
                run_id="run-5",
                candidate_id="candidate-no-domain",
                producer_name="brave",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://example.org/news/4"),
                normalized_url=HttpUrl("https://example.org/news/4"),
                domain=None,
                discovered_at=now - timedelta(days=1),
                metadata={"query_kind": "broad"},
            ),
        ]
    )
    persistence.append_attempts(
        [
            ScrapeAttempt(
                candidate_id="candidate-example-2",
                started_at=now - timedelta(hours=8),
                finished_at=now - timedelta(hours=8),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.SUCCESS,
                source_adapter_name="example_adapter",
            )
        ]
    )

    report = build_discovery_diagnostic_report(config=config)

    assert report.source_suggestions.suggestions[0].domain == "example.com"
    assert report.source_suggestions.suggestions[0].run_count == 2
    suggestion_domains = {suggestion.domain for suggestion in report.source_suggestions.suggestions}
    assert "facebook.com" not in suggestion_domains
    assert "ynet.co.il" not in suggestion_domains
    assert "www.facebook.com" not in suggestion_domains
    assert "www.ynet.co.il" not in suggestion_domains
    assert "Source suggestions" in render_discovery_diagnostic_report(report)


def test_build_discovery_diagnostic_report_ignores_candidates_without_domain(
    tmp_path: Path,
) -> None:
    """Domain-less candidates should be ignored by source-suggestion grouping."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    candidate = _candidate(
        "candidate-no-domain",
        url="https://example.org/news/4",
        discovered_via=["brave"],
        status=CandidateStatus.NEW,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now - timedelta(hours=1),
    ).model_copy(
        update={"domain": None},
        deep=True,
    )

    report = build_discovery_diagnostic_report(config=config, candidates_override=[candidate])

    assert report.source_suggestions.suggestions == []


def test_build_discovery_diagnostic_report_normalizes_source_domains(tmp_path: Path) -> None:
    """Enabled source domains and excluded domains should be compared in normalized form."""
    now = datetime.now(UTC)
    config = Config(
        store={"state_root": tmp_path},
        sources=[
            {
                "name": "ynet",
                "type": "rss",
                "url": "https://www.ynet.co.il/feed.xml",
                "enabled": True,
            }
        ],
    )
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    persistence.upsert_candidates(
        [
            _candidate(
                "candidate-ynet",
                url="https://ynet.co.il/news/article/3",
                discovered_via=["brave"],
                status=CandidateStatus.NEW,
                first_seen_at=now - timedelta(days=2),
                last_seen_at=now - timedelta(hours=2),
            ),
            _candidate(
                "candidate-facebook",
                url="https://facebook.com/story.php?story_fbid=3&id=4",
                discovered_via=["brave"],
                status=CandidateStatus.UNSUPPORTED_SOURCE,
                first_seen_at=now - timedelta(days=2),
                last_seen_at=now - timedelta(hours=1),
            ),
        ]
    )
    persistence.append_provenance(
        [
            CandidateProvenance(
                run_id="run-1",
                candidate_id="candidate-ynet",
                producer_name="brave",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://ynet.co.il/news/article/3"),
                normalized_url=HttpUrl("https://ynet.co.il/news/article/3"),
                discovered_at=now - timedelta(days=2),
            ),
            CandidateProvenance(
                run_id="run-2",
                candidate_id="candidate-facebook",
                producer_name="brave",
                producer_kind=ProducerKind.SOCIAL_SEARCH,
                raw_url=HttpUrl("https://facebook.com/story.php?story_fbid=3&id=4"),
                normalized_url=HttpUrl("https://facebook.com/story.php?story_fbid=3&id=4"),
                discovered_at=now - timedelta(days=2),
            ),
        ]
    )

    report = build_discovery_diagnostic_report(config=config)

    assert report.source_suggestions.suggestions == []


def test_build_discovery_diagnostic_report_keeps_generic_source_families_suggested(
    tmp_path: Path,
) -> None:
    """Generic-fetch labels alone should not hide unsupported source-family backlog."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    persistence.upsert_candidates(
        [
            _candidate(
                "candidate-globes",
                url="https://www.globes.co.il/news/article.aspx?did=1001531007",
                discovered_via=["exa"],
                status=CandidateStatus.NEW,
                first_seen_at=now - timedelta(days=2),
                last_seen_at=now - timedelta(hours=2),
            ),
            _candidate(
                "candidate-themarker",
                url="https://www.themarker.com/news/2026-01-04/ty-article/abc",
                discovered_via=["brave"],
                status=CandidateStatus.PARTIALLY_SCRAPED,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now - timedelta(hours=1),
            ),
            _candidate(
                "candidate-example",
                url="https://example.com/news/article",
                discovered_via=["brave"],
                status=CandidateStatus.NEW,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now - timedelta(minutes=30),
            ),
        ]
    )

    report = build_discovery_diagnostic_report(config=config)

    suggestion_domains = {suggestion.domain for suggestion in report.source_suggestions.suggestions}
    assert "globes.co.il" in suggestion_domains
    assert "themarker.com" in suggestion_domains
    assert "example.com" in suggestion_domains


def test_build_discovery_diagnostic_report_ignores_blank_provenance_domains(tmp_path: Path) -> None:
    """Blank provenance domains should not contribute to source-suggestion run counts."""
    now = datetime.now(UTC)
    config = Config(store={"state_root": tmp_path})
    persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    persistence.upsert_candidates(
        [
            _candidate(
                "candidate-example-1",
                url="https://example.com/news/1",
                discovered_via=["brave"],
                status=CandidateStatus.NEW,
                first_seen_at=now - timedelta(days=2),
                last_seen_at=now - timedelta(hours=2),
            )
        ]
    )
    persistence.append_provenance(
        [
            CandidateProvenance(
                run_id="run-blank-domain",
                candidate_id="candidate-example-1",
                producer_name="brave",
                producer_kind=ProducerKind.SEARCH_ENGINE,
                raw_url=HttpUrl("https://example.com/news/1"),
                normalized_url=HttpUrl("https://example.com/news/1"),
                domain="   ",
                discovered_at=now - timedelta(days=2),
            )
        ]
    )

    report = build_discovery_diagnostic_report(config=config)

    assert report.source_suggestions.suggestions[0].domain == "example.com"
    assert report.source_suggestions.suggestions[0].run_count == 0


def test_normalize_domain_returns_none_for_blank_values() -> None:
    """Blank domain strings should normalize away."""
    assert _normalize_domain(None) is None
    assert _normalize_domain("") is None
    assert _normalize_domain("   ") is None


def test_read_jsonl_ignores_blank_lines(tmp_path: Path) -> None:
    """Blank lines in JSONL files should be skipped."""
    path = tmp_path / "latest_candidates.jsonl"
    candidate = _candidate(
        "candidate-1",
        url="https://www.ynet.co.il/news/article/1",
        discovered_via=["ynet"],
        status=CandidateStatus.NEW,
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    path.write_text(f"{candidate.model_dump_json()}\n\n", encoding="utf-8")

    rows = _read_jsonl(path, PersistentCandidate)

    assert [row.candidate_id for row in rows] == ["candidate-1"]


def test_load_operational_record_urls_handles_store_creation_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Store-construction failures should become diagnostic notes."""
    config = Config(store={"state_root": tmp_path})
    monkeypatch.setattr(
        "denbust.diagnostics.discovery.create_operational_store",
        lambda _config: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    urls, notes = _load_operational_record_urls(config)

    assert urls == set()
    assert notes == ["Operational store unavailable: RuntimeError: boom"]


def test_load_operational_record_urls_handles_fetch_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fetch failures should become notes and still close the store."""

    class FakeStore:
        def __init__(self) -> None:
            self.closed = False

        def fetch_records(
            self, dataset_name: str, *, limit: int | None = None
        ) -> list[dict[str, object]]:
            del dataset_name, limit
            raise RuntimeError("fetch failed")

        def close(self) -> None:
            self.closed = True

    store = FakeStore()
    config = Config(store={"state_root": tmp_path})
    monkeypatch.setattr(
        "denbust.diagnostics.discovery.create_operational_store",
        lambda _config: store,
    )

    urls, notes = _load_operational_record_urls(config)

    assert urls == set()
    assert notes == ["Operational record fetch failed: RuntimeError: fetch failed"]
    assert store.closed is True


def test_build_failure_summaries_ignores_success_attempts_and_rejects_unknown_keys() -> None:
    """Only failed attempts should contribute to summaries, and keys must be supported."""
    now = datetime.now(UTC)
    candidates = [
        _candidate(
            "candidate-1",
            url="https://www.mako.co.il/news/article/1",
            discovered_via=["brave"],
            status=CandidateStatus.SCRAPE_FAILED,
            first_seen_at=now,
            last_seen_at=now,
        )
    ]
    attempts = [
        ScrapeAttempt(
            candidate_id="candidate-1",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.SUCCESS,
            source_adapter_name="mako",
        ),
        ScrapeAttempt(
            candidate_id="candidate-1",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.FAILED,
            source_adapter_name="mako",
            error_code="candidate_not_found",
        ),
    ]

    summaries = _build_failure_summaries(candidates, attempts, key="source")

    assert summaries == [summaries[0]]
    assert summaries[0].name == "mako"
    assert summaries[0].count == 1
    with pytest.raises(ValueError, match="Unsupported failure summary key: nope"):
        _build_failure_summaries(candidates, attempts, key="nope")


def test_build_scrape_failure_diagnostics_groups_failures_for_self_heal_triage() -> None:
    """Failure diagnostics should retain source, status, kind, domain, and self-heal counts."""
    now = datetime.now(UTC)
    candidates = [
        _candidate(
            "candidate-1",
            url="https://www.mako.co.il/news/article/1",
            discovered_via=["brave"],
            status=CandidateStatus.SCRAPE_FAILED,
            first_seen_at=now,
            last_seen_at=now,
            self_heal_eligible=True,
        ),
        _candidate(
            "candidate-2",
            url="https://www.mako.co.il/news/article/2",
            discovered_via=["google_cse"],
            status=CandidateStatus.SCRAPE_FAILED,
            first_seen_at=now,
            last_seen_at=now,
        ),
        _candidate(
            "candidate-3",
            url="https://www.mako.co.il/news/article/3",
            discovered_via=["exa"],
            status=CandidateStatus.SCRAPE_SUCCEEDED,
            first_seen_at=now,
            last_seen_at=now,
            self_heal_eligible=True,
        ),
    ]
    attempts = [
        ScrapeAttempt(
            candidate_id="candidate-1",
            started_at=now - timedelta(minutes=2),
            finished_at=now - timedelta(minutes=2),
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.FAILED,
            source_adapter_name="mako",
            error_code="candidate_not_found",
        ),
        ScrapeAttempt(
            candidate_id="candidate-2",
            started_at=now - timedelta(minutes=1),
            finished_at=now - timedelta(minutes=1),
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.FAILED,
            source_adapter_name="mako",
            error_code="candidate_not_found",
        ),
        ScrapeAttempt(
            candidate_id="candidate-1",
            started_at=now,
            finished_at=now,
            attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
            fetch_status=FetchStatus.SUCCESS,
        ),
        ScrapeAttempt(
            candidate_id="candidate-3",
            started_at=now - timedelta(seconds=30),
            finished_at=now - timedelta(seconds=30),
            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            fetch_status=FetchStatus.FAILED,
            source_adapter_name="mako",
            error_code="candidate_not_found",
        ),
    ]

    diagnostics = _build_scrape_failure_diagnostics(candidates, attempts)

    assert len(diagnostics) == 1
    diagnostic = diagnostics[0]
    assert diagnostic.attempt_kind == "source_adapter"
    assert diagnostic.fetch_status == "failed"
    assert diagnostic.error_code == "candidate_not_found"
    assert diagnostic.source_adapter_name == "mako"
    assert diagnostic.domain == "www.mako.co.il"
    assert diagnostic.count == 3
    assert diagnostic.self_heal_eligible_count == 1
    assert diagnostic.latest_attempt_at == now - timedelta(seconds=30)
