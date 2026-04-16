"""Unit tests for discovery-layer diagnostics reporting."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import HttpUrl

from denbust.config import Config, OperationalProvider
from denbust.diagnostics.discovery import (
    build_discovery_diagnostic_report,
    persist_discovery_diagnostic_artifacts,
    render_discovery_diagnostic_report,
)
from denbust.discovery.models import (
    CandidateStatus,
    FetchStatus,
    PersistentCandidate,
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
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        next_scrape_attempt_at=next_scrape_attempt_at,
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
            ),
            _candidate(
                "candidate-google-partial",
                url="https://www.maariv.co.il/news/article/google-partial",
                discovered_via=["google_cse"],
                status=CandidateStatus.PARTIALLY_SCRAPED,
                first_seen_at=now - timedelta(days=1),
                last_seen_at=now - timedelta(hours=3),
                next_scrape_attempt_at=now + timedelta(hours=12),
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
    assert report.queue_health.stale_candidates == 1
    assert report.queue_health.retry_backlog_candidates == 1
    assert report.candidate_conversion.scrape_succeeded_candidates == 1
    assert report.candidate_conversion.operational_record_matches == 1
    assert report.candidate_conversion.per_producer[0].candidate_count >= 1
    assert report.top_failure_sources[0].name == "mako"
    assert report.top_failure_domains[0].name in {"www.mako.co.il", "www.maariv.co.il"}
    assert "Overlap" in render_discovery_diagnostic_report(report)


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
