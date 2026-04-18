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
    _load_operational_record_urls,
    _read_jsonl,
    build_discovery_diagnostic_report,
    persist_discovery_diagnostic_artifacts,
    render_discovery_diagnostic_report,
    run_discovery_diagnostics,
)
from denbust.discovery.models import (
    CandidateStatus,
    ContentBasis,
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
    content_basis: ContentBasis = ContentBasis.CANDIDATE_ONLY,
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
    assert report.candidate_conversion.scrape_succeeded_candidates == 1
    assert report.candidate_conversion.search_result_only_candidates == 1
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
