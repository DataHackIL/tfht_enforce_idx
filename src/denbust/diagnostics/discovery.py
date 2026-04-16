"""Discovery-layer diagnostics and observability reporting."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from denbust.config import Config, OperationalProvider, load_config
from denbust.discovery.models import CandidateStatus, PersistentCandidate, ScrapeAttempt
from denbust.discovery.state_paths import write_metrics_snapshot
from denbust.news_items.normalize import canonicalize_news_url
from denbust.ops.factory import create_operational_store

_SEARCH_ENGINE_NAMES: tuple[str, ...] = ("brave", "exa", "google_cse")
_RETRY_BACKLOG_STATUSES: tuple[CandidateStatus, ...] = (
    CandidateStatus.QUEUED,
    CandidateStatus.SCRAPE_PENDING,
    CandidateStatus.SCRAPE_FAILED,
    CandidateStatus.PARTIALLY_SCRAPED,
    CandidateStatus.UNSUPPORTED_SOURCE,
)
_STALE_CANDIDATE_STATUSES: tuple[CandidateStatus, ...] = (
    CandidateStatus.NEW,
    CandidateStatus.QUEUED,
    CandidateStatus.SCRAPE_PENDING,
    CandidateStatus.SCRAPE_IN_PROGRESS,
    CandidateStatus.SCRAPE_FAILED,
    CandidateStatus.PARTIALLY_SCRAPED,
    CandidateStatus.UNSUPPORTED_SOURCE,
)


class EngineOverlapMetrics(BaseModel):
    """Overlap counts across source-native and search-engine discovery producers."""

    source_native: int = 0
    brave: int = 0
    exa: int = 0
    google_cse: int = 0
    source_native_brave_shared: int = 0
    source_native_exa_shared: int = 0
    source_native_google_cse_shared: int = 0
    brave_exa_shared: int = 0
    brave_google_cse_shared: int = 0
    exa_google_cse_shared: int = 0
    shared_all_candidates: int = 0

    def to_legacy_payload(self) -> dict[str, int]:
        """Return the existing flat overlap artifact payload."""
        return self.model_dump(mode="json")


class SourceSearchCoverageMetrics(BaseModel):
    """Coverage split between source-native and search-engine discovery."""

    total_candidates: int = 0
    source_native_candidates: int = 0
    search_engine_candidates: int = 0
    source_native_only_candidates: int = 0
    search_engine_only_candidates: int = 0
    shared_candidates: int = 0
    source_native_share: float = 0.0
    search_engine_share: float = 0.0
    shared_share: float = 0.0


class QueueHealthMetrics(BaseModel):
    """Current queue state for durable discovery candidates."""

    total_candidates: int = 0
    new_candidates: int = 0
    queued_candidates: int = 0
    scrape_pending_candidates: int = 0
    scrape_in_progress_candidates: int = 0
    scrape_failed_candidates: int = 0
    partially_scraped_candidates: int = 0
    unsupported_source_candidates: int = 0
    stale_candidates: int = 0
    retry_backlog_candidates: int = 0


class ProducerConversionMetrics(BaseModel):
    """Conversion summary for one discovery producer."""

    producer_name: str
    candidate_count: int = 0
    scrape_succeeded_candidates: int = 0
    partially_scraped_candidates: int = 0
    scrape_failed_candidates: int = 0
    unsupported_source_candidates: int = 0
    operational_record_matches: int = 0
    scrape_success_rate: float = 0.0
    operational_match_rate: float = 0.0


class CandidateConversionMetrics(BaseModel):
    """Downstream conversion summary for the candidate layer."""

    total_candidates: int = 0
    scrape_succeeded_candidates: int = 0
    partially_scraped_candidates: int = 0
    scrape_failed_candidates: int = 0
    unsupported_source_candidates: int = 0
    never_scraped_candidates: int = 0
    operational_record_matches: int = 0
    scrape_success_rate: float = 0.0
    operational_match_rate: float = 0.0
    per_producer: list[ProducerConversionMetrics] = Field(default_factory=list)


class FailureSummary(BaseModel):
    """Top scrape-failure grouping for diagnostics output."""

    name: str
    count: int


class DiscoveryDiagnosticReport(BaseModel):
    """Full discovery observability report."""

    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    config_path: str
    dataset_name: str
    stale_after_days: int
    latest_candidates_path: str
    scrape_attempts_path: str
    operational_records_available: bool
    notes: list[str] = Field(default_factory=list)
    engine_overlap: EngineOverlapMetrics = Field(default_factory=EngineOverlapMetrics)
    source_search_coverage: SourceSearchCoverageMetrics = Field(
        default_factory=SourceSearchCoverageMetrics
    )
    queue_health: QueueHealthMetrics = Field(default_factory=QueueHealthMetrics)
    candidate_conversion: CandidateConversionMetrics = Field(
        default_factory=CandidateConversionMetrics
    )
    top_failure_sources: list[FailureSummary] = Field(default_factory=list)
    top_failure_domains: list[FailureSummary] = Field(default_factory=list)
    top_failure_codes: list[FailureSummary] = Field(default_factory=list)


def run_discovery_diagnostics(
    *,
    config_path: Path,
    stale_after_days: int = 7,
) -> DiscoveryDiagnosticReport:
    """Build a discovery-layer diagnostics report from persisted state."""
    config = load_config(config_path)
    return build_discovery_diagnostic_report(
        config=config,
        config_path=config_path,
        stale_after_days=stale_after_days,
    )


def build_discovery_diagnostic_report(
    *,
    config: Config,
    config_path: Path | None = None,
    stale_after_days: int = 7,
    candidates_override: list[PersistentCandidate] | None = None,
    attempts_override: list[ScrapeAttempt] | None = None,
) -> DiscoveryDiagnosticReport:
    """Build a discovery observability report for the current persisted state."""
    paths = config.discovery_state_paths
    candidates = (
        candidates_override
        if candidates_override is not None
        else _read_jsonl(paths.latest_candidates_path, PersistentCandidate)
    )
    attempts = (
        attempts_override
        if attempts_override is not None
        else _read_jsonl(paths.scrape_attempts_path, ScrapeAttempt)
    )
    now = datetime.now(UTC)
    operational_urls, operational_notes = _load_operational_record_urls(config)
    report = DiscoveryDiagnosticReport(
        config_path=str(config_path) if config_path is not None else "<in-memory-config>",
        dataset_name=config.dataset_name.value,
        stale_after_days=stale_after_days,
        latest_candidates_path=str(paths.latest_candidates_path),
        scrape_attempts_path=str(paths.scrape_attempts_path),
        operational_records_available=bool(operational_urls),
        notes=operational_notes,
        engine_overlap=_build_engine_overlap_metrics(candidates),
        source_search_coverage=_build_source_search_coverage(candidates),
        queue_health=_build_queue_health_metrics(
            candidates, now=now, stale_after_days=stale_after_days
        ),
        candidate_conversion=_build_candidate_conversion_metrics(
            candidates,
            operational_urls=operational_urls,
        ),
        top_failure_sources=_build_failure_summaries(candidates, attempts, key="source"),
        top_failure_domains=_build_failure_summaries(candidates, attempts, key="domain"),
        top_failure_codes=_build_failure_summaries(candidates, attempts, key="error_code"),
    )
    if not candidates:
        report.notes.append("No persisted discovery candidates were found.")
    if not attempts:
        report.notes.append("No persisted scrape attempts were found.")
    if not operational_urls and config.operational.provider is not OperationalProvider.NONE:
        report.notes.append(
            "No operational records were available for candidate-to-news-item matching."
        )
    return report


def persist_discovery_diagnostic_artifacts(
    *,
    config: Config,
    report: DiscoveryDiagnosticReport,
) -> None:
    """Write the combined discovery diagnostics artifact set."""
    paths = config.discovery_state_paths
    write_metrics_snapshot(
        paths.engine_overlap_latest_path,
        report.engine_overlap.to_legacy_payload(),
    )
    write_metrics_snapshot(
        paths.discovery_diagnostics_latest_path,
        report.model_dump(mode="json"),
    )


def render_discovery_diagnostic_report(report: DiscoveryDiagnosticReport) -> str:
    """Render a compact human-readable discovery diagnostics report."""
    overlap = report.engine_overlap
    coverage = report.source_search_coverage
    queue = report.queue_health
    conversion = report.candidate_conversion

    lines = ["Discovery diagnostics"]
    lines.append(f"dataset: {report.dataset_name}")
    lines.append(f"generated_at: {report.generated_at.isoformat()}")
    lines.append(f"candidates: {coverage.total_candidates}")
    lines.append("")
    lines.append("Overlap")
    lines.append(
        "  source_native={source_native} brave={brave} exa={exa} google_cse={google_cse}".format(
            **overlap.model_dump(mode="json")
        )
    )
    lines.append(
        "  shared source/brave={source_native_brave_shared} "
        "source/exa={source_native_exa_shared} "
        "source/google={source_native_google_cse_shared}".format(**overlap.model_dump(mode="json"))
    )
    lines.append(
        "  shared brave/exa={brave_exa_shared} brave/google={brave_google_cse_shared} "
        "exa/google={exa_google_cse_shared} all={shared_all_candidates}".format(
            **overlap.model_dump(mode="json")
        )
    )
    lines.append("")
    lines.append("Coverage")
    lines.append(
        "  source_native={source_native_candidates} search_engine={search_engine_candidates} "
        "shared={shared_candidates}".format(**coverage.model_dump(mode="json"))
    )
    lines.append(
        "  source_only={source_native_only_candidates} search_only={search_engine_only_candidates}"
    )
    lines.append("")
    lines.append("Queue health")
    lines.append(
        "  new={new_candidates} queued={queued_candidates} pending={scrape_pending_candidates} "
        "in_progress={scrape_in_progress_candidates}".format(**queue.model_dump(mode="json"))
    )
    lines.append(
        "  failed={scrape_failed_candidates} partial={partially_scraped_candidates} "
        "unsupported={unsupported_source_candidates} stale={stale_candidates} "
        "retry_backlog={retry_backlog_candidates}".format(**queue.model_dump(mode="json"))
    )
    lines.append("")
    lines.append("Conversion")
    lines.append(
        "  scrape_succeeded={scrape_succeeded_candidates} partial={partially_scraped_candidates} "
        "failed={scrape_failed_candidates} unsupported={unsupported_source_candidates} "
        "never_scraped={never_scraped_candidates}".format(**conversion.model_dump(mode="json"))
    )
    lines.append(
        f"  scrape_success_rate={conversion.scrape_success_rate:.2%} "
        f"operational_match_rate={conversion.operational_match_rate:.2%}"
    )
    if report.top_failure_sources:
        lines.append("")
        lines.append(
            "Top failure sources: "
            + ", ".join(f"{item.name}={item.count}" for item in report.top_failure_sources)
        )
    if report.top_failure_domains:
        lines.append(
            "Top failure domains: "
            + ", ".join(f"{item.name}={item.count}" for item in report.top_failure_domains)
        )
    if report.top_failure_codes:
        lines.append(
            "Top failure codes: "
            + ", ".join(f"{item.name}={item.count}" for item in report.top_failure_codes)
        )
    if report.notes:
        lines.append("")
        lines.append("Notes")
        lines.extend(f"  - {note}" for note in report.notes)
    return "\n".join(lines)


def _read_jsonl(path: Path, model: type[PersistentCandidate] | type[ScrapeAttempt]) -> list[Any]:
    if not path.exists():
        return []
    rows: list[Any] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(model.model_validate_json(line))
    return rows


def _is_search_engine_candidate(candidate: PersistentCandidate) -> bool:
    return any(name in _SEARCH_ENGINE_NAMES for name in candidate.discovered_via)


def _is_source_native_candidate(candidate: PersistentCandidate) -> bool:
    return candidate.source_discovery_only or any(
        name not in _SEARCH_ENGINE_NAMES for name in candidate.discovered_via
    )


def _build_engine_overlap_metrics(candidates: list[PersistentCandidate]) -> EngineOverlapMetrics:
    source_native_ids = {
        candidate.candidate_id for candidate in candidates if _is_source_native_candidate(candidate)
    }
    brave_ids = {
        candidate.candidate_id for candidate in candidates if "brave" in candidate.discovered_via
    }
    exa_ids = {
        candidate.candidate_id for candidate in candidates if "exa" in candidate.discovered_via
    }
    google_ids = {
        candidate.candidate_id
        for candidate in candidates
        if "google_cse" in candidate.discovered_via
    }
    return EngineOverlapMetrics(
        source_native=len(source_native_ids),
        brave=len(brave_ids),
        exa=len(exa_ids),
        google_cse=len(google_ids),
        source_native_brave_shared=len(source_native_ids & brave_ids),
        source_native_exa_shared=len(source_native_ids & exa_ids),
        source_native_google_cse_shared=len(source_native_ids & google_ids),
        brave_exa_shared=len(brave_ids & exa_ids),
        brave_google_cse_shared=len(brave_ids & google_ids),
        exa_google_cse_shared=len(exa_ids & google_ids),
        shared_all_candidates=len(source_native_ids & brave_ids & exa_ids & google_ids),
    )


def _safe_ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _build_source_search_coverage(
    candidates: list[PersistentCandidate],
) -> SourceSearchCoverageMetrics:
    source_native = 0
    search_engine = 0
    source_only = 0
    search_only = 0
    shared = 0
    for candidate in candidates:
        has_source_native = _is_source_native_candidate(candidate)
        has_search_engine = _is_search_engine_candidate(candidate)
        if has_source_native:
            source_native += 1
        if has_search_engine:
            search_engine += 1
        if has_source_native and has_search_engine:
            shared += 1
        elif has_source_native:
            source_only += 1
        elif has_search_engine:
            search_only += 1
    total = len(candidates)
    return SourceSearchCoverageMetrics(
        total_candidates=total,
        source_native_candidates=source_native,
        search_engine_candidates=search_engine,
        source_native_only_candidates=source_only,
        search_engine_only_candidates=search_only,
        shared_candidates=shared,
        source_native_share=_safe_ratio(source_native, total),
        search_engine_share=_safe_ratio(search_engine, total),
        shared_share=_safe_ratio(shared, total),
    )


def _build_queue_health_metrics(
    candidates: list[PersistentCandidate],
    *,
    now: datetime,
    stale_after_days: int,
) -> QueueHealthMetrics:
    stale_cutoff = now - timedelta(days=stale_after_days)
    retry_backlog = 0
    stale = 0
    for candidate in candidates:
        if (
            candidate.candidate_status in _RETRY_BACKLOG_STATUSES
            and candidate.next_scrape_attempt_at is not None
            and candidate.next_scrape_attempt_at <= now
        ):
            retry_backlog += 1
        if (
            candidate.candidate_status in _STALE_CANDIDATE_STATUSES
            and candidate.last_seen_at <= stale_cutoff
        ):
            stale += 1
    status_counts = Counter(candidate.candidate_status for candidate in candidates)
    return QueueHealthMetrics(
        total_candidates=len(candidates),
        new_candidates=status_counts[CandidateStatus.NEW],
        queued_candidates=status_counts[CandidateStatus.QUEUED],
        scrape_pending_candidates=status_counts[CandidateStatus.SCRAPE_PENDING],
        scrape_in_progress_candidates=status_counts[CandidateStatus.SCRAPE_IN_PROGRESS],
        scrape_failed_candidates=status_counts[CandidateStatus.SCRAPE_FAILED],
        partially_scraped_candidates=status_counts[CandidateStatus.PARTIALLY_SCRAPED],
        unsupported_source_candidates=status_counts[CandidateStatus.UNSUPPORTED_SOURCE],
        stale_candidates=stale,
        retry_backlog_candidates=retry_backlog,
    )


def _candidate_identity_urls(candidate: PersistentCandidate) -> set[str]:
    urls: set[str] = set()
    urls.add(canonicalize_news_url(str(candidate.current_url)))
    if candidate.canonical_url is not None:
        urls.add(canonicalize_news_url(str(candidate.canonical_url)))
    return urls


def _load_operational_record_urls(config: Config) -> tuple[set[str], list[str]]:
    notes: list[str] = []
    try:
        store = create_operational_store(config)
    except Exception as exc:
        return set(), [f"Operational store unavailable: {type(exc).__name__}: {exc}"]

    try:
        rows = store.fetch_records(config.dataset_name.value)
    except Exception as exc:
        notes.append(f"Operational record fetch failed: {type(exc).__name__}: {exc}")
        return set(), notes
    finally:
        store.close()

    urls = {
        canonicalize_news_url(str(row["canonical_url"])) for row in rows if row.get("canonical_url")
    }
    return urls, notes


def _build_candidate_conversion_metrics(
    candidates: list[PersistentCandidate],
    *,
    operational_urls: set[str],
) -> CandidateConversionMetrics:
    total = len(candidates)
    matched_candidate_ids = {
        candidate.candidate_id
        for candidate in candidates
        if operational_urls and _candidate_identity_urls(candidate) & operational_urls
    }
    status_counts = Counter(candidate.candidate_status for candidate in candidates)
    per_producer: list[ProducerConversionMetrics] = []
    all_producers = sorted(
        {producer_name for candidate in candidates for producer_name in candidate.discovered_via}
    )
    for producer_name in all_producers:
        producer_candidates = [
            candidate for candidate in candidates if producer_name in candidate.discovered_via
        ]
        producer_total = len(producer_candidates)
        producer_status_counts = Counter(
            candidate.candidate_status for candidate in producer_candidates
        )
        producer_matches = sum(
            1
            for candidate in producer_candidates
            if candidate.candidate_id in matched_candidate_ids
        )
        per_producer.append(
            ProducerConversionMetrics(
                producer_name=producer_name,
                candidate_count=producer_total,
                scrape_succeeded_candidates=producer_status_counts[
                    CandidateStatus.SCRAPE_SUCCEEDED
                ],
                partially_scraped_candidates=producer_status_counts[
                    CandidateStatus.PARTIALLY_SCRAPED
                ],
                scrape_failed_candidates=producer_status_counts[CandidateStatus.SCRAPE_FAILED],
                unsupported_source_candidates=producer_status_counts[
                    CandidateStatus.UNSUPPORTED_SOURCE
                ],
                operational_record_matches=producer_matches,
                scrape_success_rate=_safe_ratio(
                    producer_status_counts[CandidateStatus.SCRAPE_SUCCEEDED],
                    producer_total,
                ),
                operational_match_rate=_safe_ratio(producer_matches, producer_total),
            )
        )

    return CandidateConversionMetrics(
        total_candidates=total,
        scrape_succeeded_candidates=status_counts[CandidateStatus.SCRAPE_SUCCEEDED],
        partially_scraped_candidates=status_counts[CandidateStatus.PARTIALLY_SCRAPED],
        scrape_failed_candidates=status_counts[CandidateStatus.SCRAPE_FAILED],
        unsupported_source_candidates=status_counts[CandidateStatus.UNSUPPORTED_SOURCE],
        never_scraped_candidates=(
            status_counts[CandidateStatus.NEW]
            + status_counts[CandidateStatus.QUEUED]
            + status_counts[CandidateStatus.SCRAPE_PENDING]
            + status_counts[CandidateStatus.SCRAPE_IN_PROGRESS]
        ),
        operational_record_matches=len(matched_candidate_ids),
        scrape_success_rate=_safe_ratio(
            status_counts[CandidateStatus.SCRAPE_SUCCEEDED],
            total,
        ),
        operational_match_rate=_safe_ratio(len(matched_candidate_ids), total),
        per_producer=per_producer,
    )


def _build_failure_summaries(
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
    *,
    key: str,
) -> list[FailureSummary]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    counts: Counter[str] = Counter()
    for attempt in attempts:
        if attempt.fetch_status.value == "success":
            continue
        candidate = candidate_by_id.get(attempt.candidate_id)
        if key == "source":
            value = attempt.source_adapter_name or "unknown"
        elif key == "domain":
            value = (
                candidate.domain
                if candidate is not None and candidate.domain is not None
                else "unknown"
            )
        elif key == "error_code":
            value = attempt.error_code or "unknown"
        else:
            raise ValueError(f"Unsupported failure summary key: {key}")
        counts[value] += 1
    return [FailureSummary(name=name, count=count) for name, count in counts.most_common(5)]
