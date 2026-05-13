"""Discovery-layer diagnostics and observability reporting."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from denbust.config import Config, OperationalProvider, load_config
from denbust.discovery.models import (
    CandidateProvenance,
    CandidateStatus,
    ContentBasis,
    FetchStatus,
    PersistentCandidate,
    ScrapeAttempt,
    ScrapeAttemptKind,
)
from denbust.discovery.queries import enabled_source_domains
from denbust.discovery.scrape_queue import (
    SCRAPEABLE_CANDIDATE_STATUSES,
    order_scrape_eligible_candidates,
)
from denbust.discovery.state_paths import write_metrics_snapshot
from denbust.news_items.normalize import canonicalize_news_url
from denbust.ops.factory import create_operational_store
from denbust.taxonomy import default_taxonomy

_SEARCH_ENGINE_NAMES: tuple[str, ...] = ("brave", "exa", "google_cse")
_SOURCE_SUGGESTION_EXCLUDED_DOMAINS: frozenset[str] = frozenset({"facebook.com"})
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
_QUEUE_DRAIN_TEXT_ORDER_LIMIT = 10


@dataclass(frozen=True)
class OperationalRecordIndex:
    """Lookup tables for candidate-related operational records."""

    rows: list[dict[str, Any]]
    rows_by_candidate_id: dict[str, list[dict[str, Any]]]
    rows_by_canonical_url: dict[str, list[dict[str, Any]]]


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
    search_result_only_candidates: int = 0
    partial_page_candidates: int = 0
    full_article_page_candidates: int = 0
    stale_candidates: int = 0
    retry_backlog_candidates: int = 0
    self_heal_eligible_candidates: int = 0
    search_noise_filter_reason_counts: dict[str, int] = Field(default_factory=dict)


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
    search_result_only_candidates: int = 0
    partial_page_candidates: int = 0
    full_article_page_candidates: int = 0
    operational_record_matches: int = 0
    scrape_success_rate: float = 0.0
    operational_match_rate: float = 0.0
    per_producer: list[ProducerConversionMetrics] = Field(default_factory=list)


class FailureSummary(BaseModel):
    """Top scrape-failure grouping for diagnostics output."""

    name: str
    count: int


class ScrapeFailureDiagnostic(BaseModel):
    """Structured scrape-failure group for future self-heal triage."""

    attempt_kind: str
    fetch_status: str
    error_code: str
    source_adapter_name: str
    domain: str
    count: int = 0
    self_heal_eligible_count: int = 0
    latest_attempt_at: datetime | None = None


class SourceMixEntry(BaseModel):
    """Candidate counts grouped by inferred source for queue-drain diagnostics."""

    source: str
    candidate_count: int = 0


class QueueDrainCandidate(BaseModel):
    """One candidate in a diagnostic queue-drain order."""

    position: int
    candidate_id: str
    source: str
    status: str
    retry_priority: int = 0
    next_scrape_attempt_at: datetime | None = None
    last_seen_at: datetime
    current_url: str


class QueueDrainDiagnostics(BaseModel):
    """Selection-order, source-mix, and budget-cap diagnostics for bounded drains."""

    max_candidate_budget: int
    persisted_attempted_candidate_count: int = 0
    persisted_scrape_attempt_count: int = 0
    remaining_eligible_candidate_count: int = 0
    inferred_stop_reason: str = "no_eligible_candidates"
    persisted_attempted_candidate_order: list[QueueDrainCandidate] = Field(default_factory=list)
    remaining_eligible_candidate_order: list[QueueDrainCandidate] = Field(default_factory=list)
    attempted_source_mix: list[SourceMixEntry] = Field(default_factory=list)
    remaining_eligible_source_mix: list[SourceMixEntry] = Field(default_factory=list)


class ClassifierWarningSignals(BaseModel):
    """Classifier-related conversion signals visible from persisted operational rows."""

    candidate_fallback_record_count: int = 0
    partial_page_fallback_record_count: int = 0
    search_result_only_fallback_record_count: int = 0
    fallback_record_without_taxonomy_count: int = 0
    partial_page_fallback_without_taxonomy_count: int = 0
    low_confidence_fallback_record_count: int = 0
    invalid_taxonomy_pair_record_count: int = 0


class PartialPageDiagnostics(BaseModel):
    """Detailed partial-page interpretation for candidate conversion diagnostics."""

    operational_matching_enabled: bool = False
    operational_records_available: bool = False
    partial_candidate_count: int = 0
    retained_operational_record_candidate_count: int = 0
    retained_operational_record_count: int = 0
    metadata_only_partial_candidate_count: int = 0
    search_result_only_candidate_count: int = 0
    generic_fetch_partial_candidate_count: int = 0
    source_adapter_partial_candidate_count: int = 0
    partial_after_source_adapter_attempt_count: int = 0
    partial_without_source_adapter_attempt_count: int = 0
    blocked_generic_fetch_candidate_count: int = 0
    failed_generic_fetch_candidate_count: int = 0
    timeout_generic_fetch_candidate_count: int = 0
    partial_candidates_by_domain: list[FailureSummary] = Field(default_factory=list)
    partial_candidates_by_source: list[FailureSummary] = Field(default_factory=list)
    partial_attempts_by_kind: list[FailureSummary] = Field(default_factory=list)
    partial_attempts_by_source_adapter: list[FailureSummary] = Field(default_factory=list)
    generic_fetch_error_code_counts: list[FailureSummary] = Field(default_factory=list)
    classifier_warning_signals: ClassifierWarningSignals = Field(
        default_factory=ClassifierWarningSignals
    )


class SourceSuggestion(BaseModel):
    """Advisory suggestion for a candidate-heavy unseen domain."""

    domain: str
    candidate_count: int = 0
    run_count: int = 0
    candidate_only_count: int = 0
    search_result_only_count: int = 0
    scrape_attempt_count: int = 0
    scrape_success_count: int = 0
    scrape_failure_count: int = 0
    unsupported_count: int = 0
    score: float = 0.0


class SourceSuggestionReport(BaseModel):
    """Structured source-suggestion payload for diagnostics artifacts."""

    suggestions: list[SourceSuggestion] = Field(default_factory=list)


class DiscoveryDiagnosticReport(BaseModel):
    """Full discovery observability report."""

    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    config_path: str
    dataset_name: str
    stale_after_days: int
    latest_candidates_path: str
    scrape_attempts_path: str
    candidate_provenance_path: str
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
    scrape_failure_diagnostics: list[ScrapeFailureDiagnostic] = Field(default_factory=list)
    queue_drain: QueueDrainDiagnostics = Field(
        default_factory=lambda: QueueDrainDiagnostics(max_candidate_budget=0)
    )
    partial_page_diagnostics: PartialPageDiagnostics = Field(default_factory=PartialPageDiagnostics)
    source_suggestions: SourceSuggestionReport = Field(default_factory=SourceSuggestionReport)


def run_discovery_diagnostics(
    *,
    config_path: Path,
    stale_after_days: int = 7,
    include_operational_matches: bool = True,
) -> DiscoveryDiagnosticReport:
    """Build a discovery-layer diagnostics report from persisted state."""
    config = load_config(config_path)
    return build_discovery_diagnostic_report(
        config=config,
        config_path=config_path,
        stale_after_days=stale_after_days,
        include_operational_matches=include_operational_matches,
    )


def build_discovery_diagnostic_report(
    *,
    config: Config,
    config_path: Path | None = None,
    stale_after_days: int = 7,
    candidates_override: list[PersistentCandidate] | None = None,
    attempts_override: list[ScrapeAttempt] | None = None,
    overlap_candidates_override: list[PersistentCandidate] | None = None,
    include_operational_matches: bool = True,
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
    overlap_candidates = (
        overlap_candidates_override if overlap_candidates_override is not None else candidates
    )
    provenance = _read_jsonl(paths.candidate_provenance_path, CandidateProvenance)
    now = datetime.now(UTC)
    operational_rows: list[dict[str, Any]] = []
    operational_notes: list[str] = []
    if include_operational_matches:
        operational_rows, operational_notes = _load_operational_records(config)
    elif config.operational.provider is not OperationalProvider.NONE:
        operational_notes.append(
            "Operational record matching was skipped for this diagnostics artifact."
        )
    operational_index = _build_operational_record_index(operational_rows)
    report = DiscoveryDiagnosticReport(
        config_path=str(config_path) if config_path is not None else "<in-memory-config>",
        dataset_name=config.dataset_name.value,
        stale_after_days=stale_after_days,
        latest_candidates_path=str(paths.latest_candidates_path),
        scrape_attempts_path=str(paths.scrape_attempts_path),
        candidate_provenance_path=str(paths.candidate_provenance_path),
        operational_records_available=bool(operational_rows),
        notes=operational_notes,
        engine_overlap=_build_engine_overlap_metrics(overlap_candidates),
        source_search_coverage=_build_source_search_coverage(candidates),
        queue_health=_build_queue_health_metrics(
            candidates, now=now, stale_after_days=stale_after_days
        ),
        candidate_conversion=_build_candidate_conversion_metrics(
            candidates,
            operational_index=operational_index,
        ),
        top_failure_sources=_build_failure_summaries(candidates, attempts, key="source"),
        top_failure_domains=_build_failure_summaries(candidates, attempts, key="domain"),
        top_failure_codes=_build_failure_summaries(candidates, attempts, key="error_code"),
        scrape_failure_diagnostics=_build_scrape_failure_diagnostics(candidates, attempts),
        queue_drain=_build_queue_drain_diagnostics(
            config=config,
            candidates=candidates,
            attempts=attempts,
            now=now,
        ),
        partial_page_diagnostics=_build_partial_page_diagnostics(
            candidates=candidates,
            attempts=attempts,
            operational_index=operational_index,
            operational_matching_enabled=include_operational_matches,
        ),
        source_suggestions=_build_source_suggestion_report(
            config=config,
            candidates=candidates,
            attempts=attempts,
            provenance=provenance,
        ),
    )
    if not candidates:
        report.notes.append("No persisted discovery candidates were found.")
    if not attempts:
        report.notes.append("No persisted scrape attempts were found.")
    if (
        include_operational_matches
        and not operational_rows
        and config.operational.provider is not OperationalProvider.NONE
    ):
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
    write_metrics_snapshot(
        paths.source_suggestions_latest_path,
        report.source_suggestions.model_dump(mode="json"),
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
        "retry_backlog={retry_backlog_candidates} "
        "self_heal_eligible={self_heal_eligible_candidates}".format(**queue.model_dump(mode="json"))
    )
    lines.append(
        "  basis search_result_only={search_result_only_candidates} "
        "partial_page={partial_page_candidates} full_article_page={full_article_page_candidates}".format(
            **queue.model_dump(mode="json")
        )
    )
    if queue.search_noise_filter_reason_counts:
        lines.append(
            "  search_noise_filters "
            + ", ".join(
                f"{reason}={count}"
                for reason, count in queue.search_noise_filter_reason_counts.items()
            )
        )
    lines.append("")
    lines.append("Conversion")
    lines.append(
        "  scrape_succeeded={scrape_succeeded_candidates} partial={partially_scraped_candidates} "
        "failed={scrape_failed_candidates} unsupported={unsupported_source_candidates} "
        "never_scraped={never_scraped_candidates}".format(**conversion.model_dump(mode="json"))
    )
    lines.append(
        "  basis search_result_only={search_result_only_candidates} "
        "partial_page={partial_page_candidates} full_article_page={full_article_page_candidates}".format(
            **conversion.model_dump(mode="json")
        )
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
    if report.scrape_failure_diagnostics:
        lines.append("")
        lines.append("Structured scrape failures")
        for failure in report.scrape_failure_diagnostics:
            lines.append(
                "  {error_code} source={source_adapter_name} domain={domain} "
                "kind={attempt_kind} status={fetch_status} count={count} "
                "self_heal_eligible={self_heal_eligible_count}".format(
                    **failure.model_dump(mode="json")
                )
            )
    drain = report.queue_drain
    lines.append("")
    lines.append("Queue drain")
    lines.append(
        "  max_candidates={max_candidate_budget} "
        "persisted_attempted_candidates={persisted_attempted_candidate_count} "
        "persisted_scrape_attempts={persisted_scrape_attempt_count} "
        "remaining_eligible={remaining_eligible_candidate_count} "
        "inferred_stop_reason={inferred_stop_reason}".format(**drain.model_dump(mode="json"))
    )
    if drain.attempted_source_mix:
        lines.append(
            "  attempted_source_mix: "
            + ", ".join(
                f"{entry.source}={entry.candidate_count}" for entry in drain.attempted_source_mix
            )
        )
    if drain.remaining_eligible_source_mix:
        lines.append(
            "  remaining_eligible_source_mix: "
            + ", ".join(
                f"{entry.source}={entry.candidate_count}"
                for entry in drain.remaining_eligible_source_mix
            )
        )
    if drain.persisted_attempted_candidate_order:
        _append_queue_drain_order_lines(
            lines,
            label="persisted_attempted_order",
            candidates=drain.persisted_attempted_candidate_order,
            total_count=drain.persisted_attempted_candidate_count,
        )
    if drain.remaining_eligible_candidate_order:
        _append_queue_drain_order_lines(
            lines,
            label="remaining_eligible_order",
            candidates=drain.remaining_eligible_candidate_order,
            total_count=drain.remaining_eligible_candidate_count,
        )
    partials = report.partial_page_diagnostics
    lines.append("")
    lines.append("Partial pages")
    lines.append(
        "  operational_matching={matching} operational_records_available={available}".format(
            matching="enabled" if partials.operational_matching_enabled else "skipped",
            available=str(partials.operational_records_available).lower(),
        )
    )
    lines.append(
        "  partial_candidates={partial_candidate_count} "
        "retained_operational_candidate_matches={retained_operational_record_candidate_count} "
        "retained_operational_records={retained_operational_record_count} "
        "metadata_only_partial_candidates={metadata_only_partial_candidate_count} "
        "search_result_only_candidates={search_result_only_candidate_count}".format(
            **partials.model_dump(mode="json")
        )
    )
    lines.append(
        "  generic_partial_candidates={generic_fetch_partial_candidate_count} "
        "source_adapter_partial_candidates={source_adapter_partial_candidate_count} "
        "partial_after_source_adapter_attempts={partial_after_source_adapter_attempt_count} "
        "partial_without_source_adapter_attempts={partial_without_source_adapter_attempt_count} "
        "blocked_generic_fetch_candidates={blocked_generic_fetch_candidate_count} "
        "failed_generic_fetch_candidates={failed_generic_fetch_candidate_count} "
        "timeout_generic_fetch_candidates={timeout_generic_fetch_candidate_count}".format(
            **partials.model_dump(mode="json")
        )
    )
    if partials.partial_candidates_by_domain:
        lines.append(
            "  partial_domains: "
            + ", ".join(
                f"{item.name}={item.count}" for item in partials.partial_candidates_by_domain
            )
        )
    if partials.partial_candidates_by_source:
        lines.append(
            "  partial_sources: "
            + ", ".join(
                f"{item.name}={item.count}" for item in partials.partial_candidates_by_source
            )
        )
    if partials.partial_attempts_by_kind:
        lines.append(
            "  partial_attempt_kinds: "
            + ", ".join(f"{item.name}={item.count}" for item in partials.partial_attempts_by_kind)
        )
    if partials.partial_attempts_by_source_adapter:
        lines.append(
            "  partial_attempt_sources: "
            + ", ".join(
                f"{item.name}={item.count}" for item in partials.partial_attempts_by_source_adapter
            )
        )
    if partials.generic_fetch_error_code_counts:
        lines.append(
            "  generic_fetch_errors: "
            + ", ".join(
                f"{item.name}={item.count}" for item in partials.generic_fetch_error_code_counts
            )
        )
    classifier = partials.classifier_warning_signals
    if classifier.candidate_fallback_record_count:
        lines.append(
            "  classifier_signals candidate_fallback_records={candidate_fallback_record_count} "
            "partial_fallback_records={partial_page_fallback_record_count} "
            "fallback_without_taxonomy={fallback_record_without_taxonomy_count} "
            "partial_without_taxonomy={partial_page_fallback_without_taxonomy_count} "
            "low_confidence_fallback={low_confidence_fallback_record_count} "
            "invalid_taxonomy_pairs={invalid_taxonomy_pair_record_count}".format(
                **classifier.model_dump(mode="json")
            )
        )
    if report.source_suggestions.suggestions:
        lines.append("")
        lines.append("Source suggestions")
        for suggestion in report.source_suggestions.suggestions:
            lines.append(
                "  {domain} score={score:.2f} candidates={candidate_count} runs={run_count} "
                "candidate_only={candidate_only_count} successes={scrape_success_count} "
                "failures={scrape_failure_count}".format(**suggestion.model_dump(mode="json"))
            )
    if report.notes:
        lines.append("")
        lines.append("Notes")
        lines.extend(f"  - {note}" for note in report.notes)
    return "\n".join(lines)


def _append_queue_drain_order_lines(
    lines: list[str],
    *,
    label: str,
    candidates: list[QueueDrainCandidate],
    total_count: int,
) -> None:
    shown = candidates[:_QUEUE_DRAIN_TEXT_ORDER_LIMIT]
    suffix = f" showing first {len(shown)} of {total_count}" if total_count > len(shown) else ""
    lines.append(f"  {label}:{suffix}")
    for candidate in shown:
        lines.append(
            "    {position}. {candidate_id} source={source} status={status} "
            "priority={retry_priority}".format(**candidate.model_dump(mode="json"))
        )


def _read_jsonl(
    path: Path,
    model: type[PersistentCandidate] | type[ScrapeAttempt] | type[CandidateProvenance],
) -> list[Any]:
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
    basis_counts = Counter(candidate.content_basis for candidate in candidates)
    search_noise_filter_reason_counts = Counter(
        reason
        for candidate in candidates
        if candidate.candidate_status is CandidateStatus.UNSUPPORTED_SOURCE
        for reason in [_search_noise_filter_reason(candidate)]
        if reason is not None
    )
    return QueueHealthMetrics(
        total_candidates=len(candidates),
        new_candidates=status_counts[CandidateStatus.NEW],
        queued_candidates=status_counts[CandidateStatus.QUEUED],
        scrape_pending_candidates=status_counts[CandidateStatus.SCRAPE_PENDING],
        scrape_in_progress_candidates=status_counts[CandidateStatus.SCRAPE_IN_PROGRESS],
        scrape_failed_candidates=status_counts[CandidateStatus.SCRAPE_FAILED],
        partially_scraped_candidates=status_counts[CandidateStatus.PARTIALLY_SCRAPED],
        unsupported_source_candidates=status_counts[CandidateStatus.UNSUPPORTED_SOURCE],
        search_result_only_candidates=basis_counts[ContentBasis.SEARCH_RESULT_ONLY],
        partial_page_candidates=basis_counts[ContentBasis.PARTIAL_PAGE],
        full_article_page_candidates=basis_counts[ContentBasis.FULL_ARTICLE_PAGE],
        stale_candidates=stale,
        retry_backlog_candidates=retry_backlog,
        self_heal_eligible_candidates=sum(
            1
            for candidate in candidates
            if candidate.candidate_status is CandidateStatus.SCRAPE_FAILED
            and candidate.self_heal_eligible
        ),
        search_noise_filter_reason_counts=dict(sorted(search_noise_filter_reason_counts.items())),
    )


def _search_noise_filter_reason(candidate: PersistentCandidate) -> str | None:
    reason = candidate.metadata.get("unsupported_source_reason")
    if (
        candidate.metadata.get("unsupported_source_filter") == "search_noise"
        and isinstance(reason, str)
        and reason
    ):
        return reason
    latest_discovery_metadata = candidate.metadata.get("latest_discovery_metadata")
    if not isinstance(latest_discovery_metadata, dict):
        return None
    legacy_reason = latest_discovery_metadata.get("search_noise_filter_reason")
    return legacy_reason if isinstance(legacy_reason, str) and legacy_reason else None


def _candidate_identity_urls(candidate: PersistentCandidate) -> set[str]:
    urls: set[str] = set()
    urls.add(canonicalize_news_url(str(candidate.current_url)))
    if candidate.canonical_url is not None:
        urls.add(canonicalize_news_url(str(candidate.canonical_url)))
    return urls


def _load_operational_record_urls(config: Config) -> tuple[set[str], list[str]]:
    rows, notes = _load_operational_records(config)
    return _operational_record_urls(rows), notes


def _load_operational_records(config: Config) -> tuple[list[dict[str, Any]], list[str]]:
    notes: list[str] = []
    try:
        store = create_operational_store(config)
    except Exception as exc:
        return [], [f"Operational store unavailable: {type(exc).__name__}: {exc}"]

    try:
        rows = store.fetch_records(config.dataset_name.value)
    except Exception as exc:
        notes.append(f"Operational record fetch failed: {type(exc).__name__}: {exc}")
        return [], notes
    finally:
        store.close()
    return [dict(row) for row in rows], notes


def _operational_record_urls(rows: list[dict[str, Any]]) -> set[str]:
    urls = {
        canonicalize_news_url(str(row["canonical_url"])) for row in rows if row.get("canonical_url")
    }
    return urls


def _build_candidate_conversion_metrics(
    candidates: list[PersistentCandidate],
    *,
    operational_index: OperationalRecordIndex,
) -> CandidateConversionMetrics:
    total = len(candidates)
    matched_candidate_ids = {
        candidate.candidate_id
        for candidate in candidates
        if _candidate_has_operational_record_match(
            candidate=candidate,
            operational_index=operational_index,
        )
    }
    status_counts = Counter(candidate.candidate_status for candidate in candidates)
    basis_counts = Counter(candidate.content_basis for candidate in candidates)
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
        search_result_only_candidates=basis_counts[ContentBasis.SEARCH_RESULT_ONLY],
        partial_page_candidates=basis_counts[ContentBasis.PARTIAL_PAGE],
        full_article_page_candidates=basis_counts[ContentBasis.FULL_ARTICLE_PAGE],
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
        if attempt.fetch_status is FetchStatus.SUCCESS:
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


def _build_scrape_failure_diagnostics(
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
) -> list[ScrapeFailureDiagnostic]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    grouped: dict[tuple[str, str, str, str, str], ScrapeFailureDiagnostic] = {}
    for attempt in attempts:
        if attempt.fetch_status is FetchStatus.SUCCESS:
            continue
        candidate = candidate_by_id.get(attempt.candidate_id)
        domain = (
            candidate.domain
            if candidate is not None and candidate.domain is not None
            else "unknown"
        )
        source_adapter_name = attempt.source_adapter_name or "unknown"
        error_code = attempt.error_code or "unknown"
        key = (
            attempt.attempt_kind.value,
            attempt.fetch_status.value,
            error_code,
            source_adapter_name,
            domain,
        )
        diagnostic = grouped.get(key)
        if diagnostic is None:
            diagnostic = ScrapeFailureDiagnostic(
                attempt_kind=attempt.attempt_kind.value,
                fetch_status=attempt.fetch_status.value,
                error_code=error_code,
                source_adapter_name=source_adapter_name,
                domain=domain,
            )
            grouped[key] = diagnostic
        diagnostic.count += 1
        if (
            candidate is not None
            and candidate.candidate_status is CandidateStatus.SCRAPE_FAILED
            and candidate.self_heal_eligible
        ):
            diagnostic.self_heal_eligible_count += 1
        finished_or_started = attempt.finished_at or attempt.started_at
        if (
            diagnostic.latest_attempt_at is None
            or finished_or_started > diagnostic.latest_attempt_at
        ):
            diagnostic.latest_attempt_at = finished_or_started
    return sorted(
        grouped.values(),
        key=lambda item: (
            -item.self_heal_eligible_count,
            -item.count,
            item.error_code,
            item.source_adapter_name,
            item.domain,
        ),
    )[:10]


def _candidate_source(candidate: PersistentCandidate) -> str:
    if candidate.source_hints:
        return candidate.source_hints[0]
    for producer in candidate.discovered_via:
        if producer not in _SEARCH_ENGINE_NAMES:
            return producer
    return candidate.domain or "unknown"


def _source_mix(candidates: list[PersistentCandidate]) -> list[SourceMixEntry]:
    counts = Counter(_candidate_source(candidate) for candidate in candidates)
    return [
        SourceMixEntry(source=source, candidate_count=count)
        for source, count in sorted(counts.items())
    ]


def _attempted_source_mix(
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
) -> list[SourceMixEntry]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    source_by_candidate_id: dict[str, str] = {}
    for attempt in sorted(attempts, key=lambda item: (item.started_at, item.attempt_id)):
        if attempt.candidate_id not in candidate_by_id:
            continue
        if attempt.source_adapter_name is None and attempt.candidate_id in source_by_candidate_id:
            continue
        source_by_candidate_id[attempt.candidate_id] = (
            attempt.source_adapter_name or _candidate_source(candidate_by_id[attempt.candidate_id])
        )
    counts = Counter(source_by_candidate_id.values())
    return [
        SourceMixEntry(source=source, candidate_count=count)
        for source, count in sorted(counts.items())
    ]


def _queue_drain_candidate(
    candidate: PersistentCandidate,
    *,
    position: int,
) -> QueueDrainCandidate:
    return QueueDrainCandidate(
        position=position,
        candidate_id=candidate.candidate_id,
        source=_candidate_source(candidate),
        status=candidate.candidate_status.value,
        retry_priority=candidate.retry_priority,
        next_scrape_attempt_at=candidate.next_scrape_attempt_at,
        last_seen_at=candidate.last_seen_at,
        current_url=str(candidate.current_url),
    )


def _persisted_attempted_candidates(
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
) -> list[PersistentCandidate]:
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    ordered_attempts = sorted(
        attempts,
        key=lambda attempt: (
            attempt.started_at,
            attempt.finished_at or attempt.started_at,
            attempt.attempt_id,
        ),
    )
    seen_ids: set[str] = set()
    attempted_candidates: list[PersistentCandidate] = []
    for attempt in ordered_attempts:
        if attempt.candidate_id in seen_ids:
            continue
        candidate = candidate_by_id.get(attempt.candidate_id)
        if candidate is None:
            continue
        attempted_candidates.append(candidate)
        seen_ids.add(attempt.candidate_id)
    return attempted_candidates


def _queue_drain_stop_reason(
    *,
    persisted_attempted_candidate_count: int,
    remaining_eligible_candidate_count: int,
    max_candidate_budget: int,
) -> str:
    if (
        max_candidate_budget > 0
        and persisted_attempted_candidate_count >= max_candidate_budget
        and remaining_eligible_candidate_count > 0
    ):
        return "budget_cap_reached"
    if remaining_eligible_candidate_count == 0:
        return "no_eligible_candidates"
    if persisted_attempted_candidate_count == 0:
        return "no_scrape_attempts_recorded"
    return "another_reason"


def _build_queue_drain_diagnostics(
    *,
    config: Config,
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
    now: datetime,
) -> QueueDrainDiagnostics:
    eligible_candidates = [
        candidate
        for candidate in candidates
        if candidate.candidate_status in SCRAPEABLE_CANDIDATE_STATUSES
    ]
    remaining_eligible = order_scrape_eligible_candidates(eligible_candidates, now=now)
    persisted_attempted = _persisted_attempted_candidates(candidates, attempts)
    max_candidate_budget = config.max_articles
    return QueueDrainDiagnostics(
        max_candidate_budget=max_candidate_budget,
        persisted_attempted_candidate_count=len(persisted_attempted),
        persisted_scrape_attempt_count=len(attempts),
        remaining_eligible_candidate_count=len(remaining_eligible),
        inferred_stop_reason=_queue_drain_stop_reason(
            persisted_attempted_candidate_count=len(persisted_attempted),
            remaining_eligible_candidate_count=len(remaining_eligible),
            max_candidate_budget=max_candidate_budget,
        ),
        persisted_attempted_candidate_order=[
            _queue_drain_candidate(candidate, position=index)
            for index, candidate in enumerate(persisted_attempted, start=1)
        ],
        remaining_eligible_candidate_order=[
            _queue_drain_candidate(candidate, position=index)
            for index, candidate in enumerate(remaining_eligible, start=1)
        ],
        attempted_source_mix=_attempted_source_mix(candidates, attempts),
        remaining_eligible_source_mix=_source_mix(remaining_eligible),
    )


def _operational_row_key(row: dict[str, Any]) -> str:
    raw_key = row.get("id") or row.get("canonical_url")
    return str(raw_key) if raw_key else str(id(row))


def _build_operational_record_index(rows: list[dict[str, Any]]) -> OperationalRecordIndex:
    rows_by_candidate_id: dict[str, list[dict[str, Any]]] = {}
    rows_by_canonical_url: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        event_candidate_ids = row.get("event_candidate_ids")
        if isinstance(event_candidate_ids, list):
            for candidate_id in event_candidate_ids:
                if isinstance(candidate_id, str) and candidate_id:
                    rows_by_candidate_id.setdefault(candidate_id, []).append(row)
        canonical_url = row.get("canonical_url")
        if isinstance(canonical_url, str) and canonical_url:
            normalized_url = canonicalize_news_url(canonical_url)
            rows_by_canonical_url.setdefault(normalized_url, []).append(row)
    return OperationalRecordIndex(
        rows=rows,
        rows_by_candidate_id=rows_by_candidate_id,
        rows_by_canonical_url=rows_by_canonical_url,
    )


def _operational_record_matches_filter(
    row: dict[str, Any],
    *,
    require_candidate_fallback: bool,
    require_content_basis: ContentBasis | None,
) -> bool:
    if require_candidate_fallback and not _record_is_candidate_fallback(row):
        return False
    return not (
        require_content_basis is not None
        and _record_content_basis(row) != require_content_basis.value
    )


def _candidate_operational_record_matches(
    *,
    candidate: PersistentCandidate,
    operational_index: OperationalRecordIndex,
    require_candidate_fallback: bool = False,
    require_content_basis: ContentBasis | None = None,
) -> list[dict[str, Any]]:
    matched_by_key: dict[str, dict[str, Any]] = {}
    for row in operational_index.rows_by_candidate_id.get(candidate.candidate_id, []):
        if _operational_record_matches_filter(
            row,
            require_candidate_fallback=require_candidate_fallback,
            require_content_basis=require_content_basis,
        ):
            matched_by_key[_operational_row_key(row)] = row
    for candidate_url in _candidate_identity_urls(candidate):
        for row in operational_index.rows_by_canonical_url.get(candidate_url, []):
            if _operational_record_matches_filter(
                row,
                require_candidate_fallback=require_candidate_fallback,
                require_content_basis=require_content_basis,
            ):
                matched_by_key[_operational_row_key(row)] = row
    matched_rows = list(matched_by_key.values())
    return matched_rows


def _candidate_has_operational_record_match(
    *,
    candidate: PersistentCandidate,
    operational_index: OperationalRecordIndex,
    require_candidate_fallback: bool = False,
    require_content_basis: ContentBasis | None = None,
) -> bool:
    for row in operational_index.rows_by_candidate_id.get(candidate.candidate_id, []):
        if _operational_record_matches_filter(
            row,
            require_candidate_fallback=require_candidate_fallback,
            require_content_basis=require_content_basis,
        ):
            return True
    for candidate_url in _candidate_identity_urls(candidate):
        for row in operational_index.rows_by_canonical_url.get(candidate_url, []):
            if _operational_record_matches_filter(
                row,
                require_candidate_fallback=require_candidate_fallback,
                require_content_basis=require_content_basis,
            ):
                return True
    return False


def _candidate_attempts_by_id(
    attempts: list[ScrapeAttempt],
) -> dict[str, list[ScrapeAttempt]]:
    attempts_by_candidate_id: dict[str, list[ScrapeAttempt]] = {}
    for attempt in attempts:
        attempts_by_candidate_id.setdefault(attempt.candidate_id, []).append(attempt)
    return attempts_by_candidate_id


def _candidate_has_attempt(
    attempts: list[ScrapeAttempt],
    *,
    attempt_kind: ScrapeAttemptKind | None = None,
    fetch_status: FetchStatus | None = None,
) -> bool:
    return any(
        (attempt_kind is None or attempt.attempt_kind is attempt_kind)
        and (fetch_status is None or attempt.fetch_status is fetch_status)
        for attempt in attempts
    )


def _candidate_has_generic_fetch_status(
    attempts: list[ScrapeAttempt],
    statuses: set[FetchStatus],
) -> bool:
    return any(
        attempt.attempt_kind is ScrapeAttemptKind.GENERIC_FETCH and attempt.fetch_status in statuses
        for attempt in attempts
    )


def _failure_summaries_from_counter(
    counts: Counter[str], *, limit: int = 5
) -> list[FailureSummary]:
    return [FailureSummary(name=name, count=count) for name, count in counts.most_common(limit)]


def _record_content_basis(row: dict[str, Any]) -> str:
    value = row.get("content_basis")
    if isinstance(value, ContentBasis):
        return value.value
    if isinstance(value, str) and value:
        return value
    return ""


def _record_is_candidate_fallback(row: dict[str, Any]) -> bool:
    return row.get("annotation_source") == "candidate_fallback"


def _record_has_taxonomy_pair(row: dict[str, Any]) -> bool:
    return bool(row.get("taxonomy_category_id") and row.get("taxonomy_subcategory_id"))


def _record_has_valid_taxonomy_pair(row: dict[str, Any]) -> bool:
    category = row.get("taxonomy_category_id")
    subcategory = row.get("taxonomy_subcategory_id")
    if not isinstance(category, str) or not isinstance(subcategory, str):
        return False
    return default_taxonomy().has_pair(category, subcategory)


def _partial_attempt_source_label(attempt: ScrapeAttempt) -> str:
    if attempt.source_adapter_name:
        return attempt.source_adapter_name
    if attempt.attempt_kind is ScrapeAttemptKind.SOURCE_ADAPTER:
        return "unknown_source_adapter"
    return "generic_fetch"


def _candidate_scoped_operational_rows(
    *,
    candidates: list[PersistentCandidate],
    operational_index: OperationalRecordIndex,
) -> list[dict[str, Any]]:
    scoped_rows_by_id_or_index: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for row in _candidate_operational_record_matches(
            candidate=candidate,
            operational_index=operational_index,
        ):
            scoped_rows_by_id_or_index[_operational_row_key(row)] = row
    return list(scoped_rows_by_id_or_index.values())


def _build_classifier_warning_signals(
    operational_rows: list[dict[str, Any]],
) -> ClassifierWarningSignals:
    fallback_rows = [row for row in operational_rows if _record_is_candidate_fallback(row)]
    partial_rows = [
        row
        for row in fallback_rows
        if _record_content_basis(row) == ContentBasis.PARTIAL_PAGE.value
    ]
    search_result_only_rows = [
        row
        for row in fallback_rows
        if _record_content_basis(row) == ContentBasis.SEARCH_RESULT_ONLY.value
    ]
    fallback_without_taxonomy = [row for row in fallback_rows if not _record_has_taxonomy_pair(row)]
    invalid_taxonomy_pairs = [
        row
        for row in fallback_rows
        if _record_has_taxonomy_pair(row) and not _record_has_valid_taxonomy_pair(row)
    ]
    low_confidence_rows = [
        row
        for row in fallback_rows
        if row.get("record_confidence") == "low" or row.get("classification_confidence") == "low"
    ]
    return ClassifierWarningSignals(
        candidate_fallback_record_count=len(fallback_rows),
        partial_page_fallback_record_count=len(partial_rows),
        search_result_only_fallback_record_count=len(search_result_only_rows),
        fallback_record_without_taxonomy_count=len(fallback_without_taxonomy),
        partial_page_fallback_without_taxonomy_count=sum(
            1 for row in partial_rows if not _record_has_taxonomy_pair(row)
        ),
        low_confidence_fallback_record_count=len(low_confidence_rows),
        invalid_taxonomy_pair_record_count=len(invalid_taxonomy_pairs),
    )


def _build_partial_page_diagnostics(
    *,
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
    operational_index: OperationalRecordIndex,
    operational_matching_enabled: bool,
) -> PartialPageDiagnostics:
    attempts_by_candidate_id = _candidate_attempts_by_id(attempts)
    partial_candidates = [
        candidate
        for candidate in candidates
        if candidate.content_basis is ContentBasis.PARTIAL_PAGE
        or candidate.candidate_status is CandidateStatus.PARTIALLY_SCRAPED
    ]
    search_result_only_candidates = [
        candidate
        for candidate in candidates
        if candidate.content_basis is ContentBasis.SEARCH_RESULT_ONLY
    ]
    matched_partial_candidate_ids: set[str] = set()
    retained_record_count = 0
    scoped_operational_rows = (
        _candidate_scoped_operational_rows(
            candidates=candidates,
            operational_index=operational_index,
        )
        if operational_matching_enabled
        else []
    )
    if operational_matching_enabled:
        for candidate in partial_candidates:
            matched_rows = _candidate_operational_record_matches(
                candidate=candidate,
                operational_index=operational_index,
                require_candidate_fallback=True,
                require_content_basis=ContentBasis.PARTIAL_PAGE,
            )
            if matched_rows:
                matched_partial_candidate_ids.add(candidate.candidate_id)
                retained_record_count += len(matched_rows)

    partial_attempts = [
        attempt for attempt in attempts if attempt.fetch_status is FetchStatus.PARTIAL
    ]
    generic_fetch_error_code_counts = Counter(
        attempt.error_code or "unknown"
        for attempt in attempts
        if attempt.attempt_kind is ScrapeAttemptKind.GENERIC_FETCH
        and attempt.fetch_status in {FetchStatus.BLOCKED, FetchStatus.FAILED, FetchStatus.TIMEOUT}
    )
    return PartialPageDiagnostics(
        operational_matching_enabled=operational_matching_enabled,
        operational_records_available=bool(operational_index.rows),
        partial_candidate_count=len(partial_candidates),
        retained_operational_record_candidate_count=len(matched_partial_candidate_ids),
        retained_operational_record_count=retained_record_count,
        metadata_only_partial_candidate_count=(
            len(partial_candidates) - len(matched_partial_candidate_ids)
            if operational_matching_enabled
            else 0
        ),
        search_result_only_candidate_count=len(search_result_only_candidates),
        generic_fetch_partial_candidate_count=sum(
            1
            for candidate in partial_candidates
            if _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.PARTIAL,
            )
        ),
        source_adapter_partial_candidate_count=sum(
            1
            for candidate in partial_candidates
            if _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                fetch_status=FetchStatus.PARTIAL,
            )
        ),
        partial_after_source_adapter_attempt_count=sum(
            1
            for candidate in partial_candidates
            if _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            )
            and _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.PARTIAL,
            )
        ),
        partial_without_source_adapter_attempt_count=sum(
            1
            for candidate in partial_candidates
            if not _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
            )
            and _candidate_has_attempt(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.PARTIAL,
            )
        ),
        blocked_generic_fetch_candidate_count=sum(
            1
            for candidate in candidates
            if _candidate_has_generic_fetch_status(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                {FetchStatus.BLOCKED},
            )
        ),
        failed_generic_fetch_candidate_count=sum(
            1
            for candidate in candidates
            if _candidate_has_generic_fetch_status(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                {FetchStatus.FAILED},
            )
        ),
        timeout_generic_fetch_candidate_count=sum(
            1
            for candidate in candidates
            if _candidate_has_generic_fetch_status(
                attempts_by_candidate_id.get(candidate.candidate_id, []),
                {FetchStatus.TIMEOUT},
            )
        ),
        partial_candidates_by_domain=_failure_summaries_from_counter(
            Counter(candidate.domain or "unknown" for candidate in partial_candidates)
        ),
        partial_candidates_by_source=_failure_summaries_from_counter(
            Counter(_candidate_source(candidate) for candidate in partial_candidates)
        ),
        partial_attempts_by_kind=_failure_summaries_from_counter(
            Counter(attempt.attempt_kind.value for attempt in partial_attempts)
        ),
        partial_attempts_by_source_adapter=_failure_summaries_from_counter(
            Counter(_partial_attempt_source_label(attempt) for attempt in partial_attempts)
        ),
        generic_fetch_error_code_counts=_failure_summaries_from_counter(
            generic_fetch_error_code_counts
        ),
        classifier_warning_signals=_build_classifier_warning_signals(scoped_operational_rows),
    )


def _build_source_suggestion_report(
    *,
    config: Config,
    candidates: list[PersistentCandidate],
    attempts: list[ScrapeAttempt],
    provenance: list[CandidateProvenance],
) -> SourceSuggestionReport:
    known_domains = {
        normalized
        for _, domain in enabled_source_domains(config)
        if (normalized := _normalize_domain(domain)) is not None
    }
    attempts_by_candidate_id: dict[str, list[ScrapeAttempt]] = {}
    for attempt in attempts:
        attempts_by_candidate_id.setdefault(attempt.candidate_id, []).append(attempt)
    provenance_runs_by_domain: dict[str, set[str]] = {}
    for event in provenance:
        normalized_domain = _normalize_domain(event.domain)
        if normalized_domain is None:
            continue
        provenance_runs_by_domain.setdefault(normalized_domain, set()).add(event.run_id)

    suggestions: list[SourceSuggestion] = []
    for domain, domain_candidates in _group_candidates_by_domain(candidates).items():
        if domain in known_domains or domain in _SOURCE_SUGGESTION_EXCLUDED_DOMAINS:
            continue
        domain_attempts = [
            attempt
            for candidate in domain_candidates
            for attempt in attempts_by_candidate_id.get(candidate.candidate_id, [])
        ]
        success_count = sum(
            1 for attempt in domain_attempts if attempt.fetch_status is FetchStatus.SUCCESS
        )
        failure_count = sum(
            1 for attempt in domain_attempts if attempt.fetch_status is not FetchStatus.SUCCESS
        )
        candidate_only_count = sum(
            1
            for candidate in domain_candidates
            if candidate.content_basis is ContentBasis.CANDIDATE_ONLY
        )
        search_result_only_count = sum(
            1
            for candidate in domain_candidates
            if candidate.content_basis is ContentBasis.SEARCH_RESULT_ONLY
        )
        unsupported_count = sum(
            1
            for candidate in domain_candidates
            if candidate.candidate_status is CandidateStatus.UNSUPPORTED_SOURCE
        )
        run_count = len(provenance_runs_by_domain.get(domain, set()))
        score = (
            len(domain_candidates)
            + run_count
            + success_count * 1.5
            + candidate_only_count * 0.25
            - failure_count * 0.25
            - unsupported_count * 0.5
        )
        suggestions.append(
            SourceSuggestion(
                domain=domain,
                candidate_count=len(domain_candidates),
                run_count=run_count,
                candidate_only_count=candidate_only_count,
                search_result_only_count=search_result_only_count,
                scrape_attempt_count=len(domain_attempts),
                scrape_success_count=success_count,
                scrape_failure_count=failure_count,
                unsupported_count=unsupported_count,
                score=score,
            )
        )
    suggestions.sort(key=lambda item: (-item.score, -item.candidate_count, item.domain))
    return SourceSuggestionReport(suggestions=suggestions[:5])


def _group_candidates_by_domain(
    candidates: list[PersistentCandidate],
) -> dict[str, list[PersistentCandidate]]:
    grouped: dict[str, list[PersistentCandidate]] = {}
    for candidate in candidates:
        normalized_domain = _normalize_domain(candidate.domain)
        if normalized_domain is None:
            continue
        grouped.setdefault(normalized_domain, []).append(candidate)
    return grouped


def _normalize_domain(domain: str | None) -> str | None:
    """Normalize domain strings to the canonical host form used by candidates."""
    if domain is None:
        return None
    normalized = domain.strip().casefold()
    if not normalized:
        return None
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized or None
