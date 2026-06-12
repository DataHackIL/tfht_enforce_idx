"""Pipeline orchestration and dataset/job dispatch."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict
from uuid import uuid4

from denbust.classifier.relevance import (
    PARSE_FAILURE_CATEGORY_KEYS,
    PARSE_FAILURE_JSON_ERROR_KIND_KEYS,
    PARSE_FAILURE_SAMPLE_KEYS,
    Classifier,
    ClassifierProviderError,
    create_classifier,
    sanitize_provider_error_message,
)
from denbust.config import Config, OutputFormat, SourceType, load_config
from denbust.data_models import ClassifiedArticle, RawArticle, SourceReference, UnifiedItem
from denbust.datasets.jobs import ensure_default_jobs_registered
from denbust.datasets.registry import require_job_handler
from denbust.dedup.similarity import Deduplicator, create_deduplicator
from denbust.diagnostics.discovery import (
    build_discovery_diagnostic_report,
    persist_discovery_diagnostic_artifacts,
)
from denbust.discovery import jsonl_io
from denbust.discovery.backfill import (
    BACKFILL_BATCH_ID_ENV,
    BACKFILL_DATE_FROM_ENV,
    BACKFILL_DATE_TO_ENV,
    BackfillWindow,
    backfill_metadata,
    build_backfill_queries,
    plan_backfill_windows,
    resolve_backfill_request_window,
)
from denbust.discovery.balanced_selection import (
    candidate_month,
    domain_frequencies,
    filter_by_domain_frequency,
    plan_balanced_scrape_batch,
)
from denbust.discovery.base import DiscoveryContext, SourceDiscoveryContext
from denbust.discovery.domain_verdicts import DomainVerdictStore, filter_by_domain_verdict
from denbust.discovery.engine_checkpoint import (
    cache_path as _engine_cache_path,
)
from denbust.discovery.engine_checkpoint import (
    load_cached_candidates,
    query_last_run_at,
    save_cached_candidates,
)
from denbust.discovery.engines.brave import BraveSearchEngine
from denbust.discovery.engines.exa import ExaSearchEngine
from denbust.discovery.engines.google_cse import GoogleCseSearchEngine
from denbust.discovery.models import (
    BackfillBatch,
    BackfillBatchStatus,
    DiscoveredCandidate,
    DiscoveryQuery,
    DiscoveryRun,
    DiscoveryRunStatus,
    ExecutedBackfillQuery,
    PersistentCandidate,
)
from denbust.discovery.queries import build_discovery_queries, select_run_queries
from denbust.discovery.query_yield import QueryYieldStore
from denbust.discovery.scrape_queue import (
    SCRAPEABLE_CANDIDATE_STATUSES,
    CandidateScrapeBatch,
    order_scrape_eligible_candidates,
    scrape_candidates,
    select_backfill_candidates_for_scrape,
    select_candidates_for_scrape,
)
from denbust.discovery.search_budget import (
    SearchBudgetLedger,
    affordable_query_count,
)
from denbust.discovery.source_native import (
    PersistedSourceDiscovery,
    SourceDiscoveryAdapter,
    persist_discovered_candidates,
    raw_article_to_discovered_candidate,
)
from denbust.discovery.storage import DiscoveryPersistence, create_discovery_persistence
from denbust.models.common import DatasetName, JobName
from denbust.models.policies import PublicationStatus, ReviewStatus
from denbust.models.runs import RunSnapshot
from denbust.news_items.annotations import (
    apply_manual_annotations,
    parse_missing_news_items,
    parse_news_item_corrections,
)
from denbust.news_items.backup import execute_latest_backup
from denbust.news_items.enrich import fallback_enrichment
from denbust.news_items.ingest import (
    build_operational_records,
    parse_suppression_rules,
    summarize_privacy_mix,
)
from denbust.news_items.models import NewsItemOperationalRecord
from denbust.news_items.monthly_report import (
    MONTHLY_REPORT_HQ_ACTIVITY_ENV,
    MONTHLY_REPORT_HQ_ACTIVITY_FILE_ENV,
    MONTHLY_REPORT_JSON_ENV,
    MONTHLY_REPORT_MARKDOWN_ENV,
    MONTHLY_REPORT_MONTH_ENV,
    MonthlyReport,
    generate_monthly_report,
    hq_activity_from_inputs,
    persist_monthly_report_artifacts,
    report_env_summary,
    resolve_report_month,
    select_monthly_report_records,
    write_report_copy,
    write_report_json_copy,
)
from denbust.news_items.normalize import canonicalize_news_url, deduplicate_strings
from denbust.news_items.policy import infer_privacy_risk, merge_privacy_risk
from denbust.news_items.publication import publish_release_bundle
from denbust.news_items.release import (
    NewsItemsReleaseBuilder,
    parse_operational_records,
    select_releasable_records,
)
from denbust.ops.factory import create_operational_store
from denbust.ops.storage import OperationalStore
from denbust.output.email import send_email_report
from denbust.output.formatter import print_items
from denbust.prefilter.adapters import PersistentCandidateView, RawArticleCandidateView
from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterMode
from denbust.prefilter.models import PrefilterDecision
from denbust.prefilter.state_paths import resolve_prefilter_state_paths
from denbust.sources.base import Source
from denbust.sources.haaretz import create_haaretz_source
from denbust.sources.ice import create_ice_source
from denbust.sources.maariv import create_maariv_source
from denbust.sources.mako import create_mako_source
from denbust.sources.rss import RSSSource, YnetRSSSource
from denbust.sources.walla import create_walla_source
from denbust.store.run_snapshots import (
    write_run_debug_log,
    write_run_debug_summary,
    write_run_snapshot,
)
from denbust.store.seen import SeenStore, create_seen_store

logger = logging.getLogger(__name__)

_OPERATIONAL_STORE_JOBS = {
    JobName.INGEST,
    JobName.SCRAPE_CANDIDATES,
    JobName.BACKFILL_SCRAPE,
    JobName.MONTHLY_REPORT,
    JobName.RELEASE,
    JobName.BACKUP,
}
_CLASSIFIER_WARNING_COUNT_KEYS = (
    "parse_failure_count",
    "invalid_taxonomy_pair_count",
    "invalid_legacy_pair_count",
    "relevant_without_usable_taxonomy_count",
    "double_wrapper_recovery_count",
)
_PARSE_FAILURE_SAMPLE_STRING_KEYS = {
    "shape_signature",
    "tail_shape_signature",
}
_PARSE_FAILURE_SAMPLE_INT_KEYS = {
    "response_length",
    "normalized_length",
    "line_count",
    "leading_brace_count",
    "trailing_brace_count",
    "brace_balance",
}
_PARSE_FAILURE_SAMPLE_OPTIONAL_INT_KEYS = {
    "json_error_position",
    "json_error_line",
    "json_error_column",
}
_PARSE_FAILURE_SAMPLE_BOOL_KEYS = {
    "starts_with_code_fence",
    "ends_with_code_fence",
    "starts_with_double_open_object",
    "ends_with_double_close_object",
    "outer_wrapper_candidate",
    "inner_object_candidate",
    "contains_balanced_inner_object",
    "inner_json_object_candidate",
}


def _sanitize_classifier_provider_error(error: ClassifierProviderError) -> str:
    """Return a compact provider error string safe for run artifacts."""
    return sanitize_provider_error_message(error)


def _mark_classifier_provider_error(
    result: RunSnapshot,
    error: ClassifierProviderError,
) -> str:
    """Mark a run fatal after a classifier provider failure."""
    sanitized = _sanitize_classifier_provider_error(error)
    result.fatal = True
    result.errors.append(f"classifier_provider_error={sanitized}")
    return sanitized


def _classifier_warning_counts(classifier: object) -> dict[str, int]:
    """Read passive classifier parser warning/recovery counters when exposed."""
    raw_counts = getattr(classifier, "warning_counts", None)
    if callable(raw_counts):
        raw_counts = raw_counts()
    if not isinstance(raw_counts, Mapping):
        return dict.fromkeys(_CLASSIFIER_WARNING_COUNT_KEYS, 0)
    counts: dict[str, int] = {}
    for key in _CLASSIFIER_WARNING_COUNT_KEYS:
        value = raw_counts.get(key, 0)
        counts[key] = value if isinstance(value, int) and not isinstance(value, bool) else 0
    return counts


def _classifier_parse_failure_diagnostics(classifier: object) -> dict[str, object]:
    """Read sanitized parse-failure shape diagnostics when exposed by the classifier."""
    raw_diagnostics = getattr(classifier, "parse_failure_diagnostics", None)
    if callable(raw_diagnostics):
        raw_diagnostics = raw_diagnostics()
    if not isinstance(raw_diagnostics, Mapping):
        return _normalize_classifier_parse_failure_diagnostics(None)
    return _normalize_classifier_parse_failure_diagnostics(raw_diagnostics)


def _reset_classifier_warning_counts(classifier: object) -> None:
    """Reset classifier parser warning counters when the classifier exposes a reset hook."""
    reset = getattr(classifier, "reset_warning_counts", None)
    if callable(reset):
        reset()


def release_publication_dir(config: Config) -> Path:
    """Return the publication directory that holds built release bundles."""
    if config.job_name == JobName.RELEASE:
        return config.state_paths.publication_dir
    return config.store.publication_dir or (
        config.store.state_root / config.dataset_name / JobName.RELEASE / "publication"
    )


# ---------------------------------------------------------------------------
# Pre-classification filter (prefilter) helpers
# ---------------------------------------------------------------------------


def _build_cascade_orchestrator(config: Config) -> CascadeOrchestrator | None:
    """Construct a :class:`CascadeOrchestrator` for *config*, or ``None``.

    Returns ``None`` when ``config.prefilter.enabled`` is ``False`` or the
    configured mode is ``OFF`` so callers can short-circuit filter wiring
    without the cost of loading stage model artifacts.
    """
    if not config.prefilter.enabled or config.prefilter.mode == PrefilterMode.OFF:
        return None
    prefilter_paths = resolve_prefilter_state_paths(
        state_root=config.store.state_root,
        dataset_name=config.dataset_name,
    )
    return CascadeOrchestrator(
        config=config.prefilter,
        decisions_dir=prefilter_paths.decisions_dir,
        models_dir=prefilter_paths.models_dir,
    )


def _thin_pass_prefilter(
    candidates: list[PersistentCandidate],
    orchestrator: CascadeOrchestrator | None,
) -> tuple[list[PersistentCandidate], list[PrefilterDecision]]:
    """Apply the thin (pre-scrape) prefilter pass to *candidates*.

    Returns ``(passed_candidates, all_decisions)``.  When *orchestrator* is
    ``None`` all candidates pass through unchanged and the decision list is
    empty.  In SHADOW mode the orchestrator downgrades ``"drop"`` to ``"pass"``
    internally, so all candidates still flow to the scrape queue — only the
    telemetry reflects the simulated drop.
    """
    if orchestrator is None:
        return candidates, []
    passed: list[PersistentCandidate] = []
    decisions: list[PrefilterDecision] = []
    for candidate in candidates:
        decision = orchestrator.evaluate_thin(PersistentCandidateView(candidate))
        decisions.append(decision)
        if decision.verdict != "drop":
            passed.append(candidate)
    if decisions:
        logger.info(
            "Prefilter thin pass: %d/%d candidates passed (mode=%s).",
            len(passed),
            len(candidates),
            orchestrator.mode.value,
        )
    return passed, decisions


def _thick_pass_prefilter(
    articles: list[RawArticle],
    orchestrator: CascadeOrchestrator | None,
    *,
    candidate_id_map: dict[str, str],
) -> tuple[list[RawArticle], list[PrefilterDecision]]:
    """Apply the thick (post-scrape, pre-classifier) prefilter pass to *articles*.

    Returns ``(passed_articles, all_decisions)``.  When *orchestrator* is
    ``None`` all articles pass through unchanged and the decision list is empty.

    Parameters
    ----------
    candidate_id_map:
        Mapping from canonicalized URL strings to ``candidate_id`` values built
        from the scrape batch's ``selected_candidates``.  When a URL has no
        entry the raw URL string is used as the decision ``candidate_id``.
    """
    if orchestrator is None:
        return articles, []
    passed: list[RawArticle] = []
    decisions: list[PrefilterDecision] = []
    for article in articles:
        url_key = canonicalize_news_url(str(article.url))
        candidate_id = candidate_id_map.get(url_key, str(article.url))
        view = RawArticleCandidateView(article, candidate_id=candidate_id)
        # TODO: pass body=article.body once RawArticle gains a full-body field
        # from the scraper.  Until then, body=None lets each stage fall back to
        # candidate.snippet, which is the same text as the thin pass already had.
        decision = orchestrator.evaluate_thick(view, body=None)
        decisions.append(decision)
        if decision.verdict != "drop":
            passed.append(article)
    if decisions:
        logger.info(
            "Prefilter thick pass: %d/%d articles passed (mode=%s).",
            len(passed),
            len(articles),
            orchestrator.mode.value,
        )
    return passed, decisions


class _PrefilterPassSummary(TypedDict):
    """Typed schema for one prefilter pass entry in ``prefilter_summary.json``."""

    evaluated: int
    passed: int
    dropped: int
    stage_stopped_counts: dict[str, int]


def _build_prefilter_pass_dict(decisions: list[PrefilterDecision]) -> _PrefilterPassSummary:
    """Build a typed per-pass summary from a list of :class:`PrefilterDecision` records.

    ``stage_stopped_counts`` counts decisions where ``stopped_at_stage``
    is a stage name (not ``"passed_all"``).  This captures both effective drops
    (ENFORCE mode) and shadow would-drops (SHADOW mode) per stage.
    """
    evaluated = len(decisions)
    dropped = sum(1 for d in decisions if d.verdict == "drop")
    stage_stopped: dict[str, int] = {}
    for d in decisions:
        if d.stopped_at_stage != "passed_all":
            stage = str(d.stopped_at_stage)
            stage_stopped[stage] = stage_stopped.get(stage, 0) + 1
    return _PrefilterPassSummary(
        evaluated=evaluated,
        passed=evaluated - dropped,
        dropped=dropped,
        stage_stopped_counts=stage_stopped,
    )


def _write_prefilter_run_summary(
    config: Config,
    *,
    thin_decisions: list[PrefilterDecision],
    thick_decisions: list[PrefilterDecision],
) -> None:
    """Write ``prefilter_summary.json`` to the prefilter reports directory.

    Silently skipped when the prefilter is disabled (both decision lists empty
    and ``config.prefilter.enabled`` is ``False``).  Errors during write are
    caught and logged so they never abort the surrounding pipeline job.
    """
    if not (config.prefilter.enabled or thin_decisions or thick_decisions):
        return
    try:
        prefilter_paths = resolve_prefilter_state_paths(
            state_root=config.store.state_root,
            dataset_name=config.dataset_name,
        )
        prefilter_paths.reports_dir.mkdir(parents=True, exist_ok=True)
        summary: dict[str, object] = {
            "schema_version": "prefilter.run_summary.v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "mode": config.prefilter.mode.value,
            "thin_pass": _build_prefilter_pass_dict(thin_decisions),
            "thick_pass": _build_prefilter_pass_dict(thick_decisions),
        }
        out_path = prefilter_paths.reports_dir / "prefilter_summary.json"
        out_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.debug("Prefilter run summary written to %s", out_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to write prefilter run summary: %s", exc)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )


def create_sources(config: Config) -> list[Source]:
    """Create source instances from config."""
    sources: list[Source] = []

    for source_cfg in config.sources:
        if not source_cfg.enabled:
            continue

        if source_cfg.type == SourceType.RSS:
            if source_cfg.url:
                if source_cfg.name == "ynet":
                    sources.append(YnetRSSSource(feed_url=source_cfg.url))
                else:
                    sources.append(RSSSource(source_name=source_cfg.name, feed_url=source_cfg.url))
            else:
                logger.warning("RSS source %s missing URL, skipping", source_cfg.name)

        elif source_cfg.type == SourceType.SCRAPER:
            if source_cfg.name == "mako":
                sources.append(create_mako_source(browser_config=config.browser))
            elif source_cfg.name == "maariv":
                sources.append(create_maariv_source())
            elif source_cfg.name == "ice":
                sources.append(create_ice_source())
            elif source_cfg.name == "haaretz":
                sources.append(create_haaretz_source(browser_config=config.browser))
            elif source_cfg.name == "walla":
                sources.append(create_walla_source())
            else:
                logger.warning("Unknown scraper source: %s", source_cfg.name)

    logger.info("Created %s news sources", len(sources))
    return sources


async def fetch_all_sources(
    sources: list[Source], days: int, keywords: list[str]
) -> tuple[list[RawArticle], list[str]]:
    """Fetch articles from all sources."""
    all_articles: list[RawArticle] = []
    errors: list[str] = []

    for source in sources:
        try:
            logger.info("Fetching from %s...", source.name)
            articles = await source.fetch(days=days, keywords=keywords)
            all_articles.extend(articles)
            logger.info("Found %s articles from %s", len(articles), source.name)
        except Exception as exc:
            logger.exception("Error fetching from %s: %s", source.name, exc)
            errors.append(f"{source.name}: {exc}")

    logger.info("Total raw articles: %s", len(all_articles))
    return all_articles, errors


def filter_seen(articles: list[RawArticle], seen_store: SeenStore) -> list[RawArticle]:
    """Filter out already-seen articles."""
    unseen: list[RawArticle] = []
    for article in articles:
        canonical = canonicalize_news_url(str(article.url))
        if seen_store.is_seen(canonical):
            logger.debug(
                "skip url=%s reason=seen source=%s",
                canonical,
                article.source_name,
            )
        else:
            unseen.append(article)
    logger.info("Filtered to %s unseen articles (was %s)", len(unseen), len(articles))
    return unseen


async def classify_articles(
    articles: list[RawArticle], classifier: Classifier
) -> list[ClassifiedArticle]:
    """Classify all articles for relevance."""
    classified = await classifier.classify_batch(articles)
    relevant = [
        classified_article
        for classified_article in classified
        if classified_article.classification.relevant
    ]
    logger.info("Classified %s articles, %s are relevant", len(articles), len(relevant))
    return relevant


def deduplicate_articles(
    articles: list[ClassifiedArticle], deduplicator: Deduplicator
) -> list[UnifiedItem]:
    """Deduplicate and unify articles."""
    items = deduplicator.deduplicate(articles)
    logger.info("Deduplicated to %s unique stories", len(items))
    return items


def _fallback_source_name(candidate: PersistentCandidate) -> str:
    metadata = candidate.metadata
    source_name = metadata.get("fallback_source_name")
    if isinstance(source_name, str) and source_name.strip():
        return source_name.strip()
    for value in [*candidate.source_hints, *candidate.discovered_via]:
        if value.strip():
            return value.strip()
    return candidate.domain or "candidate_fallback"


def _fallback_publication_datetime(candidate: PersistentCandidate) -> datetime | None:
    value = candidate.metadata.get("fallback_publication_datetime")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
    return None


def _fallback_publication_datetime_or_seen(candidate: PersistentCandidate) -> datetime:
    """Return a fallback publication datetime for legacy non-backfill paths."""
    return _fallback_publication_datetime(candidate) or candidate.last_seen_at


def _fallback_text(candidate: PersistentCandidate) -> tuple[str | None, str | None]:
    metadata = candidate.metadata
    title = metadata.get("fallback_title")
    if not isinstance(title, str) or not title.strip():
        title = next((item for item in candidate.titles if item.strip()), None)
    snippet = metadata.get("fallback_snippet")
    if not isinstance(snippet, str) or not snippet.strip():
        snippet = next((item for item in candidate.snippets if item.strip()), None)
    normalized_title = " ".join(title.split()).strip() if isinstance(title, str) else None
    normalized_snippet = " ".join(snippet.split()).strip() if isinstance(snippet, str) else None
    return normalized_title or None, normalized_snippet or None


def _fallback_input_article(
    candidate: PersistentCandidate,
    *,
    require_publication_datetime: bool = False,
) -> RawArticle | None:
    title, snippet = _fallback_text(candidate)
    if title is None and snippet is None:
        return None
    publication_datetime = _fallback_publication_datetime(candidate)
    if require_publication_datetime and publication_datetime is None:
        return None
    return RawArticle(
        url=candidate.canonical_url or candidate.current_url,
        title=title or snippet or str(candidate.current_url),
        snippet=snippet or title or "",
        date=publication_datetime or candidate.last_seen_at,
        source_name=_fallback_source_name(candidate),
    )


def _article_in_window(article: RawArticle, *, start: datetime, end: datetime) -> bool:
    article_date = article.date
    if article_date.tzinfo is None:
        article_date = article_date.replace(tzinfo=UTC)
    else:
        article_date = article_date.astimezone(UTC)
    return start <= article_date <= end


def _fallback_combined_privacy_input(item: UnifiedItem) -> str:
    return " ".join(
        segment
        for segment in (
            item.headline,
            item.summary,
            item.taxonomy_category_id or "",
            item.taxonomy_subcategory_id or "",
            item.category.value,
            item.sub_category.value if item.sub_category else "",
        )
        if segment
    )


def _build_fallback_unified_item(
    candidate: PersistentCandidate,
    article: RawArticle,
    classified_article: ClassifiedArticle,
) -> UnifiedItem:
    classification = classified_article.classification
    return UnifiedItem(
        headline=article.title,
        summary=article.snippet or article.title,
        sources=[SourceReference(source_name=article.source_name, url=article.url)],
        date=article.date,
        enforcement_related=classification.enforcement_related,
        index_relevant=classification.index_relevant,
        taxonomy_version=classification.taxonomy_version,
        taxonomy_category_id=classification.taxonomy_category_id,
        taxonomy_subcategory_id=classification.taxonomy_subcategory_id,
        category=classification.category,
        sub_category=classification.sub_category,
        canonical_url=candidate.canonical_url or candidate.current_url,
        primary_source_name=article.source_name,
    )


async def _build_fallback_operational_records(
    *,
    candidates: list[PersistentCandidate],
    classifier: Classifier,
    require_publication_datetime: bool = False,
    publication_window: tuple[datetime, datetime] | None = None,
) -> list[NewsItemOperationalRecord]:
    fallback_inputs: list[tuple[PersistentCandidate, RawArticle]] = []
    for candidate in candidates:
        article = _fallback_input_article(
            candidate,
            require_publication_datetime=require_publication_datetime,
        )
        if article is not None and publication_window is not None:
            start, end = publication_window
            if not _article_in_window(article, start=start, end=end):
                article = None
        if article is not None:
            fallback_inputs.append((candidate, article))
    if not fallback_inputs:
        return []

    classified = await classifier.classify_batch([article for _, article in fallback_inputs])
    if len(classified) != len(fallback_inputs):
        raise ValueError(
            "classifier.classify_batch() returned "
            f"{len(classified)} results for {len(fallback_inputs)} fallback inputs"
        )
    records: list[NewsItemOperationalRecord] = []
    retrieval_datetime = datetime.now(UTC)
    for (candidate, article), classified_article in zip(fallback_inputs, classified, strict=True):
        classification = classified_article.classification
        if not classification.relevant:
            continue
        item = _build_fallback_unified_item(candidate, article, classified_article)
        enrichment = fallback_enrichment(item)
        rule_risk, rule_reason = infer_privacy_risk(_fallback_combined_privacy_input(item))
        privacy_risk = merge_privacy_risk(enrichment.privacy_risk_level, rule_risk)
        if privacy_risk is not enrichment.privacy_risk_level:
            enrichment = enrichment.model_copy(
                update={
                    "privacy_risk_level": privacy_risk,
                    "privacy_reason": rule_reason or enrichment.privacy_reason,
                }
            )
        record = NewsItemOperationalRecord.from_unified_item(
            item,
            retrieval_datetime=retrieval_datetime,
            enrichment=enrichment,
            classification_confidence=classification.confidence,
            review_status=ReviewStatus.NEEDS_FACT_REVIEW,
            publication_status=PublicationStatus.INTERNAL_ONLY,
            privacy_reason=rule_reason or enrichment.privacy_reason,
        ).model_copy(
            update={
                "event_candidate_ids": [candidate.candidate_id],
                "content_basis": candidate.content_basis,
                "record_confidence": "low",
                "annotation_source": "candidate_fallback",
            }
        )
        records.append(record)
    return records


def _source_discovery_enabled_for_source(config: Config, source_name: str) -> bool:
    """Return whether source-native candidacy is enabled for a source."""
    source_config = config.source_discovery.sources.get(source_name)
    if source_config is None:
        return True
    return source_config.enabled


def _group_articles_by_source(articles: list[RawArticle]) -> dict[str, list[RawArticle]]:
    """Group fetched articles by source name in a single pass."""
    grouped: defaultdict[str, list[RawArticle]] = defaultdict(list)
    for article in articles:
        grouped[article.source_name].append(article)
    return dict(grouped)


def _write_discovery_diagnostic_artifacts(
    config: Config,
    *,
    config_path: Path | None,
    overlap_candidates: list[PersistentCandidate],
) -> None:
    """Persist the latest discovery diagnostics and overlap artifacts."""
    persisted_candidates_available = jsonl_io.state_file_exists(
        config.discovery_state_paths.latest_candidates_path
    )
    persisted_attempts_available = jsonl_io.state_file_exists(
        config.discovery_state_paths.scrape_attempts_path
    )
    if persisted_candidates_available or persisted_attempts_available:
        report = build_discovery_diagnostic_report(
            config=config,
            config_path=config_path,
            overlap_candidates_override=overlap_candidates,
            include_operational_matches=False,
        )
    else:
        report = build_discovery_diagnostic_report(
            config=config,
            config_path=config_path,
            candidates_override=overlap_candidates,
            overlap_candidates_override=overlap_candidates,
            include_operational_matches=False,
        )
    persist_discovery_diagnostic_artifacts(config=config, report=report)


async def _persist_source_native_candidates(
    *,
    config: Config,
    raw_articles: list[RawArticle],
    run_id: str,
) -> PersistedSourceDiscovery:
    """Persist source-native candidates derived from fetched source articles."""
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=0,
    )
    persistence = create_discovery_persistence(config)
    try:
        discovered_candidates = [
            raw_article_to_discovered_candidate(article)
            for article in raw_articles
            if _source_discovery_enabled_for_source(config, article.source_name)
        ]
        return persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=discovered_candidates,
            persistence=persistence,
        )
    finally:
        persistence.close()


def _tag_backfill_discovered_candidates(
    discovered_candidates: list[DiscoveredCandidate],
    *,
    batch_id: str,
    window: BackfillWindow,
) -> list[DiscoveredCandidate]:
    """Attach backfill metadata to discovered candidates before persistence."""
    metadata = backfill_metadata(batch_id=batch_id, window=window)
    return [
        candidate.model_copy(update={"metadata": {**candidate.metadata, **metadata}})
        for candidate in discovered_candidates
    ]


def _batch_candidate_counts(
    persistence: DiscoveryPersistence,
    *,
    batch_id: str,
) -> tuple[int, int]:
    """Return merged-candidate and queued-for-scrape counts for one backfill batch."""
    counts = persistence.count_backfill_batch_candidates(
        batch_id=batch_id,
        scrapeable_statuses=SCRAPEABLE_CANDIDATE_STATUSES,
    )
    return counts.merged_candidate_count, counts.queued_for_scrape_count


def _update_backfill_batch_state(
    persistence: DiscoveryPersistence,
    *,
    batch: BackfillBatch,
    status: BackfillBatchStatus,
    query_count: int | None = None,
    candidate_count: int | None = None,
    scrape_attempt_count: int | None = None,
    scraped_candidate_count: int | None = None,
    warnings: list[str] | None = None,
    errors: list[str] | None = None,
    finished: bool | None = None,
) -> BackfillBatch:
    """Persist one batch state transition with refreshed aggregate counts."""
    merged_candidate_count, queued_for_scrape_count = _batch_candidate_counts(
        persistence,
        batch_id=batch.batch_id,
    )
    if finished is True:
        finished_at = datetime.now(UTC)
    elif finished is False:
        finished_at = None
    else:
        finished_at = batch.finished_at
    updated = batch.model_copy(
        update={
            "updated_at": datetime.now(UTC),
            "status": status,
            "query_count": query_count if query_count is not None else batch.query_count,
            "candidate_count": (
                candidate_count if candidate_count is not None else batch.candidate_count
            ),
            "merged_candidate_count": merged_candidate_count,
            "queued_for_scrape_count": queued_for_scrape_count,
            "scrape_attempt_count": (
                scrape_attempt_count
                if scrape_attempt_count is not None
                else batch.scrape_attempt_count
            ),
            "scraped_candidate_count": (
                scraped_candidate_count
                if scraped_candidate_count is not None
                else batch.scraped_candidate_count
            ),
            "warnings": warnings if warnings is not None else batch.warnings,
            "errors": errors if errors is not None else batch.errors,
            "finished_at": finished_at,
        }
    )
    persistence.upsert_backfill_batches([updated])
    return updated


async def _run_source_native_backfill_discovery(
    *,
    config: Config,
    sources: list[Source],
    run_id: str,
    window: BackfillWindow,
    batch_id: str,
) -> PersistedSourceDiscovery:
    """Fetch source-native historical candidates when the source supports explicit windows."""
    enabled_sources = [
        source for source in sources if _source_discovery_enabled_for_source(config, source.name)
    ]
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(enabled_sources),
    )
    context = SourceDiscoveryContext(
        run_id=run_id,
        source_names=[source.name for source in enabled_sources],
        keywords=config.keywords,
        date_from=window.date_from,
        date_to=window.date_to,
    )
    persistence = create_discovery_persistence(config)
    warnings: list[str] = []
    try:
        discovered_candidates = []
        errors: list[str] = []
        for source in enabled_sources:
            adapter = SourceDiscoveryAdapter(source)
            if not adapter.supports_historical_window:
                warnings.append(f"{source.name}: historical window discovery is unsupported")
                continue
            try:
                discovered_candidates.extend(
                    _tag_backfill_discovered_candidates(
                        await adapter.discover_candidates_for_window(context),
                        batch_id=batch_id,
                        window=window,
                    )
                )
            except Exception as exc:
                logger.exception(
                    "Error discovering backfill candidates from %s: %s", source.name, exc
                )
                errors.append(f"{source.name}: {exc}")

        discovery_run.errors = errors
        persisted = persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=discovered_candidates,
            persistence=persistence,
        )
        persisted.warnings = warnings
        return persisted
    finally:
        persistence.close()


async def _run_source_native_discovery(
    *,
    config: Config,
    sources: list[Source],
    run_id: str,
    days: int,
) -> PersistedSourceDiscovery:
    """Fetch source-native candidates and persist them to the durable candidate layer."""
    enabled_sources = [
        source for source in sources if _source_discovery_enabled_for_source(config, source.name)
    ]
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(enabled_sources),
    )
    context = SourceDiscoveryContext(
        run_id=run_id,
        source_names=[source.name for source in enabled_sources],
        days=days,
        keywords=config.keywords,
    )
    persistence = create_discovery_persistence(config)
    try:
        discovered_candidates = []
        errors: list[str] = []
        for source in enabled_sources:
            adapter = SourceDiscoveryAdapter(source)
            try:
                discovered_candidates.extend(await adapter.discover_candidates(context))
            except Exception as exc:
                logger.exception("Error discovering candidates from %s: %s", source.name, exc)
                errors.append(f"{source.name}: {exc}")

        discovery_run.errors = errors
        persisted = persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=discovered_candidates,
            persistence=persistence,
        )
        return persisted
    finally:
        persistence.close()


# Flush accumulated search-engine candidates to the store after this many
# queries.  Keeps memory bounded and ensures progress is durable even if the
# process is killed mid-run.
_DISCOVERY_PERSIST_BATCH: int = 50


def _query_yield_callback(config: Config) -> Callable[[DiscoveryQuery], int] | None:
    """Return a yield lookup from the cached query-yield map, or None if empty."""
    yield_map = QueryYieldStore(config.discovery_state_paths.query_yield_path).load()
    if not yield_map:
        return None
    return lambda query: yield_map.get(query.query_text, 0)


def _guard_search_budget(
    config: Config,
    *,
    engine: str,
    queries: list[DiscoveryQuery],
    cache_dir: Path,
    yield_of: Callable[[DiscoveryQuery], int] | None = None,
) -> list[DiscoveryQuery]:
    """Cap *queries* to what the engine's remaining monthly budget can afford.

    No-op when the engine has no free allowance and no budget. Otherwise spends
    the free monthly allowance first, then the paid budget, keeping the best
    queries that fit (highest yield, then kind priority, then least-recently-run
    rotation) so the spend goes to the most productive queries.
    """
    engine_cfg = getattr(config.discovery.engines, engine, None)
    budget = getattr(engine_cfg, "monthly_budget_usd", None)
    free = getattr(engine_cfg, "monthly_free_queries", 0)
    if (budget is None and free <= 0) or not queries:
        return queries
    ledger = SearchBudgetLedger(config.discovery_state_paths.search_budget_path)
    now = datetime.now(UTC)
    spent_queries, _ = ledger.month_spend(year_month=now.strftime("%Y-%m"), engine=engine)
    affordable = affordable_query_count(
        engine=engine,
        requested=len(queries),
        queries_spent=spent_queries,
        monthly_budget_usd=budget,
        monthly_free_queries=free,
    )
    if affordable < len(queries):
        logger.warning(
            "%s budget guard: capping %d -> %d queries (%d used this month; free=%d, budget=%s).",
            engine,
            len(queries),
            affordable,
            spent_queries,
            free,
            f"${budget:.2f}" if budget is not None else "none",
        )
        queries = select_run_queries(
            queries,
            affordable,
            last_run_at=lambda query: query_last_run_at(cache_dir, engine, query),
            yield_of=yield_of,
        )
    return queries


def _record_search_spend(config: Config, *, engine: str, run_id: str, live_queries: int) -> None:
    """Record the live (non-cached) search requests this run issued on *engine*."""
    if live_queries <= 0:
        return
    SearchBudgetLedger(config.discovery_state_paths.search_budget_path).record(
        engine=engine, queries=live_queries, run_id=run_id, now=datetime.now(UTC)
    )


async def _run_brave_discovery(
    *,
    config: Config,
    run_id: str,
    days: int,
) -> PersistedSourceDiscovery:
    """Run Brave-powered discovery with per-query checkpointing and incremental persist."""
    cache_dir = config.discovery_state_paths.engine_query_cache_dir
    yield_of = _query_yield_callback(config)
    queries = _guard_search_budget(
        config,
        engine="brave",
        queries=build_discovery_queries(
            config,
            days=days,
            last_run_at=lambda q: query_last_run_at(cache_dir, "brave", q),
            yield_of=yield_of,
        ),
        cache_dir=cache_dir,
        yield_of=yield_of,
    )
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(queries),
    )
    persistence = create_discovery_persistence(config)
    if not queries:
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    brave_api_key = config.brave_search_api_key
    if not brave_api_key:
        discovery_run.errors.append("brave: missing DENBUST_BRAVE_SEARCH_API_KEY")
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    engine = BraveSearchEngine(
        api_key=brave_api_key,
        max_results_per_query=config.discovery.engines.brave.max_results_per_query,
    )
    context = DiscoveryContext(
        run_id=run_id,
        max_results_per_query=config.discovery.engines.brave.max_results_per_query,
        metadata={"days": days, "engine": "brave"},
    )
    live_queries = 0
    try:
        batch: list[DiscoveredCandidate] = []
        last_result: PersistedSourceDiscovery | None = None
        for i, query in enumerate(queries):
            is_last = i == len(queries) - 1
            cp = _engine_cache_path(cache_dir, "brave", query)
            cached = load_cached_candidates(cp)
            if cached is not None:
                batch.extend(cached)
            else:
                live_queries += 1
                try:
                    fresh = await engine.discover([query], context)
                except Exception as exc:
                    discovery_run.errors.append(f"brave: {type(exc).__name__}: {exc}")
                    fresh = []
                save_cached_candidates(cp, fresh)
                batch.extend(fresh)
            if len(batch) >= _DISCOVERY_PERSIST_BATCH or is_last:
                last_result = persist_discovered_candidates(
                    run=discovery_run,
                    discovered_candidates=batch,
                    persistence=persistence,
                    finalize=is_last,
                )
                batch = []
        return last_result or persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=[],
            persistence=persistence,
        )
    finally:
        await engine.aclose()
        persistence.close()
        _record_search_spend(config, engine="brave", run_id=run_id, live_queries=live_queries)


async def _run_exa_discovery(
    *,
    config: Config,
    run_id: str,
    days: int,
) -> PersistedSourceDiscovery:
    """Run Exa-powered discovery with per-query checkpointing and incremental persist."""
    cache_dir = config.discovery_state_paths.engine_query_cache_dir
    yield_of = _query_yield_callback(config)
    queries = _guard_search_budget(
        config,
        engine="exa",
        queries=build_discovery_queries(
            config,
            days=days,
            last_run_at=lambda q: query_last_run_at(cache_dir, "exa", q),
            yield_of=yield_of,
        ),
        cache_dir=cache_dir,
        yield_of=yield_of,
    )
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(queries),
    )
    persistence = create_discovery_persistence(config)
    if not queries:
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    exa_api_key = config.exa_api_key
    if not exa_api_key:
        discovery_run.errors.append("exa: missing DENBUST_EXA_API_KEY")
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    engine = ExaSearchEngine(
        api_key=exa_api_key,
        max_results_per_query=config.discovery.engines.exa.max_results_per_query,
    )
    context = DiscoveryContext(
        run_id=run_id,
        max_results_per_query=config.discovery.engines.exa.max_results_per_query,
        metadata={
            "days": days,
            "engine": "exa",
            "allow_find_similar": config.discovery.engines.exa.allow_find_similar,
        },
    )
    live_queries = 0
    try:
        batch: list[DiscoveredCandidate] = []
        last_result: PersistedSourceDiscovery | None = None
        for i, query in enumerate(queries):
            is_last = i == len(queries) - 1
            cp = _engine_cache_path(cache_dir, "exa", query)
            cached = load_cached_candidates(cp)
            if cached is not None:
                batch.extend(cached)
            else:
                live_queries += 1
                try:
                    fresh = await engine.discover([query], context)
                except Exception as exc:
                    discovery_run.errors.append(f"exa: {type(exc).__name__}: {exc}")
                    fresh = []
                save_cached_candidates(cp, fresh)
                batch.extend(fresh)
            if len(batch) >= _DISCOVERY_PERSIST_BATCH or is_last:
                last_result = persist_discovered_candidates(
                    run=discovery_run,
                    discovered_candidates=batch,
                    persistence=persistence,
                    finalize=is_last,
                )
                batch = []
        return last_result or persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=[],
            persistence=persistence,
        )
    finally:
        await engine.aclose()
        persistence.close()
        _record_search_spend(config, engine="exa", run_id=run_id, live_queries=live_queries)


async def _run_google_cse_discovery(
    *,
    config: Config,
    run_id: str,
    days: int,
) -> PersistedSourceDiscovery:
    """Run Google CSE-powered discovery with per-query checkpointing and incremental persist."""
    cache_dir = config.discovery_state_paths.engine_query_cache_dir
    yield_of = _query_yield_callback(config)
    queries = _guard_search_budget(
        config,
        engine="google_cse",
        queries=build_discovery_queries(
            config,
            days=days,
            last_run_at=lambda q: query_last_run_at(cache_dir, "google_cse", q),
            yield_of=yield_of,
        ),
        cache_dir=cache_dir,
        yield_of=yield_of,
    )
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(queries),
    )
    persistence = create_discovery_persistence(config)
    if not queries:
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    google_api_key = config.google_cse_api_key
    google_cse_id = config.google_cse_id
    if not google_api_key:
        discovery_run.errors.append("google_cse: missing DENBUST_GOOGLE_CSE_API_KEY")
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()
    if not google_cse_id:
        discovery_run.errors.append("google_cse: missing DENBUST_GOOGLE_CSE_ID")
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    engine = GoogleCseSearchEngine(
        api_key=google_api_key,
        cse_id=google_cse_id,
        max_results_per_query=config.discovery.engines.google_cse.max_results_per_query,
    )
    context = DiscoveryContext(
        run_id=run_id,
        max_results_per_query=config.discovery.engines.google_cse.max_results_per_query,
        metadata={"days": days, "engine": "google_cse"},
    )
    live_queries = 0
    try:
        batch: list[DiscoveredCandidate] = []
        last_result: PersistedSourceDiscovery | None = None
        for i, query in enumerate(queries):
            is_last = i == len(queries) - 1
            cp = _engine_cache_path(cache_dir, "google_cse", query)
            cached = load_cached_candidates(cp)
            if cached is not None:
                batch.extend(cached)
            else:
                live_queries += 1
                try:
                    fresh = await engine.discover([query], context)
                except Exception as exc:
                    discovery_run.errors.append(f"google_cse: {type(exc).__name__}: {exc}")
                    fresh = []
                save_cached_candidates(cp, fresh)
                batch.extend(fresh)
            if len(batch) >= _DISCOVERY_PERSIST_BATCH or is_last:
                last_result = persist_discovered_candidates(
                    run=discovery_run,
                    discovered_candidates=batch,
                    persistence=persistence,
                    finalize=is_last,
                )
                batch = []
        return last_result or persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=[],
            persistence=persistence,
        )
    finally:
        await engine.aclose()
        persistence.close()
        _record_search_spend(config, engine="google_cse", run_id=run_id, live_queries=live_queries)


def _backfill_query_execution_key(engine: str, query: DiscoveryQuery) -> tuple[str, ...]:
    return (
        engine,
        query.query_kind.value,
        query.query_text,
        query.source_hint or "",
        query.date_from.isoformat() if query.date_from is not None else "",
        query.date_to.isoformat() if query.date_to is not None else "",
    )


async def _run_backfill_engine_discovery(
    *,
    config: Config,
    run_id: str,
    batch_id: str,
    window: BackfillWindow,
    engine_name: str,
) -> PersistedSourceDiscovery:
    """Run one search engine over a historical window and persist tagged candidates."""
    all_queries = build_backfill_queries(config, window=window)
    persistence = create_discovery_persistence(config)
    executed_keys = persistence.load_executed_backfill_query_keys()
    queries = [
        q for q in all_queries if _backfill_query_execution_key(engine_name, q) not in executed_keys
    ]
    skipped_count = len(all_queries) - len(queries)
    if skipped_count:
        logger.info(
            "backfill engine=%s window=%d: %d/%d queries skipped (already executed), running %d",
            engine_name,
            window.index,
            skipped_count,
            len(all_queries),
            len(queries),
        )
    discovery_run = DiscoveryRun(
        run_id=run_id,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        status=DiscoveryRunStatus.RUNNING,
        query_count=len(queries),
        skipped_query_count=skipped_count,
    )
    if not queries:
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    engine: Any | None = None
    context: DiscoveryContext | None = None
    missing_error: str | None = None
    if engine_name == "brave":
        if not config.brave_search_api_key:
            env_name = config.discovery.engines.brave.api_key_env or "DENBUST_BRAVE_SEARCH_API_KEY"
            missing_error = f"brave: missing {env_name}"
        else:
            engine = BraveSearchEngine(
                api_key=config.brave_search_api_key,
                max_results_per_query=config.discovery.engines.brave.max_results_per_query,
            )
            context = DiscoveryContext(
                run_id=run_id,
                max_results_per_query=config.discovery.engines.brave.max_results_per_query,
                metadata={"engine": "brave", **backfill_metadata(batch_id=batch_id, window=window)},
            )
    elif engine_name == "exa":
        if not config.exa_api_key:
            env_name = config.discovery.engines.exa.api_key_env or "DENBUST_EXA_API_KEY"
            missing_error = f"exa: missing {env_name}"
        else:
            engine = ExaSearchEngine(
                api_key=config.exa_api_key,
                max_results_per_query=config.discovery.engines.exa.max_results_per_query,
            )
            context = DiscoveryContext(
                run_id=run_id,
                max_results_per_query=config.discovery.engines.exa.max_results_per_query,
                metadata={
                    "engine": "exa",
                    "allow_find_similar": config.discovery.engines.exa.allow_find_similar,
                    **backfill_metadata(batch_id=batch_id, window=window),
                },
            )
    elif engine_name == "google_cse":
        if not config.google_cse_api_key:
            env_name = (
                config.discovery.engines.google_cse.api_key_env or "DENBUST_GOOGLE_CSE_API_KEY"
            )
            missing_error = f"google_cse: missing {env_name}"
        elif not config.google_cse_id:
            env_name = config.discovery.engines.google_cse.cse_id_env or "DENBUST_GOOGLE_CSE_ID"
            missing_error = f"google_cse: missing {env_name}"
        else:
            engine = GoogleCseSearchEngine(
                api_key=config.google_cse_api_key,
                cse_id=config.google_cse_id,
                max_results_per_query=config.discovery.engines.google_cse.max_results_per_query,
            )
            context = DiscoveryContext(
                run_id=run_id,
                max_results_per_query=config.discovery.engines.google_cse.max_results_per_query,
                metadata={
                    "engine": "google_cse",
                    **backfill_metadata(batch_id=batch_id, window=window),
                },
            )
    else:
        raise ValueError(f"Unsupported backfill engine: {engine_name}")

    if missing_error is not None:
        discovery_run.errors.append(missing_error)
        try:
            return persist_discovered_candidates(
                run=discovery_run,
                discovered_candidates=[],
                persistence=persistence,
            )
        finally:
            persistence.close()

    assert engine is not None
    assert context is not None
    try:
        try:
            discovered_candidates = _tag_backfill_discovered_candidates(
                await engine.discover(queries, context=context),
                batch_id=batch_id,
                window=window,
            )
            executed_at = datetime.now(UTC)
            persistence.append_executed_backfill_queries(
                [
                    ExecutedBackfillQuery(
                        engine=engine_name,
                        query_kind=q.query_kind,
                        query_text=q.query_text,
                        source_hint=q.source_hint,
                        date_from=q.date_from or window.date_from,
                        date_to=q.date_to or window.date_to,
                        executed_at=executed_at,
                        batch_id=batch_id,
                    )
                    for q in queries
                ]
            )
        except Exception as exc:
            discovery_run.errors.append(f"{engine_name}: {type(exc).__name__}: {exc}")
            discovered_candidates = []
        return persist_discovered_candidates(
            run=discovery_run,
            discovered_candidates=discovered_candidates,
            persistence=persistence,
        )
    finally:
        await engine.aclose()
        persistence.close()


async def _scrape_candidate_batch(
    *,
    config: Config,
    candidates: list[PersistentCandidate],
    sources: list[Source],
    preloaded_source_articles: dict[str, list[RawArticle]] | None = None,
    backfill_mode: bool = False,
) -> CandidateScrapeBatch:
    """Materialize durable candidates into raw articles and record scrape attempts."""
    persistence = create_discovery_persistence(config)
    try:
        return await scrape_candidates(
            config=config,
            persistence=persistence,
            candidates=candidates,
            sources=sources,
            preloaded_source_articles=preloaded_source_articles,
            backfill_mode=backfill_mode,
        )
    finally:
        persistence.close()


async def _run_candidate_scrape_job(
    *,
    config: Config,
    sources: list[Source],
    limit: int,
    orchestrator: CascadeOrchestrator | None = None,
    pub_date_from: datetime | None = None,
    balanced_batch_size: int | None = None,
    min_domain_frequency: int | None = None,
    use_domain_verdicts: bool = False,
) -> tuple[CandidateScrapeBatch, list[PrefilterDecision]]:
    """Select queued candidates, apply the thin prefilter, and run the scrape-attempt layer.

    Returns ``(scrape_batch, thin_decisions)``.  When *orchestrator* is ``None``
    the thin pass is skipped and ``thin_decisions`` is empty.  When
    *pub_date_from* is set only candidates published on or after that date are
    considered (targeted / recent-only scrape).  When *balanced_batch_size* is
    set the selection is a month-frequency-weighted, source-balanced batch of
    that size drawn from the full prefilter-passing pool (instead of the
    priority-ordered ``limit`` head).  When *min_domain_frequency* is set,
    candidates on a domain seen fewer than that many times across the store are
    held back (the domain-frequency gate); curated known outlets are exempt.
    """
    persistence = create_discovery_persistence(config)
    try:
        if balanced_batch_size is not None:
            pool = persistence.list_candidates(statuses=SCRAPEABLE_CANDIDATE_STATUSES)
            eligible = order_scrape_eligible_candidates(pool)
            if pub_date_from is not None:
                cutoff_month = pub_date_from.strftime("%Y-%m")
                eligible = [
                    candidate
                    for candidate in eligible
                    if (month := candidate_month(candidate)) is not None and month >= cutoff_month
                ]
            passed_pool, thin_decisions = _thin_pass_prefilter(eligible, orchestrator)
            if min_domain_frequency is not None and min_domain_frequency > 1:
                frequencies = domain_frequencies(persistence.list_candidates())
                before = len(passed_pool)
                passed_pool = filter_by_domain_frequency(
                    passed_pool,
                    min_frequency=min_domain_frequency,
                    frequencies=frequencies,
                )
                logger.info(
                    "Domain-frequency gate (min=%d): %d/%d candidates passed.",
                    min_domain_frequency,
                    len(passed_pool),
                    before,
                )
            if use_domain_verdicts:
                verdict_store = DomainVerdictStore(
                    config.discovery_state_paths.domain_verdicts_path
                )
                verdicts = verdict_store.load()
                before = len(passed_pool)
                passed_pool = filter_by_domain_verdict(passed_pool, verdicts=verdicts)
                logger.info(
                    "Domain-verdict gate: %d/%d candidates passed (%d cached verdicts).",
                    len(passed_pool),
                    before,
                    len(verdicts),
                )
            passed_candidates = plan_balanced_scrape_batch(
                passed_pool,
                batch_size=balanced_batch_size,
            )
        else:
            selected_candidates = select_candidates_for_scrape(
                persistence,
                limit=limit,
                pub_date_from=pub_date_from,
            )
            passed_candidates, thin_decisions = _thin_pass_prefilter(
                selected_candidates, orchestrator
            )
    finally:
        persistence.close()

    batch = await _scrape_candidate_batch(
        config=config,
        candidates=passed_candidates,
        sources=sources,
    )
    return batch, thin_decisions


async def _run_backfill_candidate_scrape_job(
    *,
    config: Config,
    sources: list[Source],
    limit: int,
    batch_id: str | None = None,
    orchestrator: CascadeOrchestrator | None = None,
) -> tuple[CandidateScrapeBatch, list[PrefilterDecision]]:
    """Select one historical batch, apply the thin prefilter, and run the scrape-attempt layer.

    Returns ``(scrape_batch, thin_decisions)``.  When *orchestrator* is ``None``
    the thin pass is skipped and ``thin_decisions`` is empty.
    """
    persistence = create_discovery_persistence(config)
    try:
        selected_candidates = select_backfill_candidates_for_scrape(
            persistence,
            limit=limit,
            batch_id=batch_id,
        )
    finally:
        persistence.close()

    passed_candidates, thin_decisions = _thin_pass_prefilter(selected_candidates, orchestrator)
    batch = await _scrape_candidate_batch(
        config=config,
        candidates=passed_candidates,
        sources=sources,
        backfill_mode=True,
    )
    return batch, thin_decisions


async def _process_ingest_articles(
    *,
    config: Config,
    result: RunSnapshot,
    source_names: list[str],
    sources: list[Source],
    all_articles: list[RawArticle],
    seen_store: SeenStore,
    classifier: Classifier,
    deduplicator: Deduplicator,
    operational_store: OperationalStore | None,
) -> RunSnapshot:
    """Run the classifier/dedup/store portion of ingest over fetched articles."""
    unseen_articles: list[RawArticle] = []
    classified_articles: list[ClassifiedArticle] = []
    unified_items: list[UnifiedItem] = []

    if not all_articles:
        logger.info("No articles found from any source")
        result.seen_count_after = seen_store.count
        result.finish("no articles found")
        result.set_debug_payload(
            _build_ingest_debug_payload(
                result=result,
                sources=sources,
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
                classifier_warning_counts=_classifier_warning_counts(classifier),
                classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(
                    classifier
                ),
            )
        )
        return result

    unseen_articles = filter_seen(all_articles, seen_store)
    result.unseen_article_count = len(unseen_articles)
    if not unseen_articles:
        logger.info("All articles were already seen")
        result.seen_count_after = seen_store.count
        result.finish("all fetched articles were already seen")
        result.set_debug_payload(
            _build_ingest_debug_payload(
                result=result,
                sources=sources,
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
                classifier_warning_counts=_classifier_warning_counts(classifier),
                classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(
                    classifier
                ),
            )
        )
        return result

    if len(unseen_articles) > config.max_articles:
        warning = (
            f"Article count ({len(unseen_articles)}) exceeds max_articles threshold "
            f"({config.max_articles}). Consider adding a pre-filter stage or reducing "
            f"the number of days/sources. Proceeding with classification anyway."
        )
        logger.warning(warning)
        result.warnings.append(warning)

    try:
        classified_articles = await classifier.classify_batch(unseen_articles)
    except ClassifierProviderError as exc:
        sanitized_error = _mark_classifier_provider_error(result, exc)
        logger.error("Classifier provider failed during ingest: %s", sanitized_error)
        result.seen_count_after = seen_store.count
        result.finish("fatal: classifier provider error")
        result.set_debug_payload(
            _build_ingest_debug_payload(
                result=result,
                sources=sources,
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
                classifier_error=sanitized_error,
                classifier_warning_counts=_classifier_warning_counts(classifier),
                classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(
                    classifier
                ),
            )
        )
        return result
    relevant_articles = [
        article for article in classified_articles if article.classification.relevant
    ]
    result.relevant_article_count = len(relevant_articles)
    if not relevant_articles:
        logger.info("No relevant articles found")
        result.seen_count_after = seen_store.count
        result.set_debug_payload(
            _build_ingest_debug_payload(
                result=result.finish("no relevant articles found"),
                sources=sources,
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
                classifier_warning_counts=_classifier_warning_counts(classifier),
                classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(
                    classifier
                ),
            )
        )
        return result

    unified_items = deduplicate_articles(relevant_articles, deduplicator)
    result.unified_item_count = len(unified_items)
    result.items = unified_items

    store = operational_store or create_operational_store(config)
    records = await build_operational_records(
        unified_items,
        config=config,
        operational_store=store,
    )
    store.upsert_records(
        config.dataset_name.value,
        [record.model_dump(mode="json") for record in records],
    )
    privacy_counts = summarize_privacy_mix(records)
    if privacy_counts:
        risk_summary = ", ".join(
            f"{risk.value}:{count}"
            for risk, count in sorted(privacy_counts.items(), key=lambda item: item[0].value)
        )
        result.warnings.append(f"privacy_risk_distribution={risk_summary}")

    mark_seen(unified_items, seen_store)
    result.seen_count_after = seen_store.count
    result.finish(f"ingest completed with {len(unified_items)} unified item(s)")
    result.set_debug_payload(
        _build_ingest_debug_payload(
            result=result,
            sources=sources,
            source_names=source_names,
            raw_articles=all_articles,
            unseen_articles=unseen_articles,
            classified_articles=classified_articles,
            unified_items=unified_items,
            classifier_warning_counts=_classifier_warning_counts(classifier),
            classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(classifier),
        )
    )
    return result


def mark_seen(items: list[UnifiedItem], seen_store: SeenStore) -> None:
    """Mark all URLs in unified items as seen."""
    urls: list[str] = []
    for item in items:
        urls.extend(canonicalize_news_url(str(source.url)) for source in item.sources)

    seen_store.mark_seen(urls)
    seen_store.save()
    logger.info("Marked %s URLs as seen", len(urls))


def _build_run_snapshot(
    config: Config,
    *,
    config_path: Path | None,
    days: int,
) -> RunSnapshot:
    """Create a generalized run snapshot scaffold for a job invocation."""
    return RunSnapshot(
        run_timestamp=datetime.now(UTC),
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        config_name=config.name,
        config_path=str(config_path) if config_path is not None else None,
        days_searched=days,
        output_formats=[output_format.value for output_format in config.output.formats],
    )


def _serialize_raw_article(article: RawArticle) -> dict[str, object]:
    """Render a raw fetched article for internal debug logs."""
    return {
        "source_name": article.source_name,
        "url": str(article.url),
        "canonical_url": canonicalize_news_url(str(article.url)),
        "title": article.title,
        "snippet": article.snippet,
        "publication_datetime": article.date.isoformat(),
    }


def _serialize_classified_article(article: ClassifiedArticle) -> dict[str, object]:
    """Render a classified article for internal debug logs."""
    return {
        **_serialize_raw_article(article.article),
        "relevant": article.classification.relevant,
        "enforcement_related": article.classification.enforcement_related,
        "index_relevant": article.classification.index_relevant,
        "taxonomy_version": article.classification.taxonomy_version,
        "taxonomy_category_id": article.classification.taxonomy_category_id,
        "taxonomy_subcategory_id": article.classification.taxonomy_subcategory_id,
        "category": article.classification.category.value,
        "sub_category": (
            article.classification.sub_category.value
            if article.classification.sub_category is not None
            else None
        ),
        "confidence": article.classification.confidence,
    }


def _serialize_unified_item(item: UnifiedItem) -> dict[str, object]:
    """Render a unified item for internal debug logs."""
    return {
        "headline": item.headline,
        "summary": item.summary,
        "enforcement_related": item.enforcement_related,
        "index_relevant": item.index_relevant,
        "taxonomy_version": item.taxonomy_version,
        "taxonomy_category_id": item.taxonomy_category_id,
        "taxonomy_subcategory_id": item.taxonomy_subcategory_id,
        "category": item.category.value,
        "sub_category": item.sub_category.value if item.sub_category is not None else None,
        "canonical_url": str(item.canonical_url) if item.canonical_url is not None else None,
        "primary_source_name": item.primary_source_name,
        "publication_datetime": item.date.isoformat(),
        "sources": [
            {"source_name": source.source_name, "url": str(source.url)} for source in item.sources
        ],
    }


def _source_name_from_error(error: str) -> str | None:
    """Extract a source name from the standard `source: message` error format."""
    source_name, separator, _message = error.partition(":")
    if not separator:
        return None
    source_name = source_name.strip()
    return source_name or None


def _build_source_summaries(
    *,
    sources: list[Source],
    source_names: list[str],
    raw_articles: list[RawArticle],
    errors: list[str],
) -> list[dict[str, object]]:
    """Summarize per-source outcomes for machine-readable diagnostics."""
    article_counts: dict[str, int] = {}
    for article in raw_articles:
        article_counts[article.source_name] = article_counts.get(article.source_name, 0) + 1

    error_map: dict[str, list[str]] = {}
    for error in errors:
        source_name = _source_name_from_error(error)
        if source_name is None:
            continue
        error_map.setdefault(source_name, []).append(error)

    source_debug: dict[str, object] = {}
    for source in sources:
        get_debug_state = getattr(source, "get_debug_state", None)
        if get_debug_state is None:
            continue
        debug_state = get_debug_state()
        if debug_state is not None:
            source_debug[source.name] = debug_state

    return [
        {
            "source_name": source_name,
            "raw_article_count": article_counts.get(source_name, 0),
            "had_error": source_name in error_map,
            "error_messages": error_map.get(source_name, []),
            "returned_zero_results": article_counts.get(source_name, 0) == 0
            and source_name not in error_map,
            "runtime_debug": source_debug.get(source_name),
        }
        for source_name in source_names
    ]


def _build_classifier_summary(
    *,
    unseen_articles: list[RawArticle],
    classified_articles: list[ClassifiedArticle],
    classifier_error: str | None = None,
    classifier_warning_counts: Mapping[str, int] | None = None,
    classifier_parse_failure_diagnostics: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Summarize classifier outputs and anomalies."""
    rejected_by_category: dict[str, int] = {}
    relevant_count = 0
    rejected_count = 0
    for article in classified_articles:
        if article.classification.relevant:
            relevant_count += 1
            continue
        rejected_count += 1
        category = article.classification.category.value
        rejected_by_category[category] = rejected_by_category.get(category, 0) + 1

    return {
        "unseen_article_count": len(unseen_articles),
        "classified_article_count": len(classified_articles),
        "relevant_article_count": relevant_count,
        "rejected_article_count": rejected_count,
        "rejected_by_category": rejected_by_category,
        "warning_counts": _normalize_classifier_warning_counts(classifier_warning_counts),
        "parse_failure_diagnostics": _normalize_classifier_parse_failure_diagnostics(
            classifier_parse_failure_diagnostics
        ),
        "classification_output_anomaly": len(classified_articles) != len(unseen_articles),
        "classification_failed": classifier_error is not None,
        "classifier_error": classifier_error,
    }


def _normalize_classifier_warning_counts(
    counts: Mapping[str, int] | None,
) -> dict[str, int]:
    """Normalize classifier warning/recovery counters into stable summary keys."""
    if counts is None:
        return dict.fromkeys(_CLASSIFIER_WARNING_COUNT_KEYS, 0)
    normalized: dict[str, int] = {}
    for key in _CLASSIFIER_WARNING_COUNT_KEYS:
        value = counts.get(key, 0)
        normalized[key] = value if isinstance(value, int) and not isinstance(value, bool) else 0
    return normalized


def _is_safe_parse_failure_sample_value(key: str, value: object) -> bool:
    """Return whether a parse-failure sample value is safe to copy to artifacts."""
    if key == "category":
        return isinstance(value, str) and value in PARSE_FAILURE_CATEGORY_KEYS
    if key in _PARSE_FAILURE_SAMPLE_INT_KEYS:
        return isinstance(value, int) and not isinstance(value, bool)
    if key in _PARSE_FAILURE_SAMPLE_OPTIONAL_INT_KEYS:
        return value is None or (isinstance(value, int) and not isinstance(value, bool))
    if key in _PARSE_FAILURE_SAMPLE_BOOL_KEYS:
        return isinstance(value, bool)
    return (
        key == "json_error_kind"
        and isinstance(value, str)
        and value in PARSE_FAILURE_JSON_ERROR_KIND_KEYS
    )


def _normalize_classifier_parse_failure_diagnostics(
    diagnostics: Mapping[str, object] | None,
) -> dict[str, object]:
    """Normalize sanitized parse-failure diagnostics into stable summary keys."""
    empty_counts = dict.fromkeys(PARSE_FAILURE_CATEGORY_KEYS, 0)
    if diagnostics is None:
        return {
            "category_counts": empty_counts,
            "samples": [],
            "sample_count": 0,
            "sample_max_count": 8,
            "sample_shape_max_length": 80,
        }
    raw_counts = diagnostics.get("category_counts")
    category_counts: dict[str, int] = {}
    for key in PARSE_FAILURE_CATEGORY_KEYS:
        value = raw_counts.get(key, 0) if isinstance(raw_counts, Mapping) else 0
        category_counts[key] = (
            value if isinstance(value, int) and not isinstance(value, bool) else 0
        )

    sample_max_count = diagnostics.get("sample_max_count", 8)
    if not isinstance(sample_max_count, int) or isinstance(sample_max_count, bool):
        sample_max_count = 8
    sample_shape_max_length = diagnostics.get("sample_shape_max_length", 80)
    if not isinstance(sample_shape_max_length, int) or isinstance(sample_shape_max_length, bool):
        sample_shape_max_length = 80

    samples: list[dict[str, object]] = []
    raw_samples = diagnostics.get("samples")
    if isinstance(raw_samples, list):
        for raw_sample in raw_samples[:sample_max_count]:
            if not isinstance(raw_sample, Mapping):
                continue
            sample: dict[str, object] = {}
            for key in PARSE_FAILURE_SAMPLE_KEYS:
                if key not in raw_sample:
                    continue
                value = raw_sample[key]
                if key in _PARSE_FAILURE_SAMPLE_STRING_KEYS and isinstance(value, str):
                    sample[key] = value[:sample_shape_max_length]
                elif _is_safe_parse_failure_sample_value(key, value):
                    sample[key] = value
            samples.append(sample)

    return {
        "category_counts": category_counts,
        "samples": samples,
        "sample_count": len(samples),
        "sample_max_count": sample_max_count,
        "sample_shape_max_length": sample_shape_max_length,
    }


def _summary_int(payload: dict[str, object], key: str) -> int:
    """Extract an integer summary value from a machine-summary payload."""
    value = payload.get(key, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _build_problem_summary(
    *,
    source_summaries: list[dict[str, object]],
    classifier_summary: dict[str, object],
    result: RunSnapshot,
) -> dict[str, object]:
    """Build compact problem buckets for downstream automation."""
    source_errors = [
        summary["source_name"] for summary in source_summaries if bool(summary.get("had_error"))
    ]
    zero_result_sources = [
        summary["source_name"]
        for summary in source_summaries
        if bool(summary.get("returned_zero_results"))
    ]
    classification_output_anomaly = bool(
        classifier_summary.get("classification_output_anomaly", False)
    )
    classification_failed = bool(classifier_summary.get("classification_failed", False))
    classified_article_count = _summary_int(classifier_summary, "classified_article_count")
    unseen_article_count = _summary_int(classifier_summary, "unseen_article_count")
    rejected_article_count = _summary_int(classifier_summary, "rejected_article_count")
    all_unseen_rejected = (
        not classification_output_anomaly
        and classified_article_count == unseen_article_count
        and rejected_article_count == unseen_article_count
        and unseen_article_count > 0
    )
    return {
        "source_errors": source_errors,
        "zero_result_sources": zero_result_sources,
        "all_unseen_rejected": all_unseen_rejected,
        "no_relevant_items": result.relevant_article_count == 0,
        "classification_output_anomaly": classification_output_anomaly,
        "classification_failed": classification_failed,
    }


def _build_suspicions(
    *,
    source_summaries: list[dict[str, object]],
    classifier_summary: dict[str, object],
    result: RunSnapshot,
) -> list[str]:
    """Build a stable list of suspicion signals for automated triage."""
    suspicions: list[str] = []
    source_errors = [
        summary["source_name"] for summary in source_summaries if bool(summary.get("had_error"))
    ]
    zero_result_sources = [
        summary["source_name"]
        for summary in source_summaries
        if bool(summary.get("returned_zero_results"))
    ]
    if source_errors:
        suspicions.append("source_errors_present")
    if len(source_errors) >= max(1, len(source_summaries) // 2):
        suspicions.append("source_error_rate_high")
    if zero_result_sources:
        suspicions.append("sources_returned_zero_results")
    classification_output_anomaly = bool(
        classifier_summary.get("classification_output_anomaly", False)
    )
    classified_article_count = _summary_int(classifier_summary, "classified_article_count")
    unseen_article_count = _summary_int(classifier_summary, "unseen_article_count")
    rejected_article_count = _summary_int(classifier_summary, "rejected_article_count")
    all_unseen_rejected = (
        not classification_output_anomaly
        and classified_article_count == unseen_article_count
        and rejected_article_count == unseen_article_count
        and unseen_article_count > 0
    )
    if all_unseen_rejected:
        suspicions.append("all_unseen_rejected")
    if result.relevant_article_count == 0:
        suspicions.append("no_relevant_items")
    if bool(classifier_summary.get("classification_output_anomaly", False)):
        suspicions.append("classification_output_anomaly")
    if bool(classifier_summary.get("classification_failed", False)):
        suspicions.append("classifier_provider_error")
    return suspicions


def _workflow_metadata() -> dict[str, object]:
    """Collect lightweight workflow metadata when running under GitHub Actions."""
    run_id = os.getenv("GITHUB_RUN_ID")
    repository = os.getenv("GITHUB_REPOSITORY")
    server_url = os.getenv("GITHUB_SERVER_URL")
    run_url: str | None = None
    if run_id and repository and server_url:
        run_url = f"{server_url}/{repository}/actions/runs/{run_id}"

    return {
        "workflow_name": os.getenv("GITHUB_WORKFLOW"),
        "job_name": os.getenv("GITHUB_JOB"),
        "run_id": run_id,
        "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
        "repository": repository,
        "ref_name": os.getenv("GITHUB_REF_NAME"),
        "run_url": run_url,
    }


def _build_ingest_debug_payload(
    *,
    result: RunSnapshot,
    sources: list[Source],
    source_names: list[str],
    raw_articles: list[RawArticle],
    unseen_articles: list[RawArticle],
    classified_articles: list[ClassifiedArticle],
    unified_items: list[UnifiedItem],
    classifier_error: str | None = None,
    classifier_warning_counts: Mapping[str, int] | None = None,
    classifier_parse_failure_diagnostics: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build a detailed ingest diagnostic log for state-repo inspection."""
    relevant_articles = [
        article for article in classified_articles if article.classification.relevant
    ]
    rejected_articles = [
        article for article in classified_articles if not article.classification.relevant
    ]
    source_runtime_debug: dict[str, object] = {}
    for source in sources:
        get_debug_state = getattr(source, "get_debug_state", None)
        if get_debug_state is None:
            continue
        debug_state = get_debug_state()
        if debug_state is not None:
            source_runtime_debug[source.name] = debug_state
    source_summaries = _build_source_summaries(
        sources=sources,
        source_names=source_names,
        raw_articles=raw_articles,
        errors=result.errors,
    )
    classifier_summary = _build_classifier_summary(
        unseen_articles=unseen_articles,
        classified_articles=classified_articles,
        classifier_error=classifier_error,
        classifier_warning_counts=classifier_warning_counts,
        classifier_parse_failure_diagnostics=classifier_parse_failure_diagnostics,
    )
    problems = _build_problem_summary(
        source_summaries=source_summaries,
        classifier_summary=classifier_summary,
        result=result,
    )
    return {
        "schema_version": "news_items.ingest.debug.v1",
        "run_timestamp": result.run_timestamp.isoformat(),
        "dataset_name": result.dataset_name.value,
        "job_name": result.job_name.value,
        "config_name": result.config_name,
        "config_path": result.config_path,
        "days_searched": result.days_searched,
        "result_summary": result.result_summary,
        "workflow": _workflow_metadata(),
        "counts": {
            "source_count": result.source_count,
            "raw_article_count": result.raw_article_count,
            "unseen_article_count": result.unseen_article_count,
            "relevant_article_count": result.relevant_article_count,
            "unified_item_count": result.unified_item_count,
            "seen_count_before": result.seen_count_before,
            "seen_count_after": result.seen_count_after,
        },
        "source_runtime_debug": source_runtime_debug,
        "source_summaries": source_summaries,
        "classifier_summary": classifier_summary,
        "problems": problems,
        "suspicions": _build_suspicions(
            source_summaries=source_summaries,
            classifier_summary=classifier_summary,
            result=result,
        ),
        "warnings": result.warnings,
        "errors": result.errors,
        "raw_articles": [_serialize_raw_article(article) for article in raw_articles],
        "unseen_articles": [_serialize_raw_article(article) for article in unseen_articles],
        "classified_articles": [
            _serialize_classified_article(article) for article in classified_articles
        ],
        "relevant_articles": [
            _serialize_classified_article(article) for article in relevant_articles
        ],
        "rejected_articles": [
            _serialize_classified_article(article) for article in rejected_articles
        ],
        "unified_items": [_serialize_unified_item(item) for item in unified_items],
    }


def _build_scrape_candidate_debug_payload(
    *,
    result: RunSnapshot,
    classifier: Classifier,
    scrape_batch: CandidateScrapeBatch,
    fallback_record_count: int,
    batch_id: str | None = None,
) -> dict[str, object]:
    """Build a compact scrape/backfill diagnostic payload for fallback-only drains."""
    fallback_input_count = sum(
        1 for candidate in scrape_batch.fallback_candidates if _fallback_input_article(candidate)
    )
    classifier_summary = _build_classifier_summary(
        unseen_articles=[],
        classified_articles=[],
        classifier_warning_counts=_classifier_warning_counts(classifier),
        classifier_parse_failure_diagnostics=_classifier_parse_failure_diagnostics(classifier),
    )
    fallback_classifier_summary = {
        "fallback_classifier_input_count": fallback_input_count,
        "fallback_operational_record_count": fallback_record_count,
        "warning_counts": _classifier_warning_counts(classifier),
        "parse_failure_diagnostics": _classifier_parse_failure_diagnostics(classifier),
    }
    return {
        "schema_version": "news_items.scrape_candidates.debug.v1",
        "run_timestamp": result.run_timestamp.isoformat(),
        "dataset_name": result.dataset_name.value,
        "job_name": result.job_name.value,
        "config_name": result.config_name,
        "config_path": result.config_path,
        "days_searched": result.days_searched,
        "result_summary": result.result_summary,
        "workflow": _workflow_metadata(),
        "counts": {
            "source_count": result.source_count,
            "raw_article_count": result.raw_article_count,
            "selected_candidate_count": len(scrape_batch.selected_candidates),
            "fallback_candidate_count": len(scrape_batch.fallback_candidates),
            "fallback_operational_record_count": fallback_record_count,
            "scrape_attempt_count": len(scrape_batch.attempts),
            "unified_item_count": result.unified_item_count,
            "seen_count_before": result.seen_count_before,
            "seen_count_after": result.seen_count_after,
        },
        "batch_id": batch_id,
        "classifier_summary": classifier_summary,
        "fallback_classifier_summary": fallback_classifier_summary,
        "problems": {
            "classification_failed": False,
            "classification_output_anomaly": False,
        },
        "suspicions": [],
        "warnings": result.warnings,
        "errors": result.errors,
    }


async def run_news_ingest_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Run the current news ingest pipeline as a registered dataset job."""
    days = days_override if days_override is not None else config.days
    result = _build_run_snapshot(config, config_path=config_path, days=days)

    if not config.anthropic_api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        result.fatal = True
        result.errors.append("ANTHROPIC_API_KEY not set")
        return result.finish("fatal: missing anthropic api key")

    sources = create_sources(config)
    source_names = [source.name for source in sources]
    result.source_count = len(sources)
    if not sources:
        logger.warning("No sources configured")
        result.fatal = True
        result.errors.append("No sources configured")
        return result.finish("fatal: no sources configured")

    classifier = create_classifier(
        api_key=config.anthropic_api_key,
        model=config.classifier.model,
        system_prompt=config.classifier.system_prompt,
        user_prompt_template=config.classifier.user_prompt_template,
    )
    _reset_classifier_warning_counts(classifier)
    deduplicator = create_deduplicator(threshold=config.dedup.similarity_threshold)
    seen_store = create_seen_store(config.state_paths.seen_path)
    result.seen_count_before = seen_store.count

    all_articles, source_errors = await fetch_all_sources(
        sources=sources,
        days=days,
        keywords=config.keywords,
    )
    result.raw_article_count = len(all_articles)
    result.errors.extend(source_errors)
    if source_errors:
        result.warnings.append(f"{len(source_errors)} source(s) reported errors")

    ingest_articles = list(all_articles)
    if (
        config.source_discovery.enabled
        and config.source_discovery.persist_candidates
        and all_articles
    ):
        try:
            persisted_source_discovery = await _persist_source_native_candidates(
                config=config,
                raw_articles=all_articles,
                run_id=result.run_timestamp.astimezone(UTC).isoformat(),
            )
            result.warnings.append(
                f"source_native_candidates_persisted={len(persisted_source_discovery.candidates)}"
            )
            try:
                scrape_batch = await _scrape_candidate_batch(
                    config=config,
                    candidates=persisted_source_discovery.candidates,
                    sources=sources,
                    preloaded_source_articles={
                        source_name: grouped_articles
                        for source_name, grouped_articles in _group_articles_by_source(
                            all_articles
                        ).items()
                        if source_name in source_names
                    },
                )
                if scrape_batch.errors:
                    result.warnings.append(f"candidate_scrape_failures={len(scrape_batch.errors)}")
                passthrough_articles = [
                    article
                    for article in all_articles
                    if not _source_discovery_enabled_for_source(config, article.source_name)
                ]
                ingest_articles = [
                    *scrape_batch.raw_articles,
                    *[
                        article
                        for article in passthrough_articles
                        if article not in scrape_batch.raw_articles
                    ],
                ]
                if not ingest_articles and all_articles:
                    result.warnings.append("candidate_scrape_layer_fell_back_to_direct_articles")
                    ingest_articles = list(all_articles)
            except Exception as exc:
                logger.warning("Candidate scrape layer failed during ingest: %s", exc)
                result.warnings.append(f"candidate_scrape_layer_failed={type(exc).__name__}: {exc}")
        except Exception as exc:
            logger.warning("Source-native candidate persistence failed during ingest: %s", exc)
            result.warnings.append(
                f"source_native_candidate_persistence_failed={type(exc).__name__}: {exc}"
            )

    # Thick prefilter pass — applied to the final article list before classification.
    # For the ingest job there is no candidate_id_map (articles come from RSS feeds
    # directly), so the article URL itself is used as the candidate_id.
    orchestrator = _build_cascade_orchestrator(config)
    thick_articles, thick_decisions = _thick_pass_prefilter(
        ingest_articles, orchestrator, candidate_id_map={}
    )
    if len(thick_articles) < len(ingest_articles):
        result.warnings.append(
            f"prefilter_thick_dropped={len(ingest_articles) - len(thick_articles)}"
        )
    _write_prefilter_run_summary(config, thin_decisions=[], thick_decisions=thick_decisions)

    result.raw_article_count = len(thick_articles)
    return await _process_ingest_articles(
        config=config,
        result=result,
        source_names=source_names,
        sources=sources,
        all_articles=thick_articles,
        seen_store=seen_store,
        classifier=classifier,
        deduplicator=deduplicator,
        operational_store=operational_store,
    )


async def run_news_discover_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Run configured discovery producers and persist durable candidates."""
    del operational_store
    days = days_override if days_override is not None else config.days
    result = _build_run_snapshot(config, config_path=config_path, days=days)
    sources = create_sources(config)
    result.source_count = len(sources)
    source_native_requested = config.source_discovery.enabled
    brave_requested = config.discovery.enabled and config.discovery.engines.brave.enabled
    exa_requested = config.discovery.enabled and config.discovery.engines.exa.enabled
    google_cse_requested = config.discovery.enabled and config.discovery.engines.google_cse.enabled
    source_native_can_run = (
        source_native_requested and config.source_discovery.persist_candidates and bool(sources)
    )
    brave_can_run = brave_requested and config.discovery.persist_candidates
    exa_can_run = exa_requested and config.discovery.persist_candidates
    google_cse_can_run = google_cse_requested and config.discovery.persist_candidates

    if (
        (brave_requested or exa_requested or google_cse_requested)
        and not config.discovery.persist_candidates
        and not source_native_can_run
    ):
        result.fatal = True
        result.errors.append("discovery.persist_candidates is false")
        return result.finish("fatal: engine candidate persistence disabled")
    if (
        source_native_requested
        and not config.source_discovery.persist_candidates
        and not brave_can_run
        and not exa_can_run
        and not google_cse_can_run
    ):
        result.fatal = True
        result.errors.append("source_discovery.persist_candidates is false")
        return result.finish("fatal: source-native candidate persistence disabled")
    if (
        not source_native_requested
        and not brave_can_run
        and not exa_can_run
        and not google_cse_can_run
    ):
        result.fatal = True
        result.errors.append("source_discovery.enabled is false")
        return result.finish("fatal: source-native discovery disabled")
    if not sources and not brave_can_run and not exa_can_run and not google_cse_can_run:
        result.fatal = True
        result.errors.append("No sources configured")
        return result.finish("fatal: no sources configured")

    persisted_runs: list[tuple[str, PersistedSourceDiscovery]] = []
    run_base = result.run_timestamp.astimezone(UTC).isoformat()

    if source_native_can_run:
        persisted_runs.append(
            (
                "source_native",
                await _run_source_native_discovery(
                    config=config,
                    sources=sources,
                    run_id=f"{run_base}:source_native",
                    days=days,
                ),
            )
        )
    elif source_native_requested and not config.source_discovery.persist_candidates:
        result.warnings.append("source-native discovery skipped because persistence is disabled")
    elif source_native_requested and not sources:
        result.warnings.append("source-native discovery skipped because no sources are configured")

    if brave_can_run:
        persisted_runs.append(
            (
                "brave",
                await _run_brave_discovery(
                    config=config,
                    run_id=f"{run_base}:brave",
                    days=days,
                ),
            )
        )

    if exa_can_run:
        persisted_runs.append(
            (
                "exa",
                await _run_exa_discovery(
                    config=config,
                    run_id=f"{run_base}:exa",
                    days=days,
                ),
            )
        )

    if google_cse_can_run:
        persisted_runs.append(
            (
                "google_cse",
                await _run_google_cse_discovery(
                    config=config,
                    run_id=f"{run_base}:google_cse",
                    days=days,
                ),
            )
        )

    merged_candidate_ids: set[str] = set()
    merged_candidates_by_id: dict[str, PersistentCandidate] = {}
    overlap_producers_by_candidate_id: dict[str, list[str]] = {}
    failed_runs = 0
    for producer_name, persisted in persisted_runs:
        result.raw_article_count += persisted.run.candidate_count
        result.unseen_article_count += persisted.run.candidate_count
        merged_candidate_ids.update(candidate.candidate_id for candidate in persisted.candidates)
        for candidate in persisted.candidates:
            existing_candidate = merged_candidates_by_id.get(candidate.candidate_id)
            overlap_producers_by_candidate_id[candidate.candidate_id] = deduplicate_strings(
                [
                    *overlap_producers_by_candidate_id.get(candidate.candidate_id, []),
                    producer_name,
                ]
            )
            merged_candidates_by_id[candidate.candidate_id] = candidate.model_copy(
                update={
                    "discovered_via": deduplicate_strings(
                        [
                            *(existing_candidate.discovered_via if existing_candidate else []),
                            *candidate.discovered_via,
                            producer_name,
                        ]
                    )
                }
            )
        result.errors.extend(persisted.run.errors)
        if producer_name == "source_native" and persisted.run.status is DiscoveryRunStatus.PARTIAL:
            result.warnings.append("source-native discovery completed with partial source failures")
        if producer_name == "brave" and persisted.run.status is DiscoveryRunStatus.PARTIAL:
            result.warnings.append("brave discovery completed with partial engine failures")
        if producer_name == "exa" and persisted.run.status is DiscoveryRunStatus.PARTIAL:
            result.warnings.append("exa discovery completed with partial engine failures")
        if producer_name == "google_cse" and persisted.run.status is DiscoveryRunStatus.PARTIAL:
            result.warnings.append("google_cse discovery completed with partial engine failures")
        if persisted.run.status is DiscoveryRunStatus.FAILED:
            failed_runs += 1

    overlap_candidates = [
        candidate.model_copy(
            update={
                "discovered_via": overlap_producers_by_candidate_id[candidate_id],
                "source_discovery_only": overlap_producers_by_candidate_id[candidate_id]
                == ["source_native"],
            }
        )
        for candidate_id, candidate in merged_candidates_by_id.items()
    ]
    _write_discovery_diagnostic_artifacts(
        config,
        config_path=config_path,
        overlap_candidates=overlap_candidates,
    )
    result.unified_item_count = len(merged_candidate_ids)

    if failed_runs == len(persisted_runs):
        result.fatal = True

    result.finish(
        "discovery persisted "
        f"{result.unified_item_count} merged candidate(s) from "
        f"{result.raw_article_count} discovered result(s)"
    )
    return result


async def run_news_scrape_candidates_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Scrape queued candidates and feed successful results into the ingest path."""
    days = days_override if days_override is not None else config.days
    result = _build_run_snapshot(config, config_path=config_path, days=days)

    if not config.anthropic_api_key:
        result.fatal = True
        result.errors.append("ANTHROPIC_API_KEY not set")
        return result.finish("fatal: missing anthropic api key")

    sources = create_sources(config)
    source_names = [source.name for source in sources]
    result.source_count = len(sources)
    if not sources:
        result.fatal = True
        result.errors.append("No sources configured")
        return result.finish("fatal: no sources configured")

    classifier = create_classifier(
        api_key=config.anthropic_api_key,
        model=config.classifier.model,
        system_prompt=config.classifier.system_prompt,
        user_prompt_template=config.classifier.user_prompt_template,
    )
    _reset_classifier_warning_counts(classifier)
    deduplicator = create_deduplicator(threshold=config.dedup.similarity_threshold)
    seen_store = create_seen_store(config.state_paths.seen_path)
    result.seen_count_before = seen_store.count
    store = operational_store
    owns_store = False
    orchestrator = _build_cascade_orchestrator(config)

    try:
        scrape_batch, thin_decisions = await _run_candidate_scrape_job(
            config=config.model_copy(update={"days": days}),
            sources=sources,
            limit=config.max_articles,
            orchestrator=orchestrator,
            pub_date_from=config.scrape_pub_date_from,
            balanced_batch_size=config.scrape_balanced_batch_size,
            min_domain_frequency=config.scrape_min_domain_frequency,
            use_domain_verdicts=config.scrape_use_domain_verdicts,
        )
        result.raw_article_count = len(scrape_batch.raw_articles)
        if scrape_batch.errors:
            result.errors.extend(scrape_batch.errors)
            result.warnings.append(f"candidate_scrape_failures={len(scrape_batch.errors)}")
        if not scrape_batch.selected_candidates:
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result.finish("no queued candidates eligible for scrape")

        if store is None and (scrape_batch.raw_articles or scrape_batch.fallback_candidates):
            store = create_operational_store(config)
            owns_store = True

        try:
            fallback_records = await _build_fallback_operational_records(
                candidates=scrape_batch.fallback_candidates,
                classifier=classifier,
            )
        except ClassifierProviderError as exc:
            sanitized_error = _mark_classifier_provider_error(result, exc)
            logger.error(
                "Classifier provider failed during candidate fallback retention: %s",
                sanitized_error,
            )
            result.seen_count_after = seen_store.count
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result.finish("fatal: classifier provider error")
        if fallback_records and store is not None:
            store.upsert_records(
                config.dataset_name.value,
                [record.model_dump(mode="json") for record in fallback_records],
            )
            result.warnings.append(f"fallback_operational_records={len(fallback_records)}")

        if not scrape_batch.raw_articles:
            result.unified_item_count = len(fallback_records)
            if fallback_records:
                result.finish(
                    f"fallback retention completed with {len(fallback_records)} provisional row(s)"
                )
            else:
                result.finish("candidate scrape completed with no materializable rows")
            result.set_debug_payload(
                _build_scrape_candidate_debug_payload(
                    result=result,
                    classifier=classifier,
                    scrape_batch=scrape_batch,
                    fallback_record_count=len(fallback_records),
                )
            )
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result

        # Build URL → candidate_id map for the thick pass.
        candidate_id_map = {
            canonicalize_news_url(str(c.canonical_url or c.current_url)): c.candidate_id
            for c in scrape_batch.selected_candidates
        }
        thick_articles, thick_decisions = _thick_pass_prefilter(
            scrape_batch.raw_articles, orchestrator, candidate_id_map=candidate_id_map
        )
        if len(thick_articles) < len(scrape_batch.raw_articles):
            result.warnings.append(
                f"prefilter_thick_dropped={len(scrape_batch.raw_articles) - len(thick_articles)}"
            )

        _write_prefilter_run_summary(
            config, thin_decisions=thin_decisions, thick_decisions=thick_decisions
        )
        processed = await _process_ingest_articles(
            config=config,
            result=result,
            source_names=source_names,
            sources=sources,
            all_articles=thick_articles,
            seen_store=seen_store,
            classifier=classifier,
            deduplicator=deduplicator,
            operational_store=store,
        )
        return processed
    finally:
        if owns_store and store is not None:
            store.close()


async def run_news_backfill_discover_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Discover historical candidates over one requested backfill range."""
    del days_override, operational_store
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    try:
        requested_date_from, requested_date_to = resolve_backfill_request_window()
    except ValueError as exc:
        result.fatal = True
        result.errors.append(str(exc))
        return result.finish("fatal: invalid backfill request window")

    windows = plan_backfill_windows(
        date_from=requested_date_from,
        date_to=requested_date_to,
        batch_window_days=config.backfill.batch_window_days,
    )
    sources = create_sources(config)
    result.source_count = len(sources)
    source_native_can_run = (
        config.source_discovery.enabled
        and config.source_discovery.persist_candidates
        and bool(sources)
    )
    engine_can_run = (
        config.discovery.enabled
        and config.discovery.persist_candidates
        and any(
            [
                config.discovery.engines.brave.enabled,
                config.discovery.engines.exa.enabled,
                config.discovery.engines.google_cse.enabled,
            ]
        )
    )
    if not source_native_can_run and not engine_can_run:
        result.fatal = True
        result.errors.append("No backfill discovery producers configured")
        return result.finish("fatal: no backfill discovery producers configured")
    batch_id = os.getenv(BACKFILL_BATCH_ID_ENV) or str(uuid4())
    persistence = create_discovery_persistence(config)
    try:
        if (
            os.getenv(BACKFILL_BATCH_ID_ENV)
            and persistence.get_backfill_batch(batch_id) is not None
        ):
            result.fatal = True
            result.errors.append(f"Backfill batch {batch_id} already exists")
            return result.finish(f"fatal: backfill batch {batch_id} already exists")
        enabled_engines = [
            name
            for name, engine_cfg in [
                ("brave", config.discovery.engines.brave),
                ("exa", config.discovery.engines.exa),
                ("google_cse", config.discovery.engines.google_cse),
            ]
            if engine_cfg.enabled
        ]
        batch = BackfillBatch(
            batch_id=batch_id,
            created_at=result.run_timestamp,
            updated_at=result.run_timestamp,
            started_at=result.run_timestamp,
            dataset_name=config.dataset_name,
            job_name=config.job_name,
            status=BackfillBatchStatus.RUNNING,
            requested_date_from=requested_date_from,
            requested_date_to=requested_date_to,
            window_count=len(windows),
            metadata={
                "requested_date_from_env": BACKFILL_DATE_FROM_ENV,
                "requested_date_to_env": BACKFILL_DATE_TO_ENV,
                "keywords": list(config.keywords),
                "keyword_count": len(config.keywords),
                "engines": enabled_engines,
                "query_kinds": [
                    k.value if hasattr(k, "value") else str(k)
                    for k in (config.backfill.query_kinds or config.discovery.default_query_kinds)
                ],
            },
        )
        persistence.upsert_backfill_batches([batch])
        run_base = result.run_timestamp.astimezone(UTC).isoformat()
        query_count = 0
        discovered_count = 0
        batch_warnings: list[str] = []
        batch_errors: list[str] = []
        persisted_runs: list[PersistedSourceDiscovery] = []

        for window in windows:
            window_run_start = len(persisted_runs)

            if source_native_can_run:
                persisted = await _run_source_native_backfill_discovery(
                    config=config,
                    sources=sources,
                    run_id=f"{run_base}:source_native:{window.index}",
                    window=window,
                    batch_id=batch.batch_id,
                )
                persisted_runs.append(persisted)
                if persisted.warnings:
                    batch_warnings.extend(persisted.warnings)

            if engine_can_run:
                if config.discovery.engines.brave.enabled:
                    persisted_runs.append(
                        await _run_backfill_engine_discovery(
                            config=config,
                            run_id=f"{run_base}:brave:{window.index}",
                            batch_id=batch.batch_id,
                            window=window,
                            engine_name="brave",
                        )
                    )
                if config.discovery.engines.exa.enabled:
                    persisted_runs.append(
                        await _run_backfill_engine_discovery(
                            config=config,
                            run_id=f"{run_base}:exa:{window.index}",
                            batch_id=batch.batch_id,
                            window=window,
                            engine_name="exa",
                        )
                    )
                if config.discovery.engines.google_cse.enabled:
                    persisted_runs.append(
                        await _run_backfill_engine_discovery(
                            config=config,
                            run_id=f"{run_base}:google_cse:{window.index}",
                            batch_id=batch.batch_id,
                            window=window,
                            engine_name="google_cse",
                        )
                    )

            discovered_count += sum(
                p.run.candidate_count for p in persisted_runs[window_run_start:]
            )
            if discovered_count >= config.backfill.max_candidates_per_run:
                logger.info(
                    "backfill batch %s: candidate cap %d reached after window %d/%d, stopping early",
                    batch.batch_id,
                    config.backfill.max_candidates_per_run,
                    window.index,
                    len(windows),
                )
                batch_warnings.append(
                    f"candidate cap {config.backfill.max_candidates_per_run} reached after "
                    f"window {window.index}/{len(windows)}"
                )
                break

        total_skipped = 0
        for persisted in persisted_runs:
            query_count += persisted.run.query_count
            total_skipped += persisted.run.skipped_query_count
            result.raw_article_count += persisted.run.candidate_count
            result.errors.extend(persisted.run.errors)
            batch_errors.extend(persisted.run.errors)
        logger.info(
            "backfill batch %s: %d queries run, %d skipped (already executed), %d candidates discovered",
            batch.batch_id,
            query_count,
            total_skipped,
            discovered_count,
        )

        merged_candidate_count, _ = _batch_candidate_counts(persistence, batch_id=batch.batch_id)
        status = BackfillBatchStatus.DISCOVERED
        if batch_errors and not merged_candidate_count:
            status = BackfillBatchStatus.FAILED
            result.fatal = True
        elif batch_errors:
            status = BackfillBatchStatus.PARTIAL

        batch = _update_backfill_batch_state(
            persistence,
            batch=batch,
            status=status,
            query_count=query_count,
            candidate_count=discovered_count,
            warnings=deduplicate_strings(batch_warnings),
            errors=deduplicate_strings(batch_errors),
            finished=status is BackfillBatchStatus.FAILED,
        )
    finally:
        persistence.close()

    result.unified_item_count = batch.merged_candidate_count
    result.warnings.extend(batch.warnings)
    result.set_debug_payload(
        {
            "batch_id": batch.batch_id,
            "requested_date_from": requested_date_from.isoformat(),
            "requested_date_to": requested_date_to.isoformat(),
            "window_count": len(windows),
        }
    )
    if result.fatal:
        return result.finish(f"fatal: backfill discovery failed for batch {batch.batch_id}")
    return result.finish(
        f"backfill discovery persisted {batch.merged_candidate_count} candidate(s) for batch {batch.batch_id}"
    )


async def run_news_backfill_scrape_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Drain one historical backfill batch through the candidate scrape pipeline."""
    days = days_override if days_override is not None else config.days
    result = _build_run_snapshot(config, config_path=config_path, days=days)
    if not config.anthropic_api_key:
        result.fatal = True
        result.errors.append("ANTHROPIC_API_KEY not set")
        return result.finish("fatal: missing anthropic api key")

    sources = create_sources(config)
    result.source_count = len(sources)
    if not sources:
        result.fatal = True
        result.errors.append("No sources configured")
        return result.finish("fatal: no sources configured")

    batch_id = os.getenv(BACKFILL_BATCH_ID_ENV)
    persistence = create_discovery_persistence(config)
    try:
        scrape_candidates_for_batch = select_backfill_candidates_for_scrape(
            persistence,
            limit=config.backfill.max_scrape_attempts_per_run,
            batch_id=batch_id,
        )
        if not scrape_candidates_for_batch:
            return result.finish("no queued backfill candidates eligible for scrape")
        batch_id = scrape_candidates_for_batch[0].backfill_batch_id
        batch = persistence.get_backfill_batch(batch_id) if batch_id is not None else None
        if batch is None:
            result.fatal = True
            result.errors.append("Missing backfill batch metadata")
            return result.finish("fatal: missing backfill batch metadata")
        batch = _update_backfill_batch_state(
            persistence,
            batch=batch,
            status=BackfillBatchStatus.SCRAPING,
            warnings=batch.warnings,
            errors=batch.errors,
            finished=False,
        )
    finally:
        persistence.close()

    classifier = create_classifier(
        api_key=config.anthropic_api_key,
        model=config.classifier.model,
        system_prompt=config.classifier.system_prompt,
        user_prompt_template=config.classifier.user_prompt_template,
    )
    _reset_classifier_warning_counts(classifier)
    deduplicator = create_deduplicator(threshold=config.dedup.similarity_threshold)
    seen_store = create_seen_store(config.state_paths.seen_path)
    result.seen_count_before = seen_store.count
    store = operational_store
    owns_store = False
    scrape_batch: CandidateScrapeBatch | None = None
    orchestrator = _build_cascade_orchestrator(config)
    thin_decisions: list[PrefilterDecision] = []

    try:
        scrape_batch, thin_decisions = await _run_backfill_candidate_scrape_job(
            config=config.model_copy(update={"days": days}),
            sources=sources,
            limit=config.backfill.max_scrape_attempts_per_run,
            batch_id=batch_id,
            orchestrator=orchestrator,
        )
        result.raw_article_count = len(scrape_batch.raw_articles)
        if scrape_batch.errors:
            result.errors.extend(scrape_batch.errors)
            result.warnings.append(f"candidate_scrape_failures={len(scrape_batch.errors)}")
        if not scrape_batch.selected_candidates:
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result.finish("no queued backfill candidates eligible for scrape")

        if store is None and (scrape_batch.raw_articles or scrape_batch.fallback_candidates):
            store = create_operational_store(config)
            owns_store = True

        try:
            fallback_records = await _build_fallback_operational_records(
                candidates=scrape_batch.fallback_candidates,
                classifier=classifier,
                require_publication_datetime=True,
                publication_window=(batch.requested_date_from, batch.requested_date_to),
            )
        except ClassifierProviderError as exc:
            sanitized_error = _mark_classifier_provider_error(result, exc)
            logger.error(
                "Classifier provider failed during backfill fallback retention: %s",
                sanitized_error,
            )
            result.seen_count_after = seen_store.count
            return result.finish("fatal: classifier provider error")
        skipped_fallback_count = len(scrape_batch.fallback_candidates) - len(fallback_records)
        if skipped_fallback_count > 0:
            result.warnings.append(
                f"fallback_candidates_without_retained_rows={skipped_fallback_count}"
            )
        if fallback_records and store is not None:
            store.upsert_records(
                config.dataset_name.value,
                [record.model_dump(mode="json") for record in fallback_records],
            )
            result.warnings.append(f"fallback_operational_records={len(fallback_records)}")

        if not scrape_batch.raw_articles:
            result.unified_item_count = len(fallback_records)
            result.finish(
                "fallback retention completed with "
                f"{len(fallback_records)} provisional row(s) for backfill batch {batch_id}"
            )
            result.set_debug_payload(
                _build_scrape_candidate_debug_payload(
                    result=result,
                    classifier=classifier,
                    scrape_batch=scrape_batch,
                    fallback_record_count=len(fallback_records),
                    batch_id=batch_id,
                )
            )
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result

        out_of_window_article_count = len(scrape_batch.raw_articles)
        scrape_batch.raw_articles = [
            article
            for article in scrape_batch.raw_articles
            if _article_in_window(
                article,
                start=batch.requested_date_from,
                end=batch.requested_date_to,
            )
        ]
        out_of_window_article_count -= len(scrape_batch.raw_articles)
        if out_of_window_article_count > 0:
            result.warnings.append(
                f"raw_articles_skipped_outside_backfill_window={out_of_window_article_count}"
            )
        if not scrape_batch.raw_articles:
            result.unified_item_count = len(fallback_records)
            result.finish(
                "backfill batch "
                f"{batch_id}: no raw articles remained inside requested backfill window; "
                f"retained {len(fallback_records)} fallback row(s)"
            )
            result.set_debug_payload(
                _build_scrape_candidate_debug_payload(
                    result=result,
                    classifier=classifier,
                    scrape_batch=scrape_batch,
                    fallback_record_count=len(fallback_records),
                    batch_id=batch_id,
                )
            )
            _write_prefilter_run_summary(config, thin_decisions=thin_decisions, thick_decisions=[])
            return result

        # Thick prefilter pass — post-window-filter, pre-classifier.
        candidate_id_map = {
            canonicalize_news_url(str(c.canonical_url or c.current_url)): c.candidate_id
            for c in scrape_batch.selected_candidates
        }
        thick_articles, thick_decisions = _thick_pass_prefilter(
            scrape_batch.raw_articles, orchestrator, candidate_id_map=candidate_id_map
        )
        if len(thick_articles) < len(scrape_batch.raw_articles):
            result.warnings.append(
                f"prefilter_thick_dropped={len(scrape_batch.raw_articles) - len(thick_articles)}"
            )
        _write_prefilter_run_summary(
            config, thin_decisions=thin_decisions, thick_decisions=thick_decisions
        )

        processed = await _process_ingest_articles(
            config=config,
            result=result,
            source_names=[source.name for source in sources],
            sources=sources,
            all_articles=thick_articles,
            seen_store=seen_store,
            classifier=classifier,
            deduplicator=deduplicator,
            operational_store=store,
        )
        processed.result_summary = f"backfill batch {batch_id}: {processed.result_summary}"
        processed.set_debug_payload(
            {
                **(processed.debug_payload or {}),
                "batch_id": batch_id,
            }
        )
        return processed
    finally:
        updated_persistence = create_discovery_persistence(config)
        try:
            if batch_id is not None:
                existing_batch = updated_persistence.get_backfill_batch(batch_id)
                if existing_batch is not None:
                    remaining_candidates = updated_persistence.list_candidates(
                        statuses=SCRAPEABLE_CANDIDATE_STATUSES,
                        backfill_batch_id=batch_id,
                        limit=1,
                    )
                    scrape_attempts = len(scrape_batch.attempts) if scrape_batch is not None else 0
                    scraped_candidates = (
                        len(scrape_batch.selected_candidates) if scrape_batch is not None else 0
                    )
                    final_status = (
                        BackfillBatchStatus.PARTIAL
                        if result.errors
                        else BackfillBatchStatus.COMPLETED
                    )
                    if remaining_candidates:
                        final_status = (
                            BackfillBatchStatus.PARTIAL
                            if result.errors
                            else BackfillBatchStatus.DISCOVERED
                        )
                    _update_backfill_batch_state(
                        updated_persistence,
                        batch=existing_batch,
                        status=final_status,
                        scrape_attempt_count=existing_batch.scrape_attempt_count + scrape_attempts,
                        scraped_candidate_count=(
                            existing_batch.scraped_candidate_count + scraped_candidates
                        ),
                        warnings=deduplicate_strings([*existing_batch.warnings, *result.warnings]),
                        errors=deduplicate_strings([*existing_batch.errors, *result.errors]),
                        finished=final_status is BackfillBatchStatus.COMPLETED,
                    )
        finally:
            updated_persistence.close()
        if owns_store and store is not None:
            store.close()


async def run_pipeline_async(config: Config, days: int) -> RunSnapshot:
    """Backward-compatible alias for the news ingest job."""
    ingest_config = config.model_copy(
        update={
            "dataset_name": DatasetName.NEWS_ITEMS,
            "job_name": JobName.INGEST,
        }
    )
    return await run_news_ingest_job(ingest_config, config_path=None, days_override=days)


async def run_news_items_release_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Build and publish a metadata-only release bundle for news_items."""
    del days_override
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    store = operational_store or create_operational_store(config)
    builder = NewsItemsReleaseBuilder(config=config)
    dataset_name = config.dataset_name.value
    corrected_records = _load_corrected_news_item_records(store, dataset_name=dataset_name)
    corrected_rows = [record.model_dump(mode="json") for record in corrected_records]
    manifest = builder.build_release_bundle(
        publication_dir=config.state_paths.publication_dir,
        rows=corrected_rows,
    )
    if config.release.publish_public_targets:
        published_targets = publish_release_bundle(
            config=config,
            release_dir=config.state_paths.publication_dir / manifest.release_version,
            manifest=manifest,
        )
    else:
        published_targets = []
    result.release_manifest = manifest.model_dump(mode="json")
    result.unified_item_count = manifest.row_count
    if published_targets:
        result.warnings.append(f"published_targets={','.join(published_targets)}")
        public_ids = [
            row.id
            for row in select_releasable_records(
                corrected_rows,
                release_version=manifest.release_version,
            )
        ]
        store.mark_publication_state(
            dataset_name,
            public_ids,
            "published",
        )
    elif not config.release.publish_public_targets:
        result.warnings.append(
            "Public publication disabled by release config; built release bundle only."
        )
    else:
        result.warnings.append("No publication targets configured; built release bundle only.")
    return result.finish(f"release built for {manifest.row_count} public row(s)")


def _load_corrected_news_item_records(
    store: OperationalStore,
    *,
    dataset_name: str,
) -> list[NewsItemOperationalRecord]:
    rows = store.fetch_records(dataset_name)
    return apply_manual_annotations(
        parse_operational_records(rows),
        corrections=parse_news_item_corrections(store.fetch_news_item_corrections(dataset_name)),
        missing_items=parse_missing_news_items(store.fetch_missing_news_items(dataset_name)),
        suppression_rules=parse_suppression_rules(store.fetch_suppression_rules(dataset_name)),
    )


async def _run_news_items_monthly_report_with_options(
    config: Config,
    *,
    config_path: Path | None,
    store: OperationalStore,
    month_value: str | None,
    markdown_output_path: Path | None,
    json_output_path: Path | None,
    hq_activity: str | None,
    hq_activity_file: Path | None,
) -> tuple[RunSnapshot, MonthlyReport]:
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    dataset_name = config.dataset_name.value
    corrected_records = _load_corrected_news_item_records(store, dataset_name=dataset_name)
    month = resolve_report_month(month_value)
    eligible_record_count = len(select_monthly_report_records(corrected_records, month=month))
    report = generate_monthly_report(
        corrected_records,
        month=month,
        hq_activity=hq_activity_from_inputs(
            hq_activity=hq_activity,
            hq_activity_file=hq_activity_file,
        ),
    )
    artifacts = persist_monthly_report_artifacts(config.state_paths.publication_dir, report)
    if markdown_output_path is not None:
        write_report_copy(markdown_output_path, report.rendered_markdown)
    if json_output_path is not None:
        write_report_json_copy(json_output_path, report)
    result.unified_item_count = eligible_record_count
    result.set_debug_payload(
        report_env_summary(
            month=month,
            markdown_path=artifacts.markdown_path,
            json_path=artifacts.json_path,
        )
    )
    if not report.stats:
        result.warnings.append("monthly_report_contains_zero_index_relevant_public_rows")
    return result.finish(
        f"monthly report built for {report.month_key} ({len(report.selected_cases)} case(s))"
    ), report


async def run_news_items_monthly_report_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Build a monthly public report bundle for news_items."""
    del days_override
    owns_store = operational_store is None
    store = operational_store or create_operational_store(config)
    try:
        hq_activity_file = os.environ.get(MONTHLY_REPORT_HQ_ACTIVITY_FILE_ENV)
        result, _ = await _run_news_items_monthly_report_with_options(
            config,
            config_path=config_path,
            store=store,
            month_value=os.environ.get(MONTHLY_REPORT_MONTH_ENV),
            markdown_output_path=Path(os.environ[MONTHLY_REPORT_MARKDOWN_ENV])
            if os.environ.get(MONTHLY_REPORT_MARKDOWN_ENV)
            else None,
            json_output_path=Path(os.environ[MONTHLY_REPORT_JSON_ENV])
            if os.environ.get(MONTHLY_REPORT_JSON_ENV)
            else None,
            hq_activity=os.environ.get(MONTHLY_REPORT_HQ_ACTIVITY_ENV),
            hq_activity_file=Path(hq_activity_file) if hq_activity_file else None,
        )
        return result
    finally:
        if owns_store:
            store.close()


async def run_news_items_backup_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Upload the latest release bundle to configured backup targets."""
    del days_override
    del operational_store
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    manifest = execute_latest_backup(config, publication_root=release_publication_dir(config))
    result.backup_manifest = manifest.model_dump(mode="json")
    if not manifest.targets:
        result.warnings.append("No backup targets configured.")
    return result.finish(f"backup completed for {len(manifest.targets)} target(s)")


async def run_scaffolded_release_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Backward-compatible alias for the Phase B release job."""
    return await run_news_items_release_job(
        config,
        config_path=config_path,
        days_override=days_override,
        operational_store=operational_store,
    )


async def run_scaffolded_backup_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Backward-compatible alias for the Phase B backup job."""
    return await run_news_items_backup_job(
        config,
        config_path=config_path,
        days_override=days_override,
        operational_store=operational_store,
    )


async def run_job_async(
    config: Config,
    *,
    config_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Dispatch a dataset/job run through the registry."""
    ensure_default_jobs_registered()
    handler = require_job_handler(config.dataset_name, config.job_name)
    store: OperationalStore | None = operational_store
    owns_store = False
    if store is None and config.job_name in _OPERATIONAL_STORE_JOBS:
        store = create_operational_store(config)
        owns_store = True
    result: RunSnapshot | None = None
    try:
        result = await handler(config, config_path, days_override, store)
        if store is not None:
            try:
                store.write_run_metadata(result)
            except Exception as exc:
                logger.warning(
                    "Failed to persist operational run metadata for %s/%s: %s",
                    config.dataset_name.value,
                    config.job_name.value,
                    exc,
                )
                result.warnings.append(
                    f"operational_run_metadata_write_failed={type(exc).__name__}: {exc}"
                )
        return result
    finally:
        if owns_store and store is not None:
            try:
                store.close()
            except Exception as exc:
                logger.warning("Failed to close operational store: %s", exc)
                if result is not None:
                    result.warnings.append(
                        f"operational_store_close_failed={type(exc).__name__}: {exc}"
                    )


def _load_config_or_exit(config_path: Path, overlay_path: Path | None = None) -> Config:
    """Load a config file (optionally with an overlay) and exit helpfully on failure."""
    try:
        return load_config(config_path, overlay_path=overlay_path)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"Error loading config: {exc}")
        sys.exit(1)


def _run_job_from_config(
    *,
    config_path: Path,
    dataset_name: DatasetName | None,
    job_name: JobName | None,
    config_overlay_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
    scrape_pub_date_from: str | None = None,
    scrape_balanced_batch_size: int | None = None,
    scrape_min_domain_frequency: int | None = None,
    scrape_use_domain_verdicts: bool = False,
    query_budget: int | None = None,
) -> RunSnapshot:
    """Shared sync wrapper for CLI-triggered job runs."""
    setup_logging()
    config = _load_config_or_exit(config_path, config_overlay_path)

    update: dict[str, object] = {}
    if dataset_name is not None:
        update["dataset_name"] = DatasetName(dataset_name)
    if job_name is not None:
        update["job_name"] = JobName(job_name)
    if query_budget is not None:
        if query_budget < 1:
            print("Error: --query-budget must be a positive integer")
            sys.exit(1)
        update["discovery"] = config.discovery.model_copy(
            update={"max_queries_per_run": query_budget}
        )
    if scrape_pub_date_from is not None:
        try:
            update["scrape_pub_date_from"] = datetime.fromisoformat(scrape_pub_date_from).replace(
                tzinfo=UTC
            )
        except ValueError as exc:
            print(f"Error: --pub-date-from must be an ISO date (YYYY-MM-DD): {exc}")
            sys.exit(1)
    if scrape_balanced_batch_size is not None:
        if scrape_balanced_batch_size < 1:
            print("Error: --balanced-batch must be a positive integer")
            sys.exit(1)
        update["scrape_balanced_batch_size"] = scrape_balanced_batch_size
    if scrape_min_domain_frequency is not None:
        if scrape_min_domain_frequency < 1:
            print("Error: --min-domain-frequency must be a positive integer")
            sys.exit(1)
        update["scrape_min_domain_frequency"] = scrape_min_domain_frequency
    if scrape_use_domain_verdicts:
        update["scrape_use_domain_verdicts"] = True
    if update:
        config = config.model_copy(update=update)

    days = days_override if days_override is not None else config.days
    logger.info(
        "Starting %s/%s: %s, searching last %s days",
        config.dataset_name,
        config.job_name,
        config.name,
        days,
    )

    try:
        if operational_store is None:
            result = asyncio.run(
                run_job_async(
                    config,
                    config_path=config_path,
                    days_override=days_override,
                )
            )
        else:
            result = asyncio.run(
                run_job_async(
                    config,
                    config_path=config_path,
                    days_override=days_override,
                    operational_store=operational_store,
                )
            )
    except ValueError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    if config.job_name == JobName.INGEST and not result.fatal:
        output_errors = output_items(result.items, config)
        result.errors.extend(output_errors)

    if result.debug_payload is not None:
        try:
            write_run_debug_log(config.state_paths.logs_dir, result, result.debug_payload)
        except Exception as exc:
            logger.warning("Failed to write run debug log: %s", exc)
            result.errors.append(f"Failed to write run debug log: {exc}")
        try:
            write_run_debug_summary(config.state_paths.logs_dir, result, result.debug_payload)
        except Exception as exc:
            logger.warning("Failed to write run debug summary: %s", exc)
            result.errors.append(f"Failed to write run debug summary: {exc}")
    write_run_snapshot(config.state_paths.runs_dir, result)

    if config.job_name != JobName.INGEST:
        print(result.result_summary or f"{config.job_name.value} job completed")

    if result.fatal:
        sys.exit(1)
    return result


def run_pipeline(config_path: Path, days_override: int | None = None) -> None:
    """Run the news ingest pipeline through the generic job runner."""
    _run_job_from_config(
        config_path=config_path,
        dataset_name=DatasetName.NEWS_ITEMS,
        job_name=JobName.INGEST,
        days_override=days_override,
    )


def run_job(
    *,
    config_path: Path,
    dataset_name: DatasetName,
    job_name: JobName,
    config_overlay_path: Path | None = None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
    scrape_pub_date_from: str | None = None,
    scrape_balanced_batch_size: int | None = None,
    scrape_min_domain_frequency: int | None = None,
    scrape_use_domain_verdicts: bool = False,
    query_budget: int | None = None,
) -> None:
    """Run a dataset/job pair through the generic registry."""
    _run_job_from_config(
        config_path=config_path,
        dataset_name=dataset_name,
        job_name=job_name,
        config_overlay_path=config_overlay_path,
        days_override=days_override,
        operational_store=operational_store,
        scrape_pub_date_from=scrape_pub_date_from,
        scrape_balanced_batch_size=scrape_balanced_batch_size,
        scrape_min_domain_frequency=scrape_min_domain_frequency,
        scrape_use_domain_verdicts=scrape_use_domain_verdicts,
        query_budget=query_budget,
    )


def run_news_items_monthly_report(
    *,
    config_path: Path,
    month: str,
    output_path: Path | None = None,
    json_output_path: Path | None = None,
    hq_activity: str | None = None,
    hq_activity_file: Path | None = None,
) -> MonthlyReport:
    """Run the news_items monthly report job with explicit CLI-style options."""
    setup_logging()
    config = _load_config_or_exit(config_path).model_copy(
        update={
            "dataset_name": DatasetName.NEWS_ITEMS,
            "job_name": JobName.MONTHLY_REPORT,
        }
    )
    logger.info("Starting %s/%s for month %s", config.dataset_name, config.job_name, month)
    store = create_operational_store(config)
    try:
        result, report = asyncio.run(
            _run_news_items_monthly_report_with_options(
                config,
                config_path=config_path,
                store=store,
                month_value=month,
                markdown_output_path=output_path,
                json_output_path=json_output_path,
                hq_activity=hq_activity,
                hq_activity_file=hq_activity_file,
            )
        )
        try:
            store.write_run_metadata(result)
        except Exception as exc:
            logger.warning(
                "Failed to persist operational run metadata for %s/%s: %s",
                config.dataset_name.value,
                config.job_name.value,
                exc,
            )
            result.warnings.append(
                f"operational_run_metadata_write_failed={type(exc).__name__}: {exc}"
            )
        write_run_snapshot(config.state_paths.runs_dir, result)
        return report
    finally:
        store.close()


def run_release(
    *,
    config_path: Path,
    dataset_name: DatasetName,
    operational_store: OperationalStore | None = None,
) -> None:
    """Run the scaffolded release job for a dataset."""
    _run_job_from_config(
        config_path=config_path,
        dataset_name=dataset_name,
        job_name=JobName.RELEASE,
        operational_store=operational_store,
    )


def run_backup(
    *,
    config_path: Path,
    dataset_name: DatasetName,
    operational_store: OperationalStore | None = None,
) -> None:
    """Run the scaffolded backup job for a dataset."""
    _run_job_from_config(
        config_path=config_path,
        dataset_name=dataset_name,
        job_name=JobName.BACKUP,
        operational_store=operational_store,
    )


def output_items(items: list[UnifiedItem], config: Config) -> list[str]:
    """Output unified items to the configured output channels."""
    cli_requested = OutputFormat.CLI in config.output.formats
    errors: list[str] = []

    for output_format in config.output.formats:
        if output_format == OutputFormat.CLI:
            print_items(items)
            continue

        if output_format == OutputFormat.EMAIL:
            try:
                send_output_email(items, config)
                recipients = ", ".join(config.email_to)
                print(f"Email report sent to: {recipients}")
            except Exception as exc:
                logger.exception("Failed to send email report: %s", exc)
                print(f"Error sending email report: {exc}")
                errors.append(f"email: {exc}")
                if not cli_requested:
                    print_items(items)
            continue

        if output_format == OutputFormat.TELEGRAM:
            logger.warning("Telegram output is not implemented yet, falling back to CLI output")
            errors.append("telegram: not implemented")
            if not cli_requested:
                print_items(items)
            continue

    return errors


def send_output_email(items: list[UnifiedItem], config: Config) -> None:
    """Send unified items as an email report."""
    smtp_host = config.email_smtp_host
    sender = config.email_from
    recipients = config.email_to

    if not smtp_host:
        raise ValueError("DENBUST_EMAIL_SMTP_HOST is required for email output")
    if not sender:
        raise ValueError("DENBUST_EMAIL_FROM is required for email output")
    if not recipients:
        raise ValueError("DENBUST_EMAIL_TO is required for email output")

    send_email_report(
        items=items,
        smtp_host=smtp_host,
        smtp_port=config.email_smtp_port,
        sender=sender,
        recipients=recipients,
        subject=config.email_subject,
        username=config.email_smtp_username,
        password=config.email_smtp_password,
        use_tls=config.email_use_tls,
    )
