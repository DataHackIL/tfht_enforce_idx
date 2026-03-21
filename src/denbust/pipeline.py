"""Pipeline orchestration and dataset/job dispatch."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from denbust.classifier.relevance import Classifier, create_classifier
from denbust.config import Config, OutputFormat, SourceType, load_config
from denbust.data_models import ClassifiedArticle, RawArticle, UnifiedItem
from denbust.datasets.jobs import ensure_default_jobs_registered
from denbust.datasets.registry import require_job_handler
from denbust.dedup.similarity import Deduplicator, create_deduplicator
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot
from denbust.news_items.backup import execute_latest_backup
from denbust.news_items.ingest import build_operational_records, summarize_privacy_mix
from denbust.news_items.normalize import canonicalize_news_url
from denbust.news_items.publication import publish_release_bundle
from denbust.news_items.release import NewsItemsReleaseBuilder, select_releasable_records
from denbust.ops.factory import create_operational_store
from denbust.ops.storage import OperationalStore
from denbust.output.email import send_email_report
from denbust.output.formatter import print_items
from denbust.sources.base import Source
from denbust.sources.haaretz import create_haaretz_source
from denbust.sources.ice import create_ice_source
from denbust.sources.maariv import create_maariv_source
from denbust.sources.mako import create_mako_source
from denbust.sources.rss import RSSSource
from denbust.sources.walla import create_walla_source
from denbust.store.run_snapshots import (
    write_run_debug_log,
    write_run_debug_summary,
    write_run_snapshot,
)
from denbust.store.seen import SeenStore, create_seen_store

logger = logging.getLogger(__name__)


def release_publication_dir(config: Config) -> Path:
    """Return the publication directory that holds built release bundles."""
    if config.job_name == JobName.RELEASE:
        return config.state_paths.publication_dir
    return config.store.publication_dir or (
        config.store.state_root / config.dataset_name / JobName.RELEASE / "publication"
    )


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
                sources.append(RSSSource(source_name=source_cfg.name, feed_url=source_cfg.url))
            else:
                logger.warning("RSS source %s missing URL, skipping", source_cfg.name)

        elif source_cfg.type == SourceType.SCRAPER:
            if source_cfg.name == "mako":
                sources.append(create_mako_source())
            elif source_cfg.name == "maariv":
                sources.append(create_maariv_source())
            elif source_cfg.name == "ice":
                sources.append(create_ice_source())
            elif source_cfg.name == "haaretz":
                sources.append(create_haaretz_source())
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
    unseen = [
        article
        for article in articles
        if not seen_store.is_seen(canonicalize_news_url(str(article.url)))
    ]
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

    return [
        {
            "source_name": source_name,
            "raw_article_count": article_counts.get(source_name, 0),
            "had_error": source_name in error_map,
            "error_messages": error_map.get(source_name, []),
            "returned_zero_results": article_counts.get(source_name, 0) == 0
            and source_name not in error_map,
        }
        for source_name in source_names
    ]


def _build_classifier_summary(
    *,
    unseen_articles: list[RawArticle],
    classified_articles: list[ClassifiedArticle],
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
        "classification_output_anomaly": len(classified_articles) != len(unseen_articles),
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
    source_names: list[str],
    raw_articles: list[RawArticle],
    unseen_articles: list[RawArticle],
    classified_articles: list[ClassifiedArticle],
    unified_items: list[UnifiedItem],
) -> dict[str, object]:
    """Build a detailed ingest diagnostic log for state-repo inspection."""
    relevant_articles = [
        article for article in classified_articles if article.classification.relevant
    ]
    rejected_articles = [
        article for article in classified_articles if not article.classification.relevant
    ]
    source_summaries = _build_source_summaries(
        source_names=source_names,
        raw_articles=raw_articles,
        errors=result.errors,
    )
    classifier_summary = _build_classifier_summary(
        unseen_articles=unseen_articles,
        classified_articles=classified_articles,
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
    )
    deduplicator = create_deduplicator(threshold=config.dedup.similarity_threshold)
    seen_store = create_seen_store(config.state_paths.seen_path)
    result.seen_count_before = seen_store.count

    all_articles, source_errors = await fetch_all_sources(
        sources=sources,
        days=days,
        keywords=config.keywords,
    )
    unseen_articles: list[RawArticle] = []
    classified_articles: list[ClassifiedArticle] = []
    unified_items: list[UnifiedItem] = []
    result.raw_article_count = len(all_articles)
    result.errors.extend(source_errors)
    if source_errors:
        result.warnings.append(f"{len(source_errors)} source(s) reported errors")

    if not all_articles:
        logger.info("No articles found from any source")
        result.seen_count_after = seen_store.count
        result.finish("no articles found")
        result.set_debug_payload(
            _build_ingest_debug_payload(
                result=result,
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
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
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
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

    classified_articles = await classifier.classify_batch(unseen_articles)
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
                source_names=source_names,
                raw_articles=all_articles,
                unseen_articles=unseen_articles,
                classified_articles=classified_articles,
                unified_items=unified_items,
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
            source_names=source_names,
            raw_articles=all_articles,
            unseen_articles=unseen_articles,
            classified_articles=classified_articles,
            unified_items=unified_items,
        )
    )
    return result


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
    rows = store.fetch_records(config.dataset_name.value)
    manifest = builder.build_release_bundle(
        publication_dir=config.state_paths.publication_dir,
        rows=rows,
    )
    published_targets = publish_release_bundle(
        config=config,
        release_dir=config.state_paths.publication_dir / manifest.release_version,
        manifest=manifest,
    )
    result.release_manifest = manifest.model_dump(mode="json")
    result.unified_item_count = manifest.row_count
    if published_targets:
        result.warnings.append(f"published_targets={','.join(published_targets)}")
        public_ids = [
            row.id
            for row in select_releasable_records(rows, release_version=manifest.release_version)
        ]
        store.mark_publication_state(
            config.dataset_name.value,
            public_ids,
            "published",
        )
    else:
        result.warnings.append("No publication targets configured; built release bundle only.")
    return result.finish(f"release built for {manifest.row_count} public row(s)")


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
    store = operational_store or create_operational_store(config)
    owns_store = operational_store is None
    result: RunSnapshot | None = None
    try:
        result = await handler(config, config_path, days_override, store)
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
        if owns_store:
            try:
                store.close()
            except Exception as exc:
                logger.warning("Failed to close operational store: %s", exc)
                if result is not None:
                    result.warnings.append(
                        f"operational_store_close_failed={type(exc).__name__}: {exc}"
                    )


def _load_config_or_exit(config_path: Path) -> Config:
    """Load a config file and exit with a helpful message on failure."""
    try:
        return load_config(config_path)
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    except Exception as exc:
        print(f"Error loading config: {exc}")
        sys.exit(1)


def _run_job_from_config(
    *,
    config_path: Path,
    dataset_name: DatasetName | None,
    job_name: JobName | None,
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Shared sync wrapper for CLI-triggered job runs."""
    setup_logging()
    config = _load_config_or_exit(config_path)

    update: dict[str, object] = {}
    if dataset_name is not None:
        update["dataset_name"] = DatasetName(dataset_name)
    if job_name is not None:
        update["job_name"] = JobName(job_name)
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
    days_override: int | None = None,
    operational_store: OperationalStore | None = None,
) -> None:
    """Run a dataset/job pair through the generic registry."""
    _run_job_from_config(
        config_path=config_path,
        dataset_name=dataset_name,
        job_name=job_name,
        days_override=days_override,
        operational_store=operational_store,
    )


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
