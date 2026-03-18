"""Pipeline orchestration and dataset/job dispatch."""

from __future__ import annotations

import asyncio
import logging
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
from denbust.ops.storage import NullOperationalStore, OperationalStore
from denbust.output.email import send_email_report
from denbust.output.formatter import print_items
from denbust.publish.backup import NullBackupExecutor
from denbust.publish.release import NullReleaseBuilder
from denbust.sources.base import Source
from denbust.sources.haaretz import create_haaretz_source
from denbust.sources.ice import create_ice_source
from denbust.sources.maariv import create_maariv_source
from denbust.sources.mako import create_mako_source
from denbust.sources.rss import RSSSource
from denbust.sources.walla import create_walla_source
from denbust.store.run_snapshots import write_run_snapshot
from denbust.store.seen import SeenStore, create_seen_store

logger = logging.getLogger(__name__)


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
    unseen = [article for article in articles if not seen_store.is_seen(str(article.url))]
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
        urls.extend(str(source.url) for source in item.sources)

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


async def run_news_ingest_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
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
    result.raw_article_count = len(all_articles)
    result.errors.extend(source_errors)
    if source_errors:
        result.warnings.append(f"{len(source_errors)} source(s) reported errors")

    if not all_articles:
        logger.info("No articles found from any source")
        result.seen_count_after = seen_store.count
        return result.finish("no articles found")

    unseen_articles = filter_seen(all_articles, seen_store)
    result.unseen_article_count = len(unseen_articles)
    if not unseen_articles:
        logger.info("All articles were already seen")
        result.seen_count_after = seen_store.count
        return result.finish("all fetched articles were already seen")

    if len(unseen_articles) > config.max_articles:
        warning = (
            f"Article count ({len(unseen_articles)}) exceeds max_articles threshold "
            f"({config.max_articles}). Consider adding a pre-filter stage or reducing "
            f"the number of days/sources. Proceeding with classification anyway."
        )
        logger.warning(warning)
        result.warnings.append(warning)

    relevant_articles = await classify_articles(unseen_articles, classifier)
    result.relevant_article_count = len(relevant_articles)
    if not relevant_articles:
        logger.info("No relevant articles found")
        result.seen_count_after = seen_store.count
        return result.finish("no relevant articles found")

    unified_items = deduplicate_articles(relevant_articles, deduplicator)
    result.unified_item_count = len(unified_items)
    result.items = unified_items

    mark_seen(unified_items, seen_store)
    result.seen_count_after = seen_store.count
    return result.finish(f"ingest completed with {len(unified_items)} unified item(s)")


async def run_pipeline_async(config: Config, days: int) -> RunSnapshot:
    """Backward-compatible alias for the news ingest job."""
    ingest_config = config.model_copy(
        update={
            "dataset_name": DatasetName.NEWS_ITEMS,
            "job_name": JobName.INGEST,
        }
    )
    return await run_news_ingest_job(ingest_config, config_path=None, days_override=days)


async def run_scaffolded_release_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
) -> RunSnapshot:
    """Emit a scaffold run result for the future release job."""
    del days_override
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    builder = NullReleaseBuilder()
    manifest = builder.build_manifest(config.dataset_name, config.state_paths.publication_dir)
    result.release_manifest = manifest.model_dump(mode="json")
    result.warnings.append("Release generation is scaffolded but not implemented in Phase A")
    return result.finish("release job scaffold executed")


async def run_scaffolded_backup_job(
    config: Config,
    config_path: Path | None = None,
    days_override: int | None = None,
) -> RunSnapshot:
    """Emit a scaffold run result for the future backup job."""
    del days_override
    result = _build_run_snapshot(config, config_path=config_path, days=config.days)
    executor = NullBackupExecutor()
    manifest = executor.build_manifest(config.dataset_name, config.state_paths.state_root)
    result.backup_manifest = manifest.model_dump(mode="json")
    result.warnings.append("Backup execution is scaffolded but not implemented in Phase A")
    return result.finish("backup job scaffold executed")


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
    result = await handler(config, config_path, days_override)
    (operational_store or NullOperationalStore()).write_run_metadata(result)
    return result


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
        update["dataset_name"] = dataset_name
    if job_name is not None:
        update["job_name"] = job_name
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
