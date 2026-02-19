"""Pipeline orchestration for news scanning."""

import asyncio
import logging
import sys
from pathlib import Path

from denbust.classifier.relevance import Classifier, create_classifier
from denbust.config import Config, SourceType, load_config
from denbust.dedup.similarity import Deduplicator, create_deduplicator
from denbust.data_models import ClassifiedArticle, RawArticle, UnifiedItem
from denbust.output.formatter import print_items
from denbust.sources.base import Source
from denbust.sources.maariv import create_maariv_source
from denbust.sources.mako import create_mako_source
from denbust.sources.rss import RSSSource
from denbust.store.seen import SeenStore, create_seen_store

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline.

    Args:
        verbose: Enable verbose logging.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )


def create_sources(config: Config) -> list[Source]:
    """Create source instances from config.

    Args:
        config: Configuration object.

    Returns:
        List of Source instances.
    """
    sources: list[Source] = []

    for source_cfg in config.sources:
        if not source_cfg.enabled:
            continue

        if source_cfg.type == SourceType.RSS:
            if source_cfg.url:
                sources.append(RSSSource(source_name=source_cfg.name, feed_url=source_cfg.url))
            else:
                logger.warning(f"RSS source {source_cfg.name} missing URL, skipping")

        elif source_cfg.type == SourceType.SCRAPER:
            if source_cfg.name == "mako":
                sources.append(create_mako_source())
            elif source_cfg.name == "maariv":
                sources.append(create_maariv_source())
            else:
                logger.warning(f"Unknown scraper source: {source_cfg.name}")

    logger.info(f"Created {len(sources)} news sources")
    return sources


async def fetch_all_sources(
    sources: list[Source], days: int, keywords: list[str]
) -> list[RawArticle]:
    """Fetch articles from all sources.

    Args:
        sources: List of sources to fetch from.
        days: Number of days back to search.
        keywords: Keywords to filter by.

    Returns:
        Combined list of raw articles.
    """
    all_articles: list[RawArticle] = []

    for source in sources:
        try:
            logger.info(f"Fetching from {source.name}...")
            articles = await source.fetch(days=days, keywords=keywords)
            all_articles.extend(articles)
            logger.info(f"Found {len(articles)} articles from {source.name}")
        except Exception as e:
            logger.error(f"Error fetching from {source.name}: {e}")

    logger.info(f"Total raw articles: {len(all_articles)}")
    return all_articles


def filter_seen(articles: list[RawArticle], seen_store: SeenStore) -> list[RawArticle]:
    """Filter out already-seen articles.

    Args:
        articles: List of articles.
        seen_store: Seen URL store.

    Returns:
        List of unseen articles.
    """
    unseen = [article for article in articles if not seen_store.is_seen(str(article.url))]
    logger.info(f"Filtered to {len(unseen)} unseen articles (was {len(articles)})")
    return unseen


async def classify_articles(
    articles: list[RawArticle], classifier: Classifier
) -> list[ClassifiedArticle]:
    """Classify all articles for relevance.

    Args:
        articles: List of raw articles.
        classifier: Classifier instance.

    Returns:
        List of classified articles (only relevant ones).
    """
    classified = await classifier.classify_batch(articles)

    # Filter to only relevant articles
    relevant = [c for c in classified if c.classification.relevant]
    logger.info(f"Classified {len(articles)} articles, {len(relevant)} are relevant")

    return relevant


def deduplicate_articles(
    articles: list[ClassifiedArticle], deduplicator: Deduplicator
) -> list[UnifiedItem]:
    """Deduplicate and unify articles.

    Args:
        articles: List of classified articles.
        deduplicator: Deduplicator instance.

    Returns:
        List of unified items.
    """
    items = deduplicator.deduplicate(articles)
    logger.info(f"Deduplicated to {len(items)} unique stories")
    return items


def mark_seen(items: list[UnifiedItem], seen_store: SeenStore) -> None:
    """Mark all URLs in unified items as seen.

    Args:
        items: Unified items.
        seen_store: Seen URL store.
    """
    urls = []
    for item in items:
        for source in item.sources:
            urls.append(str(source.url))

    seen_store.mark_seen(urls)
    seen_store.save()
    logger.info(f"Marked {len(urls)} URLs as seen")


async def run_pipeline_async(config: Config, days: int) -> list[UnifiedItem]:
    """Run the full pipeline asynchronously.

    Args:
        config: Configuration object.
        days: Number of days back to search.

    Returns:
        List of unified items.
    """
    # Check for API key
    if not config.anthropic_api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        return []

    # Create components
    sources = create_sources(config)
    if not sources:
        logger.warning("No sources configured")
        return []

    classifier = create_classifier(
        api_key=config.anthropic_api_key,
        model=config.classifier.model,
    )
    deduplicator = create_deduplicator(threshold=config.dedup.similarity_threshold)
    seen_store = create_seen_store(config.store.path)

    # 1. Fetch from all sources
    all_articles = await fetch_all_sources(
        sources=sources,
        days=days,
        keywords=config.keywords,
    )

    if not all_articles:
        logger.info("No articles found from any source")
        return []

    # 2. Filter out seen URLs
    unseen_articles = filter_seen(all_articles, seen_store)
    if not unseen_articles:
        logger.info("All articles were already seen")
        return []

    # 3. Check article count against max_articles threshold
    if len(unseen_articles) > config.max_articles:
        logger.warning(
            f"Article count ({len(unseen_articles)}) exceeds max_articles threshold "
            f"({config.max_articles}). Consider adding a pre-filter stage or reducing "
            f"the number of days/sources. Proceeding with classification anyway."
        )

    # 4. Classify articles
    relevant_articles = await classify_articles(unseen_articles, classifier)
    if not relevant_articles:
        logger.info("No relevant articles found")
        return []

    # 5. Deduplicate
    unified_items = deduplicate_articles(relevant_articles, deduplicator)

    # 6. Mark as seen
    mark_seen(unified_items, seen_store)

    return unified_items


def run_pipeline(config_path: Path, days_override: int | None = None) -> None:
    """Run the news scanning pipeline.

    Args:
        config_path: Path to YAML config file.
        days_override: Override days from config if provided.
    """
    setup_logging()

    # Load config
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Apply overrides
    days = days_override if days_override is not None else config.days

    logger.info(f"Starting pipeline: {config.name}, searching last {days} days")

    # Run async pipeline
    items = asyncio.run(run_pipeline_async(config, days))

    # Output results
    print_items(items)
