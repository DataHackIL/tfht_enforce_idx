"""Integration tests for the news scanning pipeline."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import respx
from httpx import Response

from denbust.config import Config, SourceConfig, SourceType
from denbust.models import Category, RawArticle
from denbust.pipeline import (
    create_sources,
    deduplicate_articles,
    fetch_all_sources,
    filter_seen,
)
from denbust.sources.rss import RSSSource
from denbust.store.seen import SeenStore

# Load fixture files
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(path: str) -> str:
    """Load a fixture file."""
    return (FIXTURES_DIR / path).read_text(encoding="utf-8")


class TestCreateSources:
    """Tests for create_sources function."""

    def test_create_rss_sources(self) -> None:
        """Test creating RSS sources from config."""
        config = Config(
            sources=[
                SourceConfig(
                    name="ynet",
                    type=SourceType.RSS,
                    url="https://ynet.co.il/feed.xml",
                ),
                SourceConfig(
                    name="walla",
                    type=SourceType.RSS,
                    url="https://walla.co.il/feed.xml",
                ),
            ]
        )

        sources = create_sources(config)

        assert len(sources) == 2
        assert all(isinstance(s, RSSSource) for s in sources)

    def test_create_scraper_sources(self) -> None:
        """Test creating scraper sources from config."""
        config = Config(
            sources=[
                SourceConfig(name="mako", type=SourceType.SCRAPER),
                SourceConfig(name="maariv", type=SourceType.SCRAPER),
            ]
        )

        sources = create_sources(config)

        assert len(sources) == 2

    def test_disabled_source_not_created(self) -> None:
        """Test that disabled sources are not created."""
        config = Config(
            sources=[
                SourceConfig(
                    name="ynet",
                    type=SourceType.RSS,
                    url="https://ynet.co.il/feed.xml",
                    enabled=True,
                ),
                SourceConfig(
                    name="walla",
                    type=SourceType.RSS,
                    url="https://walla.co.il/feed.xml",
                    enabled=False,
                ),
            ]
        )

        sources = create_sources(config)

        assert len(sources) == 1


class TestFetchAllSources:
    """Tests for fetch_all_sources function."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_rss_source(self) -> None:
        """Test fetching from RSS source."""
        rss_content = load_fixture("rss/ynet_sample.xml")

        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(200, text=rss_content))

        sources = [RSSSource("ynet", "https://ynet.co.il/feed.xml")]
        keywords = ["בית בושת", "זנות", "סרסור"]

        articles = await fetch_all_sources(sources, days=14, keywords=keywords)

        # Should find articles matching keywords
        assert len(articles) >= 1
        assert all(isinstance(a, RawArticle) for a in articles)


class TestFilterSeen:
    """Tests for filter_seen function."""

    def test_filter_seen_urls(self, tmp_path: Path) -> None:
        """Test filtering already seen URLs."""
        seen_store = SeenStore(tmp_path / "seen.json")
        seen_store.mark_seen(["https://example.com/1"])

        articles = [
            RawArticle(
                url="https://example.com/1",
                title="Seen Article",
                snippet="...",
                date=datetime.now(UTC),
                source_name="test",
            ),
            RawArticle(
                url="https://example.com/2",
                title="New Article",
                snippet="...",
                date=datetime.now(UTC),
                source_name="test",
            ),
        ]

        unseen = filter_seen(articles, seen_store)

        assert len(unseen) == 1
        assert str(unseen[0].url) == "https://example.com/2"


class TestDeduplicateArticles:
    """Tests for deduplicate_articles function."""

    def test_deduplicate_similar_articles(self) -> None:
        """Test deduplicating similar articles."""
        from denbust.dedup.similarity import Deduplicator
        from denbust.models import ClassificationResult, ClassifiedArticle

        articles = [
            ClassifiedArticle(
                article=RawArticle(
                    url="https://ynet.co.il/1",
                    title="פשיטה על בית בושת בתל אביב",
                    snippet="המשטרה פשטה על דירה...",
                    date=datetime.now(UTC),
                    source_name="ynet",
                ),
                classification=ClassificationResult(
                    relevant=True,
                    category=Category.BROTHEL,
                ),
            ),
            ClassifiedArticle(
                article=RawArticle(
                    url="https://walla.co.il/1",
                    title="פשיטה על בית בושת בתל אביב",
                    snippet="פשיטת משטרה על דירה...",
                    date=datetime.now(UTC),
                    source_name="walla",
                ),
                classification=ClassificationResult(
                    relevant=True,
                    category=Category.BROTHEL,
                ),
            ),
        ]

        deduplicator = Deduplicator(similarity_threshold=0.7)
        items = deduplicate_articles(articles, deduplicator)

        # Should be deduplicated to 1 item
        assert len(items) == 1
        # With 2 sources
        assert len(items[0].sources) == 2


class TestPipelineIntegration:
    """End-to-end integration tests."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_full_pipeline_mocked(self, tmp_path: Path) -> None:
        """Test full pipeline with mocked HTTP responses."""
        # Mock RSS feed
        rss_content = load_fixture("rss/ynet_sample.xml")
        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(200, text=rss_content))

        # Create config
        config = Config(
            sources=[
                SourceConfig(
                    name="ynet",
                    type=SourceType.RSS,
                    url="https://ynet.co.il/feed.xml",
                ),
            ],
            keywords=["בית בושת", "זנות", "סרסור", "צו סגירה"],
        )
        config_dict = config.model_dump()
        config_dict["store"] = {"path": str(tmp_path / "seen.json")}

        # Mock classifier
        mock_classifier = MagicMock()
        mock_classifier.classify = AsyncMock(
            return_value=MagicMock(
                relevant=True,
                category=Category.BROTHEL,
            )
        )

        # Run pipeline components
        sources = create_sources(config)
        assert len(sources) == 1

        articles = await fetch_all_sources(sources, days=14, keywords=config.keywords)

        # Should find some matching articles
        assert len(articles) >= 1

        # Verify article properties
        for article in articles:
            assert article.source_name == "ynet"
            assert "ynet.co.il" in str(article.url)
