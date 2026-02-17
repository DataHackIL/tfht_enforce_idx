"""Integration tests for scrapers with fixture HTML."""

from pathlib import Path

import pytest
import respx
from httpx import Response

from denbust.sources.maariv import MaarivScraper
from denbust.sources.mako import MakoScraper

# Load fixture files
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(path: str) -> str:
    """Load a fixture file."""
    return (FIXTURES_DIR / path).read_text(encoding="utf-8")


class TestMakoScraper:
    """Integration tests for Mako scraper."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_parse_search_results(self) -> None:
        """Test parsing Mako search results HTML."""
        html_content = load_fixture("html/mako_search.html")

        # Mock the search endpoint
        respx.get("https://www.mako.co.il/AjaxPage").mock(
            return_value=Response(200, text=html_content)
        )

        # Mock the section page (returns empty)
        respx.get("https://www.mako.co.il/men-men_news").mock(
            return_value=Response(200, text="<html></html>")
        )

        scraper = MakoScraper()
        articles = await scraper.fetch(days=14, keywords=["סרסור"])

        # Should find articles from the fixture
        assert len(articles) >= 1

        # Check article properties
        for article in articles:
            assert article.source_name == "mako"
            assert "mako.co.il" in str(article.url)
            assert article.title

    @respx.mock
    @pytest.mark.asyncio
    async def test_deduplicates_results(self) -> None:
        """Test that scraper deduplicates articles."""
        html_content = load_fixture("html/mako_search.html")

        # Return same content for multiple searches
        respx.get("https://www.mako.co.il/AjaxPage").mock(
            return_value=Response(200, text=html_content)
        )
        respx.get("https://www.mako.co.il/men-men_news").mock(
            return_value=Response(200, text=html_content)
        )

        scraper = MakoScraper()
        # Search with multiple keywords
        articles = await scraper.fetch(days=14, keywords=["סרסור", "זנות", "בית בושת"])

        # Should deduplicate by URL
        urls = [str(a.url) for a in articles]
        assert len(urls) == len(set(urls))


class TestMaarivScraper:
    """Integration tests for Maariv scraper."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_empty_results(self) -> None:
        """Test handling empty search results."""
        # Return empty HTML
        respx.get("https://www.maariv.co.il/news/law").mock(
            return_value=Response(200, text="<html><body></body></html>")
        )
        respx.get("https://www.maariv.co.il/search").mock(
            return_value=Response(200, text="<html><body></body></html>")
        )

        scraper = MaarivScraper()
        articles = await scraper.fetch(days=14, keywords=["test"])

        # Should return empty list, not crash
        assert articles == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_http_error(self) -> None:
        """Test handling HTTP errors gracefully."""
        respx.get("https://www.maariv.co.il/news/law").mock(return_value=Response(500))
        respx.get("https://www.maariv.co.il/search").mock(return_value=Response(404))

        scraper = MaarivScraper()
        articles = await scraper.fetch(days=14, keywords=["test"])

        # Should return empty list, not crash
        assert articles == []


class TestRSSSource:
    """Integration tests for RSS source."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_parse_rss_feed(self) -> None:
        """Test parsing RSS feed."""
        from denbust.sources.rss import RSSSource

        rss_content = load_fixture("rss/ynet_sample.xml")

        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(200, text=rss_content))

        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        articles = await source.fetch(days=14, keywords=["בית בושת", "זנות", "צו סגירה"])

        # Should find matching articles
        assert len(articles) >= 1

        # Check article properties
        for article in articles:
            assert article.source_name == "ynet"
            assert "ynet.co.il" in str(article.url)

    @respx.mock
    @pytest.mark.asyncio
    async def test_filters_by_keywords(self) -> None:
        """Test that RSS source filters by keywords."""
        from denbust.sources.rss import RSSSource

        rss_content = load_fixture("rss/ynet_sample.xml")

        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(200, text=rss_content))

        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")

        # Search for keyword that doesn't match
        articles = await source.fetch(days=14, keywords=["מילה_שלא_קיימת"])

        # Should not find any articles
        assert len(articles) == 0

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_feed_error(self) -> None:
        """Test handling feed errors gracefully."""
        from denbust.sources.rss import RSSSource

        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(500))

        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        articles = await source.fetch(days=14, keywords=["test"])

        # Should return empty list, not crash
        assert articles == []
