"""Integration tests for scrapers with fixture HTML."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import respx
from httpx import Response

from denbust.sources.maariv import MaarivScraper
from denbust.sources.mako import MakoScraper

# Load fixture files
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
TEST_LOOKBACK_DAYS = 5000


def load_fixture(path: str) -> str:
    """Load a fixture file."""
    return (FIXTURES_DIR / path).read_text(encoding="utf-8")


class TestMakoScraper:
    """Integration tests for Mako scraper."""

    @pytest.mark.asyncio
    async def test_parse_search_results(self) -> None:
        """Test parsing Mako search results HTML."""
        html_content = load_fixture("html/mako_search.html")
        scraper = MakoScraper()

        articles = scraper._parse_search_results(
            html_content,
            datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS),
        )

        assert len(articles) >= 1
        assert any(article.date == datetime(2026, 3, 6, tzinfo=UTC) for article in articles)

        for article in articles:
            assert article.source_name == "mako"
            assert "mako.co.il" in str(article.url)
            assert article.title

    def test_parse_section_page_filters_keywords(self) -> None:
        """Test section-page parsing keeps only keyword-matching articles."""
        html_content = load_fixture("html/mako_search.html")
        scraper = MakoScraper()

        articles = scraper._parse_section_page(
            html_content,
            datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS),
            ["סרסור"],
        )

        assert len(articles) == 1
        assert articles[0].title == "נעצרו 3 חשודים בסרסור בדרום הארץ"

    @pytest.mark.asyncio
    async def test_fetch_aggregates_browser_results(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that fetch aggregates browser-rendered search and section HTML."""
        html_content = load_fixture("html/mako_search.html")
        scraper = MakoScraper()

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session

        async def fetch_search_html(
            session: object, keyword: str, *, include_channel_ids: bool
        ) -> str:
            del session, keyword, include_channel_ids
            return html_content

        async def fetch_section_html(session: object, url: str) -> str:
            del session, url
            return html_content

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser_session)
        monkeypatch.setattr(scraper, "_fetch_search_html", fetch_search_html)
        monkeypatch.setattr(scraper, "_fetch_section_html", fetch_section_html)

        articles = await scraper.fetch(
            days=TEST_LOOKBACK_DAYS,
            keywords=["סרסור", "זנות", "בית בושת"],
        )

        urls = [str(article.url) for article in articles]
        assert len(articles) == 2
        assert len(urls) == len(set(urls))

    @pytest.mark.asyncio
    async def test_retries_search_without_channel_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test fallback search without opaque channel ids when initial results are empty."""
        html_content = load_fixture("html/mako_search.html")
        scraper = MakoScraper()
        calls: list[tuple[str, bool]] = []

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session

        async def fetch_search_html(
            session: object, keyword: str, *, include_channel_ids: bool
        ) -> str:
            del session
            calls.append((keyword, include_channel_ids))
            if include_channel_ids:
                return "<html><body></body></html>"
            return html_content

        async def fetch_section_html(session: object, url: str) -> str:
            del session, url
            return "<html></html>"

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser_session)
        monkeypatch.setattr(scraper, "_fetch_search_html", fetch_search_html)
        monkeypatch.setattr(scraper, "_fetch_section_html", fetch_section_html)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור"])

        assert len(articles) >= 1
        assert calls == [("סרסור", True), ("סרסור", False)]

    @pytest.mark.asyncio
    async def test_handles_browser_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test browser failures are handled gracefully."""
        scraper = MakoScraper()

        async def open_browser_session() -> object:
            raise RuntimeError("Chromium could not be launched")

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור"])

        assert articles == []

    def test_parse_hebrew_date_rejects_invalid_two_digit_date(self) -> None:
        """Test that invalid dd/mm/yy metadata does not parse as a real date."""
        scraper = MakoScraper()

        assert scraper._parse_hebrew_date("פלילים+ | 32/13/26 | זמן קריאה: 2.3 דק'") is None


class TestMaarivScraper:
    """Integration tests for Maariv scraper."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_empty_results(self) -> None:
        """Test handling empty search results."""
        respx.get("https://www.maariv.co.il/news/law").mock(
            return_value=Response(200, text="<html><body></body></html>")
        )
        respx.get("https://www.maariv.co.il/search").mock(
            return_value=Response(200, text="<html><body></body></html>")
        )

        scraper = MaarivScraper()
        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["test"])

        assert articles == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_http_error(self) -> None:
        """Test handling HTTP errors gracefully."""
        respx.get("https://www.maariv.co.il/news/law").mock(return_value=Response(500))
        respx.get("https://www.maariv.co.il/search").mock(return_value=Response(404))

        scraper = MaarivScraper()
        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["test"])

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
        articles = await source.fetch(
            days=TEST_LOOKBACK_DAYS, keywords=["בית בושת", "זנות", "צו סגירה"]
        )

        assert len(articles) >= 1

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
        articles = await source.fetch(days=TEST_LOOKBACK_DAYS, keywords=["מילה_שלא_קיימת"])

        assert len(articles) == 0

    @respx.mock
    @pytest.mark.asyncio
    async def test_handles_feed_error(self) -> None:
        """Test handling feed errors gracefully."""
        from denbust.sources.rss import RSSSource

        respx.get("https://ynet.co.il/feed.xml").mock(return_value=Response(500))

        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        articles = await source.fetch(days=TEST_LOOKBACK_DAYS, keywords=["test"])

        assert articles == []
