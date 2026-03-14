"""Integration tests for scrapers with fixture HTML."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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

    @staticmethod
    def _create_scraper() -> MakoScraper:
        """Create a scraper with rate limiting disabled for tests."""
        return MakoScraper(rate_limit_delay_seconds=0)

    @pytest.mark.asyncio
    async def test_parse_search_results(self) -> None:
        """Test parsing Mako search results HTML."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()

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
        scraper = self._create_scraper()

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
        scraper = self._create_scraper()

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
        scraper = self._create_scraper()
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
        scraper = self._create_scraper()

        async def open_browser_session() -> object:
            raise RuntimeError("Chromium could not be launched")

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור"])

        assert articles == []

    @pytest.mark.asyncio
    async def test_continues_after_keyword_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test a single keyword failure does not discard other collected articles."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session

        async def fetch_search_html(
            session: object, keyword: str, *, include_channel_ids: bool
        ) -> str:
            del session, include_channel_ids
            if keyword == "סרסור":
                raise RuntimeError("temporary browser timeout")
            return html_content

        async def fetch_section_html(session: object, url: str) -> str:
            del session, url
            return "<html></html>"

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser_session)
        monkeypatch.setattr(scraper, "_fetch_search_html", fetch_search_html)
        monkeypatch.setattr(scraper, "_fetch_section_html", fetch_section_html)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור", "זנות"])

        assert len(articles) == 2
        assert all("mako.co.il" in str(article.url) for article in articles)

    @pytest.mark.asyncio
    async def test_continues_after_section_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test section scrape failures do not discard keyword results."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()

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
            raise RuntimeError("section page timed out")

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser_session)
        monkeypatch.setattr(scraper, "_fetch_search_html", fetch_search_html)
        monkeypatch.setattr(scraper, "_fetch_section_html", fetch_section_html)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור"])

        assert len(articles) == 2

    @pytest.mark.asyncio
    async def test_build_search_url_toggles_channel_ids(self) -> None:
        """Test search URL generation keeps ids only on the primary query."""
        scraper = self._create_scraper()

        with_ids = scraper._build_search_url("זנות", include_channel_ids=True)
        without_ids = scraper._build_search_url("זנות", include_channel_ids=False)

        assert "channelId=" in with_ids
        assert "vgnextoid=" in with_ids
        assert "channelId=" not in without_ids
        assert "vgnextoid=" not in without_ids

    @pytest.mark.asyncio
    async def test_fetch_rendered_html_waits_for_challenge(self) -> None:
        """Test rendered HTML fetching waits through challenge redirects."""
        scraper = self._create_scraper()

        class FakePage:
            def __init__(self) -> None:
                self.url = "https://validate.perfdrive.com/challenge"
                self.goto_calls: list[str] = []
                self.wait_for_function_args: tuple[str, list[str], int] | None = None
                self.waited_for_timeout_ms: int | None = None
                self.waited_for_url = False

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                self.goto_calls.append(url)
                assert wait_until == "domcontentloaded"
                assert timeout > 0

            async def wait_for_url(self, url: Any, wait_until: str, timeout: int) -> None:
                del url
                self.waited_for_url = True
                self.url = (
                    "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA"
                )
                assert wait_until == "domcontentloaded"
                assert timeout > 0

            async def wait_for_function(
                self, expression: str, arg: list[str], timeout: int
            ) -> None:
                self.wait_for_function_args = (expression, arg, timeout)

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                self.waited_for_timeout_ms = timeout_ms

            async def content(self) -> str:
                return "<html><body><li class='articleins'></li></body></html>"

        page = FakePage()
        html = await scraper._fetch_rendered_html(
            page,
            "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA",
            ["li.articleins"],
            "search test",
        )

        assert page.goto_calls
        assert page.waited_for_url is True
        assert page.wait_for_function_args is not None
        assert page.waited_for_timeout_ms is not None
        assert "articleins" in html

    @pytest.mark.asyncio
    async def test_fetch_rendered_html_raises_on_timeout(self) -> None:
        """Test rendered HTML fetching raises a clear runtime error on timeout."""
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/Search"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def wait_for_function(
                self, expression: str, arg: list[str], timeout: int
            ) -> None:
                del expression, arg, timeout
                raise PlaywrightTimeoutError("timed out")

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                del timeout_ms

            async def content(self) -> str:
                return ""

        with pytest.raises(RuntimeError, match="never became parseable"):
            await scraper._fetch_rendered_html(
                FakePage(),
                "https://www.mako.co.il/Search",
                ["li.articleins"],
                "search timeout test",
            )

    @pytest.mark.asyncio
    async def test_open_and_close_browser_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test browser session setup and cleanup with a mocked Playwright stack."""
        import playwright.async_api as playwright_async_api

        scraper = self._create_scraper()
        events: list[str] = []

        class FakePage:
            pass

        class FakeContext:
            def __init__(self) -> None:
                self.page = FakePage()

            async def new_page(self) -> FakePage:
                events.append("new_page")
                return self.page

            async def close(self) -> None:
                events.append("context_close")

        class FakeBrowser:
            def __init__(self) -> None:
                self.context = FakeContext()

            async def new_context(self, **kwargs: Any) -> FakeContext:
                events.append("new_context")
                assert kwargs["locale"] == "he-IL"
                assert kwargs["viewport"]["width"] == 1440
                return self.context

            async def close(self) -> None:
                events.append("browser_close")

        class FakeChromium:
            async def launch(self, headless: bool) -> FakeBrowser:
                events.append("launch")
                assert headless is True
                return FakeBrowser()

        class FakePlaywright:
            chromium = FakeChromium()

        class FakeManager:
            async def __aenter__(self) -> FakePlaywright:
                events.append("enter")
                return FakePlaywright()

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                del exc_type, exc, tb
                events.append("exit")

        monkeypatch.setattr(playwright_async_api, "async_playwright", lambda: FakeManager())

        session = await scraper._open_browser_session()
        await scraper._close_browser_session(session)

        assert session.page is not None
        assert events == [
            "enter",
            "launch",
            "new_context",
            "new_page",
            "context_close",
            "browser_close",
            "exit",
        ]

    @pytest.mark.asyncio
    async def test_open_browser_session_handles_launch_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test browser session setup raises a clear error when Chromium cannot launch."""
        import playwright.async_api as playwright_async_api

        scraper = self._create_scraper()
        events: list[str] = []

        class FakeChromium:
            async def launch(self, headless: bool) -> object:
                del headless
                raise RuntimeError("launch failed")

        class FakePlaywright:
            chromium = FakeChromium()

        class FakeManager:
            async def __aenter__(self) -> FakePlaywright:
                events.append("enter")
                return FakePlaywright()

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                del exc_type, exc, tb
                events.append("exit")

        monkeypatch.setattr(playwright_async_api, "async_playwright", lambda: FakeManager())

        with pytest.raises(RuntimeError, match="Chromium could not be launched"):
            await scraper._open_browser_session()

        assert events == ["enter", "exit"]

    def test_parse_hebrew_date_rejects_invalid_two_digit_date(self) -> None:
        """Test that invalid dd/mm/yy metadata does not parse as a real date."""
        scraper = self._create_scraper()

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
