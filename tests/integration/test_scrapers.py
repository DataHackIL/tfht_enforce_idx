"""Integration tests for scrapers with fixture HTML."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import respx
from httpx import Response

from denbust.sources.maariv import MaarivScraper
from denbust.sources.mako import SEARCH_POLL_INTERVAL_MS, MakoScraper

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

    def test_normalize_article_url_strips_search_tracking_params(self) -> None:
        """Test search tracking params are removed from Mako article URLs."""
        scraper = self._create_scraper()

        normalized = scraper._normalize_article_url(
            "https://www.mako.co.il/men-men_news/Article-af751f94b02ec91027.htm?Partner=searchResults"
        )

        assert (
            normalized
            == "https://www.mako.co.il/men-men_news/Article-af751f94b02ec91027.htm"
        )

    @pytest.mark.asyncio
    async def test_fetch_aggregates_browser_results(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that fetch aggregates browser-rendered search and section HTML."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session

        async def fetch_search_html(session: object, keyword: str) -> str:
            del session, keyword
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
    async def test_fetch_uses_only_canonical_search_url(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test fetch uses one canonical search request per keyword."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()
        calls: list[str] = []

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session

        async def fetch_search_html(session: object, keyword: str) -> str:
            del session
            calls.append(keyword)
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
        assert calls == ["סרסור"]

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

        async def fetch_search_html(session: object, keyword: str) -> str:
            del session
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

        async def fetch_search_html(session: object, keyword: str) -> str:
            del session, keyword
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
        """Test search URL generation always keeps opaque ids on the canonical query."""
        scraper = self._create_scraper()

        url = scraper._build_search_url("זנות")

        assert "channelId=" in url
        assert "vgnextoid=" in url

    @pytest.mark.asyncio
    async def test_fetch_search_html_waits_for_real_result_cards(self) -> None:
        """Test search HTML waits until real result cards appear, not just the shell."""
        scraper = self._create_scraper()
        html_states = [
            "<html><body><input name='searchstring_input' value='זנות'></body></html>",
            "<html><body><input name='searchstring_input' value='זנות'><li class='articleins'><a href='/men-men_news/Article-123.htm?Partner=searchResults'>כתבה</a></li></body></html>",
        ]

        class FakePage:
            def __init__(self) -> None:
                self.url = (
                    "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA"
                )
                self.goto_calls: list[str] = []
                self.waited_for_timeout_ms: list[int] = []
                self.state_index = 0

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                self.goto_calls.append(url)
                assert wait_until == "domcontentloaded"
                assert timeout > 0

            async def title(self) -> str:
                return "mako חדשות. בידור. טלוויזיה"

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                self.waited_for_timeout_ms.append(timeout_ms)
                if timeout_ms == SEARCH_POLL_INTERVAL_MS:
                    self.state_index = min(self.state_index + 1, len(html_states) - 1)

            async def content(self) -> str:
                return html_states[self.state_index]

        page = FakePage()
        html = await scraper._fetch_search_results_html(
            page,
            "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA",
            "זנות",
        )

        assert page.goto_calls
        assert SEARCH_POLL_INTERVAL_MS in page.waited_for_timeout_ms
        assert page.waited_for_timeout_ms[-1] != SEARCH_POLL_INTERVAL_MS
        assert "articleins" in html

    @pytest.mark.asyncio
    async def test_fetch_search_html_waits_for_challenge(self) -> None:
        """Test search HTML fetching waits through challenge redirects."""
        scraper = self._create_scraper()

        class FakePage:
            def __init__(self) -> None:
                self.url = "https://validate.perfdrive.com/challenge"
                self.goto_calls: list[str] = []
                self.waited_for_timeout_ms: list[int] = []
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

            async def title(self) -> str:
                return "mako חדשות. בידור. טלוויזיה"

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                self.waited_for_timeout_ms.append(timeout_ms)

            async def content(self) -> str:
                return "<html><body><li class='articleins'><a href='/men-men_news/Article-123.htm?Partner=searchResults'>כתבה</a></li></body></html>"

        page = FakePage()
        html = await scraper._fetch_search_results_html(
            page,
            "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA",
            "זנות",
        )

        assert page.goto_calls
        assert page.waited_for_url is True
        assert page.waited_for_timeout_ms
        assert "articleins" in html

    @pytest.mark.asyncio
    async def test_fetch_search_html_returns_none_for_not_found(self) -> None:
        """Test search HTML treats Mako not-found as a terminal no-result state."""
        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/not-found"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def title(self) -> str:
                return "הודעת שגיאה |mako"

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                del timeout_ms

            async def content(self) -> str:
                return "<html><body><h1>העמוד שחיפשת לא נמצא</h1></body></html>"

        html = await scraper._fetch_search_results_html(
            FakePage(),
            "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA",
            "זנות",
        )

        assert html is None

    @pytest.mark.asyncio
    async def test_fetch_search_html_returns_empty_results_page(self) -> None:
        """Test search HTML returns cleanly when Mako renders an explicit empty state."""
        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def title(self) -> str:
                return "mako חדשות. בידור. טלוויזיה"

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                del timeout_ms

            async def content(self) -> str:
                return "<html><body><div>לא נמצאו תוצאות</div></body></html>"

        html = await scraper._fetch_search_results_html(
            FakePage(),
            "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA",
            "זנות",
        )

        assert html is not None
        assert "לא נמצאו תוצאות" in html

    @pytest.mark.asyncio
    async def test_fetch_search_html_raises_actionable_timeout(self) -> None:
        """Test search HTML timeout includes actionable diagnostics."""

        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/Search"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def title(self) -> str:
                return "mako חדשות. בידור. טלוויזיה"

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                del timeout_ms

            async def content(self) -> str:
                return "<html><body><input name='searchstring_input' value='זנות'></body></html>"

        with pytest.raises(RuntimeError, match="did not reach a terminal state"):
            await scraper._fetch_search_results_html(
                FakePage(),
                "https://www.mako.co.il/Search",
                "זנות",
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
                self.route_handler: Any = None

            async def new_page(self) -> FakePage:
                events.append("new_page")
                return self.page

            async def route(self, pattern: str, handler: Any) -> None:
                assert pattern == "**/*"
                self.route_handler = handler
                events.append("route")

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
            "route",
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

    @pytest.mark.asyncio
    async def test_browser_route_blocks_nonessential_resources(self) -> None:
        """Test browser routing blocks noisy third-party resources and media assets."""
        scraper = self._create_scraper()

        class FakeRequest:
            def __init__(self, resource_type: str, url: str) -> None:
                self.resource_type = resource_type
                self.url = url

        class FakeRoute:
            def __init__(self, request: FakeRequest) -> None:
                self.request = request
                self.action: str | None = None

            async def abort(self) -> None:
                self.action = "abort"

            async def continue_(self) -> None:
                self.action = "continue"

        blocked_route = FakeRoute(
            FakeRequest("image", "https://securepubads.g.doubleclick.net/tag")
        )
        allowed_route = FakeRoute(FakeRequest("script", "https://www.mako.co.il/js/app.js"))

        await scraper._handle_browser_route(blocked_route)
        await scraper._handle_browser_route(allowed_route)

        assert blocked_route.action == "abort"
        assert allowed_route.action == "continue"


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
