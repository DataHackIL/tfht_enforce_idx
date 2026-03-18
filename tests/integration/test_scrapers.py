"""Integration tests for scrapers with fixture HTML."""

import builtins
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx
from bs4 import BeautifulSoup, Tag
from httpx import Response

from denbust.data_models import RawArticle
from denbust.sources.haaretz import HaaretzScraper, create_haaretz_source
from denbust.sources.ice import IceScraper, create_ice_source
from denbust.sources.maariv import MaarivScraper, create_maariv_source
from denbust.sources.mako import SEARCH_POLL_INTERVAL_MS, MakoScraper, create_mako_source
from denbust.sources.rss import RSSSource
from denbust.sources.walla import WallaArchiveEntry, WallaScraper, create_walla_source

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

    def test_factory_helper_returns_named_source(self) -> None:
        """Factory helper should return the canonical Mako source."""
        scraper = create_mako_source()

        assert scraper.name == "mako"

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

        assert normalized == "https://www.mako.co.il/men-men_news/Article-af751f94b02ec91027.htm"

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
    async def test_fetch_logs_cleanup_failure_and_keeps_articles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Session cleanup failures should be logged after successful scraping."""
        html_content = load_fixture("html/mako_search.html")
        scraper = self._create_scraper()

        async def open_browser_session() -> object:
            return object()

        async def close_browser_session(session: object) -> None:
            del session
            raise RuntimeError("close failed")

        async def fetch_search_html(session: object, keyword: str) -> str:
            del session, keyword
            return html_content

        async def fetch_section_html(session: object, url: str) -> str:
            del session, url
            return "<html></html>"

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser_session)
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser_session)
        monkeypatch.setattr(scraper, "_fetch_search_html", fetch_search_html)
        monkeypatch.setattr(scraper, "_fetch_section_html", fetch_section_html)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["סרסור"])

        assert len(articles) == 2

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Rate limiting should sleep when configured with a positive delay."""
        scraper = MakoScraper(rate_limit_delay_seconds=0.25)
        calls: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            calls.append(seconds)

        monkeypatch.setattr("denbust.sources.mako.asyncio.sleep", fake_sleep)

        await scraper._rate_limit()

        assert calls == [0.25]

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
                assert kwargs["user_agent"].startswith("Mozilla/5.0")
                assert "Chrome/134.0.0.0" in kwargs["user_agent"]
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

    @pytest.mark.asyncio
    async def test_open_browser_session_reports_missing_playwright(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing Playwright should raise a clear runtime error."""
        scraper = self._create_scraper()
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "playwright.async_api":
                raise ImportError("missing playwright")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="Playwright is not installed"):
            await scraper._open_browser_session()

    def test_parse_hebrew_date_rejects_invalid_two_digit_date(self) -> None:
        """Test that invalid dd/mm/yy metadata does not parse as a real date."""
        scraper = self._create_scraper()

        assert scraper._parse_hebrew_date("פלילים+ | 32/13/26 | זמן קריאה: 2.3 דק'") is None

    def test_parse_hebrew_date_rejects_invalid_full_year_and_iso_date(self) -> None:
        """Invalid Mako dates should fail cleanly for all supported formats."""
        scraper = self._create_scraper()

        assert scraper._parse_hebrew_date("99/13/2026") is None
        assert scraper._parse_hebrew_date("2026-13-99") is None

    def test_parse_article_item_fallbacks_and_filters(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Article parsing should cover fallback selectors and filtering branches."""
        scraper = self._create_scraper()
        cutoff = datetime(2026, 3, 10, tzinfo=UTC)
        old_cutoff = datetime(2026, 3, 13, tzinfo=UTC)

        def parse(markup: str, *, parse_date: datetime | None = None) -> RawArticle | None:
            soup = BeautifulSoup(markup, "lxml")
            item = soup.select_one("li, article, div")
            monkeypatch.setattr(scraper, "_parse_date", lambda _item: parse_date)
            assert item is not None
            return scraper._parse_article_item(item, cutoff)

        article = parse(
            """
            <li>
              <a href="/men-men_news/Article-123.htm?Partner=searchResults">לינק</a>
              <h5>כותרת</h5>
              <p>תקציר</p>
            </li>
            """,
            parse_date=datetime(2026, 3, 12, tzinfo=UTC),
        )
        assert article is not None
        assert str(article.url).endswith("/men-men_news/Article-123.htm")

        empty_href = parse("<li><a href=''>לינק</a><h5>כותרת</h5></li>")
        assert empty_href is None

        non_article = parse("<li><a href='/tag/test'>לינק</a><h5>כותרת</h5></li>")
        assert non_article is None

        no_title = parse("<li><a href='/men-men_news/Article-123.htm'></a></li>")
        assert no_title is None

        monkeypatch.setattr(scraper, "_parse_date", lambda _item: datetime(2026, 3, 12, tzinfo=UTC))
        old_item = BeautifulSoup(
            "<li><a href='/men-men_news/Article-123.htm'>לינק</a><h5>כותרת</h5></li>",
            "lxml",
        ).select_one("li")
        assert old_item is not None
        assert scraper._parse_article_item(old_item, old_cutoff) is None

        monkeypatch.setattr(scraper, "_parse_date", lambda _item: None)
        no_date_item = BeautifulSoup(
            "<li><a href='/men-men_news/Article-456.htm'>לינק</a><h5>כותרת</h5></li>",
            "lxml",
        ).select_one("li")
        assert no_date_item is not None
        no_date_article = scraper._parse_article_item(no_date_item, cutoff)
        assert no_date_article is not None

        no_href_attr = BeautifulSoup(
            "<li><a>לינק</a><h5>כותרת</h5></li>",
            "lxml",
        ).select_one("li")
        assert no_href_attr is not None
        assert scraper._parse_article_item(no_href_attr, cutoff) is None

    def test_parse_date_falls_back_from_bad_datetime_and_text(self) -> None:
        """Date parsing should fall back from invalid datetime attrs to text parsing."""
        scraper = self._create_scraper()
        item = BeautifulSoup(
            """
            <li>
              <time datetime="not-a-date">פורסם: 06/03/26</time>
            </li>
            """,
            "lxml",
        ).select_one("li")

        assert item is not None
        assert scraper._parse_date(item) == datetime(2026, 3, 6, tzinfo=UTC)

    def test_parse_date_returns_none_without_any_recognized_date(self) -> None:
        """Date parsing should return None when no supported formats are present."""
        scraper = self._create_scraper()
        item = BeautifulSoup("<li><span>ללא תאריך</span></li>", "lxml").select_one("li")

        assert item is not None
        assert scraper._parse_date(item) is None

    @pytest.mark.asyncio
    async def test_fetch_search_html_delegates_to_search_results_helper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Search HTML helper should build the canonical URL and delegate once."""
        scraper = self._create_scraper()
        session = SimpleNamespace(page=object())
        calls: list[tuple[str, str]] = []

        async def fake_fetch(page: object, url: str, keyword: str) -> str:
            assert page is session.page
            calls.append((url, keyword))
            return "<html></html>"

        monkeypatch.setattr(scraper, "_fetch_search_results_html", fake_fetch)

        html = await scraper._fetch_search_html(session, "זנות")

        assert html == "<html></html>"
        assert calls == [(scraper._build_search_url("זנות"), "זנות")]

    @pytest.mark.asyncio
    async def test_search_keyword_returns_empty_on_missing_html(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Search helper should return no articles when no rendered HTML is available."""
        scraper = self._create_scraper()
        session = SimpleNamespace(page=object())

        async def fake_fetch_search_html(_session: object, _keyword: str) -> None:
            return None

        monkeypatch.setattr(scraper, "_fetch_search_html", fake_fetch_search_html)

        articles = await scraper._search_keyword(
            session, "זנות", datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS)
        )

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_section_html_delegates_to_rendered_html(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Section HTML helper should call the generic rendered-page helper."""
        scraper = self._create_scraper()
        session = SimpleNamespace(page=object())
        calls: list[tuple[object, str, list[str], str]] = []

        async def fake_fetch(page: object, url: str, selectors: list[str], description: str) -> str:
            calls.append((page, url, selectors, description))
            return "<html></html>"

        monkeypatch.setattr(scraper, "_fetch_rendered_html", fake_fetch)

        html = await scraper._fetch_section_html(session, "https://www.mako.co.il/men-men_news")

        assert html == "<html></html>"
        assert calls[0][0] is session.page
        assert calls[0][1] == "https://www.mako.co.il/men-men_news"
        assert calls[0][3] == "men-news section"

    @pytest.mark.asyncio
    async def test_fetch_rendered_html_raises_actionable_timeout(self) -> None:
        """Generic rendered-page fetch should surface parseability timeouts."""
        import playwright.async_api as playwright_async_api

        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/men-men_news"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def wait_for_function(self, *args: Any, **kwargs: Any) -> None:
                del args, kwargs
                raise playwright_async_api.TimeoutError("timed out")

        with pytest.raises(RuntimeError, match="never became parseable"):
            await scraper._fetch_rendered_html(
                FakePage(),
                "https://www.mako.co.il/men-men_news",
                ["article"],
                "men-news section",
            )

    @pytest.mark.asyncio
    async def test_fetch_rendered_html_returns_page_content(self) -> None:
        """Generic rendered-page fetch should return page content after readiness."""
        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/men-men_news"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def wait_for_function(self, *args: Any, **kwargs: Any) -> None:
                del args, kwargs

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                assert timeout_ms == 750

            async def content(self) -> str:
                return "<html><body><article>ready</article></body></html>"

        html = await scraper._fetch_rendered_html(
            FakePage(),
            "https://www.mako.co.il/men-men_news",
            ["article"],
            "men-news section",
        )

        assert "ready" in html

    @pytest.mark.asyncio
    async def test_fetch_rendered_html_reports_missing_playwright(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Generic rendered-page fetch should report missing Playwright imports."""
        scraper = self._create_scraper()
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "playwright.async_api":
                raise ImportError("missing playwright")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="Playwright is not installed"):
            await scraper._fetch_rendered_html(
                object(),
                "https://www.mako.co.il/men-men_news",
                ["article"],
                "men-news section",
            )

    @pytest.mark.asyncio
    async def test_fetch_search_html_reports_navigation_timeout(self) -> None:
        """Search helper should raise a clear error on initial navigation timeout."""
        import playwright.async_api as playwright_async_api

        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/Search"

            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout
                raise playwright_async_api.TimeoutError("timeout")

        with pytest.raises(RuntimeError, match="navigation timed out"):
            await scraper._fetch_search_results_html(
                FakePage(),
                "https://www.mako.co.il/Search",
                "זנות",
            )

    @pytest.mark.asyncio
    async def test_fetch_search_html_reports_missing_playwright(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Search helper should report missing Playwright imports."""
        scraper = self._create_scraper()
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "playwright.async_api":
                raise ImportError("missing playwright")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="Playwright is not installed"):
            await scraper._fetch_search_results_html(
                object(),
                "https://www.mako.co.il/Search",
                "זנות",
            )

    @pytest.mark.asyncio
    async def test_wait_for_challenge_resolution_false_without_challenge(self) -> None:
        """Challenge wait should return false when already on Mako."""
        scraper = self._create_scraper()

        class FakePage:
            url = "https://www.mako.co.il/Search?searchstring_input=%D7%96%D7%A0%D7%95%D7%AA"

        assert await scraper._wait_for_challenge_resolution(FakePage(), "search") is False

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

    @pytest.mark.asyncio
    async def test_browser_route_blocks_known_tracker_urls(self) -> None:
        """Known noisy third-party tracker URLs should be blocked even for scripts."""
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
            FakeRequest("script", "https://www.googletagmanager.com/gtm.js?id=123")
        )

        await scraper._handle_browser_route(blocked_route)

        assert blocked_route.action == "abort"


class TestIceScraper:
    """Integration tests for ICE scraper."""

    @staticmethod
    def _create_scraper() -> IceScraper:
        """Create a scraper with rate limiting disabled for tests."""
        return IceScraper(rate_limit_delay_seconds=0)

    def test_factory_helper_returns_named_source(self) -> None:
        """Factory helper should return the canonical ICE source."""
        scraper = create_ice_source()

        assert scraper.name == "ice"

    def test_build_search_url_uses_expected_path_shape(self) -> None:
        """ICE search URLs should use the path-based search pattern."""
        scraper = self._create_scraper()

        assert scraper._build_search_url("בית בושת") == (
            "https://www.ice.co.il/list/searchresult/%D7%91%D7%99%D7%AA%20%D7%91%D7%95%D7%A9%D7%AA"
        )
        assert scraper._build_search_url("בית בושת", page_number=2) == (
            "https://www.ice.co.il/list/searchresult/%D7%91%D7%99%D7%AA%20%D7%91%D7%95%D7%A9%D7%AA/page-2"
        )

    def test_parse_search_results(self) -> None:
        """Fixture HTML should parse into ICE search result articles."""
        html_content = load_fixture("html/ice_search.html")
        scraper = self._create_scraper()

        articles = scraper._parse_search_results(
            html_content,
            datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS),
        )

        assert len(articles) == 2
        assert articles[0].title == "חריג: הפכה את המקלט הציבורי לבית בושת"
        assert articles[0].date == datetime(2026, 3, 12, 7, 46, tzinfo=UTC)
        assert str(articles[0].url) == "https://www.ice.co.il/local-news/news/article/1099242"
        assert all(article.source_name == "ice" for article in articles)

    def test_parse_article_item_skips_missing_or_invalid_dates(self) -> None:
        """ICE items without a parseable date should be ignored safely."""
        scraper = self._create_scraper()
        cutoff = datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS)
        item = BeautifulSoup(
            """
            <li>
              <a href="/law/news/article/1086606">בית בושת אותר בתוך מקלט ציבורי</a>
              <a href="/law/news/article/1086606">עיריית בת ים פתחה בחקירה</a>
              <span>תאריך לא תקין</span>
            </li>
            """,
            "lxml",
        ).li

        assert item is not None
        assert scraper._parse_article_item(item, cutoff) is None

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Positive ICE rate limits should sleep between requests."""
        scraper = IceScraper(rate_limit_delay_seconds=0.25)
        calls: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            calls.append(seconds)

        monkeypatch.setattr("denbust.sources.ice.asyncio.sleep", fake_sleep)

        await scraper._rate_limit()

        assert calls == [0.25]

    @pytest.mark.asyncio
    async def test_search_keyword_returns_empty_without_client(self) -> None:
        """Keyword search should return no articles before a client is configured."""
        scraper = self._create_scraper()

        articles = await scraper._search_keyword(
            "בית בושת",
            datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS),
        )

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_search_page_returns_none_without_client(self) -> None:
        """Page fetches should no-op when no HTTP client exists."""
        scraper = self._create_scraper()

        html = await scraper._fetch_search_page("בית בושת", 1)

        assert html is None

    def test_parse_search_results_returns_empty_without_results_container(self) -> None:
        """Search parsing should return an empty list when the result container is absent."""
        scraper = self._create_scraper()

        articles = scraper._parse_search_results(
            "<html><body><h1>דף אחר</h1></body></html>",
            datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS),
        )

        assert articles == []

    def test_find_results_article_handles_missing_heading_and_empty_article(self) -> None:
        """Results lookup should fail cleanly when ICE search chrome is incomplete."""
        scraper = self._create_scraper()
        no_heading = BeautifulSoup(
            "<html><body><article><ul><li>test</li></ul></article></body></html>", "lxml"
        )
        no_items = BeautifulSoup(
            """
            <html>
              <body>
                <h1>נמצאו 2 תוצאות חיפוש</h1>
                <article><div>ללא רשימה</div></article>
              </body>
            </html>
            """,
            "lxml",
        )

        assert scraper._find_results_article(no_heading) is None
        assert scraper._find_results_article(no_items) is None

    def test_parse_article_item_returns_none_without_text_links(self) -> None:
        """Article parsing should skip matches whose candidate links have no visible text."""
        scraper = self._create_scraper()
        cutoff = datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS)

        class FakeLink:
            def get(self, key: str, default: str = "") -> str:
                return "/law/news/article/1086606" if key == "href" else default

            def get_text(self, separator: str = " ", strip: bool = False) -> str:
                del separator, strip
                return ""

        class FakeItem:
            def select(self, selector: str) -> list[FakeLink]:
                assert selector == "a[href]"
                return [FakeLink()]

            def select_one(self, selector: str) -> None:
                del selector
                return None

            def get_text(self, separator: str = " ", strip: bool = False) -> str:
                del separator, strip
                return "12/3/2026 6:15"

        assert scraper._parse_article_item(FakeItem(), cutoff) is None

    def test_parse_article_item_returns_none_when_title_becomes_empty(self) -> None:
        """A blank title after candidate filtering should be discarded."""
        scraper = self._create_scraper()
        cutoff = datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS)

        class FlakyLink:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, key: str, default: str = "") -> str:
                return "/law/news/article/1086606" if key == "href" else default

            def get_text(self, separator: str = " ", strip: bool = False) -> str:
                del separator, strip
                self.calls += 1
                return "כותרת" if self.calls == 1 else ""

        class FakeItem:
            def __init__(self) -> None:
                self.link = FlakyLink()

            def select(self, selector: str) -> list[FlakyLink]:
                assert selector == "a[href]"
                return [self.link]

            def select_one(self, selector: str) -> None:
                del selector
                return None

            def get_text(self, separator: str = " ", strip: bool = False) -> str:
                del separator, strip
                return "12/3/2026 6:15"

        assert scraper._parse_article_item(FakeItem(), cutoff) is None

    def test_parse_article_item_uses_paragraph_snippet_fallback(self) -> None:
        """Paragraph text should be used when no secondary snippet link exists."""
        scraper = self._create_scraper()
        cutoff = datetime.now(UTC) - timedelta(days=TEST_LOOKBACK_DAYS)
        item = BeautifulSoup(
            """
            <li>
              <a href="/law/news/article/1086606">בית בושת אותר בתוך מקלט ציבורי</a>
              <p>עיריית בת ים פתחה בחקירה</p>
              <span>12/3/2026 6:15</span>
            </li>
            """,
            "lxml",
        ).li

        assert item is not None
        article = scraper._parse_article_item(item, cutoff)

        assert article is not None
        assert article.snippet == "עיריית בת ים פתחה בחקירה"

    def test_parse_date_invalid_calendar_values_return_none(self) -> None:
        """Invalid calendar dates should be handled without raising."""
        scraper = self._create_scraper()

        assert scraper._parse_date("32/13/2026 25:61") is None

    def test_has_next_page_detects_numbered_link_without_next_label(self) -> None:
        """Pagination should also work when only a numbered page link is present."""
        scraper = self._create_scraper()
        html = """
        <html>
          <body>
            <nav>
              <a href="/list/searchresult/%D7%91%D7%99%D7%AA%20%D7%91%D7%95%D7%A9%D7%AA/page-2">2</a>
            </nav>
          </body>
        </html>
        """

        assert scraper._has_next_page(html, 1) is True

    def test_is_article_url_rejects_empty_and_external_urls(self) -> None:
        """Only internal ICE article paths should be accepted."""
        scraper = self._create_scraper()

        assert scraper._is_article_url("") is False
        assert scraper._is_article_url("https://example.com/law/news/article/1086606") is False

    def test_normalize_article_url_strips_query_and_fragment(self) -> None:
        """ICE article URLs should be canonicalized before deduplication."""
        scraper = self._create_scraper()

        normalized = scraper._normalize_article_url(
            "https://www.ice.co.il/law/news/article/1086606?utm_source=search#headline"
        )

        assert normalized == "https://www.ice.co.il/law/news/article/1086606"

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_paginates_and_deduplicates(self) -> None:
        """Fetch should follow pagination and collapse duplicate article URLs."""
        page_one = load_fixture("html/ice_search.html")
        page_two = """
        <html>
          <body>
            <main>
              <h1>נמצאו 64 תוצאות חיפוש של בית בושת</h1>
              <article>
                <ul>
                  <li>
                    <a href="/local-news/news/article/1099242?utm_source=search">חריג: הפכה את המקלט הציבורי לבית בושת</a>
                    <a href="/local-news/news/article/1099242?utm_source=search">תושבת בת ים נעצרה אחרי בדיקת פקחים</a>
                    <span>12/3/2026 7:46</span>
                  </li>
                  <li>
                    <a href="/law/news/article/1077000">מקלט נוסף הוסב לבית בושת</a>
                    <a href="/law/news/article/1077000">המשטרה סגרה את המקום</a>
                    <span>11/3/2026 11:10</span>
                  </li>
                </ul>
              </article>
            </main>
          </body>
        </html>
        """
        scraper = self._create_scraper()
        keyword = "בית בושת"
        page_one_url = scraper._build_search_url(keyword)
        page_two_url = scraper._build_search_url(keyword, page_number=2)

        respx.get(page_one_url).mock(return_value=Response(200, text=page_one))
        respx.get(page_two_url).mock(return_value=Response(200, text=page_two))

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=[keyword])

        urls = [str(article.url) for article in articles]
        assert len(articles) == 3
        assert len(urls) == len(set(urls))
        assert "https://www.ice.co.il/law/news/article/1077000" in urls

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_stops_when_next_page_has_only_old_results(self) -> None:
        """Fetch should stop paginating when the next page contains no recent articles."""
        page_one = load_fixture("html/ice_search.html")
        page_two = """
        <html>
          <body>
            <main>
              <h1>נמצאו 64 תוצאות חיפוש של בית בושת</h1>
              <article>
                <ul>
                  <li>
                    <a href="/law/news/article/1000000">כתבה ישנה</a>
                    <a href="/law/news/article/1000000">סיפור ישן</a>
                    <span>1/1/2020 8:00</span>
                  </li>
                </ul>
              </article>
              <nav class="pagination">
                <a href="/list/searchresult/%D7%91%D7%99%D7%AA%20%D7%91%D7%95%D7%A9%D7%AA/page-3">הבא</a>
              </nav>
            </main>
          </body>
        </html>
        """
        scraper = self._create_scraper()
        keyword = "בית בושת"
        page_three_url = scraper._build_search_url(keyword, page_number=3)

        respx.get(scraper._build_search_url(keyword)).mock(
            return_value=Response(200, text=page_one)
        )
        respx.get(scraper._build_search_url(keyword, page_number=2)).mock(
            return_value=Response(200, text=page_two)
        )
        page_three_route = respx.get(page_three_url).mock(
            return_value=Response(200, text="<html></html>")
        )

        articles = await scraper.fetch(days=30, keywords=[keyword])

        assert len(articles) == 2
        assert page_three_route.called is False

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetch_returns_partial_results_on_http_error(self) -> None:
        """Later-page HTTP failures should not discard already collected results."""
        scraper = self._create_scraper()
        keyword = "בית בושת"
        page_one = load_fixture("html/ice_search.html")

        respx.get(scraper._build_search_url(keyword)).mock(
            return_value=Response(200, text=page_one)
        )
        respx.get(scraper._build_search_url(keyword, page_number=2)).mock(
            return_value=Response(500, text="boom")
        )

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=[keyword])

        assert len(articles) == 2


class TestHaaretzScraper:
    """Integration tests for Haaretz scraper."""

    @staticmethod
    def _create_scraper() -> HaaretzScraper:
        """Create a scraper with rate limiting disabled for tests."""
        return HaaretzScraper(rate_limit_delay_seconds=0)

    def test_factory_helper_returns_named_source(self) -> None:
        """Factory helper should return the canonical Haaretz source."""
        scraper = create_haaretz_source()

        assert scraper.name == "haaretz"

    def test_build_search_url_uses_expected_query_shape(self) -> None:
        """Search URLs should keep q/page params in canonical form."""
        scraper = self._create_scraper()

        assert scraper._build_search_url("בית בושת") == (
            "https://www.haaretz.co.il/ty-search?q=%D7%91%D7%99%D7%AA+%D7%91%D7%95%D7%A9%D7%AA&page=1"
        )
        assert scraper._build_search_url("בית בושת", page_number=2) == (
            "https://www.haaretz.co.il/ty-search?q=%D7%91%D7%99%D7%AA+%D7%91%D7%95%D7%A9%D7%AA&page=2"
        )

    def test_parse_search_results_from_fixture(self) -> None:
        """Fixture HTML should parse into Haaretz search entries."""
        scraper = self._create_scraper()
        html_content = load_fixture("html/haaretz_search.html")

        entries = scraper._parse_search_results(html_content)

        assert len(entries) == 2
        assert entries[0].title == "פשיטה על בית בושת בבת ים הובילה למעצר חשוד בסרסרות"
        assert entries[0].date == datetime(2026, 3, 15, tzinfo=UTC)
        assert entries[0].url == (
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/.premium/0000019d-1111-d111-a1bd-f11111110000"
        )
        assert entries[1].url == (
            "https://www.haaretz.co.il/blogs/veredlee/2018-03-28/ty-article/0000017f-f8fa-d2d5-a9ff-f8fe0f460000"
        )

    def test_parse_search_results_falls_back_without_heading(self) -> None:
        """Pages with result cards but no heading should still be parsed."""
        scraper = self._create_scraper()
        html_content = """
        <html><body>
          <div class="search-results">
            <article>
              <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
              <div>תקציר קצר על סרסרות.</div>
              <time>15 במרץ 2026</time>
            </article>
          </div>
        </body></html>
        """

        entries = scraper._parse_search_results(html_content)

        assert len(entries) == 1
        assert entries[0].url == "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/abc"

    def test_parse_hebrew_date(self) -> None:
        """Visible Hebrew month names should parse into UTC dates."""
        scraper = self._create_scraper()

        assert scraper._parse_hebrew_date("15 במרץ 2026") == datetime(2026, 3, 15, tzinfo=UTC)
        assert scraper._parse_hebrew_date("16 באוגוסט 2023") == datetime(2023, 8, 16, tzinfo=UTC)
        assert scraper._parse_hebrew_date("32 באוגוסט 2023") is None
        assert scraper._parse_hebrew_date("ללא תאריך") is None

    def test_parse_search_result_rejects_non_article_and_missing_date(self) -> None:
        """Only internal Haaretz article cards with dates should be emitted."""
        scraper = self._create_scraper()

        non_article = BeautifulSoup(
            """
            <article>
              <h3><a href="https://www.themarker.com/news/2026-03-15/ty-article/123">TheMarker</a></h3>
              <time>15 במרץ 2026</time>
            </article>
            """,
            "lxml",
        ).find("article")
        missing_date = BeautifulSoup(
            """
            <article>
              <h3><a href="/news/law/2026-03-15/ty-article/123">ללא תאריך</a></h3>
            </article>
            """,
            "lxml",
        ).find("article")

        assert isinstance(non_article, Tag)
        assert isinstance(missing_date, Tag)
        assert scraper._parse_search_result(non_article) is None
        assert scraper._parse_search_result(missing_date) is None

    def test_parse_search_result_skips_wrapper_text_containing_title(self) -> None:
        """Snippet extraction should avoid wrapper text that duplicates the title."""
        scraper = self._create_scraper()
        article = BeautifulSoup(
            """
            <article>
              <div>
                <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
                <div>המשטרה עצרה חשוד בסרסרות.</div>
              </div>
              <time>15 במרץ 2026</time>
            </article>
            """,
            "lxml",
        ).find("article")

        assert isinstance(article, Tag)
        entry = scraper._parse_search_result(article)

        assert entry is not None
        assert entry.snippet == "המשטרה עצרה חשוד בסרסרות."

    def test_normalize_and_validate_article_urls(self) -> None:
        """Haaretz URL normalization should keep article paths and reject unsupported URLs."""
        scraper = self._create_scraper()

        normalized = scraper._normalize_article_url(
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/.premium/abc?utm_source=search#top"
        )

        assert normalized == "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/.premium/abc"
        assert scraper._is_article_url(normalized)
        assert not scraper._is_article_url(
            "https://www.themarker.com/news/2026-03-15/ty-article/abc"
        )
        assert not scraper._is_article_url(
            "https://www.haaretz.co.il/labels/2026-03-15/ty-article/abc"
        )

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Positive Haaretz rate limits should sleep between requests."""
        scraper = HaaretzScraper(rate_limit_delay_seconds=0.25)
        calls: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            calls.append(seconds)

        monkeypatch.setattr("denbust.sources.haaretz.asyncio.sleep", fake_sleep)

        await scraper._rate_limit()

        assert calls == [0.25]

    @pytest.mark.asyncio
    async def test_fetch_returns_empty_for_invalid_days(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Invalid day windows should be rejected before opening the browser."""
        scraper = self._create_scraper()
        open_browser = AsyncMock()
        mock_logger = MagicMock()

        monkeypatch.setattr(scraper, "_open_browser_session", open_browser)
        monkeypatch.setattr("denbust.sources.haaretz.logger", mock_logger)

        articles = await scraper.fetch(days=0, keywords=["בית בושת"])

        assert articles == []
        open_browser.assert_not_called()
        mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_open_and_close_browser_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Browser session setup and cleanup should mirror the Mako pattern."""
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

            async def route(self, pattern: str, handler: Any) -> None:
                assert pattern == "**/*"
                del handler
                events.append("route")

            async def close(self) -> None:
                events.append("context_close")

        class FakeBrowser:
            def __init__(self) -> None:
                self.context = FakeContext()

            async def new_context(self, **kwargs: Any) -> FakeContext:
                events.append("new_context")
                assert kwargs["user_agent"].startswith("Mozilla/5.0")
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
    async def test_open_browser_session_reports_missing_playwright(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing Playwright imports should raise an actionable runtime error."""
        scraper = self._create_scraper()
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "playwright.async_api":
                raise ImportError("missing playwright")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="Playwright is not installed"):
            await scraper._open_browser_session()

    @pytest.mark.asyncio
    async def test_open_browser_session_reports_launch_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Chromium launch failures should exit the manager and raise a clear error."""
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
                return FakePlaywright()

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                del exc_type, exc, tb
                events.append("exit")

        monkeypatch.setattr(playwright_async_api, "async_playwright", lambda: FakeManager())

        with pytest.raises(RuntimeError, match="Chromium could not be launched"):
            await scraper._open_browser_session()

        assert events == ["exit"]

    @pytest.mark.asyncio
    async def test_fetch_search_page_html_reports_timeout(self) -> None:
        """Search-page helper should raise a clear timeout error."""
        import playwright.async_api as playwright_async_api

        scraper = self._create_scraper()

        class FakePage:
            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout
                raise playwright_async_api.TimeoutError("timeout")

        with pytest.raises(RuntimeError, match="navigation timed out"):
            await scraper._fetch_search_page_html(FakePage(), "בית בושת", 2)

    @pytest.mark.asyncio
    async def test_fetch_search_page_html_returns_content(self) -> None:
        """Search-page helper should return rendered page content when ready."""
        scraper = self._create_scraper()

        class FakePage:
            async def goto(self, url: str, wait_until: str, timeout: int) -> None:
                del url, wait_until, timeout

            async def wait_for_function(self, script: str, timeout: int) -> None:
                del script, timeout

            async def wait_for_timeout(self, timeout_ms: int) -> None:
                assert timeout_ms == 500

            async def content(self) -> str:
                return "<html><body><h2>מציג תוצאות בנושא:</h2><article></article></body></html>"

        html = await scraper._fetch_search_page_html(FakePage(), "בית בושת", 2)

        assert "מציג תוצאות בנושא" in html

    @pytest.mark.asyncio
    async def test_fetch_search_page_html_reports_missing_playwright(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Search-page helper should report missing Playwright imports."""
        scraper = self._create_scraper()
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "playwright.async_api":
                raise ImportError("missing playwright")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="Playwright is not installed"):
            await scraper._fetch_search_page_html(object(), "בית בושת", 1)

    @pytest.mark.asyncio
    async def test_fetch_collects_keyword_matches_across_pages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Haaretz fetch should paginate numbered search pages until matches run out."""
        scraper = self._create_scraper()
        page_one = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
            <div>המשטרה עצרה חשוד בסרסרות.</div>
            <time>15 במרץ 2026</time>
          </article>
        </div></body></html>
        """
        page_two = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/blogs/veredlee/2026-03-14/ty-article/def">איך לסגור בית בושת</a></h3>
            <div>כך תסייעו במיגור הזנות.</div>
            <time>14 במרץ 2026</time>
          </article>
        </div></body></html>
        """

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page
            assert keyword == "בית בושת"
            if page_number == 1:
                return page_one
            if page_number == 2:
                return page_two
            return None

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", AsyncMock())

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert [str(article.url) for article in articles] == [
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/abc",
            "https://www.haaretz.co.il/blogs/veredlee/2026-03-14/ty-article/def",
        ]

    @pytest.mark.asyncio
    async def test_fetch_stops_when_page_has_no_entries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fetch should stop cleanly when a numbered search page has no result cards."""
        scraper = self._create_scraper()
        calls: list[int] = []

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page, keyword
            calls.append(page_number)
            return "<html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2></div></body></html>"

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", AsyncMock())

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert articles == []
        assert calls == [1]

    @pytest.mark.asyncio
    async def test_fetch_logs_cleanup_failure_and_keeps_articles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cleanup failures should be logged after successful article collection."""
        scraper = self._create_scraper()
        page_html = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
            <div>המשטרה עצרה חשוד בסרסרות.</div>
            <time>15 במרץ 2026</time>
          </article>
        </div></body></html>
        """
        mock_logger = MagicMock()

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page, keyword
            return page_html if page_number == 1 else None

        async def close_browser(_session: object) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", close_browser)
        monkeypatch.setattr("denbust.sources.haaretz.logger", mock_logger)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert [str(article.url) for article in articles] == [
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/abc"
        ]
        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_stops_when_page_is_older_than_cutoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pagination should stop once the page contains only out-of-window results."""
        scraper = self._create_scraper()
        calls: list[int] = []
        old_page = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/news/world/2012-10-29/ty-article/old">בית בושת ישן</a></h3>
            <div>כתבה ישנה.</div>
            <time>29 באוקטובר 2012</time>
          </article>
        </div></body></html>
        """

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page, keyword
            calls.append(page_number)
            return old_page

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", AsyncMock())

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert articles == []
        assert calls == [1]

    @pytest.mark.asyncio
    async def test_fetch_deduplicates_urls_across_keywords(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repeated Haaretz hits from different keywords should collapse to one article."""
        scraper = self._create_scraper()
        page_html = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
            <div>המשטרה עצרה חשוד בסרסרות.</div>
            <time>15 במרץ 2026</time>
          </article>
        </div></body></html>
        """

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page, keyword
            return page_html if page_number == 1 else None

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", AsyncMock())

        articles = await scraper.fetch(days=21, keywords=["בית בושת", "סרסור"])

        assert [str(article.url) for article in articles] == [
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/abc"
        ]

    @pytest.mark.asyncio
    async def test_fetch_returns_partial_results_on_search_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A later failing keyword should not discard earlier Haaretz results."""
        scraper = self._create_scraper()
        page_html = """
        <html><body><div><h2>מציג תוצאות בנושא: <strong>בית בושת</strong></h2>
          <article>
            <h3><a href="/news/law/2026-03-15/ty-article/abc">פשיטה על בית בושת</a></h3>
            <div>המשטרה עצרה חשוד בסרסרות.</div>
            <time>15 במרץ 2026</time>
          </article>
        </div></body></html>
        """

        async def fake_fetch(page: object, keyword: str, page_number: int) -> str | None:
            del page
            if keyword == "סרסור":
                raise RuntimeError("boom")
            return page_html if page_number == 1 else None

        monkeypatch.setattr(scraper, "_fetch_search_page_html", fake_fetch)
        monkeypatch.setattr(
            scraper, "_open_browser_session", AsyncMock(return_value=SimpleNamespace(page=object()))
        )
        monkeypatch.setattr(scraper, "_close_browser_session", AsyncMock())

        articles = await scraper.fetch(days=21, keywords=["בית בושת", "סרסור"])

        assert [str(article.url) for article in articles] == [
            "https://www.haaretz.co.il/news/law/2026-03-15/ty-article/abc"
        ]

    @pytest.mark.asyncio
    async def test_browser_route_blocks_known_tracker_urls(self) -> None:
        """Known third-party tracker URLs should be blocked during Haaretz scraping."""
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

        blocked = FakeRoute(FakeRequest("script", "https://www.googletagmanager.com/gtm.js?id=123"))
        allowed = FakeRoute(FakeRequest("script", "https://www.haaretz.co.il/_next/static/app.js"))

        await scraper._handle_browser_route(blocked)
        await scraper._handle_browser_route(allowed)

        assert blocked.action == "abort"
        assert allowed.action == "continue"


class TestWallaScraper:
    """Integration tests for Walla scraper."""

    @staticmethod
    def _create_scraper() -> WallaScraper:
        """Create a scraper with rate limiting disabled for tests."""
        return WallaScraper(rate_limit_delay_seconds=0)

    def test_factory_helper_returns_named_source(self) -> None:
        """Factory helper should return the canonical Walla source."""
        scraper = create_walla_source()

        assert scraper.name == "walla"

    def test_build_archive_url_uses_expected_query_shape(self) -> None:
        """Archive URLs should keep year/month params and optional page."""
        scraper = self._create_scraper()

        assert scraper._build_archive_url(1, 2026, 3) == (
            "https://news.walla.co.il/archive/1?year=2026&month=3"
        )
        assert scraper._build_archive_url(1, 2026, 3, page_number=2) == (
            "https://news.walla.co.il/archive/1?year=2026&month=3&page=2"
        )

    def test_parse_archive_entries(self) -> None:
        """Fixture HTML should parse into Walla archive entries."""
        html_content = load_fixture("html/walla_archive_page.html")
        scraper = self._create_scraper()

        entries = scraper._parse_archive_entries(html_content)

        assert len(entries) == 3
        assert entries[0].title == 'בית בושת אותר בתוך מקלט ציבורי: "לא נתנו להיכנס בזמן אזעקה"'
        assert entries[0].date == datetime(2026, 3, 12, 13, 18, tzinfo=UTC)
        assert entries[1].url == "https://news.walla.co.il/item/3818937"
        assert entries[2].title == "חשד לרצח: גבר כבן 30 נורה למוות ברכבו בטירה"

    def test_parse_archive_item_skips_missing_or_invalid_dates(self) -> None:
        """Archive items without parseable dates should be ignored safely."""
        scraper = self._create_scraper()
        soup = BeautifulSoup(
            """
            <li>
              <a href="https://news.walla.co.il/item/3823239">
                <article>
                  <div class="content">
                    <h3>בית בושת אותר בתוך מקלט ציבורי</h3>
                    <p>תלונה למוקד העירוני</p>
                    <footer><div class="pub-date">תאריך לא תקין</div></footer>
                  </div>
                </article>
              </a>
            </li>
            """,
            "lxml",
        )

        link = soup.select_one('a[href*="/item/"]')
        assert link is not None
        assert scraper._parse_archive_item(link) is None

    def test_parse_archive_item_skips_missing_article_container(self) -> None:
        """Archive links without an article container should be ignored."""
        scraper = self._create_scraper()
        soup = BeautifulSoup(
            """
            <li>
              <a href="https://news.walla.co.il/item/3823239">
                <div class="content">
                  <h3>בית בושת אותר בתוך מקלט ציבורי</h3>
                  <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                </div>
              </a>
            </li>
            """,
            "lxml",
        )

        link = soup.select_one("a[href]")
        assert link is not None
        assert scraper._parse_archive_item(link) is None

    def test_parse_archive_item_skips_non_article_urls_and_empty_title(self) -> None:
        """Non-article links and blank titles should be discarded."""
        scraper = self._create_scraper()
        external = BeautifulSoup(
            """
            <li>
              <a href="https://www.walla.co.il/item/3823239">
                <article>
                  <div class="content">
                    <h3>בית בושת אותר בתוך מקלט ציבורי</h3>
                    <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
            """,
            "lxml",
        ).select_one("a[href]")
        blank_title = BeautifulSoup(
            """
            <li>
              <a href="https://news.walla.co.il/item/3823239">
                <article>
                  <div class="content">
                    <h3>   </h3>
                    <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
            """,
            "lxml",
        ).select_one("a[href]")

        assert external is not None
        assert blank_title is not None
        assert scraper._parse_archive_item(external) is None
        assert scraper._parse_archive_item(blank_title) is None

    def test_parse_archive_entries_skips_invalid_items(self) -> None:
        """Only valid archive entries should survive parsing."""
        scraper = self._create_scraper()
        html = """
        <html><body>
          <ul>
            <li>
              <a href="https://news.walla.co.il/item/3823239">
                <article>
                  <div class="content">
                    <h3>בית בושת אותר</h3>
                    <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
            <li>
              <a href="https://www.walla.co.il/item/111">
                <article>
                  <div class="content">
                    <h3>לינק חיצוני</h3>
                    <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
            <li>
              <a href="https://news.walla.co.il/item/222">
                <article><div class="content"><h3>ללא תאריך</h3></div></article>
              </a>
            </li>
          </ul>
        </body></html>
        """

        entries = scraper._parse_archive_entries(html)

        assert len(entries) == 1
        assert entries[0].url == "https://news.walla.co.il/item/3823239"

    def test_has_next_page_detects_archive_pagination(self) -> None:
        """Archive pagination should detect the next numbered page."""
        scraper = self._create_scraper()
        html_content = load_fixture("html/walla_archive_page.html")

        assert scraper._has_next_page(
            html_content,
            category_id=1,
            year=2026,
            month=3,
            page_number=1,
        )
        assert not scraper._has_next_page(
            html_content,
            category_id=1,
            year=2026,
            month=3,
            page_number=2,
        )

    def test_has_next_page_ignores_param_order_and_wrong_category(self) -> None:
        """Next-page detection should parse query params, not rely on raw substrings."""
        scraper = self._create_scraper()
        html = """
        <html><body>
          <nav>
            <a href="https://news.walla.co.il/archive/10?month=3&amp;page=2&amp;year=2026">2</a>
            <a href="https://news.walla.co.il/archive/1?month=3&amp;page=2&amp;year=2026">2</a>
          </nav>
        </body></html>
        """

        assert scraper._has_next_page(html, category_id=1, year=2026, month=3, page_number=1)
        assert not scraper._has_next_page(html, category_id=10, year=2026, month=3, page_number=2)

    def test_iter_months_spans_current_and_previous_month(self) -> None:
        """Month iteration should cover the full lookback window from newest to oldest."""
        scraper = self._create_scraper()

        months = scraper._iter_months(
            datetime(2026, 2, 25, tzinfo=UTC),
            datetime(2026, 3, 17, tzinfo=UTC),
        )

        assert months == [(2026, 3), (2026, 2)]

    def test_iter_months_rolls_back_across_year_boundary(self) -> None:
        """Month iteration should handle year rollover cleanly."""
        scraper = self._create_scraper()

        months = scraper._iter_months(
            datetime(2025, 11, 30, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC),
        )

        assert months == [(2026, 1), (2025, 12), (2025, 11)]

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_when_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Positive Walla rate limits should sleep between requests."""
        scraper = WallaScraper(rate_limit_delay_seconds=0.25)
        calls: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            calls.append(seconds)

        monkeypatch.setattr("denbust.sources.walla.asyncio.sleep", fake_sleep)

        await scraper._rate_limit()

        assert calls == [0.25]

    @pytest.mark.asyncio
    async def test_scrape_archive_month_returns_empty_without_client(self) -> None:
        """Month scraping should no-op when no HTTP client is configured."""
        scraper = self._create_scraper()

        articles = await scraper._scrape_archive_month(
            1, 2026, 3, datetime(2026, 3, 1, tzinfo=UTC), ["בית בושת"]
        )

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_archive_page_handles_missing_client_and_http_error(self) -> None:
        """Archive page fetch should return None for missing client and HTTP failures."""
        scraper = self._create_scraper()
        assert await scraper._fetch_archive_page(1, 2026, 3, 1) is None

        class FakeResponse:
            text = "ignored"

            def raise_for_status(self) -> None:
                raise httpx.HTTPStatusError(
                    "boom",
                    request=httpx.Request(
                        "GET", "https://news.walla.co.il/archive/1?year=2026&month=3"
                    ),
                    response=httpx.Response(500),
                )

        class FakeClient:
            async def get(self, url: str) -> FakeResponse:
                del url
                return FakeResponse()

        scraper._client = FakeClient()
        assert await scraper._fetch_archive_page(1, 2026, 3, 1) is None

    @pytest.mark.asyncio
    async def test_fetch_archive_page_returns_response_text(self) -> None:
        """Archive page fetch should return response HTML on success."""
        scraper = self._create_scraper()

        class FakeResponse:
            text = "<html>ok</html>"

            def raise_for_status(self) -> None:
                return None

        class FakeClient:
            async def get(self, url: str) -> FakeResponse:
                assert "archive/1" in url
                return FakeResponse()

        scraper._client = FakeClient()

        assert await scraper._fetch_archive_page(1, 2026, 3, 1) == "<html>ok</html>"

    def test_parse_date_handles_value_errors(self) -> None:
        """Calendar-invalid dates should return None instead of raising."""
        scraper = self._create_scraper()

        assert scraper._parse_date("עודכן: 13:18 31/02/2026") is None

    def test_matches_keywords_is_casefolded(self) -> None:
        """Keyword matching should be case-insensitive for Latin/mixed-case terms."""
        scraper = self._create_scraper()
        entry = WallaArchiveEntry(
            url="https://news.walla.co.il/item/1",
            title="Police raided a BROTHEL in Tel Aviv",
            snippet="Investigation continues",
            date=datetime(2026, 3, 12, 13, 18, tzinfo=UTC),
        )

        assert scraper._matches_keywords(entry, ["brothel"])
        assert scraper._matches_keywords(entry, ["BROTHEL"])
        assert not scraper._matches_keywords(entry, ["escort"])

    @pytest.mark.asyncio
    async def test_fetch_collects_keyword_matches_across_pages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Walla fetch should paginate archive pages until keyword matches are found."""
        scraper = self._create_scraper()
        page_one = """
        <html><body>
          <ul>
            <li>
              <a href="https://news.walla.co.il/item/3823989">
                <article>
                  <div class="content">
                    <h3>חשד לרצח: גבר כבן 30 נורה למוות ברכבו בטירה</h3>
                    <p>המשטרה פתחה בחקירה.</p>
                    <footer><div class="pub-date">עודכן: 12:58 16/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
          </ul>
          <nav><a href="https://news.walla.co.il/archive/1?year=2026&month=3&page=2">2</a></nav>
        </body></html>
        """
        page_two = load_fixture("html/walla_archive_page.html")

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            assert year == 2026
            assert month == 3
            if category_id != 1:
                return None
            if page_number == 1:
                return page_one
            if page_number == 2:
                return page_two
            return None

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת", "זנות"])

        assert [str(article.url) for article in articles] == [
            "https://news.walla.co.il/item/3823239",
            "https://news.walla.co.il/item/3818937",
        ]

    @pytest.mark.asyncio
    async def test_fetch_stops_when_first_page_returns_no_html(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fetch should stop cleanly when an archive page fetch returns no HTML."""
        scraper = self._create_scraper()
        calls: list[tuple[int, int, int, int]] = []

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            calls.append((category_id, year, month, page_number))
            return None

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert articles == []
        assert calls == [(1, 2026, 3, 1), (10, 2026, 3, 1)]

    @pytest.mark.asyncio
    async def test_fetch_returns_empty_for_invalid_days(self) -> None:
        """Invalid non-positive lookback windows should not enter month iteration."""
        scraper = self._create_scraper()

        articles = await scraper.fetch(days=0, keywords=["בית בושת"])

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_stops_when_page_has_no_entries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fetch should stop cleanly when an archive page parses no entries."""
        scraper = self._create_scraper()
        empty_page = "<html><body><ul></ul></body></html>"
        calls: list[tuple[int, int]] = []

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            del year, month
            calls.append((category_id, page_number))
            return empty_page

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert articles == []
        assert calls == [(1, 1), (10, 1)]

    @pytest.mark.asyncio
    async def test_fetch_skips_old_entries_and_continues_matching_newer_ones(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Entries older than the cutoff should be skipped without discarding newer matches."""
        scraper = self._create_scraper()
        mixed_page = """
        <html><body>
          <ul>
            <li>
              <a href="https://news.walla.co.il/item/3823239">
                <article>
                  <div class="content">
                    <h3>בית בושת אותר בתוך מקלט ציבורי</h3>
                    <p>תלונה למוקד העירוני.</p>
                    <footer><div class="pub-date">עודכן: 13:18 12/03/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
            <li>
              <a href="https://news.walla.co.il/item/3800000">
                <article>
                  <div class="content">
                    <h3>בית בושת ישן</h3>
                    <p>כתבה ישנה.</p>
                    <footer><div class="pub-date">עודכן: 11:00 01/01/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
          </ul>
        </body></html>
        """

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            del category_id, year, month
            return mixed_page if page_number == 1 else None

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert [str(article.url) for article in articles] == [
            "https://news.walla.co.il/item/3823239"
        ]

    @pytest.mark.asyncio
    async def test_fetch_stops_when_page_is_older_than_cutoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pagination should stop once an archive page only contains out-of-window results."""
        scraper = self._create_scraper()
        calls: list[tuple[int, int]] = []
        old_page = """
        <html><body>
          <ul>
            <li>
              <a href="https://news.walla.co.il/item/3800000">
                <article>
                  <div class="content">
                    <h3>בית בושת ישן</h3>
                    <p>כתבה ישנה.</p>
                    <footer><div class="pub-date">עודכן: 11:00 01/01/2026</div></footer>
                  </div>
                </article>
              </a>
            </li>
          </ul>
          <nav><a href="https://news.walla.co.il/archive/1?year=2026&month=3&page=2">2</a></nav>
        </body></html>
        """

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            calls.append((category_id, page_number))
            del year, month
            return old_page

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert articles == []
        assert calls == [(1, 1), (10, 1)]

    @pytest.mark.asyncio
    async def test_fetch_deduplicates_urls_across_categories(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repeated archive hits from multiple categories should collapse to one article."""
        scraper = self._create_scraper()
        html_content = load_fixture("html/walla_archive_page.html")

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            del category_id, year, month
            return html_content if page_number == 1 else None

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert [str(article.url) for article in articles] == [
            "https://news.walla.co.il/item/3823239"
        ]

    @pytest.mark.asyncio
    async def test_fetch_returns_partial_results_on_http_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """HTTP failures on later pages should keep already collected Walla articles."""
        scraper = self._create_scraper()
        html_content = load_fixture("html/walla_archive_page.html")

        async def fake_fetch_archive_page(
            category_id: int, year: int, month: int, page_number: int
        ) -> str | None:
            del category_id, year, month
            if page_number == 1:
                return html_content
            return None

        monkeypatch.setattr(scraper, "_iter_months", lambda _cutoff, _now: [(2026, 3)])
        monkeypatch.setattr(scraper, "_fetch_archive_page", fake_fetch_archive_page)

        articles = await scraper.fetch(days=21, keywords=["בית בושת"])

        assert len(articles) == 1
        assert articles[0].title.startswith("בית בושת אותר")


class TestMaarivScraper:
    """Integration tests for Maariv scraper."""

    def test_factory_helper_returns_named_source(self) -> None:
        """Factory helper should return the canonical Maariv source."""
        scraper = create_maariv_source()

        assert scraper.name == "maariv"

    @pytest.mark.asyncio
    async def test_fetch_deduplicates_section_results(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fetch should deduplicate repeated section articles by canonical URL."""
        scraper = MaarivScraper()
        article = RawArticle(
            url="https://www.maariv.co.il/news/law/article-1270778",
            title="כתבה",
            snippet="תקציר",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="maariv",
        )

        async def fake_sleep(seconds: float) -> None:
            del seconds

        async def fake_scrape_section(
            url: str, cutoff: datetime, keywords: list[str]
        ) -> list[RawArticle]:
            del url, cutoff, keywords
            return [article, article]

        monkeypatch.setattr("denbust.sources.maariv.asyncio.sleep", fake_sleep)
        monkeypatch.setattr(scraper, "_scrape_section", fake_scrape_section)

        articles = await scraper.fetch(days=TEST_LOOKBACK_DAYS, keywords=["בית בושת"])

        assert len(articles) == 1

    def test_parse_search_results_extracts_article(self) -> None:
        """Search result parsing should recover article title, snippet, and URL."""
        scraper = MaarivScraper()
        html = """
        <div class="search-result">
          <a href="/news/law/article-1270778">לכתבה</a>
          <h2>חשד לבית בושת בבני ברק</h2>
          <p>המשטרה עצרה חשודים.</p>
          <time datetime="2026-03-01T10:00:00+00:00"></time>
        </div>
        """

        articles = scraper._parse_search_results(
            html,
            cutoff=datetime(2026, 2, 1, tzinfo=UTC),
        )

        assert len(articles) == 1
        assert articles[0].title == "חשד לבית בושת בבני ברק"
        assert "maariv.co.il/news/law/article-1270778" in str(articles[0].url)

    @pytest.mark.asyncio
    async def test_search_keyword_handles_client_states(self) -> None:
        """Search helper should cover no-client, success, and HTTP error branches."""
        scraper = MaarivScraper()
        cutoff = datetime(2026, 2, 1, tzinfo=UTC)
        assert await scraper._search_keyword("בית בושת", cutoff) == []

        html = """
        <div class="search-result">
          <a href="/news/law/article-1270778">לכתבה</a>
          <h2>חשד לבית בושת בבני ברק</h2>
          <p>המשטרה עצרה חשודים.</p>
          <time datetime="2026-03-01T10:00:00+00:00"></time>
        </div>
        """

        class FakeResponse:
            def __init__(self, status_code: int, text: str) -> None:
                self.status_code = status_code
                self.text = text

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    request = httpx.Request("GET", "https://www.maariv.co.il/search?q=test")
                    response = httpx.Response(self.status_code, request=request)
                    raise httpx.HTTPStatusError("bad status", request=request, response=response)

        class FakeClient:
            def __init__(self, response: FakeResponse) -> None:
                self.response = response

            async def get(self, url: str) -> FakeResponse:
                assert "q=%D7%91%D7%99%D7%AA+%D7%91%D7%95%D7%A9%D7%AA" in url
                return self.response

        scraper._client = FakeClient(FakeResponse(200, html))
        articles = await scraper._search_keyword("בית בושת", cutoff)
        assert len(articles) == 1

        scraper._client = FakeClient(FakeResponse(500, ""))
        assert await scraper._search_keyword("בית בושת", cutoff) == []

    def test_parse_section_page_filters_non_matching_keywords(self) -> None:
        """Section parsing should apply keyword filtering after article extraction."""
        scraper = MaarivScraper()
        html = """
        <article class="category-article">
          <a class="category-article-link" href="/news/law/article-1270778"></a>
          <h2>כתבה כללית</h2>
          <p>ללא מילות מפתח רלוונטיות.</p>
          <time datetime="2026-03-01T10:00:00+00:00"></time>
        </article>
        """

        articles = scraper._parse_section_page(
            html,
            cutoff=datetime(2026, 2, 1, tzinfo=UTC),
            keywords=["בית בושת"],
        )

        assert articles == []

    def test_parse_section_page_keeps_matching_articles(self) -> None:
        """Section parsing should keep matching articles after extraction."""
        scraper = MaarivScraper()
        html = """
        <article class="category-article">
          <a class="category-article-link" href="/news/law/article-1270778"></a>
          <h2>חשד לבית בושת בבני ברק</h2>
          <p>המשטרה עצרה חשודים.</p>
          <time datetime="2026-03-01T10:00:00+00:00"></time>
        </article>
        """

        articles = scraper._parse_section_page(
            html,
            cutoff=datetime(2026, 2, 1, tzinfo=UTC),
            keywords=["בית בושת"],
        )

        assert len(articles) == 1

    def test_parse_article_item_rejects_non_article_links(self) -> None:
        """Generic links that are not article URLs should be ignored."""
        scraper = MaarivScraper()
        soup = BeautifulSoup(
            """
            <article class="category-article">
              <a class="category-article-link" href="/tags/זנות">תגית</a>
              <h2>זנות</h2>
            </article>
            """,
            "lxml",
        )

        article = scraper._parse_article_item(
            soup.select_one("article"),
            cutoff=datetime(2026, 2, 1, tzinfo=UTC),
        )

        assert article is None

    def test_parse_article_item_covers_fallbacks_and_filters(self) -> None:
        """Article parsing should cover fallback selectors and filter branches."""
        scraper = MaarivScraper()
        cutoff = datetime(2026, 2, 1, tzinfo=UTC)

        article_from_news_link = scraper._parse_article_item(
            BeautifulSoup(
                """
                <article>
                  <a href="/news/law/123">לינק</a>
                  <h2>כותרת</h2>
                </article>
                """,
                "lxml",
            ).select_one("article"),
            cutoff,
        )
        assert article_from_news_link is not None

        no_href = scraper._parse_article_item(
            BeautifulSoup("<article><a>לינק</a></article>", "lxml").select_one("article"),
            cutoff,
        )
        assert no_href is None

        external = scraper._parse_article_item(
            BeautifulSoup(
                "<article><a href='https://example.com/article-1'>לינק</a><h2>כותרת</h2></article>",
                "lxml",
            ).select_one("article"),
            cutoff,
        )
        assert external is None

        no_title = scraper._parse_article_item(
            BeautifulSoup(
                "<article><a href='/news/law/article-1270778'></a></article>",
                "lxml",
            ).select_one("article"),
            cutoff,
        )
        assert no_title is None

        old = scraper._parse_article_item(
            BeautifulSoup(
                """
                <article>
                  <a href="/news/law/article-1270778">לינק</a>
                  <h2>כותרת</h2>
                  <time datetime="2026-01-01T10:00:00+00:00"></time>
                </article>
                """,
                "lxml",
            ).select_one("article"),
            datetime(2026, 2, 1, tzinfo=UTC),
        )
        assert old is None

        no_date = scraper._parse_article_item(
            BeautifulSoup(
                "<article><a href='/news/law/article-1270778'>לינק</a><h2>כותרת</h2></article>",
                "lxml",
            ).select_one("article"),
            cutoff,
        )
        assert no_date is not None

    def test_parse_date_prefers_datetime_attribute(self) -> None:
        """ISO datetime attributes should be parsed directly."""
        scraper = MaarivScraper()
        soup = BeautifulSoup(
            '<article><time datetime="2026-03-01T10:00:00Z">ignored</time></article>',
            "lxml",
        )

        parsed = scraper._parse_date(soup.select_one("article"))

        assert parsed == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)

    def test_parse_date_handles_invalid_datetime_and_text_fallback(self) -> None:
        """Date parsing should fall back from invalid datetime attrs to visible text."""
        scraper = MaarivScraper()
        item = BeautifulSoup(
            "<article><time datetime='bad'>15.02.2026</time></article>",
            "lxml",
        ).select_one("article")

        assert item is not None
        assert scraper._parse_date(item) == datetime(2026, 2, 15, tzinfo=UTC)

    def test_parse_date_uses_article_text_and_handles_invalid_text(self) -> None:
        """Date parsing should inspect article text and reject invalid values cleanly."""
        scraper = MaarivScraper()
        with_text = BeautifulSoup("<article>פורסם בתאריך 2026-02-15</article>", "lxml").select_one(
            "article"
        )
        invalid_text = BeautifulSoup(
            "<article>פורסם בתאריך 2026-13-99</article>", "lxml"
        ).select_one("article")

        assert with_text is not None
        assert invalid_text is not None
        assert scraper._parse_date(with_text) == datetime(2026, 2, 15, tzinfo=UTC)
        assert scraper._parse_date(invalid_text) is None

    def test_parse_hebrew_date_supports_dotted_format(self) -> None:
        """Maariv date parser should accept dd.mm.yyyy strings."""
        scraper = MaarivScraper()

        parsed = scraper._parse_hebrew_date("פורסם בתאריך 15.02.2026")

        assert parsed == datetime(2026, 2, 15, tzinfo=UTC)

    def test_parse_hebrew_date_supports_iso_and_rejects_invalid_dates(self) -> None:
        """Maariv date parser should support ISO dates and reject impossible values."""
        scraper = MaarivScraper()

        assert scraper._parse_hebrew_date("2026-02-15") == datetime(2026, 2, 15, tzinfo=UTC)
        assert scraper._parse_hebrew_date("2026-13-40") is None

    @pytest.mark.asyncio
    async def test_scrape_section_handles_client_states(self) -> None:
        """Section helper should cover no-client, success, and HTTP error branches."""
        scraper = MaarivScraper()
        cutoff = datetime(2026, 2, 1, tzinfo=UTC)
        assert await scraper._scrape_section("https://www.maariv.co.il/news/law", cutoff, []) == []

        html = """
        <article class="category-article">
          <a class="category-article-link" href="/news/law/article-1270778"></a>
          <h2>חשד לבית בושת בבני ברק</h2>
          <p>המשטרה עצרה חשודים.</p>
          <time datetime="2026-03-01T10:00:00+00:00"></time>
        </article>
        """

        class FakeResponse:
            def __init__(self, status_code: int, text: str) -> None:
                self.status_code = status_code
                self.text = text

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    request = httpx.Request("GET", "https://www.maariv.co.il/news/law")
                    response = httpx.Response(self.status_code, request=request)
                    raise httpx.HTTPStatusError("bad status", request=request, response=response)

        class FakeClient:
            def __init__(self, response: FakeResponse) -> None:
                self.response = response

            async def get(self, url: str) -> FakeResponse:
                assert url == "https://www.maariv.co.il/news/law"
                return self.response

        scraper._client = FakeClient(FakeResponse(200, html))
        articles = await scraper._scrape_section(
            "https://www.maariv.co.il/news/law", cutoff, ["בית בושת"]
        )
        assert len(articles) == 1

        scraper._client = FakeClient(FakeResponse(500, ""))
        assert (
            await scraper._scrape_section("https://www.maariv.co.il/news/law", cutoff, ["בית בושת"])
            == []
        )

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

    def test_parse_entry_requires_link_and_title(self) -> None:
        """Entries missing a link or title should be discarded."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        cutoff = datetime(2026, 1, 1, tzinfo=UTC)

        assert source._parse_entry({"title": "Title"}, cutoff, ["title"]) is None
        assert source._parse_entry({"link": "https://example.com/1"}, cutoff, ["title"]) is None

    def test_parse_entry_filters_old_entries(self) -> None:
        """Entries older than the cutoff should be ignored."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        cutoff = datetime(2026, 2, 15, tzinfo=UTC)
        entry = {
            "link": "https://example.com/1",
            "title": "בית בושת",
            "summary": "summary",
            "published": "Fri, 14 Feb 2026 10:00:00 GMT",
        }

        assert source._parse_entry(entry, cutoff, ["בית בושת"]) is None

    def test_parse_date_uses_struct_time_fallback(self) -> None:
        """Parsed struct_time fields should be converted when strings are absent."""
        from time import mktime

        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        parsed_time = (2026, 2, 15, 10, 0, 0, 0, 46, 0)
        entry = {"published_parsed": parsed_time}

        parsed = source._parse_date(entry)

        expected = datetime.fromtimestamp(mktime(parsed_time), tz=UTC)
        assert parsed == expected

    def test_parse_date_invalid_values_return_none(self) -> None:
        """Invalid date inputs should not raise."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")

        parsed = source._parse_date({"published": "not a date", "published_parsed": "bad"})

        assert parsed is None

    def test_clean_html_decodes_entities(self) -> None:
        """HTML cleaning should strip tags, decode entities, and normalize spaces."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")

        cleaned = source._clean_html("<p>שלום&nbsp;&amp;&nbsp;<b>עולם</b></p>")

        assert cleaned == "שלום & עולם"

    def test_parse_entry_without_date_defaults_to_now(self) -> None:
        """Entries without dates should be treated as recent."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        cutoff = datetime.now(UTC) - timedelta(days=1)
        entry = {
            "link": "https://example.com/1",
            "title": "בית בושת",
            "summary": "summary",
        }

        article = source._parse_entry(entry, cutoff, ["בית בושת"])

        assert article is not None
        assert article.title == "בית בושת"

    @pytest.mark.asyncio
    async def test_fetch_logs_bozo_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bozo feeds should still parse while logging the parser warning."""
        source = RSSSource("ynet", "https://ynet.co.il/feed.xml")
        cutoff_entry = {
            "link": "https://example.com/1",
            "title": "בית בושת",
            "summary": "summary",
            "published": "Fri, 14 Mar 2026 10:00:00 GMT",
        }

        async def fake_fetch_feed() -> str:
            return "<xml />"

        monkeypatch.setattr(source, "_fetch_feed", fake_fetch_feed)
        monkeypatch.setattr(
            "denbust.sources.rss.feedparser.parse",
            lambda _content: SimpleNamespace(
                bozo=True,
                bozo_exception=ValueError("bad feed"),
                entries=[cutoff_entry],
            ),
        )

        articles = await source.fetch(days=TEST_LOOKBACK_DAYS, keywords=["בית בושת"])

        assert len(articles) == 1

    def test_factory_helpers_create_expected_sources(self) -> None:
        """Factory helpers should return the canonical source names and URLs."""
        from denbust.sources.rss import create_ynet_source

        ynet = create_ynet_source()
        walla = create_walla_source()

        assert ynet.name == "ynet"
        assert walla.name == "walla"
        assert "ynet.co.il" in ynet._feed_url

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
