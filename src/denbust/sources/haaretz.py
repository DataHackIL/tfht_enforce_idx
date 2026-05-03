"""Haaretz search scraper."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.config import BrowserConfig
from denbust.data_models import RawArticle
from denbust.sources.base import Source
from denbust.sources.browser import (
    ScraperBrowserSession,
    close_scraper_browser_session,
    open_scraper_browser_session,
)

if TYPE_CHECKING:
    from playwright.async_api import Page

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)

HAARETZ_BASE_URL = "https://www.haaretz.co.il"
HAARETZ_SEARCH_URL = f"{HAARETZ_BASE_URL}/ty-search"
MAX_SEARCH_PAGES = 5
NAVIGATION_TIMEOUT_MS = 30_000
READY_TIMEOUT_MS = 10_000
POST_READY_DELAY_MS = 500
DEFAULT_RATE_LIMIT_DELAY_SECONDS = 1.5
BLOCKED_RESOURCE_TYPES = {"font", "image", "media"}
BLOCKED_RESOURCE_URL_FRAGMENTS = (
    "doubleclick.net",
    "google-analytics.com",
    "googlesyndication.com",
    "googletagmanager.com",
    "outbrain.com",
    "taboola.com",
)
HAARETZ_CONTEXTUAL_LIVUI_PHRASES = (
    "נערות ליווי",
    "שירותי ליווי",
    "מכון ליווי",
    "דירת ליווי",
    "סוכנות ליווי",
    "ליווי בזנות",
)
HAARETZ_MONTHS = {
    "ינואר": 1,
    "פברואר": 2,
    "מרץ": 3,
    "אפריל": 4,
    "מאי": 5,
    "יוני": 6,
    "יולי": 7,
    "אוגוסט": 8,
    "ספטמבר": 9,
    "אוקטובר": 10,
    "נובמבר": 11,
    "דצמבר": 12,
}


class _ViewportSize(TypedDict):
    """Viewport dimensions for Playwright browser contexts."""

    width: int
    height: int


VIEWPORT: _ViewportSize = {"width": 1440, "height": 2000}


@dataclass
class _HaaretzSearchEntry:
    """Parsed Haaretz search result entry before keyword filtering."""

    url: str
    title: str
    snippet: str
    date: datetime


class HaaretzScraper(Source):
    """Browser-backed search scraper for Haaretz."""

    def __init__(
        self,
        rate_limit_delay_seconds: float = DEFAULT_RATE_LIMIT_DELAY_SECONDS,
        browser_config: BrowserConfig | None = None,
    ) -> None:
        self._name = "haaretz"
        self._rate_limit_delay_seconds = rate_limit_delay_seconds
        self._browser_config = browser_config or BrowserConfig()
        self._debug_state: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self._name

    def get_debug_state(self) -> dict[str, Any] | None:
        """Return structured runtime telemetry for debug logs."""
        return self._debug_state or None

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch recent keyword-matching articles from Haaretz search results."""
        logger.info("Scraping Haaretz for articles in last %s days", days)
        if days < 1:
            logger.warning("Skipping Haaretz search because days=%s is invalid", days)
            return []

        cutoff = datetime.now(UTC) - timedelta(days=days)
        articles: list[RawArticle] = []
        self._debug_state = {
            "days": days,
            "keywords": list(keywords),
            "browser_session": {"status": "pending"},
        }

        try:
            session = await self._open_browser_session()
            self._debug_state["browser_session"] = {
                "status": "ok",
                **self._browser_session_diagnostics(session),
            }
        except Exception as e:
            self._debug_state["browser_session"] = {
                "status": "error",
                "error": str(e),
            }
            logger.exception("Haaretz browser session could not be opened: %s", e)
            return []

        try:
            for keyword in keywords:
                await self._rate_limit()
                try:
                    found = await self._search_keyword(session, keyword, cutoff)
                except Exception as e:
                    logger.exception("Haaretz search failed for keyword '%s': %s", keyword, e)
                    continue

                articles.extend(found)
        finally:
            try:
                await self._close_browser_session(session)
            except Exception as e:
                logger.exception("Haaretz browser session cleanup failed: %s", e)

        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info("Found %s unique articles from Haaretz", len(unique))
        return unique

    async def _rate_limit(self) -> None:
        if self._rate_limit_delay_seconds <= 0:
            return
        await asyncio.sleep(self._rate_limit_delay_seconds)

    def _browser_session_diagnostics(self, session: object) -> dict[str, Any]:
        """Return browser diagnostics when the concrete session provides them."""
        diagnostics = getattr(session, "diagnostics", None)
        if callable(diagnostics):
            session_diagnostics = diagnostics()
            if isinstance(session_diagnostics, dict):
                return session_diagnostics
        return {}

    async def _open_browser_session(self) -> ScraperBrowserSession:
        """Open a Playwright browser session for Haaretz scraping."""
        return await open_scraper_browser_session(
            source_name=self._name,
            browser_config=self._browser_config,
            user_agent=USER_AGENT,
            locale="he-IL",
            viewport=VIEWPORT,
            route_handler=self._handle_browser_route,
        )

    async def _close_browser_session(self, session: ScraperBrowserSession) -> None:
        """Close all Playwright resources for Haaretz scraping."""
        await close_scraper_browser_session(session)

    async def _search_keyword(
        self, session: ScraperBrowserSession, keyword: str, cutoff: datetime
    ) -> list[RawArticle]:
        """Search Haaretz for a specific keyword across paginated results."""
        articles: list[RawArticle] = []

        for page_number in range(1, MAX_SEARCH_PAGES + 1):
            html = await self._fetch_search_page_html(session.page, keyword, page_number)
            if not html:
                break

            entries = self._parse_search_results(html)
            if not entries:
                break

            if not any(entry.date >= cutoff for entry in entries):
                break

            for entry in entries:
                if entry.date < cutoff:
                    continue
                if not self._matches_keywords(entry, [keyword]):
                    continue

                articles.append(
                    RawArticle(
                        url=HttpUrl(entry.url),
                        title=entry.title,
                        snippet=entry.snippet[:300],
                        date=entry.date,
                        source_name=self._name,
                    )
                )

            await self._rate_limit()

        return articles

    async def _fetch_search_page_html(
        self, page: Page, keyword: str, page_number: int
    ) -> str | None:
        """Fetch rendered Haaretz search page HTML via Playwright."""
        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        except ImportError as e:
            raise RuntimeError(
                "Playwright is not installed. Install it and Chromium with "
                "`python -m pip install playwright` and `python -m playwright install chromium`."
            ) from e

        url = self._build_search_url(keyword, page_number)
        logger.info(
            "Haaretz browser navigation started for keyword '%s' page %s", keyword, page_number
        )

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            await page.wait_for_function(
                """
                () => {
                  const heading = Array.from(document.querySelectorAll("h2"))
                    .find((node) => node.textContent?.includes("מציג תוצאות בנושא"));
                  return Boolean(heading || document.querySelector("article"));
                }
                """,
                timeout=READY_TIMEOUT_MS,
            )
            await page.wait_for_timeout(POST_READY_DELAY_MS)
            return str(await page.content())
        except PlaywrightTimeoutError as e:
            raise RuntimeError(
                f"Haaretz search navigation timed out for '{keyword}' on page {page_number}."
            ) from e

    def _build_search_url(self, keyword: str, page_number: int = 1) -> str:
        """Build the canonical Haaretz search URL for a keyword and page."""
        return f"{HAARETZ_SEARCH_URL}?{urlencode({'q': keyword, 'page': page_number})}"

    def _parse_search_results(self, html: str) -> list[_HaaretzSearchEntry]:
        """Parse Haaretz search results HTML into entries."""
        soup = BeautifulSoup(html, "lxml")
        heading = next(
            (
                tag
                for tag in soup.find_all("h2")
                if "מציג תוצאות בנושא" in tag.get_text(" ", strip=True)
            ),
            None,
        )
        container: BeautifulSoup | Tag = soup
        if isinstance(heading, Tag) and isinstance(heading.parent, Tag):
            container = heading.parent
        else:
            search_results = soup.select_one(".search-results")
            if isinstance(search_results, Tag):
                container = search_results

        entries: list[_HaaretzSearchEntry] = []
        for article in container.find_all("article"):
            entry = self._parse_search_result(article)
            if entry:
                entries.append(entry)

        return entries

    def _parse_search_result(self, article: Tag) -> _HaaretzSearchEntry | None:
        """Parse a single Haaretz search result article card."""
        links = [
            link
            for link in article.select("a[href]")
            if self._is_article_url(
                self._normalize_article_url(urljoin(HAARETZ_BASE_URL, str(link.get("href", ""))))
            )
        ]
        if not links:
            return None

        heading = article.find(["h3", "h2"])
        title_link = heading.find("a", href=True) if isinstance(heading, Tag) else links[0]
        if not isinstance(title_link, Tag):
            title_link = links[0]

        title = title_link.get_text(" ", strip=True)
        if not title:
            return None

        href = str(title_link.get("href", ""))
        url = self._normalize_article_url(urljoin(HAARETZ_BASE_URL, href))
        if not self._is_article_url(url):
            return None

        time_tag = article.find("time")
        if not isinstance(time_tag, Tag):
            return None

        date = self._parse_hebrew_date(time_tag.get_text(" ", strip=True))
        if not date:
            return None

        snippet = ""
        for candidate in article.find_all(["p", "div"]):
            if candidate.find(["h2", "h3", "a"]):
                continue
            text = candidate.get_text(" ", strip=True)
            if not text or text == title or text == "שמירת כתבה":
                continue
            if text == time_tag.get_text(" ", strip=True):
                continue
            if re.fullmatch(r"\d{1,2}\s+ב?[א-ת]+\s+\d{4}", text):
                continue
            if title in text:
                continue
            snippet = text
            break

        return _HaaretzSearchEntry(url=url, title=title, snippet=snippet, date=date)

    def _parse_hebrew_date(self, text: str) -> datetime | None:
        """Parse visible Haaretz search-result dates like '16 באוגוסט 2023'."""
        match = re.search(r"(\d{1,2})\s+([א-ת]+)\s+(\d{4})", text)
        if not match:
            return None

        day = int(match.group(1))
        month_name = match.group(2)
        year = int(match.group(3))
        normalized_month = month_name[1:] if month_name.startswith("ב") else month_name
        month = HAARETZ_MONTHS.get(normalized_month)
        if not month:
            return None

        try:
            return datetime(year, month, day, tzinfo=UTC)
        except ValueError:
            return None

    def _matches_keywords(self, entry: _HaaretzSearchEntry, keywords: list[str]) -> bool:
        """Check whether a Haaretz search result matches any monitored keyword."""
        haystack = f"{entry.title} {entry.snippet}".casefold()
        for keyword in keywords:
            normalized_keyword = keyword.casefold()
            if normalized_keyword == "ליווי":
                if any(
                    phrase.casefold() in haystack for phrase in HAARETZ_CONTEXTUAL_LIVUI_PHRASES
                ):
                    return True
                continue
            if normalized_keyword in haystack:
                return True
        return False

    def _is_article_url(self, url: str) -> bool:
        """Check whether a normalized URL points to an internal Haaretz article."""
        parsed = urlsplit(url)
        if parsed.netloc not in {"www.haaretz.co.il", "haaretz.co.il"}:
            return False
        if (
            "/labels/" in parsed.path
            or "/promotion" in parsed.path
            or "/account/" in parsed.path
            or "/talkback/" in parsed.path
        ):
            return False
        return "/ty-article" in parsed.path

    def _normalize_article_url(self, url: str) -> str:
        """Strip query and fragment components from Haaretz article URLs."""
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    async def _handle_browser_route(self, route: Any) -> None:
        """Block noisy third-party resources during browser scraping."""
        request = route.request
        url = request.url.lower()
        if request.resource_type in BLOCKED_RESOURCE_TYPES or any(
            fragment in url for fragment in BLOCKED_RESOURCE_URL_FRAGMENTS
        ):
            await route.abort()
            return

        await route.continue_()


def create_haaretz_source(browser_config: BrowserConfig | None = None) -> HaaretzScraper:
    """Create Haaretz scraper source."""
    return HaaretzScraper(browser_config=browser_config)
