"""Mako news scraper."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.sources.base import Source

if TYPE_CHECKING:
    from playwright.async_api import Page

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"

# Base URLs
MAKO_BASE_URL = "https://www.mako.co.il"
MAKO_SEARCH_URL = f"{MAKO_BASE_URL}/Search"
# Captured from Mako's live search URL on 2026-03-13. If it stops working,
# retry the same search without these opaque ids before changing the scraper.
MAKO_SEARCH_CHANNEL_ID = "3d385dd2dd5d4110VgnVCM100000290c10acRCRD"
# Men section often has crime/enforcement news
MAKO_MEN_NEWS_URL = "https://www.mako.co.il/men-men_news"
PLAYWRIGHT_INSTALL_HINT = "python -m playwright install chromium"
SEARCH_READY_SELECTORS = [
    "li.articleins",
    ".search-results-list",
    "input[name='searchstring_input']",
]
SECTION_READY_SELECTORS = [
    "a[href*='Article']",
    "article",
    ".article",
    ".item",
]


class _ViewportSize(TypedDict):
    """Viewport dimensions for Playwright browser contexts."""

    width: int
    height: int


VIEWPORT: _ViewportSize = {"width": 1440, "height": 2000}
NAVIGATION_TIMEOUT_MS = 30_000
READY_TIMEOUT_MS = 15_000
CHALLENGE_TIMEOUT_MS = 10_000
POST_READY_DELAY_MS = 750
DEFAULT_RATE_LIMIT_DELAY_SECONDS = 1.5


@dataclass
class _BrowserSession:
    """Open Playwright browser resources for a single fetch cycle."""

    manager: Any
    browser: Any
    context: Any
    page: Page


class MakoScraper(Source):
    """Scraper for Mako news website."""

    def __init__(self, rate_limit_delay_seconds: float = DEFAULT_RATE_LIMIT_DELAY_SECONDS) -> None:
        """Initialize Mako scraper."""
        self._name = "mako"
        self._rate_limit_delay_seconds = rate_limit_delay_seconds

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from Mako, filtering by date and keywords.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles matching the criteria.
        """
        logger.info("Scraping Mako for articles in last %s days", days)

        articles: list[RawArticle] = []
        cutoff = datetime.now(UTC) - timedelta(days=days)

        try:
            session = await self._open_browser_session()
        except Exception as e:
            logger.exception("Mako browser session could not be opened: %s", e)
            return []

        try:
            for keyword in keywords:
                await self._rate_limit()
                try:
                    found = await self._search_keyword(session, keyword, cutoff)
                except Exception as e:
                    logger.exception("Mako browser search failed for keyword '%s': %s", keyword, e)
                    continue

                articles.extend(found)

            await self._rate_limit()
            try:
                section_articles = await self._scrape_section(
                    session, MAKO_MEN_NEWS_URL, cutoff, keywords
                )
            except Exception as e:
                logger.exception("Mako browser section scrape failed for %s: %s", MAKO_MEN_NEWS_URL, e)
            else:
                articles.extend(section_articles)
        finally:
            try:
                await self._close_browser_session(session)
            except Exception as e:
                logger.exception("Mako browser session cleanup failed: %s", e)

        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info("Found %s unique articles from Mako", len(unique))
        return unique

    async def _rate_limit(self) -> None:
        """Sleep between Mako requests unless disabled for tests."""
        if self._rate_limit_delay_seconds <= 0:
            return

        await asyncio.sleep(self._rate_limit_delay_seconds)

    async def _open_browser_session(self) -> _BrowserSession:
        """Open a Playwright browser session for Mako scraping."""
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                "Playwright is not installed. Install it and Chromium with "
                f"`python -m pip install playwright` and `{PLAYWRIGHT_INSTALL_HINT}`."
            ) from e

        manager = async_playwright()
        playwright = await manager.__aenter__()

        try:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale="he-IL",
                viewport=VIEWPORT,
            )
            page = await context.new_page()
        except Exception as e:
            await manager.__aexit__(type(e), e, e.__traceback__)
            raise RuntimeError(
                "Chromium could not be launched for Mako scraping. "
                f"Install it with `{PLAYWRIGHT_INSTALL_HINT}`."
            ) from e

        return _BrowserSession(
            manager=manager,
            browser=browser,
            context=context,
            page=page,
        )

    async def _close_browser_session(self, session: _BrowserSession) -> None:
        """Close all Playwright resources for Mako scraping."""
        try:
            await session.context.close()
        finally:
            try:
                await session.browser.close()
            finally:
                await session.manager.__aexit__(None, None, None)

    async def _search_keyword(
        self, session: _BrowserSession, keyword: str, cutoff: datetime
    ) -> list[RawArticle]:
        """Search Mako for a specific keyword."""
        html = await self._fetch_search_html(session, keyword, include_channel_ids=True)
        articles = self._parse_search_results(html, cutoff)
        if articles:
            return articles

        logger.info(
            "Mako browser search with channel params returned no articles for '%s'; "
            "retrying without ids",
            keyword,
        )

        fallback_html = await self._fetch_search_html(session, keyword, include_channel_ids=False)
        return self._parse_search_results(fallback_html, cutoff)

    async def _scrape_section(
        self, session: _BrowserSession, url: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Scrape a Mako section page."""
        html = await self._fetch_section_html(session, url)
        return self._parse_section_page(html, cutoff, keywords)

    async def _fetch_search_html(
        self, session: _BrowserSession, keyword: str, *, include_channel_ids: bool
    ) -> str:
        """Fetch rendered search page HTML via Playwright."""
        url = self._build_search_url(keyword, include_channel_ids=include_channel_ids)
        suffix = " with opaque ids" if include_channel_ids else " without opaque ids"
        return await self._fetch_rendered_html(
            session.page,
            url,
            SEARCH_READY_SELECTORS,
            f"search for '{keyword}'{suffix}",
        )

    async def _fetch_section_html(self, session: _BrowserSession, url: str) -> str:
        """Fetch rendered section page HTML via Playwright."""
        return await self._fetch_rendered_html(
            session.page,
            url,
            SECTION_READY_SELECTORS,
            "men-news section",
        )

    async def _fetch_rendered_html(
        self,
        page: Page,
        url: str,
        ready_selectors: list[str],
        description: str,
    ) -> str:
        """Navigate to a page in Chromium and return the rendered HTML."""
        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        except ImportError as e:
            raise RuntimeError(
                "Playwright is not installed. Install it and Chromium with "
                f"`python -m pip install playwright` and `{PLAYWRIGHT_INSTALL_HINT}`."
            ) from e

        logger.info("Mako browser navigation started for %s", description)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            await self._wait_for_challenge_resolution(page, description)
            await page.wait_for_function(
                "selectors => selectors.some(selector => document.querySelector(selector))",
                arg=ready_selectors,
                timeout=READY_TIMEOUT_MS,
            )
            await page.wait_for_timeout(POST_READY_DELAY_MS)
        except PlaywrightTimeoutError as e:
            raise RuntimeError(
                f"Mako page never became parseable for {description}. "
                f"If Chromium is missing, install it with `{PLAYWRIGHT_INSTALL_HINT}`."
            ) from e

        return await page.content()

    async def _wait_for_challenge_resolution(self, page: Page, description: str) -> None:
        """Wait for Radware/Perfdrive challenge redirects to return to Mako."""
        if "validate.perfdrive.com" not in page.url:
            return

        logger.info(
            "Mako browser challenge detected for %s; waiting for redirect back", description
        )

        await page.wait_for_url(
            re.compile(r"^https://www\.mako\.co\.il/"),
            wait_until="domcontentloaded",
            timeout=CHALLENGE_TIMEOUT_MS,
        )

        logger.info("Mako browser challenge resolved for %s", description)

    def _build_search_url(self, keyword: str, *, include_channel_ids: bool) -> str:
        """Build the current Mako search URL."""
        params = {
            "searchstring_input": keyword,
            "page": "1",
            "tab": "search_results_tab_general",
            "formType": "regular",
        }
        if include_channel_ids:
            params["channelId"] = MAKO_SEARCH_CHANNEL_ID
            params["vgnextoid"] = MAKO_SEARCH_CHANNEL_ID

        return f"{MAKO_SEARCH_URL}?{urlencode(params)}"

    def _parse_search_results(self, html: str, cutoff: datetime) -> list[RawArticle]:
        """Parse Mako search results HTML.

        Args:
            html: HTML content.
            cutoff: Cutoff datetime.

        Returns:
            List of articles.
        """
        soup = BeautifulSoup(html, "lxml")
        articles: list[RawArticle] = []

        # The current Mako search page renders articles in li.articleins cards.
        # Keep older selectors as fallbacks because the site markup changes often.
        for item in soup.select("li.articleins, .search-result-item, .article-item, li.item"):
            article = self._parse_article_item(item, cutoff)
            if article:
                articles.append(article)

        return articles

    def _parse_section_page(
        self, html: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Parse Mako section page HTML.

        Args:
            html: HTML content.
            cutoff: Cutoff datetime.
            keywords: Keywords to filter by.

        Returns:
            List of matching articles.
        """
        soup = BeautifulSoup(html, "lxml")
        articles: list[RawArticle] = []

        for item in soup.select("article, .article, .item, li"):
            article = self._parse_article_item(item, cutoff)
            if article and self._matches_keywords(article, keywords):
                articles.append(article)

        return articles

    def _parse_article_item(self, item: Tag, cutoff: datetime) -> RawArticle | None:
        """Parse a single article item from HTML.

        Args:
            item: BeautifulSoup element.
            cutoff: Cutoff datetime.

        Returns:
            RawArticle or None.
        """
        link = item.select_one("a[href*='Article']")
        if not link:
            link = item.select_one("a")
        if not link or not link.get("href"):
            return None

        href = link.get("href", "")
        if not href:
            return None
        url = urljoin(MAKO_BASE_URL, str(href))

        if "mako.co.il" not in url or "Article" not in url:
            return None

        title_elem = item.select_one("h1, h2, h3, h4, h5, .title, .headline")
        title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True)

        if not title:
            return None

        snippet_elem = item.select_one(".summary, .description, .snippet, p")
        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""

        date = self._parse_date(item)
        if date and date < cutoff:
            return None
        if not date:
            date = datetime.now(UTC)

        return RawArticle(
            url=HttpUrl(url),
            title=title,
            snippet=snippet[:300],
            date=date,
            source_name=self._name,
        )

    def _parse_date(self, item: Tag) -> datetime | None:
        """Parse date from article item.

        Args:
            item: BeautifulSoup element.

        Returns:
            Parsed datetime or None.
        """
        date_elem = item.select_one("time, .date, .timestamp, [datetime]")
        if date_elem:
            dt_attr = date_elem.get("datetime")
            if dt_attr:
                try:
                    return datetime.fromisoformat(str(dt_attr).replace("Z", "+00:00"))
                except ValueError:
                    pass

            date_text = date_elem.get_text(strip=True)
            date = self._parse_hebrew_date(date_text)
            if date:
                return date

        text = item.get_text()
        date = self._parse_hebrew_date(text)
        if date:
            return date

        return None

    def _parse_hebrew_date(self, text: str) -> datetime | None:
        """Parse Hebrew date string.

        Args:
            text: Text potentially containing a date.

        Returns:
            Parsed datetime or None.
        """
        match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(year, month, day, tzinfo=UTC)
            except ValueError:
                pass

        match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2})(?!\d)", text)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(2000 + year, month, day, tzinfo=UTC)
            except ValueError:
                pass

        match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
        if match:
            try:
                year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(year, month, day, tzinfo=UTC)
            except ValueError:
                pass

        return None

    def _matches_keywords(self, article: RawArticle, keywords: list[str]) -> bool:
        """Check if article matches any keyword.

        Args:
            article: Article to check.
            keywords: Keywords to match.

        Returns:
            True if any keyword matches.
        """
        text = f"{article.title} {article.snippet}".lower()
        return any(kw.lower() in text for kw in keywords)


def create_mako_source() -> MakoScraper:
    """Create Mako scraper source."""
    return MakoScraper()


async def test_mako_scraper() -> None:
    """Test Mako scraper."""
    scraper = create_mako_source()
    articles = await scraper.fetch(days=7, keywords=["זנות", "בית בושת"])
    print(f"Found {len(articles)} articles from Mako")
    for article in articles[:5]:
        print(f"  - {article.title[:60]}...")
        print(f"    URL: {article.url}")


if __name__ == "__main__":
    asyncio.run(test_mako_scraper())
