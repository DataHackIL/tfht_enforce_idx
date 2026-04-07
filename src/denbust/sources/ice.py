"""ICE news scraper."""

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"

ICE_BASE_URL = "https://www.ice.co.il"
ICE_SEARCH_URL_PREFIX = f"{ICE_BASE_URL}/list/searchresult"
MAX_SEARCH_PAGES = 5
DEFAULT_RATE_LIMIT_DELAY_SECONDS = 1.5


class IceScraper(Source):
    """Search-based scraper for ICE news."""

    def __init__(self, rate_limit_delay_seconds: float = DEFAULT_RATE_LIMIT_DELAY_SECONDS) -> None:
        """Initialize ICE scraper."""
        self._name = "ice"
        self._client: httpx.AsyncClient | None = None
        self._rate_limit_delay_seconds = rate_limit_delay_seconds

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from ICE search results."""
        logger.info("Scraping ICE for articles in last %s days", days)

        articles: list[RawArticle] = []
        cutoff = datetime.now(UTC) - timedelta(days=days)

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            self._client = client

            for keyword in keywords:
                await self._rate_limit()
                articles.extend(await self._search_keyword(keyword, cutoff))

            self._client = None

        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info("Found %s unique articles from ICE", len(unique))
        return unique

    async def _rate_limit(self) -> None:
        """Sleep between ICE requests unless disabled for tests."""
        if self._rate_limit_delay_seconds <= 0:
            return

        await asyncio.sleep(self._rate_limit_delay_seconds)

    async def _search_keyword(self, keyword: str, cutoff: datetime) -> list[RawArticle]:
        """Search ICE for a single keyword across paginated results."""
        if not self._client:
            return []

        articles: list[RawArticle] = []

        for page_number in range(1, MAX_SEARCH_PAGES + 1):
            html = await self._fetch_search_page(keyword, page_number)
            if not html:
                break

            page_articles = self._parse_search_results(html, cutoff)
            if not page_articles:
                break

            articles.extend(page_articles)

            if not self._has_next_page(html, page_number):
                break

            await self._rate_limit()

        return articles

    async def _fetch_search_page(self, keyword: str, page_number: int) -> str | None:
        """Fetch a single ICE search results page."""
        if not self._client:
            return None

        url = self._build_search_url(keyword, page_number)

        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            logger.error("Error searching ICE for '%s' on page %s: %s", keyword, page_number, e)
            return None

    def _build_search_url(self, keyword: str, page_number: int = 1) -> str:
        """Build the canonical ICE search URL for a keyword and page."""
        quoted_keyword = quote(keyword, safe="")
        url = f"{ICE_SEARCH_URL_PREFIX}/{quoted_keyword}"
        if page_number > 1:
            url = f"{url}/page-{page_number}"
        return url

    def _parse_search_results(self, html: str, cutoff: datetime) -> list[RawArticle]:
        """Parse ICE search results HTML into articles."""
        soup = BeautifulSoup(html, "lxml")
        results_article = self._find_results_article(soup)
        if not results_article:
            return []

        articles: list[RawArticle] = []
        for item in results_article.select("ul > li"):
            article = self._parse_article_item(item, cutoff)
            if article:
                articles.append(article)

        return articles

    def _find_results_article(self, soup: BeautifulSoup) -> Tag | None:
        """Locate the primary ICE search results container."""
        heading = soup.find(
            lambda tag: (
                isinstance(tag, Tag)
                and tag.name == "h1"
                and "תוצאות חיפוש" in tag.get_text(" ", strip=True)
            )
        )
        if not heading:
            return None

        results_article = heading.find_next("article")
        if isinstance(results_article, Tag) and results_article.select("ul > li"):
            return results_article

        return None

    def _parse_article_item(self, item: Tag, cutoff: datetime) -> RawArticle | None:
        """Parse a single ICE search result item."""
        article_links = [
            link
            for link in item.select("a[href]")
            if self._is_article_url(str(link.get("href", "")))
        ]
        if not article_links:
            return None

        text_links = [link for link in article_links if link.get_text(" ", strip=True)]
        if not text_links:
            return None

        title = text_links[0].get_text(" ", strip=True)
        if not title:
            return None

        href = str(text_links[0].get("href", ""))
        url = self._normalize_article_url(urljoin(ICE_BASE_URL, href))

        snippet = ""
        if len(text_links) > 1:
            candidate = text_links[1].get_text(" ", strip=True)
            if candidate != title:
                snippet = candidate

        if not snippet:
            snippet_elem = item.select_one("p")
            if snippet_elem:
                snippet = snippet_elem.get_text(" ", strip=True)

        date = self._parse_date(item.get_text(" ", strip=True))
        if date is None:
            logger.debug("ice date parse failed, treating as recent: url=%s", url)
            date = datetime.now(UTC)
        elif date < cutoff:
            logger.debug("skip ice article reason=date_before_cutoff url=%s", url)
            return None

        return RawArticle(
            url=HttpUrl(url),
            title=title,
            snippet=snippet[:300],
            date=date,
            source_name=self._name,
        )

    def _parse_date(self, text: str) -> datetime | None:
        """Parse ICE's visible dd/mm/YYYY and optional time format."""
        match = re.search(
            r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?",
            text,
        )
        if not match:
            return None

        try:
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3))
            hour = int(match.group(4) or 0)
            minute = int(match.group(5) or 0)
            return datetime(year, month, day, hour, minute, tzinfo=UTC)
        except ValueError:
            return None

    def _has_next_page(self, html: str, page_number: int) -> bool:
        """Check whether the current ICE results page links to the next page."""
        soup = BeautifulSoup(html, "lxml")

        next_link = soup.find("a", string=lambda text: bool(text and "הבא" in text))
        if isinstance(next_link, Tag) and str(next_link.get("href", "")).endswith(
            f"/page-{page_number + 1}"
        ):
            return True

        return soup.find("a", href=re.compile(rf"/page-{page_number + 1}$")) is not None

    def _is_article_url(self, href: str) -> bool:
        """Check whether an ICE href points to an internal article."""
        if not href:
            return False

        parsed = urlsplit(urljoin(ICE_BASE_URL, href))
        if parsed.netloc not in {"ice.co.il", "www.ice.co.il"}:
            return False

        return "/article/" in parsed.path

    def _normalize_article_url(self, url: str) -> str:
        """Strip tracking params from ICE article URLs."""
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def create_ice_source() -> IceScraper:
    """Create ICE scraper source."""
    return IceScraper()
