"""Walla archive scraper."""

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import NamedTuple
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"

WALLA_BASE_URL = "https://news.walla.co.il"
WALLA_ARCHIVE_CATEGORY_IDS = (1, 10)
DEFAULT_RATE_LIMIT_DELAY_SECONDS = 1.5
MAX_ARCHIVE_PAGES_PER_MONTH = 12


class WallaArchiveEntry(NamedTuple):
    """Parsed Walla archive entry before keyword filtering."""

    url: str
    title: str
    snippet: str
    date: datetime


class WallaScraper(Source):
    """Archive-based scraper for Walla news."""

    def __init__(self, rate_limit_delay_seconds: float = DEFAULT_RATE_LIMIT_DELAY_SECONDS) -> None:
        """Initialize Walla scraper."""
        self._name = "walla"
        self._client: httpx.AsyncClient | None = None
        self._rate_limit_delay_seconds = rate_limit_delay_seconds

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch recent keyword-matching articles from Walla archive pages."""
        logger.info("Scraping Walla for articles in last %s days", days)
        if days < 1:
            logger.warning("Skipping Walla archive scrape because days=%s is invalid", days)
            return []

        now = datetime.now(UTC)
        cutoff = now - timedelta(days=days)
        months = list(self._iter_months(cutoff, now))
        articles: list[RawArticle] = []

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            self._client = client

            for category_id in WALLA_ARCHIVE_CATEGORY_IDS:
                for year, month in months:
                    await self._rate_limit()
                    articles.extend(
                        await self._scrape_archive_month(category_id, year, month, cutoff, keywords)
                    )

            self._client = None

        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info("Found %s unique articles from Walla", len(unique))
        return unique

    async def _rate_limit(self) -> None:
        """Sleep between requests unless disabled for tests."""
        if self._rate_limit_delay_seconds <= 0:
            return

        await asyncio.sleep(self._rate_limit_delay_seconds)

    async def _scrape_archive_month(
        self,
        category_id: int,
        year: int,
        month: int,
        cutoff: datetime,
        keywords: list[str],
    ) -> list[RawArticle]:
        """Scrape one Walla archive month across paginated archive pages."""
        if not self._client:
            return []

        articles: list[RawArticle] = []

        for page_number in range(1, MAX_ARCHIVE_PAGES_PER_MONTH + 1):
            html = await self._fetch_archive_page(category_id, year, month, page_number)
            if not html:
                break

            entries = self._parse_archive_entries(html)
            if not entries:
                break

            if not any(entry.date >= cutoff for entry in entries):
                break

            for entry in entries:
                if entry.date < cutoff:
                    continue
                if not self._matches_keywords(entry, keywords):
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

            if not self._has_next_page(html, category_id, year, month, page_number):
                break

            await self._rate_limit()

        return articles

    async def _fetch_archive_page(
        self, category_id: int, year: int, month: int, page_number: int
    ) -> str | None:
        """Fetch a single Walla archive page."""
        if not self._client:
            return None

        url = self._build_archive_url(category_id, year, month, page_number)

        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as exc:
            logger.error(
                "Error fetching Walla archive for category %s, %04d-%02d page %s: %s",
                category_id,
                year,
                month,
                page_number,
                exc,
            )
            return None

    def _build_archive_url(
        self, category_id: int, year: int, month: int, page_number: int = 1
    ) -> str:
        """Build a Walla archive URL."""
        query = {"year": year, "month": month}
        if page_number > 1:
            query["page"] = page_number
        return f"{WALLA_BASE_URL}/archive/{category_id}?{urlencode(query)}"

    def _parse_archive_entries(self, html: str) -> list[WallaArchiveEntry]:
        """Parse Walla archive HTML into archive entries."""
        soup = BeautifulSoup(html, "lxml")
        entries: list[WallaArchiveEntry] = []

        for link in soup.select('li > a[href*="/item/"]'):
            entry = self._parse_archive_item(link)
            if entry:
                entries.append(entry)

        return entries

    def _parse_archive_item(self, link: Tag) -> WallaArchiveEntry | None:
        """Parse a single Walla archive result item."""
        href = str(link.get("href", ""))
        url = self._normalize_article_url(urljoin(WALLA_BASE_URL, href))
        if not self._is_article_url(url):
            return None

        article = link.find("article")
        if not isinstance(article, Tag):
            return None

        title_tag = article.find("h3")
        date_tag = article.select_one(".pub-date")
        if not isinstance(title_tag, Tag) or not isinstance(date_tag, Tag):
            return None

        title = title_tag.get_text(" ", strip=True)
        if not title:
            return None

        date = self._parse_date(date_tag.get_text(" ", strip=True))
        if not date:
            return None

        snippet_tag = article.find("p")
        snippet = snippet_tag.get_text(" ", strip=True) if isinstance(snippet_tag, Tag) else ""

        return WallaArchiveEntry(url=url, title=title, snippet=snippet, date=date)

    def _parse_date(self, text: str) -> datetime | None:
        """Parse Walla's archive date format."""
        match = re.search(r"(\d{1,2}):(\d{2})\s+(\d{1,2})/(\d{1,2})/(\d{4})", text)
        if not match:
            return None

        try:
            hour = int(match.group(1))
            minute = int(match.group(2))
            day = int(match.group(3))
            month = int(match.group(4))
            year = int(match.group(5))
            return datetime(year, month, day, hour, minute, tzinfo=UTC)
        except ValueError:
            return None

    def _matches_keywords(self, entry: WallaArchiveEntry, keywords: list[str]) -> bool:
        """Check whether a Walla archive entry matches any monitored keyword."""
        haystack = f"{entry.title} {entry.snippet}".casefold()
        return any(keyword.casefold() in haystack for keyword in keywords)

    def _has_next_page(
        self, html: str, category_id: int, year: int, month: int, page_number: int
    ) -> bool:
        """Check whether the archive page links to the next page."""
        soup = BeautifulSoup(html, "lxml")
        next_page = page_number + 1
        expected_path = f"/archive/{category_id}"

        for link in soup.find_all("a", href=True):
            href = str(link.get("href", ""))
            normalized = urljoin(WALLA_BASE_URL, href).replace("&amp;", "&")
            parsed = urlsplit(normalized)
            if parsed.path != expected_path:
                continue
            params = parse_qs(parsed.query)
            if (
                params.get("year") == [str(year)]
                and params.get("month") == [str(month)]
                and params.get("page") == [str(next_page)]
            ):
                return True

        return False

    def _is_article_url(self, url: str) -> bool:
        """Check whether a normalized URL points to a Walla news article."""
        parsed = urlsplit(url)
        if parsed.netloc != "news.walla.co.il":
            return False

        return parsed.path.startswith("/item/")

    def _normalize_article_url(self, url: str) -> str:
        """Strip query and fragment components from Walla article URLs."""
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    def _iter_months(self, cutoff: datetime, now: datetime) -> list[tuple[int, int]]:
        """List archive months from newest to oldest covering the requested lookback window."""
        months: list[tuple[int, int]] = []
        year = now.year
        month = now.month

        while True:
            months.append((year, month))
            if (year, month) == (cutoff.year, cutoff.month):
                break
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        return months


def create_walla_source() -> WallaScraper:
    """Create Walla scraper source."""
    return WallaScraper()
