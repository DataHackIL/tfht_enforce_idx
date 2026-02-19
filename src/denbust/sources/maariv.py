"""Maariv news scraper."""

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode, urljoin

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"

# Base URLs
MAARIV_BASE_URL = "https://www.maariv.co.il"
# Law/crime section
MAARIV_LAW_URL = "https://www.maariv.co.il/news/law"
# Search URL
MAARIV_SEARCH_URL = "https://www.maariv.co.il/search"


class MaarivScraper(Source):
    """Scraper for Maariv news website."""

    def __init__(self) -> None:
        """Initialize Maariv scraper."""
        self._name = "maariv"
        self._client: httpx.AsyncClient | None = None

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from Maariv, filtering by date and keywords.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles matching the criteria.
        """
        logger.info(f"Scraping Maariv for articles in last {days} days")

        articles: list[RawArticle] = []
        cutoff = datetime.now(UTC) - timedelta(days=days)

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            self._client = client

            # Scrape the law section
            await asyncio.sleep(1.5)  # Rate limiting
            section_articles = await self._scrape_section(MAARIV_LAW_URL, cutoff, keywords)
            articles.extend(section_articles)

            # Search for keywords
            for keyword in keywords:
                await asyncio.sleep(1.5)  # Rate limiting
                found = await self._search_keyword(keyword, cutoff)
                articles.extend(found)

            self._client = None

        # Deduplicate by URL
        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info(f"Found {len(unique)} unique articles from Maariv")
        return unique

    async def _search_keyword(self, keyword: str, cutoff: datetime) -> list[RawArticle]:
        """Search Maariv for a specific keyword.

        Args:
            keyword: Keyword to search for.
            cutoff: Cutoff datetime for filtering.

        Returns:
            List of matching articles.
        """
        if not self._client:
            return []

        # Build search URL
        params = {"q": keyword}
        url = f"{MAARIV_SEARCH_URL}?{urlencode(params)}"

        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return self._parse_search_results(response.text, cutoff)
        except httpx.HTTPError as e:
            logger.error(f"Error searching Maariv for '{keyword}': {e}")
            return []

    async def _scrape_section(
        self, url: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Scrape a Maariv section page.

        Args:
            url: Section URL to scrape.
            cutoff: Cutoff datetime for filtering.
            keywords: Keywords to filter by.

        Returns:
            List of matching articles.
        """
        if not self._client:
            return []

        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return self._parse_section_page(response.text, cutoff, keywords)
        except httpx.HTTPError as e:
            logger.error(f"Error scraping Maariv section {url}: {e}")
            return []

    def _parse_search_results(self, html: str, cutoff: datetime) -> list[RawArticle]:
        """Parse Maariv search results HTML.

        Args:
            html: HTML content.
            cutoff: Cutoff datetime.

        Returns:
            List of articles.
        """
        soup = BeautifulSoup(html, "lxml")
        articles: list[RawArticle] = []

        # Search results are in article containers
        for item in soup.select(".search-result, .article-item, article, .item"):
            article = self._parse_article_item(item, cutoff)
            if article:
                articles.append(article)

        return articles

    def _parse_section_page(
        self, html: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Parse Maariv section page HTML.

        Args:
            html: HTML content.
            cutoff: Cutoff datetime.
            keywords: Keywords to filter by.

        Returns:
            List of articles.
        """
        soup = BeautifulSoup(html, "lxml")
        articles: list[RawArticle] = []

        # Look for article containers
        for item in soup.select("article, .article, .item, .news-item, li"):
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
        # Find the link
        link = item.select_one("a[href*='article']")
        if not link:
            link = item.select_one("a[href*='/news/']")
        if not link:
            link = item.select_one("a")
        if not link or not link.get("href"):
            return None

        href = link.get("href", "")
        if not href:
            return None
        url = urljoin(MAARIV_BASE_URL, str(href))

        # Only include Maariv article URLs
        if "maariv.co.il" not in url:
            return None
        if "article" not in url.lower() and "/news/" not in url:
            return None

        # Get title
        title_elem = item.select_one("h1, h2, h3, .title, .headline")
        title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True)

        if not title:
            return None

        # Get snippet
        snippet_elem = item.select_one(".summary, .description, .snippet, p")
        snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""

        # Parse date
        date = self._parse_date(item)
        if date and date < cutoff:
            return None
        if not date:
            date = datetime.now(UTC)

        return RawArticle(
            url=HttpUrl(url),
            title=title,
            snippet=snippet[:500],
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
        # Look for date element
        date_elem = item.select_one("time, .date, .timestamp, [datetime]")
        if date_elem:
            # Try datetime attribute
            dt_attr = date_elem.get("datetime")
            if dt_attr:
                try:
                    return datetime.fromisoformat(str(dt_attr).replace("Z", "+00:00"))
                except ValueError:
                    pass

            # Try text content
            date_text = date_elem.get_text(strip=True)
            date = self._parse_hebrew_date(date_text)
            if date:
                return date

        # Look for date in text
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
        # Match patterns like "15/02/2026" or "15.02.2026"
        match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(year, month, day, tzinfo=UTC)
            except ValueError:
                pass

        # Match patterns like "2026-02-15"
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


def create_maariv_source() -> MaarivScraper:
    """Create Maariv scraper source."""
    return MaarivScraper()


async def test_maariv_scraper() -> None:
    """Test Maariv scraper."""
    scraper = create_maariv_source()
    articles = await scraper.fetch(days=7, keywords=["זנות", "בית בושת"])
    print(f"Found {len(articles)} articles from Maariv")
    for article in articles[:5]:
        print(f"  - {article.title[:60]}...")
        print(f"    URL: {article.url}")


if __name__ == "__main__":
    asyncio.run(test_maariv_scraper())
