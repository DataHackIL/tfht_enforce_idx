"""Ynet news scraper using Google Custom Search API."""

import asyncio
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"

# Base URLs
YNET_BASE_URL = "https://www.ynet.co.il"
YNET_NEWS_URL = "https://www.ynet.co.il/news"

# Google Custom Search API
GOOGLE_CSE_API_URL = "https://www.googleapis.com/customsearch/v1"


class YnetScraper(Source):
    """Scraper for Ynet news website using Google Custom Search API."""

    def __init__(self) -> None:
        """Initialize Ynet scraper."""
        self._name = "ynet-scraper"
        self._client: httpx.AsyncClient | None = None
        self._google_api_key = os.environ.get("GOOGLE_API_KEY")
        self._google_cse_id = os.environ.get("GOOGLE_CSE_ID")

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from Ynet using Google Custom Search API.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles matching the criteria.
        """
        # Check if Google API is configured
        if not self._google_api_key or not self._google_cse_id:
            logger.warning(
                "GOOGLE_API_KEY or GOOGLE_CSE_ID not set. "
                "Ynet scraper requires Google Custom Search API. "
                "Falling back to basic scraping (limited to front page)."
            )
            return await self._fetch_via_scraping(days, keywords)

        return await self._fetch_via_google(days, keywords)

    async def _fetch_via_google(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles using Google Custom Search API.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles.
        """
        logger.info(f"Searching Ynet via Google CSE for articles in last {days} days")

        articles: list[RawArticle] = []
        cutoff = datetime.now(UTC) - timedelta(days=days)

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            self._client = client

            for keyword in keywords:
                await asyncio.sleep(0.5)  # Rate limiting for Google API
                found = await self._search_google(keyword, cutoff)
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

        logger.info(f"Found {len(unique)} unique articles from Ynet via Google CSE")
        return unique

    async def _search_google(self, keyword: str, cutoff: datetime) -> list[RawArticle]:
        """Search Ynet articles via Google Custom Search API.

        Args:
            keyword: Keyword to search for.
            cutoff: Cutoff datetime for filtering.

        Returns:
            List of matching articles.
        """
        if not self._client:
            return []

        # Build search query: site:ynet.co.il/news keyword
        query = f"site:ynet.co.il/news/article {keyword}"

        # Calculate date restriction (e.g., d7 for last 7 days)
        days_back = (datetime.now(UTC) - cutoff).days
        date_restrict = f"d{days_back}"

        params = {
            "key": self._google_api_key,
            "cx": self._google_cse_id,
            "q": query,
            "dateRestrict": date_restrict,
            "num": 10,  # Max results per query
        }

        try:
            response = await self._client.get(GOOGLE_CSE_API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            return self._parse_google_results(data, cutoff, keyword)
        except httpx.HTTPError as e:
            logger.error(f"Error searching Google for '{keyword}': {e}")
            return []

    def _parse_google_results(
        self, data: dict[str, Any], cutoff: datetime, keyword: str
    ) -> list[RawArticle]:
        """Parse Google Custom Search API response.

        Args:
            data: JSON response from Google CSE API.
            cutoff: Cutoff datetime for filtering.
            keyword: The keyword that was searched.

        Returns:
            List of articles.
        """
        articles: list[RawArticle] = []

        items = data.get("items", [])
        for item in items:
            url = item.get("link", "")

            # Only include Ynet news articles
            if "ynet.co.il" not in url or "/article/" not in url:
                continue

            title = item.get("title", "")
            snippet = item.get("snippet", "")

            # Parse date from metadata if available
            date = None
            metatags = item.get("pagemap", {}).get("metatags", [{}])
            if metatags:
                # Try various date fields
                date_str = metatags[0].get("article:published_time") or metatags[0].get(
                    "og:updated_time"
                )
                if date_str:
                    date = self._parse_iso_date(date_str)

            if not date:
                # Estimate from Google snippet date if present
                date = datetime.now(UTC)

            if date < cutoff:
                continue

            try:
                articles.append(
                    RawArticle(
                        url=HttpUrl(url),
                        title=title,
                        snippet=snippet[:300],
                        date=date,
                        source_name=self._name,
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to create article from Google result: {e}")

        logger.debug(f"Found {len(articles)} articles for keyword '{keyword}'")
        return articles

    def _parse_iso_date(self, date_str: str) -> datetime | None:
        """Parse ISO date string.

        Args:
            date_str: ISO date string.

        Returns:
            Parsed datetime or None.
        """
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            return None

    async def _fetch_via_scraping(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fallback: Fetch articles by scraping front page.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles.
        """
        logger.info(f"Scraping Ynet front page for articles in last {days} days")

        articles: list[RawArticle] = []
        cutoff = datetime.now(UTC) - timedelta(days=days)

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            self._client = client

            # Only scrape main news page (category pages are JS-rendered)
            await asyncio.sleep(1.5)
            section_articles = await self._scrape_section(YNET_NEWS_URL, cutoff, keywords)
            articles.extend(section_articles)

            self._client = None

        # Deduplicate by URL
        seen_urls: set[str] = set()
        unique: list[RawArticle] = []
        for article in articles:
            url_str = str(article.url)
            if url_str not in seen_urls:
                seen_urls.add(url_str)
                unique.append(article)

        logger.info(f"Found {len(unique)} unique articles from Ynet scraping")
        return unique

    async def _scrape_section(
        self, url: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Scrape a Ynet section page.

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
            logger.error(f"Error scraping Ynet section {url}: {e}")
            return []

    def _parse_section_page(
        self, html: str, cutoff: datetime, keywords: list[str]
    ) -> list[RawArticle]:
        """Parse Ynet section page HTML.

        Args:
            html: HTML content.
            cutoff: Cutoff datetime.
            keywords: Keywords to filter by.

        Returns:
            List of articles.
        """
        soup = BeautifulSoup(html, "lxml")
        articles: list[RawArticle] = []

        # Find all article links - Ynet uses /news/article/{ID} pattern
        # Look for links that contain 'article' in the href
        for link in soup.select("a[href*='/article/']"):
            href = link.get("href", "")
            if not href:
                continue

            # Skip non-news article links (like privacy policy, etc.)
            if "/news/article/" not in str(href):
                continue

            # Get the parent container for context (title, snippet)
            parent = link.find_parent(["div", "article", "section"])
            if parent:
                article = self._parse_article_link(link, parent, cutoff)
            else:
                article = self._parse_article_link(link, link, cutoff)

            if article and self._matches_keywords(article, keywords):
                articles.append(article)

        return articles

    def _parse_article_link(self, link: Tag, container: Tag, cutoff: datetime) -> RawArticle | None:
        """Parse an article from a link and its container.

        Args:
            link: The anchor tag with the article URL.
            container: Container element for title/snippet context.
            cutoff: Cutoff datetime.

        Returns:
            RawArticle or None.
        """
        href = link.get("href", "")
        if not href:
            return None

        url = urljoin(YNET_BASE_URL, str(href))

        # Remove URL fragments
        url = url.split("#")[0]

        # Only include Ynet news article URLs
        if "ynet.co.il" not in url or "/news/article/" not in url:
            return None

        # Get title - try various elements
        title = ""
        title_elem = container.select_one("h1, h2, h3, h4, .title, .slotTitle")
        if title_elem:
            title = title_elem.get_text(strip=True)
        if not title:
            title = link.get_text(strip=True)

        if not title or len(title) < 5:
            return None

        # Get snippet
        snippet = ""
        snippet_elem = container.select_one(".slotSubTitle, .subtitle, .description, p")
        if snippet_elem and snippet_elem != link:
            snippet = snippet_elem.get_text(strip=True)

        # Parse date (usually not available in list views, use current date)
        date = self._parse_date(container)
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
        # Look for date element
        date_elem = item.select_one("time, .date, .timestamp, .DateDisplay, [datetime]")
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


def create_ynet_source() -> YnetScraper:
    """Create Ynet scraper source."""
    return YnetScraper()


async def test_ynet_scraper() -> None:
    """Test Ynet scraper."""
    scraper = create_ynet_source()
    articles = await scraper.fetch(days=7, keywords=["זנות", "בית בושת"])
    print(f"Found {len(articles)} articles from Ynet scraper")
    for article in articles[:5]:
        print(f"  - {article.title[:60]}...")
        print(f"    URL: {article.url}")


if __name__ == "__main__":
    asyncio.run(test_ynet_scraper())
