"""RSS feed fetcher for news sources."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import feedparser
import httpx

from denbust.models import RawArticle
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"


class RSSSource(Source):
    """Fetch and filter articles from RSS feeds."""

    def __init__(self, source_name: str, feed_url: str) -> None:
        """Initialize RSS source.

        Args:
            source_name: Name of this source (e.g., "ynet").
            feed_url: URL of the RSS feed.
        """
        self._name = source_name
        self._feed_url = feed_url

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from RSS feed, filtering by date and keywords.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by (in title or summary).

        Returns:
            List of raw articles matching the criteria.
        """
        logger.info(f"Fetching RSS feed from {self._name}: {self._feed_url}")

        # Fetch the feed content
        content = await self._fetch_feed()
        if not content:
            return []

        # Parse the feed
        feed = feedparser.parse(content)

        if feed.bozo and feed.bozo_exception:
            logger.warning(f"Feed parsing warning for {self._name}: {feed.bozo_exception}")

        # Calculate cutoff date
        cutoff = datetime.now(UTC) - timedelta(days=days)

        articles: list[RawArticle] = []

        for entry in feed.entries:
            # Parse the article
            article = self._parse_entry(entry, cutoff, keywords)
            if article:
                articles.append(article)

        logger.info(f"Found {len(articles)} matching articles from {self._name}")
        return articles

    async def _fetch_feed(self) -> str | None:
        """Fetch RSS feed content.

        Returns:
            Feed content as string, or None on error.
        """
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            ) as client:
                response = await client.get(self._feed_url)
                response.raise_for_status()
                return response.text
        except httpx.HTTPError as e:
            logger.error(f"Error fetching RSS feed from {self._name}: {e}")
            return None

    def _parse_entry(
        self,
        entry: feedparser.FeedParserDict,
        cutoff: datetime,
        keywords: list[str],
    ) -> RawArticle | None:
        """Parse a feed entry into a RawArticle.

        Args:
            entry: Feed entry from feedparser.
            cutoff: Cutoff datetime for filtering.
            keywords: Keywords to match.

        Returns:
            RawArticle if entry matches criteria, None otherwise.
        """
        # Get URL
        url = entry.get("link")
        if not url:
            return None

        # Get title
        title = entry.get("title", "").strip()
        if not title:
            return None

        # Get snippet/summary
        snippet = entry.get("summary", entry.get("description", "")).strip()
        # Clean HTML from snippet
        snippet = self._clean_html(snippet)

        # Get date
        date = self._parse_date(entry)
        if not date:
            # If no date, assume it's recent
            date = datetime.now(UTC)

        # Filter by date
        if date < cutoff:
            return None

        # Filter by keywords
        if not self._matches_keywords(title, snippet, keywords):
            return None

        return RawArticle(
            url=url,
            title=title,
            snippet=snippet[:500] if snippet else "",
            date=date,
            source_name=self._name,
        )

    def _parse_date(self, entry: feedparser.FeedParserDict) -> datetime | None:
        """Parse date from feed entry.

        Args:
            entry: Feed entry.

        Returns:
            Parsed datetime or None.
        """
        # Try common date fields
        for field in ("published", "updated", "created"):
            if field in entry:
                try:
                    parsed: datetime = parsedate_to_datetime(entry[field])
                    return parsed
                except (ValueError, TypeError):
                    pass

            # Also try parsed versions
            parsed_field = f"{field}_parsed"
            if parsed_field in entry and entry[parsed_field]:
                try:
                    from time import mktime

                    return datetime.fromtimestamp(mktime(entry[parsed_field]), tz=UTC)
                except (ValueError, TypeError, OverflowError):
                    pass

        return None

    def _matches_keywords(self, title: str, snippet: str, keywords: list[str]) -> bool:
        """Check if title or snippet contains any keyword.

        Args:
            title: Article title.
            snippet: Article snippet.
            keywords: Keywords to match.

        Returns:
            True if any keyword matches.
        """
        text = f"{title} {snippet}".lower()
        return any(kw.lower() in text for kw in keywords)

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags from text.

        Args:
            text: Text potentially containing HTML.

        Returns:
            Clean text without HTML tags.
        """
        import re

        # Remove HTML tags
        clean = re.sub(r"<[^>]+>", "", text)
        # Decode common HTML entities
        clean = clean.replace("&nbsp;", " ")
        clean = clean.replace("&amp;", "&")
        clean = clean.replace("&lt;", "<")
        clean = clean.replace("&gt;", ">")
        clean = clean.replace("&quot;", '"')
        # Normalize whitespace
        clean = re.sub(r"\s+", " ", clean)
        return clean.strip()


# Pre-configured RSS sources
def create_ynet_source() -> RSSSource:
    """Create Ynet RSS source."""
    return RSSSource(
        source_name="ynet",
        feed_url="https://www.ynet.co.il/Integration/StoryRss2.xml",
    )


def create_walla_source() -> RSSSource:
    """Create Walla RSS source."""
    return RSSSource(
        source_name="walla",
        feed_url="https://rss.walla.co.il/feed/1",
    )


async def test_rss_source() -> None:
    """Test RSS source fetching."""
    source = create_ynet_source()
    articles = await source.fetch(days=7, keywords=["ישראל", "חדשות"])
    print(f"Found {len(articles)} articles from {source.name}")
    for article in articles[:5]:
        print(f"  - {article.title[:60]}...")


if __name__ == "__main__":
    asyncio.run(test_rss_source())
