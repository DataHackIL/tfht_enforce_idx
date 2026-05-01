"""RSS feed fetcher for news sources."""

import calendar
import logging
import re
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin

import feedparser
import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.news_items.normalize import canonicalize_news_url
from denbust.sources.base import Source

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "denbust/0.1.0 (news monitoring bot; +https://github.com/denbust)"
YNET_CATEGORY_URL = "https://www.ynet.co.il/news/category/190"
YNET_ARTICLE_URL_RE = re.compile(r"^https://www\.ynet\.co\.il/news/article/[^/?#]+")

YNET_SUPPLEMENTAL_KEYWORDS = [
    "חשד לבית בושת",
    "בית בושת אותר",
    "חשד לזנות",
    "שידול לזנות",
    "מכון עיסוי",
    "מכון ליווי",
    "סרסורות",
    "סחר בנשים",
]


def effective_keywords_for_source(source_name: str, keywords: list[str]) -> list[str]:
    """Return the effective keyword set for an RSS source."""
    seen: set[str] = set()
    values: list[str] = []

    supplemental_keywords = YNET_SUPPLEMENTAL_KEYWORDS if source_name == "ynet" else []
    for keyword in [*keywords, *supplemental_keywords]:
        candidate = " ".join(keyword.split()).strip()
        if not candidate:
            continue
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        values.append(candidate)

    return values


def matches_keywords_for_source(
    source_name: str, title: str, snippet: str, keywords: list[str]
) -> bool:
    """Check whether text matches the effective keyword set for an RSS source."""
    effective_keywords = effective_keywords_for_source(source_name, keywords)
    return _matches_keywords_in_text(title, snippet, effective_keywords)


def _matches_keywords_in_text(title: str, snippet: str, keywords: list[str]) -> bool:
    """Check whether text contains any keyword from an already-normalized list."""
    text = f"{title} {snippet}".casefold()
    return any(keyword.casefold() in text for keyword in keywords)


class YnetCategoryParseResult:
    """Structured parse telemetry for the Ynet category page."""

    def __init__(
        self,
        *,
        container_count: int,
        parsed_article_count: int,
        keyword_match_count: int,
        stale_article_count: int,
        article_urls: list[str],
        articles: list[RawArticle],
    ) -> None:
        self.container_count = container_count
        self.parsed_article_count = parsed_article_count
        self.keyword_match_count = keyword_match_count
        self.stale_article_count = stale_article_count
        self.article_urls = article_urls
        self.articles = articles


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
        effective_keywords = effective_keywords_for_source(self._name, keywords)

        articles: list[RawArticle] = []

        for entry in feed.entries:
            # Parse the article
            article = self._parse_entry(entry, cutoff, effective_keywords)
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
        effective_keywords: list[str],
    ) -> RawArticle | None:
        """Parse a feed entry into a RawArticle.

        Args:
            entry: Feed entry from feedparser.
            cutoff: Cutoff datetime for filtering.
            effective_keywords: Precomputed effective keywords to match.

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
        if not self._matches_keywords(title, snippet, effective_keywords):
            return None

        return RawArticle(
            url=url,
            title=title,
            snippet=snippet[:300] if snippet else "",
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
                    return datetime.fromtimestamp(calendar.timegm(entry[parsed_field]), tz=UTC)
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
        return _matches_keywords_in_text(title, snippet, keywords)

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


class YnetRSSSource(RSSSource):
    """Ynet RSS source with a non-browser category-page backstop."""

    def __init__(self, feed_url: str, category_url: str = YNET_CATEGORY_URL) -> None:
        super().__init__(source_name="ynet", feed_url=feed_url)
        self._category_url = category_url
        self._debug_state: dict[str, Any] = {}

    def get_debug_state(self) -> dict[str, Any] | None:
        """Return runtime telemetry for RSS and category-page discovery legs."""
        return self._debug_state or None

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch Ynet RSS first, then supplement it from the category page."""
        self._debug_state = {
            "rss": {"status": "not_run", "requested_url": self._feed_url},
            "category": {"status": "not_run", "requested_url": self._category_url},
        }
        rss_articles = await self._fetch_rss_articles(days=days, keywords=keywords)

        category_articles = await self._fetch_category_articles(days=days, keywords=keywords)
        rss_urls = {canonicalize_news_url(str(article.url)) for article in rss_articles}
        deduped_category_articles: list[RawArticle] = []
        seen_category_urls: set[str] = set()
        for article in category_articles:
            canonical_url = canonicalize_news_url(str(article.url))
            if canonical_url in rss_urls or canonical_url in seen_category_urls:
                continue
            seen_category_urls.add(canonical_url)
            deduped_category_articles.append(article)

        self._debug_state["result"] = {
            "rss_article_count": len(rss_articles),
            "category_article_count": len(category_articles),
            "deduped_category_article_count": len(deduped_category_articles),
            "unique_article_count": len(rss_articles) + len(deduped_category_articles),
        }
        return [*rss_articles, *deduped_category_articles]

    async def _fetch_rss_articles(self, *, days: int, keywords: list[str]) -> list[RawArticle]:
        content = await self._fetch_feed()
        if not content:
            self._debug_state["rss"]["article_count"] = 0
            return []

        feed = feedparser.parse(content)
        parse_warning = None
        if feed.bozo and feed.bozo_exception:
            parse_warning = str(feed.bozo_exception)
            logger.warning(f"Feed parsing warning for {self._name}: {feed.bozo_exception}")

        cutoff = datetime.now(UTC) - timedelta(days=days)
        effective_keywords = effective_keywords_for_source(self._name, keywords)
        entries = list(feed.entries)
        recent_entry_count = 0
        articles: list[RawArticle] = []

        for entry in entries:
            date = self._parse_date(entry)
            if date is None:
                date = datetime.now(UTC)
            if date >= cutoff:
                recent_entry_count += 1

            article = self._parse_entry(entry, cutoff, effective_keywords)
            if article is not None:
                articles.append(article)

        if articles:
            status = "ok"
        elif recent_entry_count > 0:
            status = "low_coverage"
        else:
            status = "empty_or_stale"

        self._debug_state["rss"].update(
            {
                "status": status,
                "total_entry_count": len(entries),
                "recent_entry_count": recent_entry_count,
                "keyword_match_count": len(articles),
                "article_count": len(articles),
                "bozo": bool(getattr(feed, "bozo", 0)),
                "bozo_exception": parse_warning,
            }
        )
        logger.info(f"Found {len(articles)} matching articles from {self._name}")
        return articles

    async def _fetch_feed(self) -> str | None:
        try:
            async with httpx.AsyncClient(
                timeout=30.0,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            ) as client:
                response = await client.get(self._feed_url)
                response.raise_for_status()
                self._debug_state["rss"] = {
                    "status": "ok",
                    "requested_url": self._feed_url,
                    "final_url": str(response.url),
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "payload_length": len(response.text),
                }
                return response.text
        except httpx.HTTPError as e:
            logger.error(f"Error fetching RSS feed from {self._name}: {e}")
            self._debug_state["rss"] = {
                "status": "http_failure",
                "requested_url": self._feed_url,
                "error": str(e),
            }
            return None

    async def _fetch_category_articles(self, *, days: int, keywords: list[str]) -> list[RawArticle]:
        try:
            async with httpx.AsyncClient(
                timeout=20.0,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            ) as client:
                response = await client.get(self._category_url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Error fetching Ynet category page: %s", exc)
            self._debug_state["category"] = {
                "status": "http_failure",
                "requested_url": self._category_url,
                "error": str(exc),
            }
            return []

        cutoff = datetime.now(UTC) - timedelta(days=days)
        parsed = diagnose_ynet_category_html(
            response.text,
            cutoff=cutoff,
            keywords=keywords,
            category_url=self._category_url,
        )
        if parsed.parsed_article_count == 0 and parsed.stale_article_count > 0:
            status = "stale"
        elif parsed.parsed_article_count == 0:
            status = "parse_zero"
        elif parsed.keyword_match_count == 0:
            status = "keyword_zero"
        else:
            status = "ok"
        self._debug_state["category"] = {
            "status": status,
            "requested_url": self._category_url,
            "final_url": str(response.url),
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "payload_length": len(response.text),
            "container_count": parsed.container_count,
            "parsed_article_count": parsed.parsed_article_count,
            "keyword_match_count": parsed.keyword_match_count,
            "stale_article_count": parsed.stale_article_count,
            "article_urls": parsed.article_urls[:10],
        }
        return parsed.articles


def diagnose_ynet_category_html(
    html: str,
    *,
    cutoff: datetime,
    keywords: list[str],
    category_url: str = YNET_CATEGORY_URL,
) -> YnetCategoryParseResult:
    """Parse Ynet law/crime category HTML and return candidate telemetry."""
    soup = BeautifulSoup(html, "lxml")
    containers = [item for item in soup.select(".slotView") if isinstance(item, Tag)]
    if not containers:
        containers = _fallback_ynet_article_link_containers(soup, category_url=category_url)

    effective_keywords = effective_keywords_for_source("ynet", keywords)
    articles: list[RawArticle] = []
    article_urls: list[str] = []
    parsed_article_count = 0
    stale_article_count = 0
    seen_urls: set[str] = set()

    for container in containers:
        candidate = _parse_ynet_category_container(
            container, cutoff=cutoff, category_url=category_url
        )
        if candidate is None:
            if _ynet_category_container_is_stale(container, cutoff):
                stale_article_count += 1
            continue
        parsed_article_count += 1
        if not _matches_keywords_in_text(candidate.title, candidate.snippet, effective_keywords):
            continue
        article = RawArticle(
            url=candidate.url,
            title=candidate.title,
            snippet=candidate.snippet,
            date=candidate.date,
            source_name="ynet",
        )
        canonical_url = canonicalize_news_url(str(article.url))
        if canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)
        articles.append(article)
        article_urls.append(str(article.url))

    return YnetCategoryParseResult(
        container_count=len(containers),
        parsed_article_count=parsed_article_count,
        keyword_match_count=len(articles),
        stale_article_count=stale_article_count,
        article_urls=article_urls,
        articles=articles,
    )


def _fallback_ynet_article_link_containers(soup: BeautifulSoup, *, category_url: str) -> list[Tag]:
    containers: list[Tag] = []
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        url = _normalize_ynet_article_url(str(anchor.get("href", "")), category_url)
        if url is None:
            continue
        parent = anchor.find_parent(["article", "li", "div"])
        containers.append(parent if isinstance(parent, Tag) else anchor)
    return containers


def _parse_ynet_category_container(
    container: Tag,
    *,
    cutoff: datetime,
    category_url: str,
) -> RawArticle | None:
    url = _extract_ynet_category_url(container, category_url)
    if url is None:
        return None
    title = _extract_ynet_category_text(container, ".slotTitle") or _extract_first_anchor_text(
        container
    )
    if not title:
        return None
    snippet = _extract_ynet_category_text(container, ".slotSubTitle")
    date = _parse_ynet_category_date(_extract_ynet_category_text(container, ".dateView"))
    if date is None:
        date = datetime.now(UTC)
    if date < cutoff:
        return None
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet=snippet[:300] if snippet else "",
        date=date,
        source_name="ynet",
    )


def _ynet_category_container_is_stale(container: Tag, cutoff: datetime) -> bool:
    date = _parse_ynet_category_date(_extract_ynet_category_text(container, ".dateView"))
    return date is not None and date < cutoff


def _extract_ynet_category_url(container: Tag, category_url: str) -> str | None:
    for anchor in container.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        normalized = _normalize_ynet_article_url(str(anchor.get("href", "")), category_url)
        if normalized is not None:
            return normalized
    return None


def _normalize_ynet_article_url(raw_url: str, category_url: str) -> str | None:
    absolute_url = urljoin(category_url, raw_url)
    match = YNET_ARTICLE_URL_RE.match(absolute_url)
    if match is None:
        return None
    return match.group(0)


def _extract_ynet_category_text(container: Tag, selector: str) -> str:
    node = container.select_one(selector)
    if node is None:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())


def _extract_first_anchor_text(container: Tag) -> str:
    anchor = container.find("a", href=True)
    if not isinstance(anchor, Tag):
        return ""
    return " ".join(anchor.get_text(" ", strip=True).split())


def _parse_ynet_category_date(value: str) -> datetime | None:
    stripped = value.strip()
    if not stripped:
        return None
    match = re.search(r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{2,4})", stripped)
    if match is None:
        return None
    year = int(match.group("year"))
    if year < 100:
        year += 2000
    try:
        return datetime(
            year,
            int(match.group("month")),
            int(match.group("day")),
            tzinfo=UTC,
        )
    except ValueError:
        return None


# Pre-configured RSS sources
def create_ynet_source() -> YnetRSSSource:
    """Create Ynet RSS source."""
    return YnetRSSSource(feed_url="https://www.ynet.co.il/Integration/StoryRss190.xml")


def create_walla_source() -> RSSSource:
    """Create Walla RSS source."""
    return RSSSource(
        source_name="walla",
        feed_url="https://rss.walla.co.il/feed/1",
    )
