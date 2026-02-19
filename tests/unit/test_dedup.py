"""Unit tests for deduplication module."""

from datetime import UTC, datetime

from pydantic import HttpUrl

from denbust.dedup.similarity import ArticleGroup, Deduplicator, create_deduplicator
from denbust.data_models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SubCategory,
)


def make_article(title: str, source: str, url: str = "https://example.com/1") -> ClassifiedArticle:
    """Helper to create a classified article."""
    return ClassifiedArticle(
        article=RawArticle(
            url=HttpUrl(url),
            title=title,
            snippet="Test snippet",
            date=datetime(2026, 2, 15, tzinfo=UTC),
            source_name=source,
        ),
        classification=ClassificationResult(
            relevant=True,
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
            confidence="high",
        ),
    )


class TestArticleGroup:
    """Tests for ArticleGroup class."""

    def test_create_group(self) -> None:
        """Test creating an article group."""
        article = make_article("Test Article", "ynet")
        group = ArticleGroup(article)

        assert len(group.articles) == 1
        assert group.primary == article
        assert group.headline == "Test Article"

    def test_add_to_group(self) -> None:
        """Test adding articles to a group."""
        article1 = make_article("Test Article", "ynet", "https://ynet.co.il/1")
        article2 = make_article("Test Article", "walla", "https://walla.co.il/1")

        group = ArticleGroup(article1)
        group.add(article2)

        assert len(group.articles) == 2

    def test_primary_longest_snippet(self) -> None:
        """Test that primary is the article with longest snippet."""
        short = ClassifiedArticle(
            article=RawArticle(
                url=HttpUrl("https://example.com/1"),
                title="Short",
                snippet="Short",
                date=datetime(2026, 2, 15, tzinfo=UTC),
                source_name="a",
            ),
            classification=ClassificationResult(relevant=True, category=Category.BROTHEL),
        )

        long = ClassifiedArticle(
            article=RawArticle(
                url=HttpUrl("https://example.com/2"),
                title="Long",
                snippet="This is a much longer snippet for testing.",
                date=datetime(2026, 2, 15, tzinfo=UTC),
                source_name="b",
            ),
            classification=ClassificationResult(relevant=True, category=Category.BROTHEL),
        )

        group = ArticleGroup(short)
        group.add(long)

        assert group.primary == long


class TestDeduplicator:
    """Tests for Deduplicator class."""

    def test_no_duplicates(self) -> None:
        """Test grouping with no duplicates."""
        articles = [
            make_article("Police Raid Brothel in Tel Aviv", "ynet", "https://ynet.co.il/1"),
            make_article("Court Sentences Pimp to Prison", "walla", "https://walla.co.il/2"),
            make_article(
                "Trafficking Victims Rescued from Apartment", "mako", "https://mako.co.il/3"
            ),
        ]

        dedup = Deduplicator(similarity_threshold=0.7)
        groups = dedup.group(articles)

        assert len(groups) == 3

    def test_identical_titles(self) -> None:
        """Test grouping identical titles."""
        articles = [
            make_article("Police Raid Brothel in Tel Aviv", "ynet", "https://ynet.co.il/1"),
            make_article("Police Raid Brothel in Tel Aviv", "walla", "https://walla.co.il/1"),
        ]

        dedup = Deduplicator(similarity_threshold=0.7)
        groups = dedup.group(articles)

        assert len(groups) == 1
        assert len(groups[0].articles) == 2

    def test_similar_titles(self) -> None:
        """Test grouping similar titles."""
        articles = [
            make_article(
                "Police Raid Suspected Brothel in Tel Aviv", "ynet", "https://ynet.co.il/1"
            ),
            make_article("Police Raid Brothel in Tel Aviv Area", "walla", "https://walla.co.il/1"),
        ]

        dedup = Deduplicator(similarity_threshold=0.7)
        groups = dedup.group(articles)

        # These should be grouped together (similar enough)
        assert len(groups) == 1

    def test_different_titles(self) -> None:
        """Test that different titles are not grouped."""
        articles = [
            make_article("Police Raid Brothel", "ynet", "https://ynet.co.il/1"),
            make_article("Court Sentences Pimp", "walla", "https://walla.co.il/1"),
        ]

        dedup = Deduplicator(similarity_threshold=0.7)
        groups = dedup.group(articles)

        assert len(groups) == 2

    def test_empty_list(self) -> None:
        """Test grouping empty list."""
        dedup = Deduplicator()
        groups = dedup.group([])

        assert groups == []

    def test_deduplicate_returns_unified_items(self) -> None:
        """Test deduplicate returns UnifiedItem objects."""
        articles = [
            make_article("Test Article", "ynet", "https://ynet.co.il/1"),
            make_article("Test Article", "walla", "https://walla.co.il/1"),
        ]

        dedup = create_deduplicator(threshold=0.7)
        items = dedup.deduplicate(articles)

        assert len(items) == 1
        assert items[0].headline == "Test Article"
        assert len(items[0].sources) == 2
