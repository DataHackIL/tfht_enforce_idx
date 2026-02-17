"""Unit tests for models module."""

from datetime import UTC, datetime

from pydantic import HttpUrl

from denbust.models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SourceReference,
    SubCategory,
    UnifiedItem,
)


class TestRawArticle:
    """Tests for RawArticle model."""

    def test_create_raw_article(self) -> None:
        """Test creating a raw article."""
        article = RawArticle(
            url=HttpUrl("https://example.com/article/1"),
            title="Test Article",
            snippet="This is a test article snippet.",
            date=datetime(2026, 2, 15, tzinfo=UTC),
            source_name="test",
        )

        assert str(article.url) == "https://example.com/article/1"
        assert article.title == "Test Article"
        assert article.snippet == "This is a test article snippet."
        assert article.source_name == "test"

    def test_raw_article_frozen(self) -> None:
        """Test that RawArticle is immutable."""
        article = RawArticle(
            url=HttpUrl("https://example.com/article/1"),
            title="Test Article",
            snippet="Test snippet",
            date=datetime(2026, 2, 15, tzinfo=UTC),
            source_name="test",
        )

        # Should raise an error when trying to modify
        try:
            article.title = "New Title"  # type: ignore[misc]
            raise AssertionError("Should have raised an error")
        except Exception:
            pass


class TestClassificationResult:
    """Tests for ClassificationResult model."""

    def test_relevant_classification(self) -> None:
        """Test relevant classification."""
        result = ClassificationResult(
            relevant=True,
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
            confidence="high",
        )

        assert result.relevant is True
        assert result.category == Category.BROTHEL
        assert result.sub_category == SubCategory.CLOSURE
        assert result.confidence == "high"

    def test_not_relevant_classification(self) -> None:
        """Test not relevant classification."""
        result = ClassificationResult(
            relevant=False,
            category=Category.NOT_RELEVANT,
        )

        assert result.relevant is False
        assert result.category == Category.NOT_RELEVANT
        assert result.sub_category is None
        assert result.confidence == "medium"  # default


class TestUnifiedItem:
    """Tests for UnifiedItem model."""

    def test_create_unified_item(self) -> None:
        """Test creating a unified item."""
        item = UnifiedItem(
            headline="Police Raid Brothel",
            summary="Police raided a suspected brothel in Tel Aviv.",
            sources=[
                SourceReference(
                    source_name="ynet",
                    url=HttpUrl("https://ynet.co.il/article/1"),
                ),
                SourceReference(
                    source_name="walla",
                    url=HttpUrl("https://walla.co.il/item/1"),
                ),
            ],
            date=datetime(2026, 2, 15, tzinfo=UTC),
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
        )

        assert item.headline == "Police Raid Brothel"
        assert len(item.sources) == 2
        assert item.category == Category.BROTHEL
        assert item.sub_category == SubCategory.CLOSURE


class TestClassifiedArticle:
    """Tests for ClassifiedArticle model."""

    def test_create_classified_article(self) -> None:
        """Test creating a classified article."""
        article = RawArticle(
            url=HttpUrl("https://example.com/article/1"),
            title="Test Article",
            snippet="Test snippet",
            date=datetime(2026, 2, 15, tzinfo=UTC),
            source_name="test",
        )

        classification = ClassificationResult(
            relevant=True,
            category=Category.ENFORCEMENT,
            sub_category=SubCategory.OPERATION,
            confidence="high",
        )

        classified = ClassifiedArticle(
            article=article,
            classification=classification,
        )

        assert classified.article.title == "Test Article"
        assert classified.classification.relevant is True
        assert classified.classification.category == Category.ENFORCEMENT
