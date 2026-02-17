"""Unit tests for classifier module."""

from denbust.classifier.relevance import Classifier
from denbust.models import Category, SubCategory


class TestClassifierParsing:
    """Tests for classifier response parsing."""

    def test_parse_valid_response(self) -> None:
        """Test parsing valid JSON response."""
        # Create classifier with dummy API key (won't make actual calls)
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.relevant is True
        assert result.category == Category.BROTHEL
        assert result.sub_category == SubCategory.CLOSURE
        assert result.confidence == "high"

    def test_parse_not_relevant(self) -> None:
        """Test parsing not relevant response."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": false, "category": "not_relevant", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.relevant is False
        assert result.category == Category.NOT_RELEVANT
        assert result.sub_category is None

    def test_parse_with_markdown_code_block(self) -> None:
        """Test parsing response with markdown code block."""
        classifier = Classifier(api_key="test-key")

        response = """```json
{"relevant": true, "category": "pimping", "sub_category": "arrest", "confidence": "medium"}
```"""
        result = classifier._parse_response(response)

        assert result.relevant is True
        assert result.category == Category.PIMPING
        assert result.sub_category == SubCategory.ARREST

    def test_parse_invalid_json(self) -> None:
        """Test parsing invalid JSON returns not relevant."""
        classifier = Classifier(api_key="test-key")

        response = "This is not valid JSON"
        result = classifier._parse_response(response)

        assert result.relevant is False
        assert result.category == Category.NOT_RELEVANT
        assert result.confidence == "low"

    def test_parse_unknown_category(self) -> None:
        """Test parsing unknown category defaults to not_relevant."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "unknown_category", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.category == Category.NOT_RELEVANT

    def test_parse_unknown_subcategory(self) -> None:
        """Test parsing unknown subcategory is ignored."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "brothel", "sub_category": "unknown", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.category == Category.BROTHEL
        assert result.sub_category is None

    def test_parse_all_categories(self) -> None:
        """Test parsing all valid categories."""
        classifier = Classifier(api_key="test-key")

        for category in Category:
            response = f'{{"relevant": true, "category": "{category.value}", "confidence": "high"}}'
            result = classifier._parse_response(response)
            assert result.category == category

    def test_parse_all_subcategories(self) -> None:
        """Test parsing all valid subcategories."""
        classifier = Classifier(api_key="test-key")

        for sub_category in SubCategory:
            response = f'{{"relevant": true, "category": "enforcement", "sub_category": "{sub_category.value}", "confidence": "high"}}'
            result = classifier._parse_response(response)
            assert result.sub_category == sub_category
