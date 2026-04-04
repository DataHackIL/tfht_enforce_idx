"""Unit tests for classifier module."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import anthropic
import httpx
import pytest
from anthropic.types import TextBlock
from pydantic import HttpUrl

from denbust.classifier.relevance import CLASSIFICATION_PROMPT, Classifier, create_classifier
from denbust.data_models import Category, RawArticle, SubCategory


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

    def test_parse_invalid_confidence_defaults_to_medium(self) -> None:
        """Unknown confidence strings should be normalized."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "brothel", "confidence": "very_high"}'
        result = classifier._parse_response(response)

        assert result.confidence == "medium"


class TestClassifierRuntime:
    """Tests for classifier runtime behavior."""

    @pytest.mark.asyncio
    async def test_classify_uses_text_block_response(self) -> None:
        """TextBlock responses should be parsed into classifications."""
        classifier = Classifier(api_key="test-key")
        messages = MagicMock()
        messages.create = MagicMock(
            return_value=MagicMock(
                content=[
                    TextBlock(
                        type="text",
                        text='{"relevant": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}',
                    )
                ]
            )
        )
        classifier._client.messages = messages
        article = RawArticle(
            url=HttpUrl("https://example.com/1"),
            title="Headline",
            snippet="Snippet",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="test",
        )

        result = await classifier.classify(article)

        assert result.relevant is True
        assert result.category == Category.BROTHEL

    @pytest.mark.asyncio
    async def test_classify_uses_user_prompt_override(self) -> None:
        """Custom user prompt templates should be formatted into the API request."""
        classifier = Classifier(
            api_key="test-key",
            user_prompt_template="Headline={title}; Summary={snippet}",
        )
        messages = MagicMock()
        messages.create = MagicMock(
            return_value=MagicMock(
                content=[
                    TextBlock(
                        type="text",
                        text='{"relevant": false, "category": "not_relevant", "confidence": "high"}',
                    )
                ]
            )
        )
        classifier._client.messages = messages
        article = RawArticle(
            url=HttpUrl("https://example.com/override"),
            title="Headline",
            snippet="Snippet",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="test",
        )

        await classifier.classify(article)

        call_kwargs = messages.create.call_args.kwargs
        assert call_kwargs["messages"][0]["content"] == "Headline=Headline; Summary=Snippet"

    @pytest.mark.asyncio
    async def test_classify_without_system_override_omits_system_prompt(self) -> None:
        """System prompt should only be sent when explicitly configured."""
        classifier = Classifier(api_key="test-key")
        messages = MagicMock()
        messages.create = MagicMock(
            return_value=MagicMock(
                content=[
                    TextBlock(
                        type="text",
                        text='{"relevant": false, "category": "not_relevant", "confidence": "high"}',
                    )
                ]
            )
        )
        classifier._client.messages = messages
        article = RawArticle(
            url=HttpUrl("https://example.com/default"),
            title="Headline",
            snippet="Snippet",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="test",
        )

        await classifier.classify(article)

        call_kwargs = messages.create.call_args.kwargs
        assert "system" not in call_kwargs

    @pytest.mark.asyncio
    async def test_classify_returns_not_relevant_on_api_error(self) -> None:
        """Anthropic API failures should degrade safely."""
        classifier = Classifier(api_key="test-key")
        request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
        messages = MagicMock()
        messages.create = MagicMock(side_effect=anthropic.APIError("boom", request, body=None))
        classifier._client.messages = messages
        article = RawArticle(
            url=HttpUrl("https://example.com/1"),
            title="Headline",
            snippet="Snippet",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="test",
        )

        result = await classifier.classify(article)

        assert result.relevant is False
        assert result.category == Category.NOT_RELEVANT
        assert result.confidence == "low"

    @pytest.mark.asyncio
    async def test_classify_batch_wraps_each_article(self) -> None:
        """Batch classification should wrap each raw article result."""
        classifier = Classifier(api_key="test-key")
        classify_mock = AsyncMock(
            side_effect=[
                classifier._parse_response(
                    '{"relevant": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}'
                ),
                classifier._parse_response(
                    '{"relevant": false, "category": "not_relevant", "confidence": "low"}'
                ),
            ]
        )
        classifier.classify = classify_mock  # type: ignore[method-assign]
        articles = [
            RawArticle(
                url=HttpUrl("https://example.com/1"),
                title="One",
                snippet="Snippet",
                date=datetime(2026, 3, 1, tzinfo=UTC),
                source_name="test",
            ),
            RawArticle(
                url=HttpUrl("https://example.com/2"),
                title="Two",
                snippet="Snippet",
                date=datetime(2026, 3, 1, tzinfo=UTC),
                source_name="test",
            ),
        ]

        results = await classifier.classify_batch(articles)

        assert len(results) == 2
        assert results[0].article.title == "One"
        assert results[0].classification.relevant is True
        assert results[1].classification.relevant is False

    def test_create_classifier_uses_requested_model(self) -> None:
        """Factory should pass through the explicit model name."""
        classifier = create_classifier(api_key="test-key", model="custom-model")

        assert isinstance(classifier, Classifier)
        assert classifier._model == "custom-model"

    def test_create_classifier_uses_prompt_overrides(self) -> None:
        """Factory should pass through prompt overrides."""
        classifier = create_classifier(
            api_key="test-key",
            model="custom-model",
            system_prompt="system override",
            user_prompt_template="Title: {title}\nSnippet: {snippet}",
        )

        assert classifier._system_prompt == "system override"
        assert classifier._user_prompt_template == "Title: {title}\nSnippet: {snippet}"

    def test_default_prompt_constant_remains_available(self) -> None:
        """The module should continue exporting the default prompt string."""
        assert "anti-prostitution enforcement" in CLASSIFICATION_PROMPT
