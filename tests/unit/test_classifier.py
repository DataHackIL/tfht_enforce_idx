"""Unit tests for classifier module."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import anthropic
import httpx
import pytest
from anthropic.types import TextBlock
from pydantic import HttpUrl

from denbust.classifier.relevance import (
    ALLOWED_SUBCATEGORIES,
    CLASSIFICATION_PROMPT,
    CLASSIFICATION_SYSTEM_PROMPT,
    Classifier,
    create_classifier,
)
from denbust.data_models import Category, RawArticle, SubCategory


class TestClassifierParsing:
    """Tests for classifier response parsing."""

    def test_parse_valid_response(self) -> None:
        """Test parsing valid JSON response."""
        # Create classifier with dummy API key (won't make actual calls)
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "enforcement_related": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.relevant is True
        assert result.enforcement_related is True
        assert result.category == Category.BROTHEL
        assert result.sub_category == SubCategory.CLOSURE
        assert result.confidence == "high"

    def test_parse_not_relevant(self) -> None:
        """Test parsing not relevant response."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": false, "category": "not_relevant", "confidence": "high"}'
        result = classifier._parse_response(response)

        assert result.relevant is False
        assert result.enforcement_related is False
        assert result.category == Category.NOT_RELEVANT
        assert result.sub_category is None

    def test_parse_missing_enforcement_related_defaults_false(self) -> None:
        """Older classifier JSON without the second axis should still parse safely."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "prostitution", "sub_category": null, "confidence": "medium"}'
        result = classifier._parse_response(response)

        assert result.relevant is True
        assert result.enforcement_related is False
        assert result.category == Category.PROSTITUTION
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

    def test_parse_invalid_subcategory_for_category_clears_subcategory(self) -> None:
        """Known subcategories that do not match the category should be ignored."""
        classifier = Classifier(api_key="test-key")

        response = (
            '{"relevant": true, "category": "trafficking", '
            '"sub_category": "fine", "confidence": "high"}'
        )
        result = classifier._parse_response(response)

        assert result.category == Category.TRAFFICKING
        assert result.sub_category is None

    def test_parse_all_categories(self) -> None:
        """Test parsing all valid categories."""
        classifier = Classifier(api_key="test-key")

        for category in Category:
            response = f'{{"relevant": true, "category": "{category.value}", "confidence": "high"}}'
            result = classifier._parse_response(response)
            assert result.category == category

    def test_parse_all_subcategories(self) -> None:
        """Test parsing valid subcategories for each category."""
        classifier = Classifier(api_key="test-key")

        for category, subcategories in ALLOWED_SUBCATEGORIES.items():
            for sub_category in subcategories:
                response = (
                    f'{{"relevant": true, "category": "{category.value}", '
                    f'"sub_category": "{sub_category.value}", "confidence": "high"}}'
                )
                result = classifier._parse_response(response)
                assert result.sub_category == sub_category

    def test_parse_invalid_confidence_defaults_to_medium(self) -> None:
        """Unknown confidence strings should be normalized."""
        classifier = Classifier(api_key="test-key")

        response = '{"relevant": true, "category": "brothel", "confidence": "very_high"}'
        result = classifier._parse_response(response)

        assert result.confidence == "medium"

    def test_parse_relevant_without_taxonomy_uses_legacy_category(self) -> None:
        """Relevant null-taxonomy responses should stay internally consistent."""
        classifier = Classifier(api_key="test-key")

        response = (
            '{"relevant": true, "enforcement_related": false, '
            '"category": "prostitution", "sub_category": null, '
            '"taxonomy_category_id": null, "taxonomy_subcategory_id": null, '
            '"confidence": "low"}'
        )
        result = classifier._parse_response(response)

        assert result.relevant is True
        assert result.enforcement_related is False
        assert result.category == Category.PROSTITUTION
        assert result.sub_category is None
        assert result.taxonomy_category_id is None
        assert result.taxonomy_subcategory_id is None

    def test_parse_relevant_without_taxonomy_or_legacy_category_downgrades_to_not_relevant(
        self,
    ) -> None:
        """Relevant=true without either taxonomy ids or a usable coarse category should be rejected."""
        classifier = Classifier(api_key="test-key")

        response = (
            '{"relevant": true, "enforcement_related": false, '
            '"taxonomy_category_id": null, "taxonomy_subcategory_id": null, '
            '"confidence": "low"}'
        )
        result = classifier._parse_response(response)

        assert result.relevant is False
        assert result.enforcement_related is False
        assert result.category == Category.NOT_RELEVANT
        assert result.sub_category is None


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
    async def test_classify_uses_default_system_prompt(self) -> None:
        """Default classification should send the built-in system prompt."""
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
        assert call_kwargs["system"] == CLASSIFICATION_SYSTEM_PROMPT

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
                    '{"relevant": true, "enforcement_related": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}'
                ),
                classifier._parse_response(
                    '{"relevant": false, "enforcement_related": false, "category": "not_relevant", "confidence": "low"}'
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


class TestClassificationPromptContent:
    """Tests verifying the prompt and system prompt content matches the issue requirements."""

    def test_system_prompt_is_non_empty(self) -> None:
        """System prompt must not be empty."""
        assert CLASSIFICATION_SYSTEM_PROMPT.strip()

    def test_system_prompt_separates_inclusion_from_enforcement(self) -> None:
        """System prompt must describe the two-axis contract explicitly."""
        assert "Decide two separate things" in CLASSIFICATION_SYSTEM_PROMPT
        assert "enforcement or legal-action event" in CLASSIFICATION_SYSTEM_PROMPT

    def test_prompt_contains_hebrew_brothel_term(self) -> None:
        """Prompt must include Hebrew term for brothels (בתי בושת)."""
        assert "בתי בושת" in CLASSIFICATION_PROMPT

    def test_prompt_contains_hebrew_prostitution_term(self) -> None:
        """Prompt must include Hebrew term for prostitution (זנות)."""
        assert "זנות" in CLASSIFICATION_PROMPT

    def test_prompt_contains_hebrew_trafficking_term(self) -> None:
        """Prompt must include Hebrew term for human trafficking (סחר בבני אדם)."""
        assert "סחר בבני אדם" in CLASSIFICATION_PROMPT

    def test_prompt_contains_hebrew_pimping_term(self) -> None:
        """Prompt must include Hebrew term for pimping (סרסורות)."""
        assert "סרסורות" in CLASSIFICATION_PROMPT

    def test_prompt_lists_all_categories(self) -> None:
        """Prompt must enumerate all valid categories."""
        for category in ("brothel", "prostitution", "pimping", "trafficking", "enforcement"):
            assert category in CLASSIFICATION_PROMPT

    def test_prompt_contains_category_subcategory_table(self) -> None:
        """Prompt must spell out the valid category/sub_category mappings."""
        for category, subcategories in ALLOWED_SUBCATEGORIES.items():
            assert f"- {category.value} -> " in CLASSIFICATION_PROMPT
            for subcategory in subcategories:
                assert subcategory.value in CLASSIFICATION_PROMPT

    def test_prompt_describes_relevant_non_enforcement_outputs(self) -> None:
        """Prompt should allow topical non-enforcement coverage with a null subcategory."""
        assert "relevant=true, enforcement_related=false" in CLASSIFICATION_PROMPT
        assert "include the best legacy coarse category in category" in CLASSIFICATION_PROMPT

    def test_prompt_excludes_lifestyle_and_profile_stories(self) -> None:
        """Prompt should explicitly exclude the out-of-scope celebrity/profile class."""
        assert "Celebrity, lifestyle, profile, or entertainment stories" in CLASSIFICATION_PROMPT


class TestClassifyPassesSystemPrompt:
    """Tests that classify() passes the system prompt to the Anthropic API."""

    @pytest.mark.asyncio
    async def test_classify_passes_system_prompt(self) -> None:
        """classify() must include CLASSIFICATION_SYSTEM_PROMPT in the API call."""
        classifier = Classifier(api_key="test-key")
        mock_create = MagicMock(
            return_value=MagicMock(
                content=[
                    TextBlock(
                        type="text",
                        text='{"relevant": true, "enforcement_related": true, "category": "trafficking", "sub_category": "rescue", "confidence": "high"}',
                    )
                ]
            )
        )
        classifier._client.messages = MagicMock(create=mock_create)
        article = RawArticle(
            url=HttpUrl("https://example.com/trafficking"),
            title="הוא הבטיח להן הארה, והפך אותן לקורבנות אונס וסחר בבני אדם",
            snippet="כתבה על קורבנות סחר בבני אדם",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="haaretz",
        )

        await classifier.classify(article)

        call_kwargs = mock_create.call_args.kwargs
        assert "system" in call_kwargs
        assert call_kwargs["system"] == CLASSIFICATION_SYSTEM_PROMPT

    @pytest.mark.asyncio
    async def test_classify_trafficking_article_returns_relevant(self) -> None:
        """A trafficking article (no enforcement) should be classified as relevant."""
        classifier = Classifier(api_key="test-key")
        classifier._client.messages = MagicMock(
            create=MagicMock(
                return_value=MagicMock(
                    content=[
                        TextBlock(
                            type="text",
                            text='{"relevant": true, "enforcement_related": true, "category": "trafficking", "sub_category": "rescue", "confidence": "high"}',
                        )
                    ]
                )
            )
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/trafficking"),
            title="ישראלי שידל לזנות קטינה בת 13 מוונצואלה",
            snippet="דיווח על ישראלי שניסה לגייס קטינה לזנות",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="mako",
        )

        result = await classifier.classify(article)

        assert result.relevant is True
        assert result.enforcement_related is True
        assert result.category == Category.TRAFFICKING

    @pytest.mark.asyncio
    async def test_classify_brothel_article_returns_relevant(self) -> None:
        """A brothel discovery article should be classified as relevant."""
        classifier = Classifier(api_key="test-key")
        classifier._client.messages = MagicMock(
            create=MagicMock(
                return_value=MagicMock(
                    content=[
                        TextBlock(
                            type="text",
                            text='{"relevant": true, "enforcement_related": true, "category": "brothel", "sub_category": "closure", "confidence": "high"}',
                        )
                    ]
                )
            )
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/brothel"),
            title="בית בושת אותר בתוך מקלט ציבורי",
            snippet="כוחות הביטחון איתרו בית בושת שפעל בתוך מקלט ציבורי",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="walla",
        )

        result = await classifier.classify(article)

        assert result.relevant is True
        assert result.enforcement_related is True
        assert result.category == Category.BROTHEL

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("title", "snippet", "category", "sub_category"),
        [
            (
                '"שידול לזנות": עידן דביר הוא עו"ד החשוד בסחיטה מינית של חיילות',
                "כתבה על חשד לשידול לזנות וסחיטה מינית של חיילות בתמורה לייצוג",
                Category.PROSTITUTION,
                SubCategory.ARREST,
            ),
            (
                "הוא הבטיח להן הארה, והפך אותן לקורבנות אונס וסחר בבני אדם",
                "כתבה על קורבנות אונס וסחר בבני אדם",
                Category.TRAFFICKING,
                SubCategory.RESCUE,
            ),
            (
                "בית בושת אותר בתוך מקלט ציבורי",
                "כוחות אכיפה איתרו בית בושת שפעל במקלט ציבורי",
                Category.BROTHEL,
                SubCategory.CLOSURE,
            ),
            (
                "בני זוג שעלו מברזיל הפעילו רשת זנות בארץ",
                "חשד להפעלת רשת זנות בישראל על ידי בני זוג שעלו מברזיל",
                Category.PROSTITUTION,
                SubCategory.ARREST,
            ),
        ],
    )
    async def test_issue_48_false_negative_examples_can_classify_as_enforcement_related(
        self,
        title: str,
        snippet: str,
        category: Category,
        sub_category: SubCategory,
    ) -> None:
        """Known missed enforcement stories should remain covered as regression examples."""
        classifier = Classifier(api_key="test-key")
        classifier._client.messages = MagicMock(
            create=MagicMock(
                return_value=MagicMock(
                    content=[
                        TextBlock(
                            type="text",
                            text=(
                                '{"relevant": true, "enforcement_related": true, '
                                f'"category": "{category.value}", '
                                f'"sub_category": "{sub_category.value}", '
                                '"confidence": "high"}'
                            ),
                        )
                    ]
                )
            )
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/regression"),
            title=title,
            snippet=snippet,
            date=datetime(2026, 4, 6, tzinfo=UTC),
            source_name="test",
        )

        result = await classifier.classify(article)

        assert result.relevant is True
        assert result.enforcement_related is True
        assert result.category == category
        assert result.sub_category == sub_category

    @pytest.mark.asyncio
    async def test_classify_non_enforcement_topical_story_returns_relevant_without_subcategory(
        self,
    ) -> None:
        """Relevant topical stories may be kept even when they are not enforcement events."""
        classifier = Classifier(api_key="test-key")
        classifier._client.messages = MagicMock(
            create=MagicMock(
                return_value=MagicMock(
                    content=[
                        TextBlock(
                            type="text",
                            text='{"relevant": true, "enforcement_related": false, "category": "prostitution", "sub_category": null, "confidence": "medium"}',
                        )
                    ]
                )
            )
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/topic-only"),
            title="דיווח על נשים בזנות בישראל",
            snippet="כתבת רקע על זנות בישראל ללא אירוע אכיפה מסוים",
            date=datetime(2026, 3, 1, tzinfo=UTC),
            source_name="test",
        )

        result = await classifier.classify(article)

        assert result.relevant is True
        assert result.enforcement_related is False
        assert result.category == Category.PROSTITUTION
        assert result.sub_category is None

    @pytest.mark.asyncio
    async def test_issue_10_style_celebrity_story_can_be_rejected(self) -> None:
        """Celebrity/profile coverage about sex work should be out of scope."""
        classifier = Classifier(api_key="test-key")
        classifier._client.messages = MagicMock(
            create=MagicMock(
                return_value=MagicMock(
                    content=[
                        TextBlock(
                            type="text",
                            text='{"relevant": false, "enforcement_related": false, "category": "not_relevant", "sub_category": null, "confidence": "high"}',
                        )
                    ]
                )
            )
        )
        article = RawArticle(
            url=HttpUrl("https://www.mako.co.il/music-news/world/Article-b6d8dc4b6d2ec91026.htm"),
            title="אני מממנת את הקריירה שלי משירותי מין",
            snippet="הווידוי המפתיע של הזמרת",
            date=datetime(2026, 3, 14, tzinfo=UTC),
            source_name="mako",
        )

        result = await classifier.classify(article)

        assert result.relevant is False
        assert result.enforcement_related is False
        assert result.category == Category.NOT_RELEVANT
