"""LLM-based article classification."""

import contextlib
import json
import logging

import anthropic
from anthropic.types import TextBlock

from denbust.data_models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SubCategory,
)

logger = logging.getLogger(__name__)

# Classification prompt (Hebrew-aware)
CLASSIFICATION_PROMPT = """You classify Hebrew news articles for relevance to anti-prostitution enforcement in Israel.

Given a news headline and snippet, determine:
1. Is this relevant to: brothels, prostitution, pimping, human trafficking, or enforcement?
2. Category: brothel | prostitution | pimping | trafficking | enforcement | not_relevant
3. Sub-category (if relevant):
   - brothel: closure | opening
   - prostitution: arrest | fine
   - pimping: arrest | sentence
   - trafficking: arrest | rescue | sentence
   - enforcement: operation | other
4. Confidence: high | medium | low

Article:
כותרת: {title}
תקציר: {snippet}

Respond with JSON only, no explanation:
{{"relevant": true/false, "category": "...", "sub_category": "...", "confidence": "..."}}"""


class Classifier:
    """LLM-based article classifier."""

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-20250514",
    ) -> None:
        """Initialize classifier.

        Args:
            api_key: Anthropic API key.
            model: Model to use for classification.
        """
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    async def classify(self, article: RawArticle) -> ClassificationResult:
        """Classify a single article.

        Args:
            article: Article to classify.

        Returns:
            Classification result.
        """
        prompt = CLASSIFICATION_PROMPT.format(
            title=article.title,
            snippet=article.snippet[:300],  # Limit snippet for token efficiency
        )

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )

            # Extract text from response
            text = ""
            if response.content:
                first_block = response.content[0]
                if isinstance(first_block, TextBlock):
                    text = first_block.text
            return self._parse_response(text)

        except anthropic.APIError as e:
            logger.error(f"Error classifying article: {e}")
            return ClassificationResult(
                relevant=False,
                category=Category.NOT_RELEVANT,
                sub_category=None,
                confidence="low",
            )

    async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
        """Classify a batch of articles.

        Args:
            articles: Articles to classify.

        Returns:
            List of classified articles.
        """
        results: list[ClassifiedArticle] = []

        for article in articles:
            classification = await self.classify(article)
            results.append(ClassifiedArticle(article=article, classification=classification))

        return results

    def _parse_response(self, text: str) -> ClassificationResult:
        """Parse LLM response into ClassificationResult.

        Args:
            text: Raw response text.

        Returns:
            Parsed classification result.
        """
        try:
            # Try to parse as JSON
            # Handle potential markdown code blocks
            text = text.strip()
            if text.startswith("```"):
                # Remove markdown code block
                lines = text.split("\n")
                text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

            data = json.loads(text)

            # Parse category
            category_str = data.get("category", "not_relevant")
            try:
                category = Category(category_str)
            except ValueError:
                category = Category.NOT_RELEVANT

            # Parse sub_category
            sub_category_str = data.get("sub_category")
            sub_category = None
            if sub_category_str:
                with contextlib.suppress(ValueError):
                    sub_category = SubCategory(sub_category_str)

            # Parse confidence
            confidence = data.get("confidence", "medium")
            if confidence not in ("high", "medium", "low"):
                confidence = "medium"

            return ClassificationResult(
                relevant=bool(data.get("relevant", False)),
                category=category,
                sub_category=sub_category,
                confidence=confidence,
            )

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse classification response: {e}")
            return ClassificationResult(
                relevant=False,
                category=Category.NOT_RELEVANT,
                sub_category=None,
                confidence="low",
            )


def create_classifier(api_key: str, model: str = "claude-sonnet-4-20250514") -> Classifier:
    """Create a classifier instance.

    Args:
        api_key: Anthropic API key.
        model: Model to use.

    Returns:
        Classifier instance.
    """
    return Classifier(api_key=api_key, model=model)
