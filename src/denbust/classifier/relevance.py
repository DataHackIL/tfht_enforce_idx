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

# System prompt that primes the model with a broad, inclusive framing
CLASSIFICATION_SYSTEM_PROMPT = (
    "You are a classifier for Hebrew news articles. "
    "Your job is to identify articles relevant to prostitution, brothels, human trafficking, "
    "pimping, and law-enforcement actions against these activities in Israel. "
    "Be inclusive: mark an article as relevant whenever any of these topics appears as a "
    "significant part of the story, even if no arrest or enforcement action has occurred."
)

# Classification user prompt (Hebrew-aware)
CLASSIFICATION_PROMPT = """Decide whether the Hebrew news article below is relevant to any of these topics in Israel:
- Brothels / בתי בושת
- Prostitution / זנות
- Pimping / סרסורות
- Human trafficking / סחר בבני אדם
- Police or legal enforcement against the above

If relevant, also assign:
- category: brothel | prostitution | pimping | trafficking | enforcement
- sub_category: closure | opening | arrest | fine | sentence | rescue | operation | other
- confidence: high | medium | low

Article:
כותרת: {title}
תקציר: {snippet}

Respond with JSON only, no explanation.
If relevant: {{"relevant": true, "category": "...", "sub_category": "...", "confidence": "..."}}
If not relevant: {{"relevant": false, "category": "not_relevant", "sub_category": null, "confidence": "high"}}"""


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
                system=CLASSIFICATION_SYSTEM_PROMPT,
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
