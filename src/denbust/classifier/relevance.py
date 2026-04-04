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
    "You are a classifier for Hebrew news articles in Israel. "
    "Mark an article as relevant whenever prostitution, brothels, pimping, "
    "human trafficking, or enforcement against these activities is a significant "
    "part of the story, even if no arrest or enforcement action occurred. "
    "Return only JSON and choose only valid category/sub_category combinations."
)

ALLOWED_SUBCATEGORIES: dict[Category, set[SubCategory]] = {
    Category.BROTHEL: {SubCategory.CLOSURE, SubCategory.OPENING},
    Category.PROSTITUTION: {SubCategory.ARREST, SubCategory.FINE},
    Category.PIMPING: {SubCategory.ARREST, SubCategory.SENTENCE},
    Category.TRAFFICKING: {
        SubCategory.ARREST,
        SubCategory.RESCUE,
        SubCategory.SENTENCE,
    },
    Category.ENFORCEMENT: {SubCategory.OPERATION, SubCategory.OTHER},
}

# Classification user prompt (Hebrew-aware)
CLASSIFICATION_PROMPT = """Decide whether the Hebrew news article below is relevant to any of these topics in Israel:
- Brothels / בתי בושת
- Prostitution / זנות
- Pimping / סרסורות
- Human trafficking / סחר בבני אדם
- Police or legal enforcement against the above

If the article is relevant, choose exactly one category and one valid sub_category from this table:

- brothel -> closure | opening
- prostitution -> arrest | fine
- pimping -> arrest | sentence
- trafficking -> arrest | rescue | sentence
- enforcement -> operation | other

Rules:
- Do not choose a sub_category that is not listed for the chosen category.
- If the article is not relevant, use category="not_relevant" and sub_category=null.
- Be inclusive: articles about the covered topics are relevant even when no arrest or raid occurred.
- Prefer the main topic of the article, not a minor passing mention.
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
        system_prompt: str | None = None,
        user_prompt_template: str | None = None,
    ) -> None:
        """Initialize classifier.

        Args:
            api_key: Anthropic API key.
            model: Model to use for classification.
            system_prompt: Optional Anthropic system prompt override.
            user_prompt_template: Optional user prompt template override.
        """
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._system_prompt = system_prompt
        self._user_prompt_template = user_prompt_template or CLASSIFICATION_PROMPT

    async def classify(self, article: RawArticle) -> ClassificationResult:
        """Classify a single article.

        Args:
            article: Article to classify.

        Returns:
            Classification result.
        """
        prompt = self._user_prompt_template.format(
            title=article.title,
            snippet=article.snippet[:300],  # Limit snippet for token efficiency
        )

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=256,
                system=self._system_prompt or CLASSIFICATION_SYSTEM_PROMPT,
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
            if sub_category is not None:
                allowed_subcategories = ALLOWED_SUBCATEGORIES.get(category, set())
                if sub_category not in allowed_subcategories:
                    logger.warning(
                        "Invalid category/sub_category pair from classifier: %s / %s",
                        category,
                        sub_category,
                    )
                    sub_category = None

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


def create_classifier(
    api_key: str,
    model: str = "claude-sonnet-4-20250514",
    system_prompt: str | None = None,
    user_prompt_template: str | None = None,
) -> Classifier:
    """Create a classifier instance.

    Args:
        api_key: Anthropic API key.
        model: Model to use.
        system_prompt: Optional Anthropic system prompt override.
        user_prompt_template: Optional user prompt template override.

    Returns:
        Classifier instance.
    """
    return Classifier(
        api_key=api_key,
        model=model,
        system_prompt=system_prompt,
        user_prompt_template=user_prompt_template,
    )
