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

# System prompt that separates topical inclusion from enforcement status
CLASSIFICATION_SYSTEM_PROMPT = (
    "You are a classifier for Hebrew news articles in Israel. "
    "Decide two separate things: whether the article is in scope for the monitored "
    "dataset, and whether it describes a concrete enforcement or legal-action event. "
    "Relevant articles must be materially about brothels, prostitution, pimping, "
    "human trafficking, or closely related Israeli legal/public-safety context. "
    "Enforcement-related articles must describe a concrete action such as a raid, arrest, "
    "closure order, indictment, sentence, rescue, investigation, or police operation. "
    "Treat criminal-operation coverage as enforcement-related when it clearly describes "
    "suspects, defendants, charges, police suspicion, criminal investigations, "
    "prostitution or trafficking networks being operated, victims being exploited, "
    "or coercive sexual exploitation, even if the snippet does not explicitly say "
    "'arrest' or 'raid'. "
    "For trafficking stories, if the article is about identified victims, systematic "
    "sexual exploitation, or a trafficking case being uncovered, default to "
    "enforcement_related=true; use rescue when victims are being located, uncovered, "
    "or extracted from exploitation even if the short summary focuses on the abuse. "
    "Exclude celebrity, lifestyle, profile, entertainment, or generic commentary stories "
    "even if they mention sex work, unless they are themselves about a concrete Israeli "
    "enforcement or legal/public-safety event. "
    "Use category 'prostitution' for solicitation to prostitution, prostitution rings, "
    "and operating prostitution businesses or networks. Use category 'pimping' mainly "
    "when the article is centrally about a pimp / סרסור or a pimping sentence. "
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

Make two separate decisions:
1. relevant: is the article in scope for the monitored dataset and reports?
2. enforcement_related: does the article describe a concrete enforcement or legal-action event?

If the article is relevant, choose exactly one category and one valid sub_category from this table:

- brothel -> closure | opening
- prostitution -> arrest | fine
- pimping -> arrest | sentence
- trafficking -> arrest | rescue | sentence
- enforcement -> operation | other

Rules:
- Do not choose a sub_category that is not listed for the chosen category.
- If the article is not relevant, use enforcement_related=false, category="not_relevant", and sub_category=null.
- Topically relevant articles with no concrete enforcement event should use relevant=true, enforcement_related=false, and sub_category=null.
- Enforcement-related articles should use relevant=true, enforcement_related=true, and the best-fitting category/sub_category.
- Mark enforcement_related=true not only for explicit raids or arrests, but also when the article clearly describes suspects, defendants, charges, criminal investigations, prostitution/trafficking networks being run, victims being exploited, or coercive sexual exploitation in Israel.
- For trafficking coverage, if the text is about identified victims or a trafficking case being uncovered, prefer enforcement_related=true; use sub_category=rescue when victims are being uncovered or extracted from exploitation.
- Celebrity, lifestyle, profile, or entertainment stories about sex work are not relevant unless they are themselves about a concrete Israeli enforcement or legal/public-safety event.
- Foreign or generic commentary stories with no Israeli monitored-angle are not relevant.
- Prefer the main topic of the article, not a minor passing mention.
- Use category prostitution for שידול לזנות, prostitution rings, and running prostitution businesses or networks. Use pimping only when the text is specifically about a pimp / סרסור or a pimping sentence.
- confidence: high | medium | low

Article:
כותרת: {title}
תקציר: {snippet}

Respond with JSON only, no explanation.
If relevant and enforcement-related: {{"relevant": true, "enforcement_related": true, "category": "...", "sub_category": "...", "confidence": "..."}}
If relevant but not enforcement-related: {{"relevant": true, "enforcement_related": false, "category": "...", "sub_category": null, "confidence": "..."}}
If not relevant: {{"relevant": false, "enforcement_related": false, "category": "not_relevant", "sub_category": null, "confidence": "high"}}"""


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
                enforcement_related=False,
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
                enforcement_related=bool(data.get("enforcement_related", False)),
                category=category,
                sub_category=sub_category,
                confidence=confidence,
            )

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse classification response: {e}")
            return ClassificationResult(
                relevant=False,
                enforcement_related=False,
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
