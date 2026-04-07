"""LLM-based article classification."""

from __future__ import annotations

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
from denbust.taxonomy import default_taxonomy

logger = logging.getLogger(__name__)

# Legacy compatibility matrix kept for older parser paths and tests.
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

# System prompt that separates topical inclusion from enforcement status.
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
    "enforcement_related=true. "
    "Exclude celebrity, lifestyle, profile, entertainment, or generic commentary stories "
    "even if they mention sex work, unless they are themselves about a concrete Israeli "
    "enforcement or legal/public-safety event. "
    "Return only JSON and choose only valid TFHT taxonomy category/subcategory pairs."
)


def _build_classification_prompt() -> str:
    taxonomy = default_taxonomy()
    prompt = """Decide whether the Hebrew news article below is relevant to the monitored TFHT dataset about brothels / בתי בושת, prostitution / זנות, pimping / סרסורות, and human trafficking / סחר בבני אדם.

Make two separate decisions:
1. relevant: is the article in scope for the monitored dataset and reports?
2. enforcement_related: does the article describe a concrete enforcement or legal-action event?

If the article is relevant, choose exactly one TFHT taxonomy category and one valid TFHT taxonomy subcategory from this table:

{taxonomy_table}

Legacy coarse mapping reference:
- brothel -> closure | opening
- prostitution -> arrest | fine
- pimping -> arrest | sentence
- trafficking -> arrest | rescue | sentence
- enforcement -> operation | other

Rules:
- Do not invent taxonomy ids. Use only ids that appear in the table.
- If the article is not relevant, use enforcement_related=false, taxonomy_category_id=null, and taxonomy_subcategory_id=null.
- If the article is relevant but the best TFHT leaf is unclear, return relevant=true, enforcement_related=false, taxonomy_category_id=null, taxonomy_subcategory_id=null, and include the best legacy coarse category in category.
- enforcement_related is independent from index_relevant. Do not guess index_relevant; it is derived downstream.
- Celebrity, lifestyle, profile, or entertainment stories about sex work are not relevant unless they are themselves about a concrete Israeli enforcement or legal/public-safety event.
- Foreign or generic commentary stories with no Israeli monitored-angle are not relevant.
- Prefer the main topic of the article, not a minor passing mention.
- confidence: high | medium | low

Article:
כותרת: {title}
תקציר: {snippet}

Respond with JSON only, no explanation.
If relevant and taxonomy is known: {{"relevant": true, "enforcement_related": true, "taxonomy_category_id": "...", "taxonomy_subcategory_id": "...", "confidence": "..."}}
If relevant but taxonomy is unclear: {{"relevant": true, "enforcement_related": false, "category": "...", "sub_category": null, "taxonomy_category_id": null, "taxonomy_subcategory_id": null, "confidence": "low"}}
If not relevant: {{"relevant": false, "enforcement_related": false, "taxonomy_category_id": null, "taxonomy_subcategory_id": null, "confidence": "high"}}""".format(
        taxonomy_table=taxonomy.prompt_table(),
        title="{title}",
        snippet="{snippet}",
    )
    escaped = prompt.replace("{", "{{").replace("}", "}}")
    return escaped.replace("{{title}}", "{title}").replace("{{snippet}}", "{snippet}")


CLASSIFICATION_PROMPT = _build_classification_prompt()


class Classifier:
    """LLM-based article classifier."""

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-20250514",
        system_prompt: str | None = None,
        user_prompt_template: str | None = None,
    ) -> None:
        """Initialize classifier."""
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._system_prompt = system_prompt
        self._user_prompt_template = user_prompt_template or CLASSIFICATION_PROMPT
        self._taxonomy = default_taxonomy()

    async def classify(self, article: RawArticle) -> ClassificationResult:
        """Classify a single article."""
        prompt = self._user_prompt_template.format(
            title=article.title,
            snippet=article.snippet[:300],
        )

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=256,
                system=self._system_prompt or CLASSIFICATION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            text = ""
            if response.content:
                first_block = response.content[0]
                if isinstance(first_block, TextBlock):
                    text = first_block.text
            return self._parse_response(text)

        except anthropic.APIError as error:
            logger.error("Error classifying article: %s", error)
            return ClassificationResult(
                relevant=False,
                enforcement_related=False,
                category=Category.NOT_RELEVANT,
                sub_category=None,
                confidence="low",
            )

    async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
        """Classify a batch of articles."""
        results: list[ClassifiedArticle] = []

        for article in articles:
            classification = await self.classify(article)
            results.append(ClassifiedArticle(article=article, classification=classification))

        return results

    def _parse_response(self, text: str) -> ClassificationResult:
        """Parse LLM response into ClassificationResult."""
        try:
            normalized_text = text.strip()
            if normalized_text.startswith("```"):
                lines = normalized_text.split("\n")
                normalized_text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

            data = json.loads(normalized_text)
            relevant = bool(data.get("relevant", False))
            enforcement_related = bool(data.get("enforcement_related", False))

            taxonomy_category_id = data.get("taxonomy_category_id")
            taxonomy_subcategory_id = data.get("taxonomy_subcategory_id")
            taxonomy_version: str | None = None
            index_relevant = False
            category = Category.NOT_RELEVANT
            sub_category = None

            if relevant and taxonomy_category_id and taxonomy_subcategory_id:
                if self._taxonomy.has_pair(taxonomy_category_id, taxonomy_subcategory_id):
                    taxonomy_version = self._taxonomy.version
                    category, sub_category = self._taxonomy.legacy_mapping(
                        taxonomy_category_id,
                        taxonomy_subcategory_id,
                    )
                    index_relevant = self._taxonomy.is_index_relevant(
                        taxonomy_category_id,
                        taxonomy_subcategory_id,
                    )
                else:
                    logger.warning(
                        "Invalid taxonomy pair from classifier: %s / %s",
                        taxonomy_category_id,
                        taxonomy_subcategory_id,
                    )
                    relevant = False
                    enforcement_related = False
                    taxonomy_category_id = None
                    taxonomy_subcategory_id = None
            else:
                taxonomy_category_id = None
                taxonomy_subcategory_id = None
                category_str = data.get("category", "not_relevant")
                try:
                    category = Category(category_str)
                except ValueError:
                    category = Category.NOT_RELEVANT

                sub_category_str = data.get("sub_category")
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
                if relevant and category == Category.NOT_RELEVANT:
                    logger.warning(
                        "Classifier returned relevant=true without a usable taxonomy leaf or legacy category"
                    )
                    relevant = False
                    enforcement_related = False

            confidence = data.get("confidence", "medium")
            if confidence not in ("high", "medium", "low"):
                confidence = "medium"

            return ClassificationResult(
                relevant=relevant,
                enforcement_related=enforcement_related,
                index_relevant=index_relevant,
                taxonomy_version=taxonomy_version,
                taxonomy_category_id=taxonomy_category_id,
                taxonomy_subcategory_id=taxonomy_subcategory_id,
                category=category,
                sub_category=sub_category,
                confidence=confidence,
            )

        except (json.JSONDecodeError, KeyError) as error:
            logger.warning("Failed to parse classification response: %s", error)
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
    """Create a classifier instance."""
    return Classifier(
        api_key=api_key,
        model=model,
        system_prompt=system_prompt,
        user_prompt_template=user_prompt_template,
    )
