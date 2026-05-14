"""LLM-based article classification."""

from __future__ import annotations

import contextlib
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

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

_SECRET_FRAGMENT_PATTERN = re.compile(
    r"(?i)(sk-ant-[a-z0-9_-]+|bearer\s+[a-z0-9._~+/=-]+|api[_-]?key[=:]\s*[^,\s]+)"
)
_PROMPT_FIELD_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SUPPORTED_PROMPT_FIELDS = frozenset({"title", "snippet"})
PARSE_FAILURE_SAMPLE_MAX_COUNT = 8
PARSE_FAILURE_SAMPLE_SHAPE_MAX_LENGTH = 80
PARSE_FAILURE_CATEGORY_KEYS = (
    "empty_response",
    "json_decode_error",
    "non_object_json_array",
    "non_object_json_scalar",
    "object_like_non_json",
    "truncated_response",
    "other_parse_failure",
)
PARSE_FAILURE_JSON_ERROR_KIND_KEYS = (
    "expecting_value",
    "missing_property_name",
    "missing_colon",
    "missing_comma_or_delimiter",
    "unterminated_string",
    "extra_data",
    "invalid_control_character",
    "unknown_json_decode_error",
)
PARSE_FAILURE_SAMPLE_KEYS = (
    "category",
    "response_length",
    "normalized_length",
    "line_count",
    "shape_signature",
    "json_error_kind",
    "json_error_position",
    "json_error_line",
    "json_error_column",
    "starts_with_code_fence",
    "ends_with_code_fence",
)

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


class ClassifierProviderError(RuntimeError):
    """Raised when the configured classifier provider fails before returning usable text."""


@dataclass
class ClassifierWarningCounts:
    """Run-local classifier parser warning counters."""

    parse_failure_count: int = 0
    invalid_taxonomy_pair_count: int = 0
    invalid_legacy_pair_count: int = 0
    relevant_without_usable_taxonomy_count: int = 0

    def as_dict(self) -> dict[str, int]:
        """Return stable artifact fields for run/debug summaries."""
        return {
            "parse_failure_count": self.parse_failure_count,
            "invalid_taxonomy_pair_count": self.invalid_taxonomy_pair_count,
            "invalid_legacy_pair_count": self.invalid_legacy_pair_count,
            "relevant_without_usable_taxonomy_count": self.relevant_without_usable_taxonomy_count,
        }


@dataclass(frozen=True)
class ClassifierParseFailureSample:
    """Sanitized classifier parse-failure shape sample for debug artifacts."""

    category: str
    response_length: int
    normalized_length: int
    line_count: int
    shape_signature: str
    json_error_kind: str | None = None
    json_error_position: int | None = None
    json_error_line: int | None = None
    json_error_column: int | None = None
    starts_with_code_fence: bool = False
    ends_with_code_fence: bool = False

    def as_dict(self) -> dict[str, object]:
        """Return artifact-safe sample metadata without raw response text."""
        return {
            "category": self.category,
            "response_length": self.response_length,
            "normalized_length": self.normalized_length,
            "line_count": self.line_count,
            "shape_signature": self.shape_signature,
            "json_error_kind": self.json_error_kind,
            "json_error_position": self.json_error_position,
            "json_error_line": self.json_error_line,
            "json_error_column": self.json_error_column,
            "starts_with_code_fence": self.starts_with_code_fence,
            "ends_with_code_fence": self.ends_with_code_fence,
        }


@dataclass
class ClassifierParseFailureDiagnostics:
    """Run-local sanitized parse-failure shape diagnostics."""

    category_counts: Counter[str]
    samples: list[ClassifierParseFailureSample]
    sample_max_count: int = PARSE_FAILURE_SAMPLE_MAX_COUNT
    sample_shape_max_length: int = PARSE_FAILURE_SAMPLE_SHAPE_MAX_LENGTH

    @classmethod
    def empty(cls) -> ClassifierParseFailureDiagnostics:
        """Create an empty diagnostics collector."""
        return cls(category_counts=Counter(), samples=[])

    def record(
        self,
        *,
        text: str,
        normalized_text: str,
        category: str,
        json_error: json.JSONDecodeError | None = None,
    ) -> None:
        """Record one sanitized parse-failure shape."""
        stable_category = (
            category if category in PARSE_FAILURE_CATEGORY_KEYS else "other_parse_failure"
        )
        self.category_counts[stable_category] += 1
        sample = ClassifierParseFailureSample(
            category=stable_category,
            response_length=len(text),
            normalized_length=len(normalized_text),
            line_count=text.count("\n") + 1 if text else 0,
            shape_signature=_shape_signature(
                normalized_text,
                max_length=self.sample_shape_max_length,
            ),
            json_error_kind=(
                _json_decode_error_kind(json_error) if json_error is not None else None
            ),
            json_error_position=json_error.pos if json_error is not None else None,
            json_error_line=json_error.lineno if json_error is not None else None,
            json_error_column=json_error.colno if json_error is not None else None,
            starts_with_code_fence=text.strip().startswith("```"),
            ends_with_code_fence=text.strip().endswith("```"),
        )
        existing_categories = {existing.category for existing in self.samples}
        if stable_category in existing_categories:
            if len(self.samples) < self.sample_max_count:
                self.samples.append(sample)
            return
        if len(self.samples) < self.sample_max_count:
            self.samples.append(sample)
            return
        duplicate_index = next(
            (
                index
                for index, existing in enumerate(self.samples)
                if sum(1 for candidate in self.samples if candidate.category == existing.category)
                > 1
            ),
            None,
        )
        if duplicate_index is not None:
            self.samples[duplicate_index] = sample

    def as_dict(self) -> dict[str, object]:
        """Return stable artifact fields for run/debug summaries."""
        category_counts = {
            key: self.category_counts.get(key, 0) for key in PARSE_FAILURE_CATEGORY_KEYS
        }
        return {
            "category_counts": category_counts,
            "samples": [sample.as_dict() for sample in self.samples],
            "sample_count": len(self.samples),
            "sample_max_count": self.sample_max_count,
            "sample_shape_max_length": self.sample_shape_max_length,
        }


def _shape_signature(text: str, *, max_length: int) -> str:
    """Return bounded structural character classes instead of raw response text."""
    signature: list[str] = []
    for char in text[:max_length]:
        if char.isalpha():
            signature.append("A")
        elif char.isdigit():
            signature.append("0")
        elif char.isspace():
            signature.append(" ")
        elif char in "{}[]():,.'\"`-_":
            signature.append(char)
        else:
            signature.append("?")
    return "".join(signature)


def _strip_markdown_fence(text: str) -> tuple[str, bool]:
    """Strip a leading Markdown code fence using the existing parser behavior."""
    normalized_text = text.strip()
    if not normalized_text.startswith("```"):
        return normalized_text, False
    lines = normalized_text.split("\n")
    return "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:]), True


def _json_value_parse_failure_category(value: Any) -> str:
    """Classify valid JSON that is not the expected object shape."""
    if isinstance(value, list):
        return "non_object_json_array"
    return "non_object_json_scalar"


def _json_decode_parse_failure_category(
    *,
    normalized_text: str,
    error: json.JSONDecodeError,
) -> str:
    """Classify malformed JSON without inspecting or retaining raw text."""
    if not normalized_text:
        return "empty_response"
    stripped = normalized_text.strip()
    if stripped[:1] in {"{", "["} and stripped[-1:] not in {"}", "]"}:
        return "truncated_response"
    if error.msg.startswith("Unterminated string"):
        return "truncated_response"
    if stripped.startswith("{"):
        return "object_like_non_json"
    return "json_decode_error"


def _json_decode_error_kind(error: json.JSONDecodeError) -> str:
    """Return a stable safe JSON error kind without persisting raw exception text."""
    message = error.msg
    if message == "Expecting value":
        return "expecting_value"
    if message == "Expecting property name enclosed in double quotes":
        return "missing_property_name"
    if message == "Expecting ':' delimiter":
        return "missing_colon"
    if message == "Expecting ',' delimiter":
        return "missing_comma_or_delimiter"
    if message.startswith("Unterminated string"):
        return "unterminated_string"
    if message == "Extra data":
        return "extra_data"
    if message.startswith("Invalid control character"):
        return "invalid_control_character"
    return "unknown_json_decode_error"


def sanitize_provider_error_message(error: BaseException | str) -> str:
    """Return compact provider error text safe for logs and artifacts."""
    raw_message = str(error) or type(error).__name__
    redacted = _SECRET_FRAGMENT_PATTERN.sub("[redacted]", raw_message)
    return " ".join(redacted.split())[:500]


def _render_classification_prompt(
    template: str,
    *,
    title: str,
    snippet: str,
) -> str:
    """Render article fields without treating prompt JSON examples as placeholders."""
    unknown_fields = sorted(
        {
            match.group(1)
            for match in _PROMPT_FIELD_PATTERN.finditer(template)
            if match.group(1) not in _SUPPORTED_PROMPT_FIELDS
        }
    )
    if unknown_fields:
        formatted = ", ".join(f"{{{field}}}" for field in unknown_fields)
        raise ValueError(f"Unsupported classifier prompt field(s): {formatted}")
    return template.replace("{title}", title).replace("{snippet}", snippet)


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
        self._warning_counts = ClassifierWarningCounts()
        self._parse_failure_diagnostics = ClassifierParseFailureDiagnostics.empty()

    @property
    def warning_counts(self) -> dict[str, int]:
        """Return run-local parser warning counters for diagnostic artifacts."""
        return self._warning_counts.as_dict()

    @property
    def parse_failure_diagnostics(self) -> dict[str, object]:
        """Return sanitized run-local parse-failure shape diagnostics."""
        return self._parse_failure_diagnostics.as_dict()

    def reset_warning_counts(self) -> None:
        """Reset parser warning counters at a pipeline run boundary."""
        self._warning_counts = ClassifierWarningCounts()
        self._parse_failure_diagnostics = ClassifierParseFailureDiagnostics.empty()

    async def classify(self, article: RawArticle) -> ClassificationResult:
        """Classify a single article."""
        prompt = _render_classification_prompt(
            self._user_prompt_template,
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
            sanitized_error = sanitize_provider_error_message(error)
            logger.error("Error classifying article: %s", sanitized_error)
            raise ClassifierProviderError(sanitized_error) from None

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
            normalized_text, _ = _strip_markdown_fence(text)

            data = json.loads(normalized_text)
            if not isinstance(data, dict):
                self._warning_counts.parse_failure_count += 1
                self._parse_failure_diagnostics.record(
                    text=text,
                    normalized_text=normalized_text,
                    category=_json_value_parse_failure_category(data),
                )
                logger.warning(
                    "Failed to parse classification response: expected JSON object, got %s",
                    type(data).__name__,
                )
                return ClassificationResult(
                    relevant=False,
                    enforcement_related=False,
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="low",
                )
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
                    self._warning_counts.invalid_taxonomy_pair_count += 1
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
                        self._warning_counts.invalid_legacy_pair_count += 1
                        logger.warning(
                            "Invalid category/sub_category pair from classifier: %s / %s",
                            category,
                            sub_category,
                        )
                        sub_category = None
                if relevant and category == Category.NOT_RELEVANT:
                    self._warning_counts.relevant_without_usable_taxonomy_count += 1
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

        except json.JSONDecodeError as error:
            self._warning_counts.parse_failure_count += 1
            self._parse_failure_diagnostics.record(
                text=text,
                normalized_text=normalized_text,
                category=_json_decode_parse_failure_category(
                    normalized_text=normalized_text,
                    error=error,
                ),
                json_error=error,
            )
            logger.warning("Failed to parse classification response: %s", error)
            return ClassificationResult(
                relevant=False,
                enforcement_related=False,
                category=Category.NOT_RELEVANT,
                sub_category=None,
                confidence="low",
            )
        except KeyError as error:
            self._warning_counts.parse_failure_count += 1
            self._parse_failure_diagnostics.record(
                text=text,
                normalized_text=normalized_text,
                category="other_parse_failure",
            )
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
