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
    "tail_shape_signature",
    "leading_brace_count",
    "trailing_brace_count",
    "brace_balance",
    "starts_with_double_open_object",
    "ends_with_double_close_object",
    "outer_wrapper_candidate",
    "inner_object_candidate",
    "contains_balanced_inner_object",
    "inner_json_object_candidate",
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

Examples (verified ground truth — use as classification anchors):

כותרת: דרמה בעיר החרדית: שוטרים פשטו על בית בושת - "משחקים אבא ואמא" | צפו
תקציר: חקירה סמויה שהתגלגלה מתלונה אחת הובילה את הכוחות אל דירה שקטה בעיר החרדית, שם נחשפה לפי החשד פעילות שנוהלה מאחורי דלת סגורה.
{{"relevant": true, "enforcement_related": true, "taxonomy_category_id": "brothels", "taxonomy_subcategory_id": "administrative_closure", "confidence": "high"}}

כותרת: פשיטה על בית בושת בבת ים: אותרו 4 קורבנות סחר למטרות מין, חשודה נעצרה
תקציר: המשטרה פשטה על דירה בבת ים ועצרו אישה בשנות ה-40 לחייה, בחשד לסחר בבני אדם למטרות שירותי מין של כמה נשים.
{{"relevant": true, "enforcement_related": true, "taxonomy_category_id": "human_trafficking", "taxonomy_subcategory_id": "trafficking_sexual_exploitation", "confidence": "high"}}

כותרת: "שידול לזנות": עו"ד נעצר לאחר שנשים וחיילות התלוננו כי סחט מהן יחסי מין בתמורה לייצוג
תקציר: בית המשפט התיר לפרסם את זהותו של יוצא הפרקליטות הצבאית, שנעצר לאחר שנשים וביניהן חיילות התלוננו נגדו על שידול לזנות.
{{"relevant": true, "enforcement_related": true, "taxonomy_category_id": "pimping_prostitution", "taxonomy_subcategory_id": "soliciting_prostitution", "confidence": "high"}}

כותרת: המשטרה לא אוכפת איסור צריכת זנות בפריפריה, ו-98% מהדו"חות ניתנו במרכז
תקציר: במטה למאבק בסחר בנשים ובזנות טוענים כי המשטרה כמעט אינה מבצעת אכיפה בפריפריה ומתריעים מפני היעדר מדיניות אכיפה ארצית ברורה.
{{"relevant": true, "enforcement_related": false, "taxonomy_category_id": "pimping_prostitution", "taxonomy_subcategory_id": "nordic_model_law", "confidence": "medium"}}

כותרת: נתניהו ביקר בצפון: "נמשיך לפעול עד שנחזיר את כל החטופים"
תקציר: ראש הממשלה בנימין נתניהו הגיע לסיור ביטחוני בצפון הארץ ונפגש עם מפקדי היחידות הלוחמות.
{{"relevant": false, "enforcement_related": false, "taxonomy_category_id": null, "taxonomy_subcategory_id": null, "confidence": "high"}}

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
    """Run-local classifier parser warning and recovery counters."""

    parse_failure_count: int = 0
    invalid_taxonomy_pair_count: int = 0
    invalid_legacy_pair_count: int = 0
    relevant_without_usable_taxonomy_count: int = 0
    double_wrapper_recovery_count: int = 0

    def as_dict(self) -> dict[str, int]:
        """Return stable artifact fields for run/debug summaries."""
        return {
            "parse_failure_count": self.parse_failure_count,
            "invalid_taxonomy_pair_count": self.invalid_taxonomy_pair_count,
            "invalid_legacy_pair_count": self.invalid_legacy_pair_count,
            "relevant_without_usable_taxonomy_count": self.relevant_without_usable_taxonomy_count,
            "double_wrapper_recovery_count": self.double_wrapper_recovery_count,
        }


@dataclass(frozen=True)
class ClassifierParseFailureSample:
    """Sanitized classifier parse-failure shape sample for debug artifacts."""

    category: str
    response_length: int
    normalized_length: int
    line_count: int
    shape_signature: str
    tail_shape_signature: str
    leading_brace_count: int
    trailing_brace_count: int
    brace_balance: int
    starts_with_double_open_object: bool
    ends_with_double_close_object: bool
    outer_wrapper_candidate: bool
    inner_object_candidate: bool
    contains_balanced_inner_object: bool
    inner_json_object_candidate: bool
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
            "tail_shape_signature": self.tail_shape_signature,
            "leading_brace_count": self.leading_brace_count,
            "trailing_brace_count": self.trailing_brace_count,
            "brace_balance": self.brace_balance,
            "starts_with_double_open_object": self.starts_with_double_open_object,
            "ends_with_double_close_object": self.ends_with_double_close_object,
            "outer_wrapper_candidate": self.outer_wrapper_candidate,
            "inner_object_candidate": self.inner_object_candidate,
            "contains_balanced_inner_object": self.contains_balanced_inner_object,
            "inner_json_object_candidate": self.inner_json_object_candidate,
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
        structure = _parse_failure_structure_metadata(normalized_text)
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
            tail_shape_signature=_tail_shape_signature(
                normalized_text,
                max_length=self.sample_shape_max_length,
            ),
            leading_brace_count=structure.leading_brace_count,
            trailing_brace_count=structure.trailing_brace_count,
            brace_balance=structure.brace_balance,
            starts_with_double_open_object=structure.starts_with_double_open_object,
            ends_with_double_close_object=structure.ends_with_double_close_object,
            outer_wrapper_candidate=structure.outer_wrapper_candidate,
            inner_object_candidate=structure.inner_object_candidate,
            contains_balanced_inner_object=structure.contains_balanced_inner_object,
            inner_json_object_candidate=structure.inner_json_object_candidate,
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


@dataclass(frozen=True)
class ParseFailureStructureMetadata:
    """Structural parse-failure indicators that do not retain response content."""

    leading_brace_count: int
    trailing_brace_count: int
    brace_balance: int
    starts_with_double_open_object: bool
    ends_with_double_close_object: bool
    outer_wrapper_candidate: bool
    inner_object_candidate: bool
    contains_balanced_inner_object: bool
    inner_json_object_candidate: bool


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


def _tail_shape_signature(text: str, *, max_length: int) -> str:
    """Return bounded structural character classes for the response tail."""
    if max_length <= 0:
        return ""
    return _shape_signature(text[-max_length:], max_length=max_length)


def _leading_char_count(text: str, char: str) -> int:
    """Count repeated leading structural characters after surrounding whitespace."""
    stripped = text.strip()
    count = 0
    for candidate in stripped:
        if candidate != char:
            break
        count += 1
    return count


def _trailing_char_count(text: str, char: str) -> int:
    """Count repeated trailing structural characters after surrounding whitespace."""
    stripped = text.strip()
    count = 0
    for candidate in reversed(stripped):
        if candidate != char:
            break
        count += 1
    return count


def _structural_brace_balance(text: str) -> int:
    """Count brace balance outside JSON double-quoted strings."""
    balance = 0
    in_string = False
    escaped = False
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            balance += 1
        elif char == "}":
            balance -= 1
    return balance


def _is_json_object(text: str) -> bool:
    """Return whether text parses as a JSON object without retaining parsed content."""
    with contextlib.suppress(json.JSONDecodeError):
        return isinstance(json.loads(text), dict)
    return False


def _parse_failure_structure_metadata(text: str) -> ParseFailureStructureMetadata:
    """Return bounded structure-only indicators for malformed object-like responses."""
    stripped = text.strip()
    leading_brace_count = _leading_char_count(stripped, "{")
    trailing_brace_count = _trailing_char_count(stripped, "}")
    brace_balance = _structural_brace_balance(stripped)
    starts_with_double_open_object = stripped.startswith("{{")
    ends_with_double_close_object = stripped.endswith("}}")
    outer_wrapper_candidate = (
        starts_with_double_open_object and ends_with_double_close_object and brace_balance == 0
    )
    inner_text = stripped[1:-1].strip() if outer_wrapper_candidate else ""
    inner_object_candidate = inner_text.startswith("{") and inner_text.endswith("}")
    contains_balanced_inner_object = (
        inner_object_candidate and _structural_brace_balance(inner_text) == 0
    )
    inner_json_object_candidate = contains_balanced_inner_object and _is_json_object(inner_text)
    return ParseFailureStructureMetadata(
        leading_brace_count=leading_brace_count,
        trailing_brace_count=trailing_brace_count,
        brace_balance=brace_balance,
        starts_with_double_open_object=starts_with_double_open_object,
        ends_with_double_close_object=ends_with_double_close_object,
        outer_wrapper_candidate=outer_wrapper_candidate,
        inner_object_candidate=inner_object_candidate,
        contains_balanced_inner_object=contains_balanced_inner_object,
        inner_json_object_candidate=inner_json_object_candidate,
    )


def _is_recoverable_double_wrapper_structure(
    structure: ParseFailureStructureMetadata,
) -> bool:
    """Return whether structure matches the proven recovery gate."""
    return (
        structure.leading_brace_count == 2
        and structure.trailing_brace_count == 2
        and structure.outer_wrapper_candidate
        and structure.inner_object_candidate
        and structure.contains_balanced_inner_object
        and structure.inner_json_object_candidate
    )


def _recover_double_wrapped_json_object(text: str) -> dict[str, Any] | None:
    """Recover the proven ``{{ ... }}`` classifier output shape only."""
    stripped = text.strip()
    structure = _parse_failure_structure_metadata(stripped)
    if not _is_recoverable_double_wrapper_structure(structure):
        return None
    inner_text = stripped[1:-1].strip()
    try:
        recovered = json.loads(inner_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(recovered, dict):
        return None
    return recovered


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
        """Return run-local parser warning and recovery counters for artifacts."""
        return self._warning_counts.as_dict()

    @property
    def parse_failure_diagnostics(self) -> dict[str, object]:
        """Return sanitized run-local parse-failure shape diagnostics."""
        return self._parse_failure_diagnostics.as_dict()

    def reset_warning_counts(self) -> None:
        """Reset parser warning and recovery counters at a pipeline run boundary."""
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
            normalized_text, stripped_markdown_fence = _strip_markdown_fence(text)

            try:
                data = json.loads(normalized_text)
            except json.JSONDecodeError as error:
                recovered_data = (
                    None
                    if stripped_markdown_fence
                    else _recover_double_wrapped_json_object(normalized_text)
                )
                if recovered_data is None:
                    raise error
                self._warning_counts.double_wrapper_recovery_count += 1
                data = recovered_data
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
