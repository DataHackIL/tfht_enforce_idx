"""Classifier variant evaluation against the permanent validation set."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml
from pydantic import HttpUrl

from denbust.classifier.relevance import create_classifier
from denbust.config import Config
from denbust.data_models import Category, RawArticle
from denbust.pipeline import setup_logging
from denbust.validation.common import (
    DEFAULT_VALIDATION_SET_PATH,
    DEFAULT_VARIANT_MATRIX_PATH,
    default_evaluation_output_path,
    parse_bool,
    parse_datetime,
    read_csv_rows,
)
from denbust.validation.models import (
    AccuracyStageMetrics,
    BinaryStageMetrics,
    ClassifierVariantMatrix,
    ClassifierVariantSpec,
    LabelBreakdownMetrics,
    LabelCountMetrics,
    ValidationDatasetSummary,
    ValidationReportPayload,
    VariantMetrics,
)


@dataclass(frozen=True)
class ValidationLabel:
    """Expected labels for one validation example."""

    relevant: bool
    enforcement_related: bool
    category: str
    sub_category: str
    index_relevant: bool
    taxonomy_version: str
    taxonomy_category_id: str
    taxonomy_subcategory_id: str

    def __eq__(self, other: object) -> bool:
        """Support legacy tuple comparisons in older tests."""
        if isinstance(other, ValidationLabel):
            return (
                self.relevant == other.relevant
                and self.enforcement_related == other.enforcement_related
                and self.category == other.category
                and self.sub_category == other.sub_category
                and self.index_relevant == other.index_relevant
                and self.taxonomy_version == other.taxonomy_version
                and self.taxonomy_category_id == other.taxonomy_category_id
                and self.taxonomy_subcategory_id == other.taxonomy_subcategory_id
            )
        if isinstance(other, tuple) and len(other) == 4:
            return (
                self.relevant,
                self.enforcement_related,
                self.category,
                self.sub_category,
            ) == other
        return False


@dataclass(frozen=True)
class ValidationEvaluateResult:
    """Result of evaluating classifier variants."""

    output_path: Path
    markdown_path: Path
    dataset_summary: ValidationDatasetSummary
    rankings: list[VariantMetrics]


def _coerce_label(value: ValidationLabel | tuple[bool, bool, str, str]) -> ValidationLabel:
    """Support legacy tuple-based test inputs as well as typed labels."""
    if isinstance(value, ValidationLabel):
        return value
    relevant, enforcement_related, category, sub_category = value
    return ValidationLabel(
        relevant=relevant,
        enforcement_related=enforcement_related,
        category=category,
        sub_category=sub_category,
        index_relevant=False,
        taxonomy_version="",
        taxonomy_category_id="",
        taxonomy_subcategory_id="",
    )


def _load_variant_matrix(path: Path) -> ClassifierVariantMatrix:
    if not path.exists():
        raise FileNotFoundError(f"Variant matrix not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    matrix = ClassifierVariantMatrix.model_validate(data)
    if not matrix.variants:
        raise ValueError("Variant matrix must include at least one variant")
    return matrix


def _load_validation_examples(path: Path) -> tuple[list[RawArticle], list[ValidationLabel]]:
    if not path.exists():
        raise FileNotFoundError(f"Validation set not found: {path}")
    rows = read_csv_rows(path)
    if not rows:
        raise ValueError("Validation set is empty")

    articles: list[RawArticle] = []
    labels: list[ValidationLabel] = []
    for row in rows:
        article = RawArticle(
            url=HttpUrl(row["url"].strip()),
            title=row["title"],
            snippet=row["snippet"],
            date=parse_datetime(row["article_date"]),
            source_name=row["source_name"].strip(),
        )
        articles.append(article)
        labels.append(
            ValidationLabel(
                relevant=parse_bool(row["relevant"]),
                enforcement_related=parse_bool(row.get("enforcement_related", "False") or "False"),
                category=Category(row["category"].strip()).value,
                sub_category=row["sub_category"].strip(),
                index_relevant=parse_bool(row.get("index_relevant", "False") or "False"),
                taxonomy_version=row.get("taxonomy_version", "").strip(),
                taxonomy_category_id=row.get("taxonomy_category_id", "").strip(),
                taxonomy_subcategory_id=row.get("taxonomy_subcategory_id", "").strip(),
            )
        )
    return articles, labels


def _binary_stage_metrics(*, tp: int, fp: int, fn: int, tn: int) -> BinaryStageMetrics:
    evaluated_examples = tp + fp + fn + tn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    accuracy = (tp + tn) / evaluated_examples if evaluated_examples else 0.0
    return BinaryStageMetrics(
        evaluated_examples=evaluated_examples,
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        precision=precision,
        recall=recall,
        f1=f1,
        accuracy=accuracy,
    )


def _accuracy_stage_metrics(*, correct: int, evaluated_examples: int) -> AccuracyStageMetrics:
    return AccuracyStageMetrics(
        evaluated_examples=evaluated_examples,
        correct=correct,
        accuracy=(correct / evaluated_examples) if evaluated_examples else 0.0,
    )


def _label_breakdown_metrics(
    counts: Counter[str],
    correct_counts: Counter[str] | None = None,
) -> list[LabelBreakdownMetrics]:
    matches = correct_counts or Counter()
    return [
        LabelBreakdownMetrics(
            label=label,
            evaluated_examples=evaluated_examples,
            correct=matches[label],
            accuracy=(matches[label] / evaluated_examples) if evaluated_examples else 0.0,
        )
        for label, evaluated_examples in sorted(
            counts.items(), key=lambda item: (-item[1], item[0])
        )
    ]


def _label_count_metrics(counts: Counter[str]) -> list[LabelCountMetrics]:
    return [
        LabelCountMetrics(
            label=label,
            evaluated_examples=evaluated_examples,
        )
        for label, evaluated_examples in sorted(
            counts.items(), key=lambda item: (-item[1], item[0])
        )
    ]


def _display_label(value: str) -> str:
    return value if value else "(none)"


def _markdown_path_for_json(path: Path) -> Path:
    return path.with_suffix(".md")


def _resolve_report_paths(
    *,
    config: Config,
    collected_at: datetime,
    output_path: Path | None,
) -> tuple[Path, Path]:
    report_path = output_path or default_evaluation_output_path(config, collected_at)
    if output_path is not None and report_path.suffix.casefold() != ".json":
        msg = f"Evaluation output path must end with .json: {report_path}"
        raise ValueError(msg)
    markdown_path = _markdown_path_for_json(report_path)
    if markdown_path == report_path:
        msg = "Markdown report path must differ from the JSON report path"
        raise ValueError(msg)
    return report_path, markdown_path


def _build_dataset_summary(labels: Sequence[ValidationLabel]) -> ValidationDatasetSummary:
    relevant_examples = 0
    legacy_only_examples = 0
    taxonomy_labeled_examples = 0
    legacy_category_counts: Counter[str] = Counter()
    legacy_subcategory_counts: Counter[str] = Counter()
    taxonomy_category_counts: Counter[str] = Counter()
    taxonomy_subcategory_counts: Counter[str] = Counter()

    for label in labels:
        if label.relevant:
            relevant_examples += 1
            legacy_category_counts[label.category] += 1
            legacy_subcategory_counts[label.sub_category] += 1
        has_taxonomy_category = bool(label.taxonomy_category_id)
        has_taxonomy_subcategory = bool(label.taxonomy_subcategory_id)
        if has_taxonomy_category and has_taxonomy_subcategory:
            taxonomy_labeled_examples += 1
            taxonomy_category_counts[label.taxonomy_category_id] += 1
            taxonomy_subcategory_counts[label.taxonomy_subcategory_id] += 1
        elif has_taxonomy_category != has_taxonomy_subcategory:
            msg = (
                "Validation label has partial taxonomy ids; both taxonomy_category_id "
                "and taxonomy_subcategory_id must be provided together."
            )
            raise ValueError(msg)
        else:
            legacy_only_examples += 1

    return ValidationDatasetSummary(
        total_examples=len(labels),
        relevant_examples=relevant_examples,
        legacy_only_examples=legacy_only_examples,
        taxonomy_labeled_examples=taxonomy_labeled_examples,
        legacy_category_counts_relevant_only=_label_count_metrics(legacy_category_counts),
        legacy_subcategory_counts_relevant_only=_label_count_metrics(legacy_subcategory_counts),
        taxonomy_category_counts=_label_count_metrics(taxonomy_category_counts),
        taxonomy_subcategory_counts=_label_count_metrics(taxonomy_subcategory_counts),
    )


def _score_predictions(
    labels: Sequence[ValidationLabel | tuple[bool, bool, str, str]],
    predictions: Sequence[ValidationLabel | tuple[bool, bool, str, str]],
    *,
    variant: ClassifierVariantSpec,
    model: str,
) -> VariantMetrics:
    tp = fp = fn = tn = 0
    relevant_rows = 0
    enforcement_tp = enforcement_fp = enforcement_fn = enforcement_tn = 0
    category_matches = 0
    subcategory_matches = 0
    exact_matches = 0

    taxonomy_labeled_rows = 0
    taxonomy_category_matches = 0
    taxonomy_subcategory_matches = 0
    index_tp = index_fp = index_fn = index_tn = 0
    legacy_category_counts: Counter[str] = Counter()
    legacy_category_correct: Counter[str] = Counter()
    legacy_subcategory_counts: Counter[str] = Counter()
    legacy_subcategory_correct: Counter[str] = Counter()
    taxonomy_category_counts: Counter[str] = Counter()
    taxonomy_category_correct: Counter[str] = Counter()
    taxonomy_subcategory_counts: Counter[str] = Counter()
    taxonomy_subcategory_correct: Counter[str] = Counter()

    for raw_true_label, raw_predicted_label in zip(labels, predictions, strict=True):
        true_label = _coerce_label(raw_true_label)
        predicted_label = _coerce_label(raw_predicted_label)
        predicted_enforcement_related = (
            predicted_label.relevant and predicted_label.enforcement_related
        )

        if true_label.relevant and predicted_label.relevant:
            tp += 1
        elif not true_label.relevant and predicted_label.relevant:
            fp += 1
        elif true_label.relevant and not predicted_label.relevant:
            fn += 1
        else:
            tn += 1

        if true_label.relevant:
            relevant_rows += 1
            legacy_category_counts[true_label.category] += 1
            legacy_subcategory_counts[true_label.sub_category] += 1
            if true_label.enforcement_related and predicted_enforcement_related:
                enforcement_tp += 1
            elif not true_label.enforcement_related and predicted_enforcement_related:
                enforcement_fp += 1
            elif true_label.enforcement_related and not predicted_enforcement_related:
                enforcement_fn += 1
            else:
                enforcement_tn += 1
            if predicted_label.relevant:
                if predicted_label.category == true_label.category:
                    category_matches += 1
                    legacy_category_correct[true_label.category] += 1
                if predicted_label.sub_category == true_label.sub_category:
                    subcategory_matches += 1
                    legacy_subcategory_correct[true_label.sub_category] += 1

        if true_label.taxonomy_category_id and true_label.taxonomy_subcategory_id:
            taxonomy_labeled_rows += 1
            taxonomy_category_counts[true_label.taxonomy_category_id] += 1
            taxonomy_subcategory_counts[true_label.taxonomy_subcategory_id] += 1
            if predicted_label.index_relevant and true_label.index_relevant:
                index_tp += 1
            elif predicted_label.index_relevant and not true_label.index_relevant:
                index_fp += 1
            elif not predicted_label.index_relevant and true_label.index_relevant:
                index_fn += 1
            else:
                index_tn += 1

            if predicted_label.taxonomy_category_id == true_label.taxonomy_category_id:
                taxonomy_category_matches += 1
                taxonomy_category_correct[true_label.taxonomy_category_id] += 1
            if predicted_label.taxonomy_subcategory_id == true_label.taxonomy_subcategory_id:
                taxonomy_subcategory_matches += 1
                taxonomy_subcategory_correct[true_label.taxonomy_subcategory_id] += 1

        exact_match = (
            predicted_label.relevant == true_label.relevant
            and predicted_enforcement_related == true_label.enforcement_related
            and predicted_label.category == true_label.category
            and predicted_label.sub_category == true_label.sub_category
        )
        if true_label.taxonomy_category_id and true_label.taxonomy_subcategory_id:
            exact_match = exact_match and (
                predicted_label.taxonomy_category_id == true_label.taxonomy_category_id
                and predicted_label.taxonomy_subcategory_id == true_label.taxonomy_subcategory_id
                and predicted_label.index_relevant == true_label.index_relevant
            )
        if exact_match:
            exact_matches += 1

    total = len(labels)
    relevance_stage = _binary_stage_metrics(tp=tp, fp=fp, fn=fn, tn=tn)
    enforcement_stage = _binary_stage_metrics(
        tp=enforcement_tp,
        fp=enforcement_fp,
        fn=enforcement_fn,
        tn=enforcement_tn,
    )
    category_stage = _accuracy_stage_metrics(
        correct=category_matches,
        evaluated_examples=relevant_rows,
    )
    subcategory_stage = _accuracy_stage_metrics(
        correct=subcategory_matches,
        evaluated_examples=relevant_rows,
    )
    overall_exact_match = exact_matches / total if total else 0.0

    index_stage = _binary_stage_metrics(
        tp=index_tp,
        fp=index_fp,
        fn=index_fn,
        tn=index_tn,
    )
    taxonomy_category_stage = _accuracy_stage_metrics(
        correct=taxonomy_category_matches,
        evaluated_examples=taxonomy_labeled_rows,
    )
    taxonomy_subcategory_stage = _accuracy_stage_metrics(
        correct=taxonomy_subcategory_matches,
        evaluated_examples=taxonomy_labeled_rows,
    )

    return VariantMetrics(
        name=variant.name,
        description=variant.description,
        model=model,
        relevance_stage=relevance_stage,
        enforcement_stage_relevant_only=enforcement_stage,
        category_stage_relevant_only=category_stage,
        subcategory_stage_relevant_only=subcategory_stage,
        taxonomy_category_stage_taxonomy_labeled=taxonomy_category_stage,
        taxonomy_subcategory_stage_taxonomy_labeled=taxonomy_subcategory_stage,
        index_relevance_stage_taxonomy_labeled=index_stage,
        legacy_category_breakdown_relevant_only=_label_breakdown_metrics(
            legacy_category_counts,
            legacy_category_correct,
        ),
        legacy_subcategory_breakdown_relevant_only=_label_breakdown_metrics(
            legacy_subcategory_counts,
            legacy_subcategory_correct,
        ),
        taxonomy_category_breakdown_taxonomy_labeled=_label_breakdown_metrics(
            taxonomy_category_counts,
            taxonomy_category_correct,
        ),
        taxonomy_subcategory_breakdown_taxonomy_labeled=_label_breakdown_metrics(
            taxonomy_subcategory_counts,
            taxonomy_subcategory_correct,
        ),
        relevance_precision=relevance_stage.precision,
        relevance_recall=relevance_stage.recall,
        relevance_f1=relevance_stage.f1,
        relevance_accuracy=relevance_stage.accuracy,
        enforcement_precision_relevant_only=enforcement_stage.precision,
        enforcement_recall_relevant_only=enforcement_stage.recall,
        enforcement_f1_relevant_only=enforcement_stage.f1,
        enforcement_accuracy_relevant_only=enforcement_stage.accuracy,
        category_accuracy_relevant_only=category_stage.accuracy,
        subcategory_accuracy_relevant_only=subcategory_stage.accuracy,
        index_relevance_precision_taxonomy_labeled=index_stage.precision,
        index_relevance_recall_taxonomy_labeled=index_stage.recall,
        index_relevance_f1_taxonomy_labeled=index_stage.f1,
        index_relevance_accuracy_taxonomy_labeled=index_stage.accuracy,
        taxonomy_category_accuracy_taxonomy_labeled=taxonomy_category_stage.accuracy,
        taxonomy_subcategory_accuracy_taxonomy_labeled=taxonomy_subcategory_stage.accuracy,
        overall_exact_match=overall_exact_match,
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        total_examples=total,
        taxonomy_labeled_examples=taxonomy_labeled_rows,
    )


def _sort_rankings(metrics: list[VariantMetrics]) -> list[VariantMetrics]:
    return sorted(
        metrics,
        key=lambda item: (
            -item.relevance_f1,
            -item.enforcement_f1_relevant_only,
            -item.taxonomy_subcategory_accuracy_taxonomy_labeled,
            -item.index_relevance_f1_taxonomy_labeled,
            -item.overall_exact_match,
            item.name,
        ),
    )


def _render_breakdown_table(
    title: str,
    breakdowns: Sequence[LabelBreakdownMetrics],
) -> list[str]:
    lines = [f"### {title}"]
    if not breakdowns:
        lines.append("")
        lines.append("_No applicable examples._")
        lines.append("")
        return lines
    headers = ("label", "n", "correct", "acc")
    rows = [
        [
            _display_label(item.label),
            str(item.evaluated_examples),
            str(item.correct),
            f"{item.accuracy:.3f}",
        ]
        for item in breakdowns
    ]
    widths = [
        max(len(header), *(len(row[column]) for row in rows))
        for column, header in enumerate(headers)
    ]
    lines.extend(
        [
            "",
            "```text",
            "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
            *[
                "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))
                for row in rows
            ],
            "```",
            "",
        ]
    )
    return lines


def _render_count_table(
    title: str,
    counts: Sequence[LabelCountMetrics],
) -> list[str]:
    lines = [f"### {title}"]
    if not counts:
        lines.append("")
        lines.append("_No applicable examples._")
        lines.append("")
        return lines
    headers = ("label", "n")
    rows = [
        [
            _display_label(item.label),
            str(item.evaluated_examples),
        ]
        for item in counts
    ]
    widths = [
        max(len(header), *(len(row[column]) for row in rows))
        for column, header in enumerate(headers)
    ]
    lines.extend(
        [
            "",
            "```text",
            "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
            *[
                "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))
                for row in rows
            ],
            "```",
            "",
        ]
    )
    return lines


def render_evaluation_markdown(
    *,
    evaluated_at: datetime,
    validation_set_path: Path,
    variants_path: Path,
    dataset_summary: ValidationDatasetSummary,
    metrics: Sequence[VariantMetrics],
) -> str:
    """Render a human-readable markdown validation report."""
    lines = [
        "# Classifier Variant Evaluation",
        "",
        f"- Evaluated at: `{evaluated_at.isoformat()}`",
        f"- Validation set: `{validation_set_path}`",
        f"- Variant matrix: `{variants_path}`",
        "",
        "## Dataset Coverage",
        "",
        f"- Total examples: `{dataset_summary.total_examples}`",
        f"- Relevant examples: `{dataset_summary.relevant_examples}`",
        f"- Taxonomy-labeled examples: `{dataset_summary.taxonomy_labeled_examples}`",
        f"- Legacy-only examples: `{dataset_summary.legacy_only_examples}`",
        "",
        "Taxonomy-aware stages use only taxonomy-labeled examples. Legacy-only rows remain included in relevance and legacy category/subcategory evaluation.",
        "",
        "## Variant Ranking",
        "",
        "```text",
        render_rankings_table(list(metrics)),
        "```",
        "",
        "## Validation Set Typology Coverage",
        "",
    ]
    lines.extend(
        _render_count_table(
            "Legacy Categories on Relevant Rows",
            dataset_summary.legacy_category_counts_relevant_only,
        )
    )
    lines.extend(
        _render_count_table(
            "Legacy Subcategories on Relevant Rows",
            dataset_summary.legacy_subcategory_counts_relevant_only,
        )
    )
    lines.extend(
        _render_count_table(
            "Taxonomy Categories",
            dataset_summary.taxonomy_category_counts,
        )
    )
    lines.extend(
        _render_count_table(
            "Taxonomy Subcategories",
            dataset_summary.taxonomy_subcategory_counts,
        )
    )

    lines.append("## Variant Details")
    lines.append("")
    for metric in metrics:
        lines.extend(
            [
                f"### {metric.name}",
                "",
                f"- Model: `{metric.model}`",
                f"- Relevance: `{metric.relevance_stage.f1:.3f}` F1 on `{metric.relevance_stage.evaluated_examples}` examples",
                f"- Enforcement (relevant only): `{metric.enforcement_stage_relevant_only.f1:.3f}` F1 on `{metric.enforcement_stage_relevant_only.evaluated_examples}` examples",
                f"- Taxonomy subcategory accuracy: `{metric.taxonomy_subcategory_stage_taxonomy_labeled.accuracy:.3f}` on `{metric.taxonomy_subcategory_stage_taxonomy_labeled.evaluated_examples}` examples",
                f"- Index relevance: `{metric.index_relevance_stage_taxonomy_labeled.f1:.3f}` F1 on `{metric.index_relevance_stage_taxonomy_labeled.evaluated_examples}` examples",
                f"- Overall exact match: `{metric.overall_exact_match:.3f}`",
                "",
            ]
        )
        lines.extend(
            _render_breakdown_table(
                f"{metric.name} Legacy Category Accuracy",
                metric.legacy_category_breakdown_relevant_only,
            )
        )
        lines.extend(
            _render_breakdown_table(
                f"{metric.name} Legacy Subcategory Accuracy",
                metric.legacy_subcategory_breakdown_relevant_only,
            )
        )
        lines.extend(
            _render_breakdown_table(
                f"{metric.name} Taxonomy Category Accuracy",
                metric.taxonomy_category_breakdown_taxonomy_labeled,
            )
        )
        lines.extend(
            _render_breakdown_table(
                f"{metric.name} Taxonomy Subcategory Accuracy",
                metric.taxonomy_subcategory_breakdown_taxonomy_labeled,
            )
        )
    return "\n".join(lines).rstrip() + "\n"


async def evaluate_classifier_variants(
    *,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
    variants_path: Path = DEFAULT_VARIANT_MATRIX_PATH,
    output_path: Path | None = None,
) -> ValidationEvaluateResult:
    """Evaluate tracked classifier variants against the permanent validation set."""
    config = Config()
    api_key = config.anthropic_api_key
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    articles, labels = _load_validation_examples(validation_set_path)
    matrix = _load_variant_matrix(variants_path)
    collected_at = datetime.now(UTC)
    dataset_summary = _build_dataset_summary(labels)

    rankings: list[VariantMetrics] = []
    default_model = matrix.defaults.model or Config().classifier.model
    default_system_prompt = matrix.defaults.system_prompt
    default_user_prompt_template = matrix.defaults.user_prompt_template

    for variant in matrix.variants:
        model = variant.model or default_model
        system_prompt = (
            variant.system_prompt if variant.system_prompt is not None else default_system_prompt
        )
        user_prompt_template = (
            variant.user_prompt_template
            if variant.user_prompt_template is not None
            else default_user_prompt_template
        )
        classifier = create_classifier(
            api_key=api_key,
            model=model,
            system_prompt=system_prompt,
            user_prompt_template=user_prompt_template,
        )
        classified_articles = await classifier.classify_batch(articles)
        predictions = [
            ValidationLabel(
                relevant=item.classification.relevant,
                enforcement_related=item.classification.enforcement_related,
                category=item.classification.category.value,
                sub_category=item.classification.sub_category.value
                if item.classification.sub_category is not None
                else "",
                index_relevant=item.classification.index_relevant,
                taxonomy_version=item.classification.taxonomy_version or "",
                taxonomy_category_id=item.classification.taxonomy_category_id or "",
                taxonomy_subcategory_id=item.classification.taxonomy_subcategory_id or "",
            )
            for item in classified_articles
        ]
        rankings.append(_score_predictions(labels, predictions, variant=variant, model=model))

    sorted_rankings = _sort_rankings(rankings)
    report_path, markdown_path = _resolve_report_paths(
        config=config,
        collected_at=collected_at,
        output_path=output_path,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = ValidationReportPayload(
        evaluated_at=collected_at,
        validation_set_path=str(validation_set_path),
        variants_path=str(variants_path),
        dataset_summary=dataset_summary,
        rankings=sorted_rankings,
    )
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(payload.model_dump(mode="json"), handle, ensure_ascii=False, indent=2)
    markdown = render_evaluation_markdown(
        evaluated_at=collected_at,
        validation_set_path=validation_set_path,
        variants_path=variants_path,
        dataset_summary=dataset_summary,
        metrics=sorted_rankings,
    )
    markdown_path.write_text(markdown, encoding="utf-8")
    return ValidationEvaluateResult(
        output_path=report_path,
        markdown_path=markdown_path,
        dataset_summary=dataset_summary,
        rankings=sorted_rankings,
    )


def render_rankings_table(metrics: list[VariantMetrics]) -> str:
    """Render a compact CLI table of ranked variants."""
    headers = (
        "rank",
        "name",
        "rel_f1",
        "enf_f1",
        "tax_sub",
        "index_f1",
        "exact",
        "tax_n",
    )
    rows = [
        [
            str(index),
            metric.name,
            f"{metric.relevance_f1:.3f}",
            f"{metric.enforcement_f1_relevant_only:.3f}",
            f"{metric.taxonomy_subcategory_accuracy_taxonomy_labeled:.3f}",
            f"{metric.index_relevance_f1_taxonomy_labeled:.3f}",
            f"{metric.overall_exact_match:.3f}",
            str(metric.taxonomy_labeled_examples),
        ]
        for index, metric in enumerate(metrics, start=1)
    ]
    widths = [
        max(len(header), *(len(row[column]) for row in rows))
        for column, header in enumerate(headers)
    ]
    header_line = "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    row_lines = [
        "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)) for row in rows
    ]
    return "\n".join([header_line, *row_lines])


def run_validation_evaluate(
    *,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
    variants_path: Path = DEFAULT_VARIANT_MATRIX_PATH,
    output_path: Path | None = None,
) -> ValidationEvaluateResult:
    """CLI wrapper for classifier variant evaluation."""
    setup_logging()
    return asyncio.run(
        evaluate_classifier_variants(
            validation_set_path=validation_set_path,
            variants_path=variants_path,
            output_path=output_path,
        )
    )
