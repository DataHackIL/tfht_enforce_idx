"""Classifier variant evaluation against the permanent validation set."""

from __future__ import annotations

import asyncio
import json
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
    ClassifierVariantMatrix,
    ClassifierVariantSpec,
    VariantMetrics,
)


@dataclass(frozen=True)
class ValidationEvaluateResult:
    """Result of evaluating classifier variants."""

    output_path: Path
    rankings: list[VariantMetrics]


def _load_variant_matrix(path: Path) -> ClassifierVariantMatrix:
    if not path.exists():
        raise FileNotFoundError(f"Variant matrix not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    matrix = ClassifierVariantMatrix.model_validate(data)
    if not matrix.variants:
        raise ValueError("Variant matrix must include at least one variant")
    return matrix


def _load_validation_examples(path: Path) -> tuple[list[RawArticle], list[tuple[bool, str, str]]]:
    if not path.exists():
        raise FileNotFoundError(f"Validation set not found: {path}")
    rows = read_csv_rows(path)
    if not rows:
        raise ValueError("Validation set is empty")

    articles: list[RawArticle] = []
    labels: list[tuple[bool, str, str]] = []
    for row in rows:
        article = RawArticle(
            url=HttpUrl(row["url"].strip()),
            title=row["title"],
            snippet=row["snippet"],
            date=parse_datetime(row["article_date"]),
            source_name=row["source_name"].strip(),
        )
        relevant = parse_bool(row["relevant"])
        category = Category(row["category"].strip())
        sub_category = row["sub_category"].strip()
        articles.append(article)
        labels.append((relevant, category.value, sub_category))
    return articles, labels


def _score_predictions(
    labels: list[tuple[bool, str, str]],
    predictions: list[tuple[bool, str, str]],
    *,
    variant: ClassifierVariantSpec,
    model: str,
) -> VariantMetrics:
    tp = fp = fn = tn = 0
    relevant_rows = 0
    category_matches = 0
    subcategory_matches = 0
    exact_matches = 0

    for (true_relevant, true_category, true_sub_category), (
        predicted_relevant,
        predicted_category,
        predicted_sub_category,
    ) in zip(labels, predictions, strict=True):
        if true_relevant and predicted_relevant:
            tp += 1
        elif not true_relevant and predicted_relevant:
            fp += 1
        elif true_relevant and not predicted_relevant:
            fn += 1
        else:
            tn += 1

        if true_relevant:
            relevant_rows += 1
            if predicted_category == true_category:
                category_matches += 1
            if predicted_sub_category == true_sub_category:
                subcategory_matches += 1

        if (
            predicted_relevant == true_relevant
            and predicted_category == true_category
            and predicted_sub_category == true_sub_category
        ):
            exact_matches += 1

    total = len(labels)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    relevance_accuracy = (tp + tn) / total if total else 0.0
    category_accuracy = category_matches / relevant_rows if relevant_rows else 0.0
    subcategory_accuracy = subcategory_matches / relevant_rows if relevant_rows else 0.0
    overall_exact_match = exact_matches / total if total else 0.0

    return VariantMetrics(
        name=variant.name,
        description=variant.description,
        model=model,
        relevance_precision=precision,
        relevance_recall=recall,
        relevance_f1=f1,
        relevance_accuracy=relevance_accuracy,
        category_accuracy_relevant_only=category_accuracy,
        subcategory_accuracy_relevant_only=subcategory_accuracy,
        overall_exact_match=overall_exact_match,
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        total_examples=total,
    )


def _sort_rankings(metrics: list[VariantMetrics]) -> list[VariantMetrics]:
    return sorted(
        metrics,
        key=lambda item: (
            -item.relevance_f1,
            -item.category_accuracy_relevant_only,
            -item.subcategory_accuracy_relevant_only,
            -item.overall_exact_match,
            item.name,
        ),
    )


async def evaluate_classifier_variants(
    *,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
    variants_path: Path = DEFAULT_VARIANT_MATRIX_PATH,
    output_path: Path | None = None,
) -> ValidationEvaluateResult:
    """Evaluate tracked classifier variants against the permanent validation set."""
    api_key = Config().anthropic_api_key
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    articles, labels = _load_validation_examples(validation_set_path)
    matrix = _load_variant_matrix(variants_path)
    collected_at = datetime.now(UTC)

    rankings: list[VariantMetrics] = []
    default_model = matrix.defaults.model or Config().classifier.model
    default_system_prompt = matrix.defaults.system_prompt
    default_user_prompt_template = matrix.defaults.user_prompt_template

    for variant in matrix.variants:
        model = variant.model or default_model
        system_prompt = (
            variant.system_prompt
            if variant.system_prompt is not None
            else default_system_prompt
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
            (
                item.classification.relevant,
                item.classification.category.value,
                item.classification.sub_category.value
                if item.classification.sub_category is not None
                else "",
            )
            for item in classified_articles
        ]
        rankings.append(
            _score_predictions(labels, predictions, variant=variant, model=model)
        )

    sorted_rankings = _sort_rankings(rankings)
    report_path = output_path or default_evaluation_output_path(Config(), collected_at)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "evaluated_at": collected_at.isoformat(),
                "validation_set_path": str(validation_set_path),
                "variants_path": str(variants_path),
                "rankings": [metric.model_dump(mode="json") for metric in sorted_rankings],
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )
    return ValidationEvaluateResult(output_path=report_path, rankings=sorted_rankings)


def render_rankings_table(metrics: list[VariantMetrics]) -> str:
    """Render a compact CLI table of ranked variants."""
    headers = (
        "rank",
        "name",
        "rel_f1",
        "rel_acc",
        "cat_acc",
        "subcat_acc",
        "exact",
        "tp",
        "fp",
        "fn",
        "tn",
    )
    rows = [
        [
            str(index),
            metric.name,
            f"{metric.relevance_f1:.3f}",
            f"{metric.relevance_accuracy:.3f}",
            f"{metric.category_accuracy_relevant_only:.3f}",
            f"{metric.subcategory_accuracy_relevant_only:.3f}",
            f"{metric.overall_exact_match:.3f}",
            str(metric.tp),
            str(metric.fp),
            str(metric.fn),
            str(metric.tn),
        ]
        for index, metric in enumerate(metrics, start=1)
    ]
    widths = [
        max(len(header), *(len(row[column]) for row in rows)) for column, header in enumerate(headers)
    ]
    header_line = "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    row_lines = [
        "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))
        for row in rows
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
