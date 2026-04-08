"""Draft validation-set collection using existing sources and classifier suggestions."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from denbust.classifier.relevance import create_classifier
from denbust.config import Config, load_config
from denbust.data_models import ClassifiedArticle, RawArticle
from denbust.pipeline import create_sources, setup_logging
from denbust.validation.common import (
    DRAFT_COLUMNS,
    default_collect_output_path,
    relaxed_validation_keywords,
    write_csv_rows,
)
from denbust.validation.models import ValidationDraftRow

logger = logging.getLogger(__name__)

RELAXED_VALIDATION_KEYWORDS = relaxed_validation_keywords()


@dataclass(frozen=True)
class ValidationCollectResult:
    """Result of building a draft annotation CSV."""

    output_path: Path
    total_rows: int
    per_source_counts: dict[str, int]
    errors: list[str]


@dataclass(frozen=True)
class _ScoredCandidate:
    article: RawArticle
    canonical_url: str
    strict_title_hits: int
    strict_snippet_hits: int
    relaxed_only_hits: int


def _keyword_hits(text: str, keywords: list[str]) -> int:
    normalized = text.casefold()
    return sum(1 for keyword in keywords if keyword.casefold() in normalized)


def _score_candidate(
    article: RawArticle,
    *,
    strict_keywords: list[str],
    relaxed_only_keywords: list[str],
    canonical_url: str,
) -> _ScoredCandidate:
    return _ScoredCandidate(
        article=article,
        canonical_url=canonical_url,
        strict_title_hits=_keyword_hits(article.title, strict_keywords),
        strict_snippet_hits=_keyword_hits(article.snippet, strict_keywords),
        relaxed_only_hits=_keyword_hits(
            f"{article.title} {article.snippet}",
            relaxed_only_keywords,
        ),
    )


def select_promising_candidates(
    articles: list[RawArticle],
    *,
    strict_keywords: list[str],
    relaxed_keywords: list[str],
    per_source: int,
    canonicalize_url: Callable[[str], str],
) -> list[_ScoredCandidate]:
    """Deduplicate and rank source-local candidates deterministically."""
    strict_keys = [keyword.strip() for keyword in strict_keywords if keyword.strip()]
    relaxed_only_keywords = [
        keyword
        for keyword in relaxed_keywords
        if keyword.casefold() not in {strict.casefold() for strict in strict_keys}
    ]

    best_by_url: dict[str, _ScoredCandidate] = {}
    for article in articles:
        canonical_url = canonicalize_url(str(article.url))
        candidate = _score_candidate(
            article,
            strict_keywords=strict_keys,
            relaxed_only_keywords=relaxed_only_keywords,
            canonical_url=canonical_url,
        )
        existing = best_by_url.get(canonical_url)
        if existing is None or _candidate_sort_key(candidate) > _candidate_sort_key(existing):
            best_by_url[canonical_url] = candidate

    ranked = sorted(
        best_by_url.values(),
        key=lambda candidate: (
            -candidate.strict_title_hits,
            -candidate.strict_snippet_hits,
            -candidate.relaxed_only_hits,
            -candidate.article.date.timestamp(),
            candidate.canonical_url,
        ),
    )
    return ranked[:per_source]


def _candidate_sort_key(candidate: _ScoredCandidate) -> tuple[int, int, int, float, str]:
    return (
        candidate.strict_title_hits,
        candidate.strict_snippet_hits,
        candidate.relaxed_only_hits,
        candidate.article.date.timestamp(),
        candidate.canonical_url,
    )


def build_draft_rows(
    classified_articles: list[ClassifiedArticle],
    *,
    canonical_urls: dict[str, str],
    collected_at: datetime,
) -> list[ValidationDraftRow]:
    """Convert classified candidates into draft CSV rows."""
    rows: list[ValidationDraftRow] = []
    for classified_article in classified_articles:
        article = classified_article.article
        classification = classified_article.classification
        category_value = classification.category.value
        sub_category_value = (
            classification.sub_category.value if classification.sub_category is not None else ""
        )
        rows.append(
            ValidationDraftRow(
                source_name=article.source_name,
                article_date=article.date,
                url=str(article.url),
                canonical_url=canonical_urls[str(article.url)],
                title=article.title,
                snippet=article.snippet,
                suggested_relevant=classification.relevant,
                suggested_enforcement_related=classification.enforcement_related,
                suggested_index_relevant=classification.index_relevant,
                suggested_taxonomy_version=classification.taxonomy_version or "",
                suggested_taxonomy_category_id=classification.taxonomy_category_id or "",
                suggested_taxonomy_subcategory_id=classification.taxonomy_subcategory_id or "",
                suggested_category=category_value,
                suggested_sub_category=sub_category_value,
                suggested_confidence=classification.confidence,
                relevant=classification.relevant,
                enforcement_related=classification.enforcement_related,
                index_relevant=classification.index_relevant,
                taxonomy_version=classification.taxonomy_version or "",
                taxonomy_category_id=classification.taxonomy_category_id or "",
                taxonomy_subcategory_id=classification.taxonomy_subcategory_id or "",
                category=category_value,
                sub_category=sub_category_value,
                review_status="pending",
                annotation_source="classifier_draft",
                annotation_notes="",
                collected_at=collected_at,
            )
        )
    return rows


def serialize_draft_rows(rows: list[ValidationDraftRow]) -> list[dict[str, str]]:
    """Serialize draft rows for CSV writing."""
    serialized: list[dict[str, str]] = []
    for row in rows:
        serialized.append(
            {
                "source_name": row.source_name,
                "article_date": row.article_date.isoformat(),
                "url": row.url,
                "canonical_url": row.canonical_url,
                "title": row.title,
                "snippet": row.snippet,
                "suggested_relevant": str(row.suggested_relevant),
                "suggested_enforcement_related": str(row.suggested_enforcement_related),
                "suggested_index_relevant": str(row.suggested_index_relevant),
                "suggested_taxonomy_version": row.suggested_taxonomy_version,
                "suggested_taxonomy_category_id": row.suggested_taxonomy_category_id,
                "suggested_taxonomy_subcategory_id": row.suggested_taxonomy_subcategory_id,
                "suggested_category": row.suggested_category,
                "suggested_sub_category": row.suggested_sub_category,
                "suggested_confidence": row.suggested_confidence,
                "relevant": str(row.relevant),
                "enforcement_related": str(row.enforcement_related),
                "index_relevant": str(row.index_relevant),
                "taxonomy_version": row.taxonomy_version,
                "taxonomy_category_id": row.taxonomy_category_id,
                "taxonomy_subcategory_id": row.taxonomy_subcategory_id,
                "category": row.category,
                "sub_category": row.sub_category,
                "review_status": row.review_status,
                "annotation_source": row.annotation_source,
                "expected_month_bucket": row.expected_month_bucket,
                "expected_city": row.expected_city,
                "expected_status": row.expected_status,
                "manual_city": row.manual_city,
                "manual_address": row.manual_address,
                "manual_event_label": row.manual_event_label,
                "manual_status": row.manual_status,
                "annotation_notes": row.annotation_notes,
                "collected_at": row.collected_at.isoformat(),
            }
        )
    return serialized


async def collect_validation_draft(
    config: Config,
    *,
    days: int = 7,
    per_source: int = 10,
    output_path: Path | None = None,
) -> ValidationCollectResult:
    """Collect a local draft CSV of promising recent articles from each enabled source."""
    if not config.anthropic_api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    if per_source < 1:
        raise ValueError("per_source must be >= 1")

    sources = create_sources(config)
    classifier = create_classifier(
        api_key=config.anthropic_api_key,
        model=config.classifier.model,
        system_prompt=config.classifier.system_prompt,
        user_prompt_template=config.classifier.user_prompt_template,
    )

    collected_at = datetime.now(UTC)
    strict_keywords = [keyword for keyword in config.keywords if keyword.strip()]
    rows: list[ValidationDraftRow] = []
    errors: list[str] = []
    per_source_counts: dict[str, int] = {}

    from denbust.news_items.normalize import canonicalize_news_url

    for source in sources:
        try:
            logger.info("Collecting validation candidates from %s", source.name)
            source_articles = await source.fetch(days=days, keywords=RELAXED_VALIDATION_KEYWORDS)
        except Exception as exc:
            logger.exception("Validation draft collection failed for %s: %s", source.name, exc)
            errors.append(f"{source.name}: {exc}")
            per_source_counts[source.name] = 0
            continue

        selected = select_promising_candidates(
            source_articles,
            strict_keywords=strict_keywords,
            relaxed_keywords=RELAXED_VALIDATION_KEYWORDS,
            per_source=per_source,
            canonicalize_url=canonicalize_news_url,
        )
        selected_articles = [candidate.article for candidate in selected]
        per_source_counts[source.name] = len(selected_articles)
        if not selected_articles:
            continue

        classified_articles = await classifier.classify_batch(selected_articles)
        canonical_urls = {
            str(candidate.article.url): candidate.canonical_url for candidate in selected
        }
        rows.extend(
            build_draft_rows(
                classified_articles,
                canonical_urls=canonical_urls,
                collected_at=collected_at,
            )
        )

    final_output_path = output_path or default_collect_output_path(config, collected_at)
    write_csv_rows(final_output_path, DRAFT_COLUMNS, serialize_draft_rows(rows))
    return ValidationCollectResult(
        output_path=final_output_path,
        total_rows=len(rows),
        per_source_counts=per_source_counts,
        errors=errors,
    )


def run_validation_collect(
    *,
    config_path: Path,
    days_override: int | None = None,
    per_source: int = 10,
    output_path: Path | None = None,
) -> ValidationCollectResult:
    """Load config and run draft collection synchronously for the CLI."""
    setup_logging()
    config = load_config(config_path)
    days = days_override if days_override is not None else 7
    return asyncio.run(
        collect_validation_draft(
            config,
            days=days,
            per_source=per_source,
            output_path=output_path,
        )
    )
