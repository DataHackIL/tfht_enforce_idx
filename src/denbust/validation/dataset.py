"""Permanent validation-set merge and CSV normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from denbust.data_models import Category, SubCategory
from denbust.taxonomy import default_taxonomy
from denbust.validation.common import (
    ALLOWED_SUBCATEGORIES_BY_CATEGORY,
    DEFAULT_VALIDATION_SET_PATH,
    VALIDATION_SET_COLUMNS,
    canonicalize_csv_url,
    normalize_category_value,
    normalize_review_status,
    normalize_subcategory_value,
    parse_bool,
    parse_datetime,
    read_csv_rows,
    write_csv_rows,
)
from denbust.validation.models import ValidationSetRow


@dataclass(frozen=True)
class ValidationFinalizeResult:
    """Result of merging reviewed rows into the permanent validation set."""

    validation_set_path: Path
    added_rows: int
    skipped_duplicates: int
    reviewed_rows: int
    total_rows: int


def _serialize_validation_rows(rows: list[ValidationSetRow]) -> list[dict[str, str]]:
    return [
        {
            "source_name": row.source_name,
            "article_date": row.article_date.isoformat(),
            "url": row.url,
            "canonical_url": row.canonical_url,
            "title": row.title,
            "snippet": row.snippet,
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
            "finalized_at": row.finalized_at.isoformat(),
            "draft_source": row.draft_source,
        }
        for row in rows
    ]


def _parse_existing_validation_row(raw_row: dict[str, str]) -> ValidationSetRow:
    return ValidationSetRow(
        source_name=raw_row["source_name"].strip(),
        article_date=parse_datetime(raw_row["article_date"]),
        url=raw_row["url"].strip(),
        canonical_url=canonicalize_csv_url(raw_row["url"], raw_row["canonical_url"]),
        title=raw_row["title"],
        snippet=raw_row["snippet"],
        relevant=parse_bool(raw_row["relevant"]),
        enforcement_related=parse_bool(raw_row.get("enforcement_related", "False") or "False"),
        index_relevant=parse_bool(raw_row.get("index_relevant", "False") or "False"),
        taxonomy_version=raw_row.get("taxonomy_version", ""),
        taxonomy_category_id=raw_row.get("taxonomy_category_id", ""),
        taxonomy_subcategory_id=raw_row.get("taxonomy_subcategory_id", ""),
        category=normalize_category_value(raw_row["category"]),
        sub_category=normalize_subcategory_value(raw_row["sub_category"]),
        review_status=normalize_review_status(raw_row["review_status"]) or "reviewed",
        annotation_source=raw_row.get("annotation_source", ""),
        expected_month_bucket=raw_row.get("expected_month_bucket", "").strip(),
        expected_city=raw_row.get("expected_city", "").strip(),
        expected_status=raw_row.get("expected_status", "").strip(),
        manual_city=raw_row.get("manual_city", ""),
        manual_address=raw_row.get("manual_address", ""),
        manual_event_label=raw_row.get("manual_event_label", ""),
        manual_status=raw_row.get("manual_status", ""),
        annotation_notes=raw_row["annotation_notes"],
        collected_at=parse_datetime(raw_row["collected_at"]),
        finalized_at=parse_datetime(raw_row["finalized_at"]),
        draft_source=raw_row["draft_source"],
    )


def _validate_reviewed_row(raw_row: dict[str, str], *, draft_source: str) -> ValidationSetRow:
    taxonomy = default_taxonomy()
    relevant = parse_bool(raw_row["relevant"])
    enforcement_related = parse_bool(raw_row.get("enforcement_related", "False") or "False")
    index_relevant = parse_bool(raw_row.get("index_relevant", "False") or "False")
    taxonomy_version = raw_row.get("taxonomy_version", "").strip()
    taxonomy_category_id = raw_row.get("taxonomy_category_id", "").strip()
    taxonomy_subcategory_id = raw_row.get("taxonomy_subcategory_id", "").strip()
    category_value = normalize_category_value(raw_row["category"])
    sub_category_value = normalize_subcategory_value(raw_row["sub_category"])

    if relevant:
        category = Category(category_value)
        if category == Category.NOT_RELEVANT:
            raise ValueError("Reviewed relevant rows cannot use category 'not_relevant'")
        sub_category = None
        if sub_category_value:
            sub_category = SubCategory(sub_category_value)
            allowed = ALLOWED_SUBCATEGORIES_BY_CATEGORY[category]
            if sub_category not in allowed:
                raise ValueError(
                    f"Invalid sub_category '{sub_category.value}' for category '{category.value}'"
                )
        elif enforcement_related:
            raise ValueError(
                "Reviewed enforcement-related rows must include a non-empty sub_category"
            )
    else:
        category = Category.NOT_RELEVANT
        enforcement_related = False
        index_relevant = False
        taxonomy_version = ""
        taxonomy_category_id = ""
        taxonomy_subcategory_id = ""
        sub_category = None

    if taxonomy_category_id or taxonomy_subcategory_id:
        if not (taxonomy_category_id and taxonomy_subcategory_id):
            raise ValueError("Taxonomy labels must include both category and subcategory ids")
        if not taxonomy.has_pair(taxonomy_category_id, taxonomy_subcategory_id):
            raise ValueError(
                f"Invalid taxonomy pair: {taxonomy_category_id}/{taxonomy_subcategory_id}"
            )
        if taxonomy_version and taxonomy_version != taxonomy.version:
            raise ValueError(
                f"Unsupported taxonomy version {taxonomy_version!r}; expected {taxonomy.version!r}"
            )
        taxonomy_version = taxonomy.version
        expected_index_relevant = taxonomy.is_index_relevant(
            taxonomy_category_id,
            taxonomy_subcategory_id,
        )
        if index_relevant != expected_index_relevant:
            raise ValueError(
                "index_relevant does not match the packaged taxonomy for the selected leaf"
            )

    finalized_at = datetime.now(UTC)
    return ValidationSetRow(
        source_name=raw_row["source_name"].strip(),
        article_date=parse_datetime(raw_row["article_date"]),
        url=raw_row["url"].strip(),
        canonical_url=canonicalize_csv_url(raw_row["url"], raw_row["canonical_url"]),
        title=raw_row["title"],
        snippet=raw_row["snippet"],
        relevant=relevant,
        enforcement_related=enforcement_related,
        index_relevant=index_relevant,
        taxonomy_version=taxonomy_version,
        taxonomy_category_id=taxonomy_category_id,
        taxonomy_subcategory_id=taxonomy_subcategory_id,
        category=category.value,
        sub_category=sub_category.value if sub_category is not None else "",
        review_status="reviewed",
        annotation_source=raw_row.get("annotation_source", "").strip(),
        expected_month_bucket=raw_row.get("expected_month_bucket", "").strip(),
        expected_city=raw_row.get("expected_city", "").strip(),
        expected_status=raw_row.get("expected_status", "").strip(),
        manual_city=raw_row.get("manual_city", ""),
        manual_address=raw_row.get("manual_address", ""),
        manual_event_label=raw_row.get("manual_event_label", ""),
        manual_status=raw_row.get("manual_status", ""),
        annotation_notes=raw_row["annotation_notes"],
        collected_at=parse_datetime(raw_row["collected_at"]),
        finalized_at=finalized_at,
        draft_source=draft_source,
    )


def finalize_validation_set(
    *,
    input_path: Path,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
) -> ValidationFinalizeResult:
    """Merge reviewed draft rows into the permanent validation CSV."""
    if not input_path.exists():
        raise FileNotFoundError(f"Draft CSV not found: {input_path}")

    raw_rows = read_csv_rows(input_path)
    reviewed_rows = [
        row for row in raw_rows if normalize_review_status(row["review_status"]) == "reviewed"
    ]

    existing_rows = [
        _parse_existing_validation_row(row) for row in read_csv_rows(validation_set_path)
    ]
    existing_keys = {(row.source_name, row.canonical_url) for row in existing_rows}

    added_rows: list[ValidationSetRow] = []
    skipped_duplicates = 0
    for raw_row in reviewed_rows:
        validated = _validate_reviewed_row(raw_row, draft_source=str(input_path))
        key = (validated.source_name, validated.canonical_url)
        if key in existing_keys:
            skipped_duplicates += 1
            continue
        existing_keys.add(key)
        added_rows.append(validated)

    merged_rows = sorted(
        [*existing_rows, *added_rows],
        key=lambda row: (row.source_name, row.article_date.isoformat(), row.canonical_url),
    )
    write_csv_rows(
        validation_set_path,
        VALIDATION_SET_COLUMNS,
        _serialize_validation_rows(merged_rows),
    )
    return ValidationFinalizeResult(
        validation_set_path=validation_set_path,
        added_rows=len(added_rows),
        skipped_duplicates=skipped_duplicates,
        reviewed_rows=len(reviewed_rows),
        total_rows=len(merged_rows),
    )


def run_validation_finalize(
    *,
    input_path: Path,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
) -> ValidationFinalizeResult:
    """CLI wrapper for permanent validation-set finalization."""
    return finalize_validation_set(
        input_path=input_path,
        validation_set_path=validation_set_path,
    )
