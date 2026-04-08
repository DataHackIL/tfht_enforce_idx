"""Manual annotations, corrections, and import helpers for news_items."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, model_validator

from denbust.data_models import Category, SubCategory
from denbust.models.policies import PublicationStatus, TakedownStatus
from denbust.news_items.models import NewsItemOperationalRecord, SuppressionRule
from denbust.news_items.normalize import (
    build_news_item_id,
    canonicalize_news_url,
    deduplicate_strings,
    source_domain_from_url,
)
from denbust.news_items.policy import (
    apply_suppression,
    derive_publication_status,
    derive_review_status,
    infer_privacy_risk,
)
from denbust.taxonomy import default_taxonomy

NEWS_ITEMS_CORRECTIONS_CSV_V1 = "news_items_corrections_csv_v1"
NEWS_ITEMS_MISSING_ITEMS_CSV_V1 = "news_items_missing_items_csv_v1"
_CSV_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_CSV_FALSE_VALUES = {"0", "false", "no", "n", "off"}
_ModelType = TypeVar("_ModelType", bound=BaseModel)
_EnumType = TypeVar("_EnumType", bound=StrEnum)


class MissingItemPromotionStatus(StrEnum):
    """Promotion state for a manually annotated missing item."""

    PROMOTED = "promoted"
    PENDING = "pending"


class NewsItemCorrection(BaseModel):
    """A manual correction for a detected news_items row."""

    record_id: str | None = None
    canonical_url: str | None = None
    relevant: bool | None = None
    enforcement_related: bool | None = None
    taxonomy_version: str | None = None
    taxonomy_category_id: str | None = None
    taxonomy_subcategory_id: str | None = None
    category: Category | None = None
    sub_category: SubCategory | None = None
    summary_one_sentence: str | None = None
    manual_city: str | None = None
    manual_address: str | None = None
    manual_event_label: str | None = None
    manual_status: str | None = None
    reviewer: str | None = None
    reviewed_at: datetime | None = None
    annotation_notes: str | None = None
    active: bool = True
    annotation_source: str = "manual_correction"

    @model_validator(mode="after")
    def _normalize(self) -> NewsItemCorrection:
        if self.canonical_url:
            self.canonical_url = canonicalize_news_url(self.canonical_url)
        if not self.record_id and not self.canonical_url:
            raise ValueError("Correction requires record_id and/or canonical_url")
        _apply_taxonomy_compatibility(self)
        return self

    def matches(self, record: NewsItemOperationalRecord) -> bool:
        """Return whether this correction targets the provided record."""
        if self.record_id and self.record_id == record.id:
            return True
        return self.canonical_url is not None and self.canonical_url == record.canonical_url


class MissingNewsItemAnnotation(BaseModel):
    """A manually curated item that the source pipeline missed."""

    annotation_id: str
    source_url: str
    canonical_url: str | None = None
    title: str
    event_date: datetime
    source_name: str
    taxonomy_version: str | None = None
    taxonomy_category_id: str
    taxonomy_subcategory_id: str
    category: Category | None = None
    sub_category: SubCategory | None = None
    summary_one_sentence: str | None = None
    manual_city: str | None = None
    manual_address: str | None = None
    manual_event_label: str | None = None
    manual_status: str | None = None
    reviewer: str | None = None
    reviewed_at: datetime | None = None
    annotation_notes: str | None = None
    active: bool = True
    promotion_status: MissingItemPromotionStatus = MissingItemPromotionStatus.PROMOTED
    annotation_source: str = "missing_item_annotation"
    index_relevant: bool | None = None

    @model_validator(mode="after")
    def _normalize(self) -> MissingNewsItemAnnotation:
        self.canonical_url = canonicalize_news_url(self.canonical_url or self.source_url)
        self.source_url = self.source_url.strip()
        self.title = " ".join(self.title.split()).strip()
        if not self.title:
            raise ValueError("Missing item annotation requires a title")
        _apply_taxonomy_compatibility(self)
        return self

    def to_operational_record(self) -> NewsItemOperationalRecord:
        """Promote a missing-item annotation into a production operational row."""
        retrieval_datetime = self.reviewed_at or datetime.now(UTC)
        canonical_url = self.canonical_url or canonicalize_news_url(self.source_url)
        summary = self.summary_one_sentence or self.manual_event_label or self.title
        privacy_risk, privacy_reason = infer_privacy_risk(
            " ".join(
                part
                for part in (
                    self.title,
                    summary,
                    self.manual_event_label or "",
                    self.manual_address or "",
                    self.manual_status or "",
                )
                if part
            )
        )
        topic_tags = deduplicate_strings(
            [
                self.category.value if self.category is not None else "",
                self.sub_category.value if self.sub_category is not None else "",
                "manually-promoted",
            ]
        )
        return NewsItemOperationalRecord(
            id=build_news_item_id(canonical_url),
            source_name=self.source_name,
            source_domain=source_domain_from_url(canonical_url),
            url=self.source_url,
            canonical_url=canonical_url,
            publication_datetime=self.event_date,
            retrieval_datetime=retrieval_datetime,
            title=self.title,
            taxonomy_version=self.taxonomy_version,
            taxonomy_category_id=self.taxonomy_category_id,
            taxonomy_subcategory_id=self.taxonomy_subcategory_id,
            index_relevant=bool(self.index_relevant),
            category=self.category or Category.NOT_RELEVANT,
            sub_category=self.sub_category,
            summary_one_sentence=summary,
            geography_city=self.manual_city,
            topic_tags=topic_tags,
            privacy_risk_level=privacy_risk,
            review_status=derive_review_status(privacy_risk),
            publication_status=derive_publication_status(privacy_risk),
            created_at=retrieval_datetime,
            updated_at=retrieval_datetime,
            source_urls=[self.source_url],
            source_count=1,
            classification_confidence="manual",
            privacy_reason=privacy_reason,
            manual_city=self.manual_city,
            manual_address=self.manual_address,
            manual_event_label=self.manual_event_label,
            manual_status=self.manual_status,
            manually_reviewed=True,
            manually_overridden=True,
            annotation_source=self.annotation_source,
            reviewer=self.reviewer,
            reviewed_at=self.reviewed_at,
            annotation_notes=self.annotation_notes,
        )


@dataclass(frozen=True)
class AnnotationImportResult:
    """Summary of a manual-annotation import run."""

    imported_rows: int
    skipped_rows: int
    warnings: list[str]


def parse_news_item_corrections(rows: list[dict[str, Any]]) -> list[NewsItemCorrection]:
    """Validate correction rows loaded from the operational store."""
    return _validated_rows(rows, NewsItemCorrection)


def parse_missing_news_items(rows: list[dict[str, Any]]) -> list[MissingNewsItemAnnotation]:
    """Validate missing-item rows loaded from the operational store."""
    return _validated_rows(rows, MissingNewsItemAnnotation)


def apply_manual_annotations(
    records: list[NewsItemOperationalRecord],
    *,
    corrections: list[NewsItemCorrection],
    missing_items: list[MissingNewsItemAnnotation],
    suppression_rules: list[SuppressionRule],
) -> list[NewsItemOperationalRecord]:
    """Apply manual corrections and promoted missing items to operational records."""
    active_corrections = [correction for correction in corrections if correction.active]
    promoted_missing = [
        annotation
        for annotation in missing_items
        if annotation.active and annotation.promotion_status is MissingItemPromotionStatus.PROMOTED
    ]
    by_id: dict[str, NewsItemOperationalRecord] = {}
    by_canonical_url: dict[str, str] = {}

    for record in records:
        corrected = _apply_matching_correction(record, active_corrections)
        suppressed = apply_suppression(corrected, suppression_rules)
        by_id[suppressed.id] = suppressed
        by_canonical_url[suppressed.canonical_url] = suppressed.id

    for annotation in promoted_missing:
        if annotation.canonical_url in by_canonical_url:
            continue
        promoted = apply_suppression(annotation.to_operational_record(), suppression_rules)
        by_id[promoted.id] = promoted
        by_canonical_url[promoted.canonical_url] = promoted.id

    rows = list(by_id.values())
    rows.sort(key=lambda record: record.publication_datetime, reverse=True)
    return rows


def import_news_item_corrections_csv(input_path: Path) -> tuple[list[NewsItemCorrection], list[str]]:
    """Read a corrections CSV into typed correction rows."""
    return _import_csv_rows(
        input_path,
        _normalize_correction_row,
        NewsItemCorrection,
    )


def import_missing_news_items_csv(
    input_path: Path,
) -> tuple[list[MissingNewsItemAnnotation], list[str]]:
    """Read a missing-items CSV into typed annotation rows."""
    return _import_csv_rows(
        input_path,
        _normalize_missing_item_row,
        MissingNewsItemAnnotation,
    )


def _apply_taxonomy_compatibility(
    model: NewsItemCorrection | MissingNewsItemAnnotation,
) -> None:
    taxonomy_category_id = model.taxonomy_category_id
    taxonomy_subcategory_id = model.taxonomy_subcategory_id
    if bool(taxonomy_category_id) != bool(taxonomy_subcategory_id):
        raise ValueError("taxonomy_category_id and taxonomy_subcategory_id must be provided together")
    if not taxonomy_category_id:
        return
    if taxonomy_subcategory_id is None:
        raise ValueError("taxonomy_subcategory_id is required when taxonomy_category_id is set")

    taxonomy = default_taxonomy()
    if not taxonomy.has_pair(taxonomy_category_id, taxonomy_subcategory_id):
        raise ValueError(
            "Unknown taxonomy pair: "
            f"{taxonomy_category_id}/{taxonomy_subcategory_id}"
        )
    legacy_category, legacy_sub_category = taxonomy.legacy_mapping(
        taxonomy_category_id,
        taxonomy_subcategory_id,
    )
    category = getattr(model, "category", None)
    sub_category = getattr(model, "sub_category", None)
    if category is not None and category is not legacy_category:
        raise ValueError("Provided category does not match taxonomy compatibility mapping")
    if sub_category is not None and sub_category is not legacy_sub_category:
        raise ValueError("Provided sub_category does not match taxonomy compatibility mapping")
    model.category = legacy_category
    model.sub_category = legacy_sub_category
    model.taxonomy_version = model.taxonomy_version or taxonomy.version
    if isinstance(model, MissingNewsItemAnnotation):
        model.index_relevant = taxonomy.is_index_relevant(
            taxonomy_category_id,
            taxonomy_subcategory_id,
        )


def _validated_rows(
    rows: list[dict[str, Any]],
    model_type: type[_ModelType],
) -> list[_ModelType]:
    validated: list[_ModelType] = []
    for row in rows:
        try:
            validated.append(model_type.model_validate(row))
        except Exception:
            continue
    return validated


def _apply_matching_correction(
    record: NewsItemOperationalRecord,
    corrections: list[NewsItemCorrection],
) -> NewsItemOperationalRecord:
    correction = next((candidate for candidate in corrections if candidate.matches(record)), None)
    if correction is None:
        return record

    update: dict[str, Any] = {
        "manually_reviewed": True,
        "manually_overridden": True,
        "annotation_source": correction.annotation_source,
        "reviewer": correction.reviewer,
        "reviewed_at": correction.reviewed_at,
        "annotation_notes": correction.annotation_notes,
        "updated_at": correction.reviewed_at or datetime.now(UTC),
    }
    if correction.taxonomy_version:
        update["taxonomy_version"] = correction.taxonomy_version
    if correction.taxonomy_category_id:
        update["taxonomy_category_id"] = correction.taxonomy_category_id
    if correction.taxonomy_subcategory_id:
        update["taxonomy_subcategory_id"] = correction.taxonomy_subcategory_id
        update["index_relevant"] = default_taxonomy().is_index_relevant(
            correction.taxonomy_category_id or "",
            correction.taxonomy_subcategory_id,
        )
    if correction.category is not None:
        update["category"] = correction.category
    if correction.sub_category is not None or correction.taxonomy_subcategory_id:
        update["sub_category"] = correction.sub_category
    if correction.summary_one_sentence:
        update["summary_one_sentence"] = correction.summary_one_sentence
    if correction.manual_city:
        update["manual_city"] = correction.manual_city
        update["geography_city"] = correction.manual_city
    if correction.manual_address:
        update["manual_address"] = correction.manual_address
    if correction.manual_event_label:
        update["manual_event_label"] = correction.manual_event_label
    if correction.manual_status:
        update["manual_status"] = correction.manual_status
    if correction.relevant is False or correction.enforcement_related is False:
        update["publication_status"] = PublicationStatus.SUPPRESSED
        update["takedown_status"] = TakedownStatus.SUPPRESSED
        update["suppression_reason"] = "manual annotation marked item outside dataset scope"
    return record.model_copy(update=update)


def _import_csv_rows(
    input_path: Path,
    normalizer: Any,
    model_type: type[_ModelType],
) -> tuple[list[_ModelType], list[str]]:
    if not input_path.exists():
        raise FileNotFoundError(f"Annotation CSV not found: {input_path}")

    warnings: list[str] = []
    rows: list[_ModelType] = []
    with open(input_path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            try:
                normalized = normalizer(row)
                rows.append(model_type.model_validate(normalized))
            except Exception as exc:
                warnings.append(f"row {row_number}: skipped because {exc}")

    return rows, warnings


def _normalize_correction_row(row: dict[str, str | None]) -> dict[str, Any]:
    normalized = _normalize_headers(row)
    return {
        "record_id": _string_value(normalized, "record_id", "id"),
        "canonical_url": _string_value(normalized, "canonical_url", "url"),
        "relevant": _optional_bool(normalized, "relevant"),
        "enforcement_related": _optional_bool(normalized, "enforcement_related"),
        "taxonomy_version": _string_value(normalized, "taxonomy_version"),
        "taxonomy_category_id": _string_value(
            normalized, "taxonomy_category_id", "category_id", "tfht_category_id"
        ),
        "taxonomy_subcategory_id": _string_value(
            normalized, "taxonomy_subcategory_id", "subcategory_id", "tfht_subcategory_id"
        ),
        "category": _enum_value(Category, _string_value(normalized, "category")),
        "sub_category": _enum_value(
            SubCategory, _string_value(normalized, "sub_category", "subcategory")
        ),
        "summary_one_sentence": _string_value(normalized, "summary_one_sentence", "summary"),
        "manual_city": _string_value(normalized, "manual_city", "city"),
        "manual_address": _string_value(normalized, "manual_address", "address"),
        "manual_event_label": _string_value(normalized, "manual_event_label", "event_label"),
        "manual_status": _string_value(normalized, "manual_status", "status"),
        "reviewer": _string_value(normalized, "reviewer"),
        "reviewed_at": _optional_datetime(normalized, "reviewed_at"),
        "annotation_notes": _string_value(normalized, "annotation_notes", "notes"),
        "active": _bool_with_default(normalized, True, "active"),
        "annotation_source": _string_value(normalized, "annotation_source", "source")
        or "manual_correction",
    }


def _normalize_missing_item_row(row: dict[str, str | None]) -> dict[str, Any]:
    normalized = _normalize_headers(row)
    source_url = _string_value(normalized, "source_url", "url")
    if not source_url:
        raise ValueError("source_url is required")
    return {
        "annotation_id": _string_value(normalized, "annotation_id", "id") or build_news_item_id(source_url),
        "source_url": source_url,
        "canonical_url": _string_value(normalized, "canonical_url"),
        "title": _string_value(normalized, "title"),
        "event_date": _required_datetime(
            normalized,
            "event_date",
            "article_date",
            "publication_datetime",
        ),
        "source_name": _string_value(normalized, "source_name") or source_domain_from_url(source_url),
        "taxonomy_version": _string_value(normalized, "taxonomy_version"),
        "taxonomy_category_id": _string_value(
            normalized, "taxonomy_category_id", "category_id", "tfht_category_id"
        ),
        "taxonomy_subcategory_id": _string_value(
            normalized, "taxonomy_subcategory_id", "subcategory_id", "tfht_subcategory_id"
        ),
        "category": _enum_value(Category, _string_value(normalized, "category")),
        "sub_category": _enum_value(
            SubCategory, _string_value(normalized, "sub_category", "subcategory")
        ),
        "summary_one_sentence": _string_value(normalized, "summary_one_sentence", "summary"),
        "manual_city": _string_value(normalized, "manual_city", "city"),
        "manual_address": _string_value(normalized, "manual_address", "address"),
        "manual_event_label": _string_value(normalized, "manual_event_label", "event_label"),
        "manual_status": _string_value(normalized, "manual_status", "status"),
        "reviewer": _string_value(normalized, "reviewer"),
        "reviewed_at": _optional_datetime(normalized, "reviewed_at"),
        "annotation_notes": _string_value(normalized, "annotation_notes", "notes"),
        "active": _bool_with_default(normalized, True, "active"),
        "promotion_status": _string_value(normalized, "promotion_status") or "promoted",
        "annotation_source": _string_value(normalized, "annotation_source", "source")
        or "missing_item_annotation",
    }


def _normalize_headers(row: dict[str, str | None]) -> dict[str, str]:
    return {
        re.sub(r"[^a-z0-9]+", "_", key.strip().casefold()).strip("_"): (value or "").strip()
        for key, value in row.items()
        if key is not None
    }


def _string_value(normalized: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = normalized.get(key, "").strip()
        if value:
            return value
    return None


def _optional_bool(normalized: dict[str, str], key: str) -> bool | None:
    value = normalized.get(key, "").strip().casefold()
    if not value:
        return None
    if value in _CSV_TRUE_VALUES:
        return True
    if value in _CSV_FALSE_VALUES:
        return False
    raise ValueError(f"{key} must be a boolean value")


def _bool_with_default(normalized: dict[str, str], default: bool, key: str) -> bool:
    parsed = _optional_bool(normalized, key)
    return default if parsed is None else parsed


def _optional_datetime(normalized: dict[str, str], key: str) -> datetime | None:
    value = normalized.get(key, "").strip()
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _required_datetime(normalized: dict[str, str], *keys: str) -> datetime:
    for key in keys:
        parsed = _optional_datetime(normalized, key)
        if parsed is not None:
            return parsed
    raise ValueError(f"one of {', '.join(keys)} is required")


def _enum_value(
    enum_type: type[_EnumType],
    raw_value: str | None,
) -> _EnumType | None:
    if raw_value is None or raw_value == "":
        return None
    return enum_type(raw_value)
