"""Shared validation-set constants and helpers."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

from denbust.config import DEFAULT_KEYWORDS, Config
from denbust.data_models import Category, SubCategory
from denbust.news_items.normalize import canonicalize_news_url

DRAFT_COLUMNS = [
    "source_name",
    "article_date",
    "url",
    "canonical_url",
    "title",
    "snippet",
    "suggested_relevant",
    "suggested_category",
    "suggested_sub_category",
    "suggested_confidence",
    "relevant",
    "category",
    "sub_category",
    "review_status",
    "annotation_notes",
    "collected_at",
]

VALIDATION_SET_COLUMNS = [
    "source_name",
    "article_date",
    "url",
    "canonical_url",
    "title",
    "snippet",
    "relevant",
    "category",
    "sub_category",
    "review_status",
    "annotation_notes",
    "collected_at",
    "finalized_at",
    "draft_source",
]

DEFAULT_VALIDATION_SET_PATH = Path("validation/news_items/classifier_validation.csv")
DEFAULT_VARIANT_MATRIX_PATH = Path("agents/validation/classifier_variants.yaml")

ALLOWED_SUBCATEGORIES_BY_CATEGORY: dict[Category, set[SubCategory]] = {
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

RELAXED_KEYWORD_ADDITIONS = [
    "בית בושת אותר",
    "זנות קטינה",
    "קטינה לזנות",
    "חשד לזנות",
    "קורבן זנות",
    "סחר בנשים",
    "סחר מיני",
    "ניצול מיני",
    "שידול לזנות",
    "דירת דיסקרט",
    "מכון עיסוי",
    "מכון ליווי",
    "שירותי מין",
    "סרסורות",
    "סחר בבנות",
]


def relaxed_validation_keywords() -> list[str]:
    """Return a recall-oriented keyword list for draft collection."""
    seen: set[str] = set()
    values: list[str] = []
    for keyword in [*DEFAULT_KEYWORDS, *RELAXED_KEYWORD_ADDITIONS]:
        candidate = " ".join(keyword.split()).strip()
        if not candidate:
            continue
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        values.append(candidate)
    return values


def validation_state_dir(config: Config) -> Path:
    """Return the local validation state root for a config."""
    return config.store.state_root / "validation" / config.dataset_name.value


def validation_drafts_dir(config: Config) -> Path:
    """Return the local draft directory."""
    return validation_state_dir(config) / "drafts"


def validation_reports_dir(config: Config) -> Path:
    """Return the local report directory."""
    return validation_state_dir(config) / "reports"


def default_collect_output_path(config: Config, timestamp: datetime) -> Path:
    """Build the default draft CSV path."""
    stamp = timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return validation_drafts_dir(config) / f"classifier_draft_{stamp}.csv"


def default_evaluation_output_path(config: Config, timestamp: datetime) -> Path:
    """Build the default evaluation report path."""
    stamp = timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return validation_reports_dir(config) / f"classifier_variant_eval_{stamp}.json"


def ensure_parent_dir(path: Path) -> None:
    """Ensure a path's parent directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """Write a CSV file with a fixed column order."""
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read CSV rows as dictionaries."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{key: value or "" for key, value in row.items()} for row in reader]


def parse_bool(value: str) -> bool:
    """Parse a human-edited boolean CSV value."""
    normalized = value.strip().casefold()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def parse_datetime(value: str) -> datetime:
    """Parse an ISO datetime string into an aware datetime."""
    parsed = datetime.fromisoformat(value.strip())
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def normalize_category_value(value: str) -> str:
    """Normalize a category string for CSV storage."""
    return value.strip().casefold()


def normalize_subcategory_value(value: str) -> str:
    """Normalize a sub-category string for CSV storage."""
    return value.strip().casefold()


def normalize_review_status(value: str) -> str:
    """Normalize a review-status string."""
    return value.strip().casefold()


def canonicalize_csv_url(url: str, canonical_url: str = "") -> str:
    """Normalize canonical URL values from CSV rows."""
    source = canonical_url.strip() or url.strip()
    return canonicalize_news_url(source)
