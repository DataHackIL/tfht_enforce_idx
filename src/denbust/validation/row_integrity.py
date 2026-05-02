"""Shared row-level validation integrity rules."""

from __future__ import annotations

from dataclasses import dataclass

from denbust.data_models import Category, SubCategory
from denbust.taxonomy import TaxonomyDefinition
from denbust.validation.common import ALLOWED_SUBCATEGORIES_BY_CATEGORY


@dataclass(frozen=True)
class RowIntegrityInput:
    """Parsed fields needed for validation row integrity checks."""

    relevant: bool | None
    enforcement_related: bool | None
    index_relevant: bool | None
    category_value: str
    sub_category_value: str
    taxonomy_version: str
    taxonomy_category_id: str
    taxonomy_subcategory_id: str
    require_taxonomy_for_relevant: bool = False
    require_taxonomy_version: bool = False
    normalize_non_relevant: bool = False


@dataclass(frozen=True)
class RowIntegrityIssue:
    """One row-level validation integrity issue."""

    field: str
    message: str


@dataclass(frozen=True)
class RowIntegrityResult:
    """Result of shared row-level integrity checks."""

    issues: list[RowIntegrityIssue]
    category: Category | None
    sub_category: SubCategory | None
    taxonomy_version: str
    taxonomy_category_id: str
    taxonomy_subcategory_id: str
    enforcement_related: bool | None
    index_relevant: bool | None


def validate_row_integrity(
    row: RowIntegrityInput,
    *,
    taxonomy: TaxonomyDefinition,
) -> RowIntegrityResult:
    """Validate shared category/taxonomy/index invariants for one row."""
    issues: list[RowIntegrityIssue] = []
    relevant = row.relevant
    enforcement_related = row.enforcement_related
    index_relevant = row.index_relevant
    taxonomy_version = row.taxonomy_version.strip()
    taxonomy_category_id = row.taxonomy_category_id.strip()
    taxonomy_subcategory_id = row.taxonomy_subcategory_id.strip()

    if relevant is False and row.normalize_non_relevant:
        enforcement_related = False
        index_relevant = False
        taxonomy_version = ""
        taxonomy_category_id = ""
        taxonomy_subcategory_id = ""
        return RowIntegrityResult(
            issues=issues,
            category=Category.NOT_RELEVANT,
            sub_category=None,
            taxonomy_version=taxonomy_version,
            taxonomy_category_id=taxonomy_category_id,
            taxonomy_subcategory_id=taxonomy_subcategory_id,
            enforcement_related=enforcement_related,
            index_relevant=index_relevant,
        )

    category = _parse_category(issues, row.category_value)
    sub_category = _parse_sub_category(issues, row.sub_category_value)

    _validate_relevant_category(issues, relevant, category)
    _validate_legacy_category_pair(issues, category, sub_category)

    if (
        relevant
        and row.require_taxonomy_for_relevant
        and not (taxonomy_version and taxonomy_category_id and taxonomy_subcategory_id)
    ):
        issues.append(
            RowIntegrityIssue(
                field="taxonomy_category_id",
                message="Relevant rows must include taxonomy_version and taxonomy ids",
            )
        )
        return RowIntegrityResult(
            issues=issues,
            category=category,
            sub_category=sub_category,
            taxonomy_version=taxonomy_version,
            taxonomy_category_id=taxonomy_category_id,
            taxonomy_subcategory_id=taxonomy_subcategory_id,
            enforcement_related=enforcement_related,
            index_relevant=index_relevant,
        )

    _validate_taxonomy(
        issues,
        taxonomy=taxonomy,
        taxonomy_version=taxonomy_version,
        taxonomy_category_id=taxonomy_category_id,
        taxonomy_subcategory_id=taxonomy_subcategory_id,
        require_taxonomy_version=row.require_taxonomy_version,
        index_relevant=index_relevant,
        category=category,
        sub_category=sub_category,
    )

    return RowIntegrityResult(
        issues=issues,
        category=category,
        sub_category=sub_category,
        taxonomy_version=taxonomy_version,
        taxonomy_category_id=taxonomy_category_id,
        taxonomy_subcategory_id=taxonomy_subcategory_id,
        enforcement_related=enforcement_related,
        index_relevant=index_relevant,
    )


def _parse_category(issues: list[RowIntegrityIssue], value: str) -> Category | None:
    try:
        return Category(value.strip())
    except ValueError:
        issues.append(
            RowIntegrityIssue(
                field="category",
                message=f"Invalid category value: {value.strip()!r}",
            )
        )
        return None


def _parse_sub_category(issues: list[RowIntegrityIssue], value: str) -> SubCategory | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return SubCategory(stripped)
    except ValueError:
        issues.append(
            RowIntegrityIssue(
                field="sub_category",
                message=f"Invalid sub_category value: {stripped!r}",
            )
        )
        return None


def _validate_legacy_category_pair(
    issues: list[RowIntegrityIssue],
    category: Category | None,
    sub_category: SubCategory | None,
) -> None:
    if category is None or sub_category is None:
        return
    allowed = ALLOWED_SUBCATEGORIES_BY_CATEGORY.get(category, set())
    if sub_category not in allowed:
        issues.append(
            RowIntegrityIssue(
                field="sub_category",
                message=f"Invalid sub_category {sub_category.value!r} for category {category.value!r}",
            )
        )


def _validate_relevant_category(
    issues: list[RowIntegrityIssue],
    relevant: bool | None,
    category: Category | None,
) -> None:
    if relevant and category == Category.NOT_RELEVANT:
        issues.append(
            RowIntegrityIssue(
                field="category",
                message="Relevant rows cannot use category 'not_relevant'",
            )
        )


def _validate_taxonomy(
    issues: list[RowIntegrityIssue],
    *,
    taxonomy: TaxonomyDefinition,
    taxonomy_version: str,
    taxonomy_category_id: str,
    taxonomy_subcategory_id: str,
    require_taxonomy_version: bool,
    index_relevant: bool | None,
    category: Category | None,
    sub_category: SubCategory | None,
) -> None:
    has_version = bool(taxonomy_version)
    has_category_id = bool(taxonomy_category_id)
    has_subcategory_id = bool(taxonomy_subcategory_id)
    if not (has_version or has_category_id or has_subcategory_id):
        return

    if not (has_category_id and has_subcategory_id):
        issues.append(
            RowIntegrityIssue(
                field="taxonomy_category_id",
                message="Taxonomy labels must include both category and subcategory ids",
            )
        )
        return

    if require_taxonomy_version and not has_version:
        issues.append(
            RowIntegrityIssue(
                field="taxonomy_category_id",
                message="Taxonomy labels must include version, category, and subcategory",
            )
        )
        return

    if taxonomy_version and taxonomy_version != taxonomy.version:
        issues.append(
            RowIntegrityIssue(
                field="taxonomy_version",
                message=(
                    f"Unsupported taxonomy version {taxonomy_version!r}; "
                    f"expected {taxonomy.version!r}"
                ),
            )
        )

    if not taxonomy.has_pair(taxonomy_category_id, taxonomy_subcategory_id):
        issues.append(
            RowIntegrityIssue(
                field="taxonomy_subcategory_id",
                message=f"Invalid taxonomy pair: {taxonomy_category_id}/{taxonomy_subcategory_id}",
            )
        )
        return

    expected_category, expected_sub_category = taxonomy.legacy_mapping(
        taxonomy_category_id,
        taxonomy_subcategory_id,
    )
    if category is not None and category != expected_category:
        issues.append(
            RowIntegrityIssue(
                field="category",
                message="category does not match the packaged taxonomy legacy mapping",
            )
        )
    if sub_category is not None and sub_category != expected_sub_category:
        issues.append(
            RowIntegrityIssue(
                field="sub_category",
                message="sub_category does not match the packaged taxonomy legacy mapping",
            )
        )

    if index_relevant is None:
        return
    expected_index_relevant = taxonomy.is_index_relevant(
        taxonomy_category_id,
        taxonomy_subcategory_id,
    )
    if index_relevant != expected_index_relevant:
        issues.append(
            RowIntegrityIssue(
                field="index_relevant",
                message=(
                    "index_relevant does not match the packaged taxonomy "
                    f"for {taxonomy_category_id}/{taxonomy_subcategory_id}"
                ),
            )
        )
