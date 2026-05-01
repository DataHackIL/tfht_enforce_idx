"""Validation-set integrity checks."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from denbust.data_models import Category, SubCategory
from denbust.taxonomy import TaxonomyDefinition, default_taxonomy
from denbust.validation.common import (
    ALLOWED_SUBCATEGORIES_BY_CATEGORY,
    DEFAULT_VALIDATION_SET_PATH,
    VALIDATION_SET_COLUMNS,
    parse_bool,
    parse_datetime,
)


@dataclass(frozen=True)
class ValidationLintIssue:
    """One validation-set lint issue."""

    row_number: int
    field: str
    message: str

    def render(self) -> str:
        """Render a stable human-readable issue."""
        return f"row {self.row_number}, field {self.field}: {self.message}"


@dataclass(frozen=True)
class ValidationLintResult:
    """Result of linting one validation CSV."""

    validation_set_path: Path
    row_count: int
    issues: list[ValidationLintIssue]

    @property
    def passed(self) -> bool:
        """Return whether the validation set passed all checks."""
        return not self.issues

    def raise_for_issues(self) -> None:
        """Raise a ValueError with all lint issues when the set is invalid."""
        if self.passed:
            return
        rendered = "\n".join(issue.render() for issue in self.issues)
        raise ValueError(f"Validation set lint failed:\n{rendered}")


def _lint_bool(
    issues: list[ValidationLintIssue],
    *,
    row_number: int,
    row: dict[str, str],
    field: str,
) -> bool | None:
    try:
        return parse_bool(row.get(field, ""))
    except ValueError as exc:
        issues.append(ValidationLintIssue(row_number=row_number, field=field, message=str(exc)))
        return None


def _lint_datetime(
    issues: list[ValidationLintIssue],
    *,
    row_number: int,
    row: dict[str, str],
    field: str,
) -> None:
    try:
        parse_datetime(row.get(field, ""))
    except ValueError as exc:
        issues.append(ValidationLintIssue(row_number=row_number, field=field, message=str(exc)))


def _lint_category(
    issues: list[ValidationLintIssue],
    *,
    row_number: int,
    row: dict[str, str],
) -> tuple[Category | None, SubCategory | None]:
    category: Category | None = None
    sub_category: SubCategory | None = None
    category_value = row.get("category", "").strip()
    sub_category_value = row.get("sub_category", "").strip()
    try:
        category = Category(category_value)
    except ValueError:
        issues.append(
            ValidationLintIssue(
                row_number=row_number,
                field="category",
                message=f"Invalid category value: {category_value!r}",
            )
        )

    if sub_category_value:
        try:
            sub_category = SubCategory(sub_category_value)
        except ValueError:
            issues.append(
                ValidationLintIssue(
                    row_number=row_number,
                    field="sub_category",
                    message=f"Invalid sub_category value: {sub_category_value!r}",
                )
            )

    if category is not None and sub_category is not None:
        allowed = ALLOWED_SUBCATEGORIES_BY_CATEGORY.get(category, set())
        if sub_category not in allowed:
            issues.append(
                ValidationLintIssue(
                    row_number=row_number,
                    field="sub_category",
                    message=(
                        f"Invalid sub_category {sub_category.value!r} "
                        f"for category {category.value!r}"
                    ),
                )
            )

    return category, sub_category


def _lint_taxonomy(
    issues: list[ValidationLintIssue],
    *,
    row_number: int,
    row: dict[str, str],
    relevant: bool | None,
    index_relevant: bool | None,
    taxonomy: TaxonomyDefinition,
) -> None:
    taxonomy_version = row.get("taxonomy_version", "").strip()
    category_id = row.get("taxonomy_category_id", "").strip()
    subcategory_id = row.get("taxonomy_subcategory_id", "").strip()

    if relevant and not (taxonomy_version and category_id and subcategory_id):
        issues.append(
            ValidationLintIssue(
                row_number=row_number,
                field="taxonomy_category_id",
                message="Relevant rows must include taxonomy_version and taxonomy ids",
            )
        )
        return

    if not (taxonomy_version or category_id or subcategory_id):
        return

    if not (taxonomy_version and category_id and subcategory_id):
        issues.append(
            ValidationLintIssue(
                row_number=row_number,
                field="taxonomy_category_id",
                message="Taxonomy labels must include version, category, and subcategory",
            )
        )
        return

    if taxonomy_version != taxonomy.version:
        issues.append(
            ValidationLintIssue(
                row_number=row_number,
                field="taxonomy_version",
                message=(
                    f"Unsupported taxonomy version {taxonomy_version!r}; "
                    f"expected {taxonomy.version!r}"
                ),
            )
        )

    if not taxonomy.has_pair(category_id, subcategory_id):
        issues.append(
            ValidationLintIssue(
                row_number=row_number,
                field="taxonomy_subcategory_id",
                message=f"Invalid taxonomy pair: {category_id}/{subcategory_id}",
            )
        )
        return

    if index_relevant is not None:
        expected = taxonomy.is_index_relevant(category_id, subcategory_id)
        if index_relevant != expected:
            issues.append(
                ValidationLintIssue(
                    row_number=row_number,
                    field="index_relevant",
                    message=(
                        "index_relevant does not match the packaged taxonomy "
                        f"for {category_id}/{subcategory_id}"
                    ),
                )
            )


def lint_validation_set(
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
) -> ValidationLintResult:
    """Lint the permanent validation CSV before model-backed evaluation."""
    if not validation_set_path.exists():
        raise FileNotFoundError(f"Validation set not found: {validation_set_path}")

    taxonomy = default_taxonomy()
    issues: list[ValidationLintIssue] = []
    row_count = 0
    with validation_set_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != VALIDATION_SET_COLUMNS:
            issues.append(
                ValidationLintIssue(
                    row_number=1,
                    field="header",
                    message="Validation CSV header does not match the tracked schema",
                )
            )
        for raw_row in reader:
            row_count += 1
            row_number = reader.line_num
            extra_values = raw_row.pop(None, None)
            if extra_values:
                issues.append(
                    ValidationLintIssue(
                        row_number=row_number,
                        field="<extra_fields>",
                        message=f"Malformed CSV row has {len(extra_values)} extra field(s)",
                    )
                )
            row = {key: value or "" for key, value in raw_row.items() if key is not None}
            missing_fields = [field for field in VALIDATION_SET_COLUMNS if field not in row]
            for field in missing_fields:
                issues.append(
                    ValidationLintIssue(
                        row_number=row_number,
                        field=field,
                        message="Missing validation CSV field",
                    )
                )

            for field in ("article_date", "collected_at", "finalized_at"):
                _lint_datetime(issues, row_number=row_number, row=row, field=field)
            relevant = _lint_bool(issues, row_number=row_number, row=row, field="relevant")
            _lint_bool(issues, row_number=row_number, row=row, field="enforcement_related")
            index_relevant = _lint_bool(
                issues,
                row_number=row_number,
                row=row,
                field="index_relevant",
            )
            category, _sub_category = _lint_category(
                issues,
                row_number=row_number,
                row=row,
            )
            if relevant and category == Category.NOT_RELEVANT:
                issues.append(
                    ValidationLintIssue(
                        row_number=row_number,
                        field="category",
                        message="Relevant rows cannot use category 'not_relevant'",
                    )
                )
            _lint_taxonomy(
                issues,
                row_number=row_number,
                row=row,
                relevant=relevant,
                index_relevant=index_relevant,
                taxonomy=taxonomy,
            )

    if row_count == 0:
        issues.append(
            ValidationLintIssue(
                row_number=1,
                field="<file>",
                message="Validation set is empty",
            )
        )
    return ValidationLintResult(
        validation_set_path=validation_set_path,
        row_count=row_count,
        issues=issues,
    )


def run_validation_lint(
    *,
    validation_set_path: Path = DEFAULT_VALIDATION_SET_PATH,
) -> ValidationLintResult:
    """CLI wrapper for validation-set linting."""
    return lint_validation_set(validation_set_path)
