"""Loader for packaged TFHT taxonomy assets."""

from __future__ import annotations

from functools import lru_cache
from importlib import resources

import yaml
from pydantic import BaseModel, Field

from denbust.data_models import Category, SubCategory

TFHT_TYPOLOGY_V1 = "tfht_typology_v1"


class SubcategoryDefinition(BaseModel):
    """Single TFHT taxonomy leaf."""

    id: str
    label_he: str
    label_en: str | None = None
    index_relevant: bool
    legacy_category: Category
    legacy_sub_category: SubCategory | None = None
    example_urls: list[str] = Field(default_factory=list)


class CategoryDefinition(BaseModel):
    """Single TFHT taxonomy category."""

    id: str
    label_he: str
    label_en: str | None = None
    subcategories: list[SubcategoryDefinition] = Field(default_factory=list)


class TaxonomyDefinition(BaseModel):
    """Loaded TFHT taxonomy."""

    version: str
    asset_name: str = TFHT_TYPOLOGY_V1
    categories: list[CategoryDefinition]

    def category_ids(self) -> list[str]:
        """Return all category identifiers in order."""
        return [category.id for category in self.categories]

    def all_subcategory_ids(self) -> list[str]:
        """Return all leaf identifiers in order."""
        return [leaf.id for category in self.categories for leaf in category.subcategories]

    def category(self, category_id: str) -> CategoryDefinition:
        """Return a top-level category definition."""
        for category in self.categories:
            if category.id == category_id:
                return category
        raise KeyError(f"Unknown taxonomy category: {category_id}")

    def subcategory(
        self,
        category_id: str,
        subcategory_id: str,
    ) -> SubcategoryDefinition:
        """Return a validated subcategory definition."""
        category = self.category(category_id)
        for leaf in category.subcategories:
            if leaf.id == subcategory_id:
                return leaf
        raise KeyError(
            f"Unknown taxonomy subcategory {subcategory_id!r} for category {category_id!r}"
        )

    def has_pair(self, category_id: str, subcategory_id: str) -> bool:
        """Return whether the category/subcategory pair exists."""
        try:
            self.subcategory(category_id, subcategory_id)
        except KeyError:
            return False
        return True

    def is_index_relevant(self, category_id: str, subcategory_id: str) -> bool:
        """Return the TFHT index relevance for a taxonomy leaf."""
        return self.subcategory(category_id, subcategory_id).index_relevant

    def legacy_mapping(
        self,
        category_id: str,
        subcategory_id: str,
    ) -> tuple[Category, SubCategory | None]:
        """Return the legacy compatibility mapping for a taxonomy leaf."""
        leaf = self.subcategory(category_id, subcategory_id)
        return leaf.legacy_category, leaf.legacy_sub_category

    def prompt_table(self) -> str:
        """Render a compact prompt block for the classifier."""
        lines: list[str] = []
        for category in self.categories:
            options = " | ".join(
                f"{leaf.id} ({leaf.label_he})" for leaf in category.subcategories
            )
            lines.append(f"- {category.id} ({category.label_he}) -> {options}")
        return "\n".join(lines)

    def validate_unique_ids(self) -> None:
        """Validate category/subcategory ids are unique."""
        category_ids = self.category_ids()
        if len(category_ids) != len(set(category_ids)):
            raise ValueError("Taxonomy contains duplicate category ids")
        subcategory_ids = self.all_subcategory_ids()
        if len(subcategory_ids) != len(set(subcategory_ids)):
            raise ValueError("Taxonomy contains duplicate subcategory ids")


def _asset_text(filename: str) -> str:
    return resources.files("denbust.taxonomy.assets").joinpath(filename).read_text(encoding="utf-8")


@lru_cache(maxsize=8)
def load_taxonomy(asset_name: str = TFHT_TYPOLOGY_V1) -> TaxonomyDefinition:
    """Load a packaged TFHT taxonomy asset."""
    payload = yaml.safe_load(_asset_text(f"{asset_name}.yaml")) or {}
    taxonomy = TaxonomyDefinition.model_validate(payload)
    taxonomy.validate_unique_ids()
    return taxonomy


@lru_cache(maxsize=1)
def default_taxonomy() -> TaxonomyDefinition:
    """Load the default TFHT taxonomy asset."""
    return load_taxonomy()


def taxonomy_examples_csv_text(asset_name: str = TFHT_TYPOLOGY_V1) -> str:
    """Return the raw packaged examples CSV for a taxonomy asset."""
    return _asset_text(f"{asset_name}_examples.csv")
