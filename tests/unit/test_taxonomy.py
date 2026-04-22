"""Unit tests for packaged TFHT taxonomy assets."""

import pytest

from denbust.classifier.relevance import Classifier
from denbust.data_models import Category, SubCategory
from denbust.taxonomy import (
    CategoryDefinition,
    SubcategoryDefinition,
    TaxonomyDefinition,
    default_taxonomy,
    taxonomy_examples_csv_text,
)


def test_default_taxonomy_contains_expected_index_relevant_flags() -> None:
    taxonomy = default_taxonomy()

    assert taxonomy.version == "1"
    assert taxonomy.category_ids() == [
        "human_trafficking",
        "pimping_prostitution",
        "brothels",
    ]
    assert taxonomy.is_index_relevant("human_trafficking", "trafficking_sexual_exploitation")
    assert taxonomy.is_index_relevant("brothels", "administrative_closure")
    assert not taxonomy.is_index_relevant("pimping_prostitution", "women_testimonies")
    assert not taxonomy.is_index_relevant("human_trafficking", "trafficking_forced_labor")
    assert taxonomy.subcategory(
        "brothels",
        "administrative_closure",
    ).discovery_terms_he == ["צו סגירה", "צו הגבלת שימוש", "סגירה מנהלית"]


def test_examples_csv_contains_known_reference_urls() -> None:
    content = taxonomy_examples_csv_text()

    assert "tfht_typology_v1" not in content  # CSV payload only
    assert "https://www.maariv.co.il/news/law/article-1270778" in content
    assert "https://www.kan-ashkelon.co.il/news/100735" in content


def test_classifier_parses_taxonomy_ids_and_derives_legacy_mapping() -> None:
    classifier = Classifier(api_key="test-key")

    result = classifier._parse_response(
        '{"relevant": true, "enforcement_related": true, '
        '"taxonomy_category_id": "brothels", '
        '"taxonomy_subcategory_id": "administrative_closure", '
        '"confidence": "high"}'
    )

    assert result.taxonomy_version == "1"
    assert result.taxonomy_category_id == "brothels"
    assert result.taxonomy_subcategory_id == "administrative_closure"
    assert result.index_relevant is True
    assert result.category == Category.BROTHEL
    assert result.sub_category == SubCategory.CLOSURE


def test_classifier_rejects_invalid_taxonomy_pair() -> None:
    classifier = Classifier(api_key="test-key")

    result = classifier._parse_response(
        '{"relevant": true, "enforcement_related": true, '
        '"taxonomy_category_id": "brothels", '
        '"taxonomy_subcategory_id": "trafficking_women", '
        '"confidence": "high"}'
    )

    assert result.relevant is False
    assert result.taxonomy_category_id is None
    assert result.taxonomy_subcategory_id is None
    assert result.category == Category.NOT_RELEVANT


def test_taxonomy_lookup_errors_and_prompt_table() -> None:
    taxonomy = default_taxonomy()

    with pytest.raises(KeyError, match="Unknown taxonomy category"):
        taxonomy.category("missing")
    with pytest.raises(KeyError, match="Unknown taxonomy subcategory"):
        taxonomy.subcategory("brothels", "missing")

    prompt_table = taxonomy.prompt_table()
    assert "- brothels (בתי בושת) -> " in prompt_table
    assert "administrative_closure" in prompt_table
    assert "closure_appeal" in prompt_table


def test_taxonomy_discovery_terms_include_curated_non_index_and_dedupe_per_leaf() -> None:
    taxonomy = default_taxonomy()

    terms = taxonomy.discovery_terms()

    assert (
        "human_trafficking",
        "trafficking_forced_marriage",
        "נישואין בכפייה",
    ) in terms
    assert ("brothels", "client_fine", "קנס צריכת זנות") in terms
    assert ("pimping_prostitution", "nordic_model_law", "המודל הנורדי") in terms
    assert ("pimping_prostitution", "women_testimonies", "עדויות של נשים בזנות") not in terms


def test_taxonomy_discovery_terms_can_exclude_adjacent_non_index_terms() -> None:
    taxonomy = default_taxonomy()

    terms = taxonomy.discovery_terms(include_adjacent_non_index=False)

    assert ("pimping_prostitution", "nordic_model_law", "המודל הנורדי") not in terms


def test_taxonomy_validate_unique_ids_rejects_duplicates() -> None:
    duplicate_category = TaxonomyDefinition(
        version="1",
        categories=[
            CategoryDefinition(id="dup", label_he="א", subcategories=[]),
            CategoryDefinition(id="dup", label_he="ב", subcategories=[]),
        ],
    )
    with pytest.raises(ValueError, match="duplicate category ids"):
        duplicate_category.validate_unique_ids()

    duplicate_subcategory = TaxonomyDefinition(
        version="1",
        categories=[
            CategoryDefinition(
                id="cat_a",
                label_he="א",
                subcategories=[
                    SubcategoryDefinition(
                        id="dup_leaf",
                        label_he="עלה א",
                        index_relevant=True,
                        legacy_category=Category.BROTHEL,
                        legacy_sub_category=SubCategory.CLOSURE,
                    )
                ],
            ),
            CategoryDefinition(
                id="cat_b",
                label_he="ב",
                subcategories=[
                    SubcategoryDefinition(
                        id="dup_leaf",
                        label_he="עלה ב",
                        index_relevant=False,
                        legacy_category=Category.PROSTITUTION,
                        legacy_sub_category=SubCategory.FINE,
                    )
                ],
            ),
        ],
    )
    with pytest.raises(ValueError, match="duplicate subcategory ids"):
        duplicate_subcategory.validate_unique_ids()
