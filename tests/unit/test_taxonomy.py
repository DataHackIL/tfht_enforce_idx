"""Unit tests for packaged TFHT taxonomy assets."""

from denbust.classifier.relevance import Classifier
from denbust.data_models import Category, SubCategory
from denbust.taxonomy import default_taxonomy, taxonomy_examples_csv_text


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
