"""Focused tests for taxonomy-aware validation scoring."""

from denbust.validation.evaluate import ValidationLabel, _score_predictions
from denbust.validation.models import ClassifierVariantSpec


def test_taxonomy_metrics_only_use_taxonomy_labeled_rows() -> None:
    metrics = _score_predictions(
        labels=[
            ValidationLabel(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=True,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
            ),
            ValidationLabel(
                relevant=True,
                enforcement_related=False,
                category="prostitution",
                sub_category="",
                index_relevant=False,
                taxonomy_version="",
                taxonomy_category_id="",
                taxonomy_subcategory_id="",
            ),
        ],
        predictions=[
            ValidationLabel(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=True,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
            ),
            ValidationLabel(
                relevant=True,
                enforcement_related=False,
                category="prostitution",
                sub_category="",
                index_relevant=True,
                taxonomy_version="1",
                taxonomy_category_id="pimping_prostitution",
                taxonomy_subcategory_id="soliciting_prostitution",
            ),
        ],
        variant=ClassifierVariantSpec(name="baseline"),
        model="claude-sonnet-4-20250514",
    )

    assert metrics.taxonomy_labeled_examples == 1
    assert metrics.taxonomy_category_accuracy_taxonomy_labeled == 1.0
    assert metrics.taxonomy_subcategory_accuracy_taxonomy_labeled == 1.0
    assert metrics.index_relevance_f1_taxonomy_labeled == 1.0
