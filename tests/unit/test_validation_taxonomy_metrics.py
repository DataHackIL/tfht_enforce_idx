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
    assert metrics.taxonomy_category_stage_taxonomy_labeled.evaluated_examples == 1
    assert metrics.taxonomy_subcategory_stage_taxonomy_labeled.evaluated_examples == 1
    assert metrics.index_relevance_stage_taxonomy_labeled.evaluated_examples == 1
    assert metrics.taxonomy_category_accuracy_taxonomy_labeled == 1.0
    assert metrics.taxonomy_subcategory_accuracy_taxonomy_labeled == 1.0
    assert metrics.index_relevance_f1_taxonomy_labeled == 1.0


def test_validation_label_equality_supports_dataclass_and_non_matching_objects() -> None:
    label = ValidationLabel(
        relevant=True,
        enforcement_related=True,
        category="brothel",
        sub_category="closure",
        index_relevant=True,
        taxonomy_version="1",
        taxonomy_category_id="brothels",
        taxonomy_subcategory_id="administrative_closure",
    )

    assert label == ValidationLabel(
        relevant=True,
        enforcement_related=True,
        category="brothel",
        sub_category="closure",
        index_relevant=True,
        taxonomy_version="1",
        taxonomy_category_id="brothels",
        taxonomy_subcategory_id="administrative_closure",
    )
    assert label != "not-a-label"


def test_taxonomy_index_metrics_cover_fp_fn_and_tn_branches() -> None:
    metrics = _score_predictions(
        labels=[
            ValidationLabel(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=False,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
            ),
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
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=False,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
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
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=False,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
            ),
            ValidationLabel(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
                index_relevant=False,
                taxonomy_version="1",
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
            ),
        ],
        variant=ClassifierVariantSpec(name="baseline"),
        model="claude-sonnet-4-20250514",
    )

    assert metrics.taxonomy_labeled_examples == 3
    assert metrics.index_relevance_stage_taxonomy_labeled.tp == 0
    assert metrics.index_relevance_stage_taxonomy_labeled.fp == 1
    assert metrics.index_relevance_stage_taxonomy_labeled.fn == 1
    assert metrics.index_relevance_stage_taxonomy_labeled.tn == 1
    assert metrics.index_relevance_precision_taxonomy_labeled == 0.0
    assert metrics.index_relevance_recall_taxonomy_labeled == 0.0
    assert metrics.index_relevance_accuracy_taxonomy_labeled == 1 / 3


def test_exact_match_ignores_predicted_taxonomy_on_legacy_labels() -> None:
    metrics = _score_predictions(
        labels=[
            ValidationLabel(
                relevant=True,
                enforcement_related=False,
                category="prostitution",
                sub_category="",
                index_relevant=False,
                taxonomy_version="",
                taxonomy_category_id="",
                taxonomy_subcategory_id="",
            )
        ],
        predictions=[
            ValidationLabel(
                relevant=True,
                enforcement_related=False,
                category="prostitution",
                sub_category="",
                index_relevant=True,
                taxonomy_version="1",
                taxonomy_category_id="pimping_prostitution",
                taxonomy_subcategory_id="soliciting_prostitution",
            )
        ],
        variant=ClassifierVariantSpec(name="baseline"),
        model="claude-sonnet-4-20250514",
    )

    assert metrics.overall_exact_match == 1.0
