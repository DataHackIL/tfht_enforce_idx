"""Typed models used by validation-set collection and evaluation."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ValidationDraftRow(BaseModel):
    """Human-review draft row for a candidate article."""

    source_name: str
    article_date: datetime
    url: str
    canonical_url: str
    title: str
    snippet: str
    suggested_relevant: bool
    suggested_enforcement_related: bool = False
    suggested_index_relevant: bool = False
    suggested_taxonomy_version: str = ""
    suggested_taxonomy_category_id: str = ""
    suggested_taxonomy_subcategory_id: str = ""
    suggested_category: str
    suggested_sub_category: str = ""
    suggested_confidence: str
    relevant: bool
    enforcement_related: bool = False
    index_relevant: bool = False
    taxonomy_version: str = ""
    taxonomy_category_id: str = ""
    taxonomy_subcategory_id: str = ""
    category: str
    sub_category: str = ""
    review_status: str = "pending"
    annotation_source: str = ""
    expected_month_bucket: str = ""
    expected_city: str = ""
    expected_status: str = ""
    manual_city: str = ""
    manual_address: str = ""
    manual_event_label: str = ""
    manual_status: str = ""
    annotation_notes: str = ""
    collected_at: datetime


class ValidationSetRow(BaseModel):
    """Permanent validated article row used for experiment evaluation."""

    source_name: str
    article_date: datetime
    url: str
    canonical_url: str
    title: str
    snippet: str
    relevant: bool
    enforcement_related: bool = False
    index_relevant: bool = False
    taxonomy_version: str = ""
    taxonomy_category_id: str = ""
    taxonomy_subcategory_id: str = ""
    category: str
    sub_category: str = ""
    review_status: str = "reviewed"
    annotation_source: str = ""
    expected_month_bucket: str = ""
    expected_city: str = ""
    expected_status: str = ""
    manual_city: str = ""
    manual_address: str = ""
    manual_event_label: str = ""
    manual_status: str = ""
    annotation_notes: str = ""
    collected_at: datetime
    finalized_at: datetime
    draft_source: str


class ClassifierVariantOverrides(BaseModel):
    """Classifier override fields used in experiment variants."""

    model: str | None = None
    system_prompt: str | None = None
    user_prompt_template: str | None = None


class ClassifierVariantSpec(ClassifierVariantOverrides):
    """Named classifier variant."""

    name: str
    description: str | None = None


class ClassifierVariantMatrix(BaseModel):
    """Tracked matrix of classifier variants to evaluate."""

    defaults: ClassifierVariantOverrides = Field(default_factory=ClassifierVariantOverrides)
    variants: list[ClassifierVariantSpec] = Field(default_factory=list)


class BinaryStageMetrics(BaseModel):
    """Precision/recall metrics for a binary evaluation stage."""

    evaluated_examples: int = 0
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    accuracy: float = 0.0


class AccuracyStageMetrics(BaseModel):
    """Match-rate metrics for a categorical evaluation stage."""

    evaluated_examples: int = 0
    correct: int = 0
    accuracy: float = 0.0


class LabelBreakdownMetrics(BaseModel):
    """Accuracy breakdown for one expected label."""

    label: str
    evaluated_examples: int = 0
    correct: int = 0
    accuracy: float = 0.0


class LabelCountMetrics(BaseModel):
    """Frequency-only count for one label in the validation set."""

    label: str
    evaluated_examples: int = 0


class ValidationDatasetSummary(BaseModel):
    """Composition summary of the validation set used for evaluation."""

    total_examples: int = 0
    relevant_examples: int = 0
    legacy_only_examples: int = 0
    taxonomy_labeled_examples: int = 0
    legacy_category_counts_relevant_only: list[LabelCountMetrics] = Field(default_factory=list)
    legacy_subcategory_counts_relevant_only: list[LabelCountMetrics] = Field(default_factory=list)
    taxonomy_category_counts: list[LabelCountMetrics] = Field(default_factory=list)
    taxonomy_subcategory_counts: list[LabelCountMetrics] = Field(default_factory=list)


class VariantMetrics(BaseModel):
    """Computed metrics for a single classifier variant."""

    name: str
    description: str | None = None
    model: str
    relevance_stage: BinaryStageMetrics = Field(default_factory=BinaryStageMetrics)
    enforcement_stage_relevant_only: BinaryStageMetrics = Field(default_factory=BinaryStageMetrics)
    category_stage_relevant_only: AccuracyStageMetrics = Field(default_factory=AccuracyStageMetrics)
    subcategory_stage_relevant_only: AccuracyStageMetrics = Field(
        default_factory=AccuracyStageMetrics
    )
    taxonomy_category_stage_taxonomy_labeled: AccuracyStageMetrics = Field(
        default_factory=AccuracyStageMetrics
    )
    taxonomy_subcategory_stage_taxonomy_labeled: AccuracyStageMetrics = Field(
        default_factory=AccuracyStageMetrics
    )
    index_relevance_stage_taxonomy_labeled: BinaryStageMetrics = Field(
        default_factory=BinaryStageMetrics
    )
    legacy_category_breakdown_relevant_only: list[LabelBreakdownMetrics] = Field(
        default_factory=list
    )
    legacy_subcategory_breakdown_relevant_only: list[LabelBreakdownMetrics] = Field(
        default_factory=list
    )
    taxonomy_category_breakdown_taxonomy_labeled: list[LabelBreakdownMetrics] = Field(
        default_factory=list
    )
    taxonomy_subcategory_breakdown_taxonomy_labeled: list[LabelBreakdownMetrics] = Field(
        default_factory=list
    )
    relevance_precision: float
    relevance_recall: float
    relevance_f1: float
    relevance_accuracy: float
    enforcement_precision_relevant_only: float
    enforcement_recall_relevant_only: float
    enforcement_f1_relevant_only: float
    enforcement_accuracy_relevant_only: float
    category_accuracy_relevant_only: float
    subcategory_accuracy_relevant_only: float
    index_relevance_precision_taxonomy_labeled: float = 0.0
    index_relevance_recall_taxonomy_labeled: float = 0.0
    index_relevance_f1_taxonomy_labeled: float = 0.0
    index_relevance_accuracy_taxonomy_labeled: float = 0.0
    taxonomy_category_accuracy_taxonomy_labeled: float = 0.0
    taxonomy_subcategory_accuracy_taxonomy_labeled: float = 0.0
    overall_exact_match: float
    tp: int
    fp: int
    fn: int
    tn: int
    total_examples: int
    taxonomy_labeled_examples: int = 0


class ValidationReportPayload(BaseModel):
    """Serialized payload written to validation evaluation JSON reports."""

    evaluated_at: datetime
    validation_set_path: str
    variants_path: str
    dataset_summary: ValidationDatasetSummary
    rankings: list[VariantMetrics] = Field(default_factory=list)
