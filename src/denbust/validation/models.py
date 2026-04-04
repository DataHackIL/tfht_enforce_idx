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
    suggested_category: str
    suggested_sub_category: str = ""
    suggested_confidence: str
    relevant: bool
    category: str
    sub_category: str = ""
    review_status: str = "pending"
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
    category: str
    sub_category: str = ""
    review_status: str = "reviewed"
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


class VariantMetrics(BaseModel):
    """Computed metrics for a single classifier variant."""

    name: str
    description: str | None = None
    model: str
    relevance_precision: float
    relevance_recall: float
    relevance_f1: float
    relevance_accuracy: float
    category_accuracy_relevant_only: float
    subcategory_accuracy_relevant_only: float
    overall_exact_match: float
    tp: int
    fp: int
    fn: int
    tn: int
    total_examples: int
