"""Core data models for denbust."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, HttpUrl


class Category(StrEnum):
    """Article category."""

    BROTHEL = "brothel"
    PROSTITUTION = "prostitution"
    PIMPING = "pimping"
    TRAFFICKING = "trafficking"
    ENFORCEMENT = "enforcement"
    NOT_RELEVANT = "not_relevant"


class SubCategory(StrEnum):
    """Article sub-category."""

    # brothel
    CLOSURE = "closure"
    OPENING = "opening"
    # prostitution
    ARREST = "arrest"
    FINE = "fine"
    # pimping/trafficking
    SENTENCE = "sentence"
    # trafficking
    RESCUE = "rescue"
    # enforcement
    OPERATION = "operation"
    OTHER = "other"


class RawArticle(BaseModel):
    """Raw article fetched from a news source."""

    url: HttpUrl
    title: str
    snippet: str
    date: datetime
    source_name: str

    model_config = {"frozen": True}


class ClassificationResult(BaseModel):
    """Result of LLM classification."""

    relevant: bool
    category: Category
    sub_category: SubCategory | None = None
    confidence: str = Field(default="medium", pattern="^(high|medium|low)$")


class ClassifiedArticle(BaseModel):
    """Article with classification results."""

    article: RawArticle
    classification: ClassificationResult


class SourceReference(BaseModel):
    """Reference to an article from a specific source."""

    source_name: str
    url: HttpUrl


class UnifiedItem(BaseModel):
    """Unified item representing a story from multiple sources."""

    headline: str
    summary: str
    sources: list[SourceReference]
    date: datetime
    category: Category
    sub_category: SubCategory | None = None
