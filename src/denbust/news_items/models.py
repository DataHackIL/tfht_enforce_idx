"""Typed models for the news_items dataset."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from denbust.data_models import Category, SubCategory, UnifiedItem
from denbust.models.policies import (
    PrivacyRisk,
    PublicationStatus,
    ReviewStatus,
    RightsClass,
    TakedownStatus,
)
from denbust.news_items.normalize import (
    build_news_item_id,
    canonicalize_news_url,
    deduplicate_strings,
    source_domain_from_url,
)


class NewsItemEnrichment(BaseModel):
    """Metadata enrichment output for a unified news item."""

    summary_one_sentence: str
    geography_region: str | None = None
    geography_city: str | None = None
    organizations_mentioned: list[str] = Field(default_factory=list)
    topic_tags: list[str] = Field(default_factory=list)
    privacy_risk_level: PrivacyRisk = PrivacyRisk.LOW
    privacy_reason: str | None = None


class NewsItemPublicRecord(BaseModel):
    """Public, metadata-only row for the news_items dataset."""

    id: str
    source_name: str
    source_domain: str
    url: str
    canonical_url: str
    publication_datetime: datetime
    retrieval_datetime: datetime
    language: str = "he"
    title: str
    category: Category
    sub_category: SubCategory | None = None
    summary_one_sentence: str
    geography_country: str = "Israel"
    geography_region: str | None = None
    geography_city: str | None = None
    organizations_mentioned: list[str] = Field(default_factory=list)
    topic_tags: list[str] = Field(default_factory=list)
    rights_class: RightsClass = RightsClass.METADATA_ONLY
    privacy_risk_level: PrivacyRisk = PrivacyRisk.LOW
    review_status: ReviewStatus = ReviewStatus.NONE
    publication_status: PublicationStatus = PublicationStatus.APPROVED
    takedown_status: TakedownStatus = TakedownStatus.NONE
    event_candidate_ids: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    release_version: str | None = None


class NewsItemOperationalRecord(NewsItemPublicRecord):
    """Operational/internal row for news_items persistence."""

    source_urls: list[str] = Field(default_factory=list)
    source_count: int = 1
    classification_confidence: str | None = None
    suppression_reason: str | None = None
    summary_generation_model: str | None = None
    privacy_reason: str | None = None

    def to_public_record(self, *, release_version: str) -> NewsItemPublicRecord:
        """Project an operational record to its public metadata-only representation."""
        data = self.model_dump(mode="python")
        for field_name in (
            "source_urls",
            "source_count",
            "classification_confidence",
            "suppression_reason",
            "summary_generation_model",
            "privacy_reason",
        ):
            data.pop(field_name, None)
        data["release_version"] = release_version
        return NewsItemPublicRecord.model_validate(data)

    @classmethod
    def from_unified_item(
        cls,
        item: UnifiedItem,
        *,
        retrieval_datetime: datetime,
        enrichment: NewsItemEnrichment,
        classification_confidence: str | None = None,
        review_status: ReviewStatus = ReviewStatus.NONE,
        publication_status: PublicationStatus = PublicationStatus.APPROVED,
        takedown_status: TakedownStatus = TakedownStatus.NONE,
        suppression_reason: str | None = None,
        summary_generation_model: str | None = None,
        privacy_reason: str | None = None,
    ) -> NewsItemOperationalRecord:
        """Build an operational record from a unified news item and enrichment output."""
        source_urls = [str(source.url) for source in item.sources]
        primary_url = str(item.canonical_url or item.sources[0].url)
        canonical_url = canonicalize_news_url(primary_url)
        title = " ".join(item.headline.split()).strip()
        created_at = retrieval_datetime
        updated_at = retrieval_datetime

        topic_tags = deduplicate_strings(
            [
                item.category.value.replace("_", "-"),
                item.sub_category.value.replace("_", "-") if item.sub_category else "",
                *enrichment.topic_tags,
            ]
        )

        return cls(
            id=build_news_item_id(canonical_url),
            source_name=item.primary_source_name or item.sources[0].source_name,
            source_domain=source_domain_from_url(canonical_url),
            url=primary_url,
            canonical_url=canonical_url,
            publication_datetime=item.date,
            retrieval_datetime=retrieval_datetime,
            title=title,
            category=item.category,
            sub_category=item.sub_category,
            summary_one_sentence=enrichment.summary_one_sentence,
            geography_region=enrichment.geography_region,
            geography_city=enrichment.geography_city,
            organizations_mentioned=deduplicate_strings(enrichment.organizations_mentioned),
            topic_tags=topic_tags,
            privacy_risk_level=enrichment.privacy_risk_level,
            review_status=review_status,
            publication_status=publication_status,
            takedown_status=takedown_status,
            created_at=created_at,
            updated_at=updated_at,
            source_urls=source_urls,
            source_count=len(source_urls),
            classification_confidence=classification_confidence,
            suppression_reason=suppression_reason,
            summary_generation_model=summary_generation_model,
            privacy_reason=privacy_reason or enrichment.privacy_reason,
        )


class SuppressionRule(BaseModel):
    """Minimal suppression/takedown rule."""

    canonical_url: str | None = None
    record_id: str | None = None
    suppression_reason: str
    active: bool = True

    def model_post_init(self, __context: Any) -> None:
        if self.canonical_url:
            self.canonical_url = canonicalize_news_url(self.canonical_url)
