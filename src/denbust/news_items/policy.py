"""Privacy, review, and suppression rules for the news_items dataset."""

from __future__ import annotations

from denbust.models.policies import (
    PrivacyRisk,
    PublicationStatus,
    ReviewStatus,
    RightsClass,
    TakedownStatus,
)
from denbust.news_items.models import NewsItemOperationalRecord, SuppressionRule

_PRIVACY_SEVERITY: dict[PrivacyRisk, int] = {
    PrivacyRisk.LOW: 0,
    PrivacyRisk.MEDIUM: 1,
    PrivacyRisk.HIGH: 2,
    PrivacyRisk.SENSITIVE_SEXUAL_OFFENCE: 3,
    PrivacyRisk.MINOR_INVOLVED: 4,
    PrivacyRisk.VICTIM_IDENTIFYING_RISK: 5,
}

_MINOR_MARKERS = (
    "קטין",
    "קטינה",
    "בת 13",
    "בן 13",
    "נערה",
    "ילדה",
)
_SENSITIVE_MARKERS = (
    "קורבן",
    "נפגעת",
    "נפגע",
    "אונס",
    "תקיפה מינית",
    "פגיעה מינית",
)
_HIGH_RISK_MARKERS = (
    "מקלט לנשים",
    "חסרת ישע",
    "פליטה",
    "אישה זרה",
)


def infer_privacy_risk(text: str) -> tuple[PrivacyRisk, str | None]:
    """Apply simple rules to identify privacy-sensitive cases."""
    normalized = text.casefold()
    if any(marker in normalized for marker in _MINOR_MARKERS):
        return PrivacyRisk.MINOR_INVOLVED, "minor marker detected"
    if any(marker in normalized for marker in _SENSITIVE_MARKERS):
        return PrivacyRisk.SENSITIVE_SEXUAL_OFFENCE, "sexual-offence/victim marker detected"
    if any(marker in normalized for marker in _HIGH_RISK_MARKERS):
        return PrivacyRisk.HIGH, "vulnerable-population marker detected"
    return PrivacyRisk.LOW, None


def merge_privacy_risk(llm_risk: PrivacyRisk, rule_risk: PrivacyRisk) -> PrivacyRisk:
    """Return the stricter privacy risk of the two inputs."""
    return max((llm_risk, rule_risk), key=lambda risk: _PRIVACY_SEVERITY[risk])


def derive_review_status(privacy_risk: PrivacyRisk) -> ReviewStatus:
    """Derive review needs from the effective privacy risk."""
    if privacy_risk is PrivacyRisk.LOW:
        return ReviewStatus.NONE
    return ReviewStatus.NEEDS_PRIVACY_REVIEW


def derive_publication_status(privacy_risk: PrivacyRisk) -> PublicationStatus:
    """Derive the default publication status from privacy risk."""
    if privacy_risk in {
        PrivacyRisk.MINOR_INVOLVED,
        PrivacyRisk.SENSITIVE_SEXUAL_OFFENCE,
        PrivacyRisk.VICTIM_IDENTIFYING_RISK,
    }:
        return PublicationStatus.INTERNAL_ONLY
    if privacy_risk in {PrivacyRisk.MEDIUM, PrivacyRisk.HIGH}:
        return PublicationStatus.DRAFT
    return PublicationStatus.APPROVED


def apply_suppression(
    record: NewsItemOperationalRecord,
    suppression_rules: list[SuppressionRule],
) -> NewsItemOperationalRecord:
    """Apply active suppression rules to a news item record."""
    for rule in suppression_rules:
        if not rule.active:
            continue
        if rule.record_id and rule.record_id == record.id:
            return record.model_copy(
                update={
                    "takedown_status": TakedownStatus.SUPPRESSED,
                    "publication_status": PublicationStatus.SUPPRESSED,
                    "suppression_reason": rule.suppression_reason,
                }
            )
        if rule.canonical_url and rule.canonical_url == record.canonical_url:
            return record.model_copy(
                update={
                    "takedown_status": TakedownStatus.SUPPRESSED,
                    "publication_status": PublicationStatus.SUPPRESSED,
                    "suppression_reason": rule.suppression_reason,
                }
            )
    return record


def is_publicly_releasable(record: NewsItemOperationalRecord) -> bool:
    """Return whether a record is eligible for public release export."""
    return (
        record.rights_class is RightsClass.METADATA_ONLY
        and record.takedown_status is TakedownStatus.NONE
        and record.review_status is ReviewStatus.NONE
        and record.publication_status in {PublicationStatus.APPROVED, PublicationStatus.PUBLISHED}
    )
