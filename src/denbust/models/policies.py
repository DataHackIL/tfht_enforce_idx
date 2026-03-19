"""Shared policy enums for future datasets and publication flows."""

from enum import StrEnum


class RightsClass(StrEnum):
    """Rights/publication classes."""

    OPEN_FULLTEXT = "open_fulltext"
    METADATA_ONLY = "metadata_only"
    LICENSED_FULLTEXT = "licensed_fulltext"
    INTERNAL_ONLY = "internal_only"
    RESTRICTED_REVIEW = "restricted_review"


class PrivacyRisk(StrEnum):
    """Privacy risk categories."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    SENSITIVE_SEXUAL_OFFENCE = "sensitive_sexual_offence"
    MINOR_INVOLVED = "minor_involved"
    VICTIM_IDENTIFYING_RISK = "victim_identifying_risk"


class ReviewStatus(StrEnum):
    """Human review requirements."""

    NONE = "none"
    NEEDS_RIGHTS_REVIEW = "needs_rights_review"
    NEEDS_PRIVACY_REVIEW = "needs_privacy_review"
    NEEDS_FACT_REVIEW = "needs_fact_review"
    NEEDS_DEDUP_REVIEW = "needs_dedup_review"
    NEEDS_ANONYMIZATION_REVIEW = "needs_anonymization_review"


class PublicationStatus(StrEnum):
    """Publication lifecycle states."""

    DRAFT = "draft"
    APPROVED = "approved"
    SUPPRESSED = "suppressed"
    PUBLISHED = "published"
    INTERNAL_ONLY = "internal_only"


class TakedownStatus(StrEnum):
    """Takedown or suppression lifecycle."""

    NONE = "none"
    REQUESTED = "requested"
    SUPPRESSED = "suppressed"
