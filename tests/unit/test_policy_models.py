"""Unit tests for shared policy enums."""

from denbust.models.policies import (
    PrivacyRisk,
    PublicationStatus,
    ReviewStatus,
    RightsClass,
)


def test_policy_enums_round_trip_from_strings() -> None:
    """Policy enums should parse and serialize to their stable string values."""
    assert RightsClass("metadata_only") == RightsClass.METADATA_ONLY
    assert str(PrivacyRisk.MINOR_INVOLVED) == "minor_involved"
    assert ReviewStatus("needs_fact_review") == ReviewStatus.NEEDS_FACT_REVIEW
    assert PublicationStatus("published") == PublicationStatus.PUBLISHED
