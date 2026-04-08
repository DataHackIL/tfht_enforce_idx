"""Ingest helpers for normalized news_items operational records."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from denbust.config import Config
from denbust.data_models import UnifiedItem
from denbust.models.policies import PrivacyRisk
from denbust.news_items.annotations import (
    apply_manual_annotations,
    parse_missing_news_items,
    parse_news_item_corrections,
)
from denbust.news_items.enrich import NewsItemEnricher, fallback_enrichment
from denbust.news_items.models import NewsItemOperationalRecord, SuppressionRule
from denbust.news_items.policy import (
    derive_publication_status,
    derive_review_status,
    infer_privacy_risk,
    merge_privacy_risk,
)
from denbust.ops.storage import OperationalStore

logger = logging.getLogger(__name__)


def parse_suppression_rules(rows: list[dict[str, object]]) -> list[SuppressionRule]:
    """Validate suppression rows loaded from the operational store."""
    rules: list[SuppressionRule] = []
    for row in rows:
        try:
            rules.append(SuppressionRule.model_validate(row))
        except Exception as exc:
            logger.warning("Skipping invalid suppression rule payload %s: %s", row, exc)
    return rules


def _combined_privacy_input(item: UnifiedItem) -> str:
    return " ".join(
        segment
        for segment in (
            item.headline,
            item.summary,
            item.taxonomy_category_id or "",
            item.taxonomy_subcategory_id or "",
            item.category.value,
            item.sub_category.value if item.sub_category else "",
        )
        if segment
    )


async def build_operational_records(
    items: list[UnifiedItem],
    *,
    config: Config,
    operational_store: OperationalStore,
) -> list[NewsItemOperationalRecord]:
    """Enrich unified items into operational news_items records."""
    retrieval_datetime = datetime.now(UTC)
    suppression_rules = parse_suppression_rules(
        operational_store.fetch_suppression_rules(config.dataset_name.value)
    )
    corrections = parse_news_item_corrections(
        operational_store.fetch_news_item_corrections(config.dataset_name.value)
    )
    missing_items = parse_missing_news_items(
        operational_store.fetch_missing_news_items(config.dataset_name.value)
    )
    enricher = (
        NewsItemEnricher(api_key=config.anthropic_api_key, model=config.classifier.model)
        if config.anthropic_api_key
        else None
    )

    records: list[NewsItemOperationalRecord] = []
    for item in items:
        enrichment = (
            await enricher.enrich(item) if enricher is not None else fallback_enrichment(item)
        )
        rule_risk, rule_reason = infer_privacy_risk(_combined_privacy_input(item))
        privacy_risk = merge_privacy_risk(enrichment.privacy_risk_level, rule_risk)
        privacy_reason = rule_reason or enrichment.privacy_reason

        if privacy_risk is not enrichment.privacy_risk_level:
            enrichment = enrichment.model_copy(
                update={
                    "privacy_risk_level": privacy_risk,
                    "privacy_reason": privacy_reason,
                }
            )

        record = NewsItemOperationalRecord.from_unified_item(
            item,
            retrieval_datetime=retrieval_datetime,
            enrichment=enrichment,
            review_status=derive_review_status(privacy_risk),
            publication_status=derive_publication_status(privacy_risk),
            summary_generation_model=enricher.model_name if enricher is not None else None,
            privacy_reason=privacy_reason,
        )
        records.append(record)

    return apply_manual_annotations(
        records,
        corrections=corrections,
        missing_items=missing_items,
        suppression_rules=suppression_rules,
    )


def summarize_privacy_mix(records: list[NewsItemOperationalRecord]) -> dict[PrivacyRisk, int]:
    """Count operational records by privacy risk for run summaries/tests."""
    counts: dict[PrivacyRisk, int] = {}
    for record in records:
        counts[record.privacy_risk_level] = counts.get(record.privacy_risk_level, 0) + 1
    return counts
