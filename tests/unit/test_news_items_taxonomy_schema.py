"""Focused tests for taxonomy-aligned news_items schema additions."""

from __future__ import annotations

from datetime import UTC, datetime

from denbust.news_items.models import NewsItemEventScaffoldRecord, NewsItemPublicRecord


def test_news_item_public_record_schema_contains_taxonomy_fields() -> None:
    schema = NewsItemPublicRecord.model_json_schema()

    assert "taxonomy_version" in schema["properties"]
    assert "taxonomy_category_id" in schema["properties"]
    assert "taxonomy_subcategory_id" in schema["properties"]
    assert "index_relevant" in schema["properties"]


def test_event_scaffold_record_serializes_future_site_shape() -> None:
    record = NewsItemEventScaffoldRecord(
        id="event-1",
        month_key="2026-01",
        event_date=datetime(2026, 1, 8, tzinfo=UTC),
        city="בני ברק",
        address_text="בני ברק",
        event_label="מעצר חשודה על החזקת מקום לשם זנות",
        relevant_details="עינת הראל נעצרה; אותר בית בושת פעיל",
        status_text="פתוח",
        source_urls=["https://www.maariv.co.il/news/law/article-1270778"],
        taxonomy_version="1",
        taxonomy_category_id="brothels",
        taxonomy_subcategory_id="keeping_brothel",
        index_relevant=True,
    )

    payload = record.model_dump(mode="json")
    assert payload["month_key"] == "2026-01"
    assert payload["taxonomy_subcategory_id"] == "keeping_brothel"
    assert payload["index_relevant"] is True
