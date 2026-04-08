"""Unit tests for manual news_items annotations and imports."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from denbust.config import Config
from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.models.policies import PublicationStatus, TakedownStatus
from denbust.news_items.annotations import (
    MissingNewsItemAnnotation,
    NewsItemCorrection,
    apply_manual_annotations,
    import_missing_news_items_csv,
    import_news_item_corrections_csv,
)
from denbust.news_items.ingest import build_operational_records
from denbust.news_items.models import NewsItemEnrichment, NewsItemOperationalRecord
from denbust.news_items.release import select_releasable_records
from denbust.ops.storage import LocalJsonOperationalStore
from denbust.ops.supabase import SupabaseOperationalStore


def _build_unified_item() -> UnifiedItem:
    return UnifiedItem(
        headline="המשטרה פשטה על בית בושת",
        summary="כתבה על פשיטה משטרתית בתל אביב.",
        sources=[
            SourceReference(
                source_name="mako",
                url="https://www.mako.co.il/news-law/Article-123.htm?utm_source=test",
            )
        ],
        date=datetime(2026, 4, 1, 8, 0, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
        taxonomy_version="1",
        taxonomy_category_id="brothels",
        taxonomy_subcategory_id="administrative_closure",
        index_relevant=True,
        enforcement_related=True,
        canonical_url="https://www.mako.co.il/news-law/Article-123.htm?utm_source=test",
        primary_source_name="mako",
    )


def _build_record() -> NewsItemOperationalRecord:
    return NewsItemOperationalRecord.from_unified_item(
        _build_unified_item(),
        retrieval_datetime=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
        enrichment=NewsItemEnrichment(
            summary_one_sentence="המשטרה סגרה מקום ששימש כבית בושת.",
            geography_city="תל אביב",
            topic_tags=["brothel"],
        ),
    )


def test_apply_manual_annotations_correction_wins_over_taxonomy_and_presentation() -> None:
    record = _build_record()
    correction = NewsItemCorrection.model_validate(
        {
            "canonical_url": record.canonical_url,
            "taxonomy_category_id": "pimping_prostitution",
            "taxonomy_subcategory_id": "pimping",
            "summary_one_sentence": "סיכום ידני קצר.",
            "manual_city": "חיפה",
            "manual_address": "רחוב הנמל 1, חיפה",
            "manual_event_label": "מעצר בגין סרסור",
            "manual_status": "נעצרו חשודים",
            "reviewer": "eden",
            "annotation_source": "csv_import",
        }
    )

    rows = apply_manual_annotations(
        [record],
        corrections=[correction],
        missing_items=[],
        suppression_rules=[],
    )

    assert len(rows) == 1
    updated = rows[0]
    assert updated.category is Category.PIMPING
    assert updated.sub_category is SubCategory.ARREST
    assert updated.taxonomy_category_id == "pimping_prostitution"
    assert updated.taxonomy_subcategory_id == "pimping"
    assert updated.summary_one_sentence == "סיכום ידני קצר."
    assert updated.geography_city == "חיפה"
    assert updated.manual_address == "רחוב הנמל 1, חיפה"
    assert updated.manual_event_label == "מעצר בגין סרסור"
    assert updated.manual_status == "נעצרו חשודים"
    assert updated.manually_overridden is True
    assert updated.annotation_source == "csv_import"
    assert updated.publication_status is PublicationStatus.APPROVED


def test_apply_manual_annotations_can_suppress_irrelevant_manual_correction() -> None:
    record = _build_record()
    correction = NewsItemCorrection.model_validate(
        {
            "record_id": record.id,
            "relevant": False,
            "annotation_source": "manual_review",
        }
    )

    rows = apply_manual_annotations(
        [record],
        corrections=[correction],
        missing_items=[],
        suppression_rules=[],
    )

    assert rows[0].publication_status is PublicationStatus.SUPPRESSED
    assert rows[0].takedown_status is TakedownStatus.SUPPRESSED
    assert rows[0].suppression_reason == "manual annotation marked item outside dataset scope"


@pytest.mark.asyncio
async def test_build_operational_records_promotes_missing_items_and_release_includes_them(
    tmp_path: Path,
) -> None:
    store = LocalJsonOperationalStore(tmp_path / "ops")
    store.upsert_news_item_corrections(
        "news_items",
        [
            {
                "canonical_url": _build_record().canonical_url,
                "manual_event_label": "סגירה ידנית",
                "annotation_source": "manual_csv",
            }
        ],
    )
    store.upsert_missing_news_items(
        "news_items",
        [
            {
                "annotation_id": "missing-1",
                "source_url": "https://www.ynet.co.il/news/article/missing1",
                "title": "צו סגירה מנהלי לדירה בחיפה",
                "event_date": "2026-04-03T08:00:00Z",
                "source_name": "ynet",
                "taxonomy_category_id": "brothels",
                "taxonomy_subcategory_id": "administrative_closure",
                "manual_city": "חיפה",
                "manual_event_label": "צו סגירה",
                "annotation_source": "missing_csv",
            }
        ],
    )
    config = Config(dataset_name="news_items")

    rows = await build_operational_records(
        [_build_unified_item()],
        config=config,
        operational_store=store,
    )

    assert len(rows) == 2
    assert any(row.manual_event_label == "סגירה ידנית" for row in rows)
    promoted = next(row for row in rows if row.annotation_source == "missing_csv")
    assert promoted.manually_overridden is True
    assert promoted.manual_event_label == "צו סגירה"

    public_rows = select_releasable_records(
        [row.model_dump(mode="json") for row in rows],
        release_version="2026-04-08",
    )
    assert len(public_rows) == 2
    assert any(row.annotation_source == "missing_csv" for row in public_rows)


def test_local_json_store_persists_corrections_and_missing_items(tmp_path: Path) -> None:
    store = LocalJsonOperationalStore(tmp_path / "ops")
    store.upsert_news_item_corrections(
        "news_items",
        [{"record_id": "row-1", "summary_one_sentence": "ידני"}],
    )
    store.upsert_missing_news_items(
        "news_items",
        [{"annotation_id": "missing-1", "canonical_url": "https://example.com/missing"}],
    )

    assert store.fetch_news_item_corrections("news_items") == [
        {"record_id": "row-1", "summary_one_sentence": "ידני"}
    ]
    assert store.fetch_missing_news_items("news_items") == [
        {"annotation_id": "missing-1", "canonical_url": "https://example.com/missing"}
    ]


def test_import_news_item_corrections_csv_normalizes_headers_and_warns(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "corrections.csv"
    input_path.write_text(
        "\n".join(
            [
                "URL,TFHT Category ID,TFHT Subcategory ID,Summary,Manual City,Active",
                "https://example.com/item,brothels,administrative_closure,סיכום ידני,חיפה,true",
                "https://example.com/bad,brothels,does_not_exist,שגוי,חיפה,true",
            ]
        ),
        encoding="utf-8",
    )

    corrections, warnings = import_news_item_corrections_csv(input_path)

    assert len(corrections) == 1
    assert corrections[0].category is Category.BROTHEL
    assert corrections[0].sub_category is SubCategory.CLOSURE
    assert corrections[0].manual_city == "חיפה"
    assert len(warnings) == 1


def test_import_missing_news_items_csv_requires_valid_taxonomy(tmp_path: Path) -> None:
    input_path = tmp_path / "missing.csv"
    input_path.write_text(
        "\n".join(
            [
                "Source URL,Title,Event Date,Source Name,TFHT Category ID,TFHT Subcategory ID",
                "https://example.com/item,כתבה,2026-04-01T00:00:00Z,ynet,brothels,administrative_closure",
                "https://example.com/bad,כתבה,2026-04-01T00:00:00Z,ynet,brothels,unknown_leaf",
            ]
        ),
        encoding="utf-8",
    )

    annotations, warnings = import_missing_news_items_csv(input_path)

    assert len(annotations) == 1
    assert isinstance(annotations[0], MissingNewsItemAnnotation)
    assert annotations[0].category is Category.BROTHEL
    assert annotations[0].sub_category is SubCategory.CLOSURE
    assert len(warnings) == 1


def test_supabase_store_supports_corrections_and_missing_item_tables() -> None:
    class FakeResponse:
        def __init__(self, payload: object) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return self._payload

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
            self.calls.append({"method": method, "url": url, **kwargs})
            if "news_items_corrections" in url:
                return FakeResponse([{"record_id": "row-1"}])
            if "news_items_missing_items" in url:
                return FakeResponse([{"annotation_id": "missing-1"}])
            return FakeResponse([])

        def close(self) -> None:
            return None

    client = FakeClient()
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=client,
    )

    store.upsert_news_item_corrections("news_items", [{"record_id": "row-1"}])
    store.upsert_missing_news_items("news_items", [{"annotation_id": "missing-1"}])
    corrections = store.fetch_news_item_corrections("news_items")
    missing_items = store.fetch_missing_news_items("news_items")

    assert corrections == [{"record_id": "row-1"}]
    assert missing_items == [{"annotation_id": "missing-1"}]
    correction_call = client.calls[0]
    missing_call = client.calls[1]
    assert correction_call["params"] == {"on_conflict": "dataset_name,record_id,canonical_url"}
    assert missing_call["params"] == {"on_conflict": "dataset_name,annotation_id"}
