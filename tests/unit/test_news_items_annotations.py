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
    _enum_value,
    _normalize_missing_item_row,
    _optional_bool,
    _required_datetime,
    apply_manual_annotations,
    import_missing_news_items_csv,
    import_news_item_corrections_csv,
    parse_news_item_corrections,
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


def test_apply_manual_annotations_skips_duplicate_promoted_missing_item() -> None:
    record = _build_record()
    annotation = MissingNewsItemAnnotation.model_validate(
        {
            "annotation_id": "missing-duplicate",
            "source_url": record.canonical_url,
            "canonical_url": record.canonical_url,
            "title": "אותו פריט בדיוק",
            "event_date": "2026-04-03T08:00:00Z",
            "source_name": "mako",
            "taxonomy_category_id": "brothels",
            "taxonomy_subcategory_id": "administrative_closure",
        }
    )

    rows = apply_manual_annotations(
        [record],
        corrections=[],
        missing_items=[annotation],
        suppression_rules=[],
    )

    assert len(rows) == 1
    assert rows[0].id == record.id


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


def test_import_missing_news_items_csv_uses_canonical_url_for_default_annotation_id(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "missing_tracking.csv"
    input_path.write_text(
        "\n".join(
            [
                "Source URL,Title,Event Date,Source Name,TFHT Category ID,TFHT Subcategory ID",
                "https://example.com/item?utm_source=one,כתבה,2026-04-01T00:00:00Z,ynet,brothels,administrative_closure",
                "https://example.com/item?utm_source=two,כתבה,2026-04-01T00:00:00Z,ynet,brothels,administrative_closure",
            ]
        ),
        encoding="utf-8",
    )

    annotations, warnings = import_missing_news_items_csv(input_path)

    assert warnings == []
    assert len(annotations) == 2
    assert annotations[0].canonical_url == annotations[1].canonical_url
    assert annotations[0].annotation_id == annotations[1].annotation_id


def test_import_news_item_annotation_csv_helpers_cover_validation_edges(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError, match="Annotation CSV not found"):
        import_news_item_corrections_csv(tmp_path / "missing.csv")

    missing_source_url = tmp_path / "missing_source_url.csv"
    missing_source_url.write_text(
        "\n".join(
            [
                "Title,Event Date,Source Name,TFHT Category ID,TFHT Subcategory ID",
                "כתבה,2026-04-01T00:00:00Z,ynet,brothels,administrative_closure",
            ]
        ),
        encoding="utf-8",
    )
    annotations, warnings = import_missing_news_items_csv(missing_source_url)
    assert annotations == []
    assert warnings == ["row 2: skipped because source_url is required"]

    bad_bool = tmp_path / "bad_bool.csv"
    bad_bool.write_text(
        "\n".join(
            [
                "URL,Record ID,Relevant",
                "https://example.com/item,row-1,maybe",
            ]
        ),
        encoding="utf-8",
    )
    corrections, warnings = import_news_item_corrections_csv(bad_bool)
    assert corrections == []
    assert warnings == ["row 2: skipped because relevant must be a boolean value"]

    missing_event_date = tmp_path / "missing_event_date.csv"
    missing_event_date.write_text(
        "\n".join(
            [
                "Source URL,Title,Source Name,TFHT Category ID,TFHT Subcategory ID",
                "https://example.com/item,כתבה,ynet,brothels,administrative_closure",
            ]
        ),
        encoding="utf-8",
    )
    annotations, warnings = import_missing_news_items_csv(missing_event_date)
    assert annotations == []
    assert warnings == [
        "row 2: skipped because one of event_date, article_date, publication_datetime is required"
    ]

    bad_enum = tmp_path / "bad_enum.csv"
    bad_enum.write_text(
        "\n".join(
            [
                "URL,Record ID,Category",
                "https://example.com/item,row-1,not-a-category",
            ]
        ),
        encoding="utf-8",
    )
    corrections, warnings = import_news_item_corrections_csv(bad_enum)
    assert corrections == []
    assert warnings == ["row 2: skipped because 'not-a-category' is not a valid Category"]


def test_annotation_helper_parsers_cover_boolean_datetime_and_enum_edges() -> None:
    assert _optional_bool({"active": "false"}, "active") is False

    with pytest.raises(ValueError, match="active must be a boolean value"):
        _optional_bool({"active": "nope"}, "active")

    with pytest.raises(
        ValueError,
        match="one of event_date, article_date is required",
    ):
        _required_datetime({}, "event_date", "article_date")

    with pytest.raises(ValueError, match="not-a-category"):
        _enum_value(Category, "not-a-category")


def test_missing_item_annotation_requires_title() -> None:
    with pytest.raises(ValueError, match="requires a title"):
        MissingNewsItemAnnotation.model_validate(
            {
                "annotation_id": "missing-1",
                "source_url": "https://example.com/item",
                "title": "   ",
                "event_date": "2026-04-01T00:00:00Z",
                "source_name": "ynet",
                "taxonomy_category_id": "brothels",
                "taxonomy_subcategory_id": "administrative_closure",
            }
        )


def test_annotation_models_require_full_taxonomy_pair() -> None:
    with pytest.raises(
        ValueError,
        match="taxonomy_category_id and taxonomy_subcategory_id must be provided together",
    ):
        NewsItemCorrection.model_validate(
            {
                "record_id": "row-1",
                "taxonomy_category_id": "brothels",
            }
        )


def test_annotation_models_reject_category_mismatch_with_taxonomy() -> None:
    with pytest.raises(
        ValueError,
        match="Provided category does not match taxonomy compatibility mapping",
    ):
        NewsItemCorrection.model_validate(
            {
                "record_id": "row-1",
                "taxonomy_category_id": "brothels",
                "taxonomy_subcategory_id": "administrative_closure",
                "category": Category.PIMPING,
            }
        )

    with pytest.raises(
        ValueError,
        match="Provided sub_category does not match taxonomy compatibility mapping",
    ):
        NewsItemCorrection.model_validate(
            {
                "record_id": "row-1",
                "taxonomy_category_id": "brothels",
                "taxonomy_subcategory_id": "administrative_closure",
                "sub_category": SubCategory.ARREST,
            }
        )


def test_normalize_missing_item_row_requires_source_url() -> None:
    with pytest.raises(ValueError, match="source_url is required"):
        _normalize_missing_item_row(
            {
                "Title": "כתבה",
                "Event Date": "2026-04-01T00:00:00Z",
                "Source Name": "ynet",
                "TFHT Category ID": "brothels",
                "TFHT Subcategory ID": "administrative_closure",
            }
        )


def test_parse_annotation_rows_logs_warning_for_invalid_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("WARNING")

    rows = parse_news_item_corrections([{"summary_one_sentence": "missing identity"}])

    assert rows == []
    assert "Skipping invalid annotation row" in caplog.text


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
    assert correction_call["params"] == {"on_conflict": "dataset_name,record_id"}
    assert missing_call["params"] == {"on_conflict": "dataset_name,annotation_id"}


def test_supabase_store_splits_correction_upserts_by_stable_key() -> None:
    class FakeResponse:
        def __init__(self) -> None:
            pass

        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return []

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
            self.calls.append({"method": method, "url": url, **kwargs})
            return FakeResponse()

        def close(self) -> None:
            return None

    client = FakeClient()
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=client,
    )

    store.upsert_news_item_corrections(
        "news_items",
        [
            {"record_id": "row-1", "summary_one_sentence": "by id"},
            {"canonical_url": "https://example.com/item", "summary_one_sentence": "by url"},
        ],
    )

    assert len(client.calls) == 2
    assert client.calls[0]["params"] == {"on_conflict": "dataset_name,record_id"}
    assert client.calls[1]["params"] == {"on_conflict": "dataset_name,canonical_url"}


def test_supabase_store_treats_non_string_record_id_as_present() -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return []

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
            self.calls.append({"method": method, "url": url, **kwargs})
            return FakeResponse()

        def close(self) -> None:
            return None

    client = FakeClient()
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=client,
    )

    store.upsert_news_item_corrections(
        "news_items",
        [{"record_id": 123, "summary_one_sentence": "numeric id"}],
    )

    assert len(client.calls) == 1
    assert client.calls[0]["params"] == {"on_conflict": "dataset_name,record_id"}


def test_supabase_store_rejects_correction_without_stable_key() -> None:
    class FakeClient:
        def request(self, method: str, url: str, **kwargs: object) -> object:
            del method, url, kwargs
            raise AssertionError("request should not be called")

        def close(self) -> None:
            return None

    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=FakeClient(),
    )

    with pytest.raises(ValueError, match="non-empty record_id or canonical_url"):
        store.upsert_news_item_corrections("news_items", [{"annotation_notes": "missing key"}])


def test_supabase_store_returns_empty_lists_for_non_list_annotation_payloads() -> None:
    class FakeResponse:
        def __init__(self, payload: object) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return self._payload

    class FakeClient:
        def request(self, method: str, url: str, **kwargs: object) -> FakeResponse:
            del method, url, kwargs
            return FakeResponse({"unexpected": True})

        def close(self) -> None:
            return None

    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=FakeClient(),
    )

    assert store.fetch_news_item_corrections("news_items") == []
    assert store.fetch_missing_news_items("news_items") == []


def test_supabase_store_annotation_upserts_return_early_for_empty_payloads() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def request(self, method: str, url: str, **kwargs: object) -> object:
            self.calls.append({"method": method, "url": url, **kwargs})
            raise AssertionError("request should not be called for empty upserts")

        def close(self) -> None:
            return None

    client = FakeClient()
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=Config(operational={"provider": "supabase"}).operational,
        client=client,
    )

    store.upsert_news_item_corrections("news_items", [])
    store.upsert_missing_news_items("news_items", [])

    assert client.calls == []
