"""Unit tests for Phase B news_items functionality."""

from __future__ import annotations

import builtins
import hashlib
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from denbust.config import Config
from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.models.policies import PrivacyRisk, PublicationStatus, ReviewStatus, TakedownStatus
from denbust.news_items.backup import (
    GoogleDriveLatestBackupUploader,
    ObjectStorageLatestBackupUploader,
    _release_version_from_dir,
    execute_latest_backup,
    find_latest_release_dir,
)
from denbust.news_items.enrich import (
    NewsItemEnricher,
    fallback_enrichment,
    sanitize_summary_one_sentence,
)
from denbust.news_items.ingest import build_operational_records, parse_suppression_rules
from denbust.news_items.models import NewsItemEnrichment, NewsItemOperationalRecord, SuppressionRule
from denbust.news_items.normalize import (
    build_news_item_id,
    canonicalize_news_url,
    deduplicate_strings,
)
from denbust.news_items.policy import (
    apply_suppression,
    derive_publication_status,
    derive_review_status,
    infer_privacy_risk,
    is_publicly_releasable,
)
from denbust.news_items.publication import (
    HuggingFacePublisher,
    KagglePublisher,
    publish_release_bundle,
)
from denbust.news_items.release import (
    NewsItemsReleaseBuilder,
    _schema_markdown,
    _serialized_row,
    parse_operational_records,
    select_releasable_records,
)
from denbust.ops.factory import create_operational_store, default_local_json_root
from denbust.ops.storage import LocalJsonOperationalStore, NullOperationalStore
from denbust.ops.supabase import SupabaseOperationalStore
from denbust.pipeline import release_publication_dir, run_news_items_release_job
from denbust.publish.release import ReleaseFormat, ReleaseManifest
from denbust.store.run_snapshots import RunSnapshot


def build_unified_item(url: str = "https://www.mako.co.il/item?a=1&utm_source=foo") -> UnifiedItem:
    return UnifiedItem(
        headline="המשטרה פשטה על בית בושת בתל אביב",
        summary="המשטרה ביצעה פשיטה על דירה ששימשה כבית בושת בעיר.",
        sources=[SourceReference(source_name="mako", url=url)],
        date=datetime(2026, 3, 18, 9, 0, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
        canonical_url=url,
        primary_source_name="mako",
    )


def build_record(
    *,
    publication_status: PublicationStatus = PublicationStatus.APPROVED,
    review_status: ReviewStatus = ReviewStatus.NONE,
    takedown_status: TakedownStatus = TakedownStatus.NONE,
) -> NewsItemOperationalRecord:
    return NewsItemOperationalRecord.from_unified_item(
        build_unified_item(),
        retrieval_datetime=datetime(2026, 3, 19, 7, 0, tzinfo=UTC),
        enrichment=NewsItemEnrichment(
            summary_one_sentence="המשטרה סגרה דירה ששימשה כבית בושת בתל אביב.",
            topic_tags=["brothel", "closure"],
        ),
        publication_status=publication_status,
        review_status=review_status,
        takedown_status=takedown_status,
    )


def test_canonicalize_news_url_removes_tracking_and_normalizes_host() -> None:
    canonical = canonicalize_news_url(
        "http://www.mako.co.il/item/123/?utm_source=newsletter&Partner=searchResults&x=1"
    )
    assert canonical == "https://mako.co.il/item/123?x=1"


def test_build_news_item_id_is_stable() -> None:
    canonical = "https://news.walla.co.il/item/3823239"
    assert build_news_item_id(canonical) == build_news_item_id(canonical)


def test_sanitize_summary_one_sentence_trims_to_first_sentence() -> None:
    summary = sanitize_summary_one_sentence("משפט ראשון. משפט שני.", "fallback")
    assert summary == "משפט ראשון."


def test_privacy_and_publication_policy_for_minor_case() -> None:
    risk, reason = infer_privacy_risk("החשוד שידל קטינה לזנות.")
    assert risk is PrivacyRisk.MINOR_INVOLVED
    assert reason is not None
    assert derive_review_status(risk) is ReviewStatus.NEEDS_PRIVACY_REVIEW
    assert derive_publication_status(risk) is PublicationStatus.INTERNAL_ONLY


def test_apply_suppression_blocks_public_release() -> None:
    record = build_record()
    suppressed = apply_suppression(
        record,
        [
            SuppressionRule(
                canonical_url=record.canonical_url,
                suppression_reason="takedown request",
            )
        ],
    )
    assert suppressed.takedown_status is TakedownStatus.SUPPRESSED
    assert suppressed.publication_status is PublicationStatus.SUPPRESSED
    assert is_publicly_releasable(suppressed) is False


def test_select_releasable_records_filters_internal_and_suppressed_rows() -> None:
    approved = build_record()
    internal = build_record(publication_status=PublicationStatus.INTERNAL_ONLY)
    suppressed = build_record(takedown_status=TakedownStatus.SUPPRESSED)

    public_rows = select_releasable_records(
        [
            approved.model_dump(mode="json"),
            internal.model_dump(mode="json"),
            suppressed.model_dump(mode="json"),
        ],
        release_version="2026-03-22",
    )

    assert [row.id for row in public_rows] == [approved.id]
    assert public_rows[0].release_version == "2026-03-22"


def test_news_items_release_builder_writes_release_bundle(tmp_path: Path) -> None:
    config = Config(job_name="release")
    builder = NewsItemsReleaseBuilder(config=config)
    record = build_record()

    manifest = builder.build_release_bundle(
        publication_dir=tmp_path,
        rows=[record.model_dump(mode="json")],
    )

    release_dir = tmp_path / manifest.release_version
    assert release_dir.exists()
    assert (release_dir / "MANIFEST.json").exists()
    assert (release_dir / "news_items.parquet").exists()
    assert (release_dir / "README.md").exists()
    assert any(artifact.format is ReleaseFormat.PARQUET for artifact in manifest.primary_files)
    manifest_sha = hashlib.sha256((release_dir / "MANIFEST.json").read_bytes()).hexdigest()
    checksums = (release_dir / "checksums.txt").read_text(encoding="utf-8")
    assert f"{manifest_sha}  MANIFEST.json" in checksums


def test_serialized_row_stringifies_non_string_scalars() -> None:
    row = build_record().model_copy(
        update={
            "source_count": 3,
            "index_relevant": True,
            "taxonomy_version": None,
        }
    )

    payload = _serialized_row(row)

    assert payload["source_count"] == "3"
    assert payload["index_relevant"] == "True"
    assert payload["taxonomy_version"] == ""


def test_execute_latest_backup_returns_empty_manifest_when_no_targets(tmp_path: Path) -> None:
    config = Config(
        job_name="backup",
        store={"publication_dir": tmp_path},
    )
    builder = NewsItemsReleaseBuilder(config=Config(job_name="release"))
    record = build_record()
    manifest = builder.build_release_bundle(
        publication_dir=tmp_path,
        rows=[record.model_dump(mode="json")],
    )

    backup_manifest = execute_latest_backup(config, publication_root=tmp_path)

    assert backup_manifest.release_version == manifest.release_version
    assert backup_manifest.targets == []


def test_local_json_operational_store_upserts_on_canonical_url(tmp_path: Path) -> None:
    store = LocalJsonOperationalStore(tmp_path / "ops")
    first = build_record().model_dump(mode="json")
    second = {**first, "summary_one_sentence": "עודכן", "updated_at": "2026-03-20T00:00:00Z"}

    store.upsert_records("news_items", [first])
    store.upsert_records("news_items", [second])

    rows = store.fetch_records("news_items")
    assert len(rows) == 1
    assert rows[0]["summary_one_sentence"] == "עודכן"
    assert store.close() is None


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload
        self.request_args: dict[str, object] = {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


class _FakeClient:
    def __init__(self, payloads: list[object] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._payloads = payloads or []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if self._payloads:
            return _FakeResponse(self._payloads.pop(0))
        if url.endswith("/suppression_rules"):
            return _FakeResponse(
                [{"dataset_name": "news_items", "suppression_reason": "x", "active": True}]
            )
        return _FakeResponse([])

    def close(self) -> None:
        self.closed = True


class _FakeTextBlock:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeDriveExecutable:
    def __init__(self, payload: dict[str, object] | None = None) -> None:
        self._payload = payload or {}

    def execute(self) -> dict[str, object]:
        return self._payload


class _FakeDriveFiles:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, object]] = []
        self.update_calls: list[dict[str, object]] = []
        self.create_calls: list[dict[str, object]] = []

    def list(self, **kwargs: object) -> _FakeDriveExecutable:
        self.list_calls.append(dict(kwargs))
        payload = (
            {"files": [{"id": "existing"}]} if "MANIFEST" in str(kwargs.get("q")) else {"files": []}
        )
        return _FakeDriveExecutable(payload)

    def update(self, **kwargs: object) -> _FakeDriveExecutable:
        self.update_calls.append(dict(kwargs))
        return _FakeDriveExecutable({"id": "updated"})

    def create(self, **kwargs: object) -> _FakeDriveExecutable:
        self.create_calls.append(dict(kwargs))
        return _FakeDriveExecutable({"id": "created"})


class _FakeDriveService:
    def __init__(self) -> None:
        self._files = _FakeDriveFiles()

    def files(self) -> _FakeDriveFiles:
        return self._files


class _FakeMediaFileUpload:
    def __init__(self, filename: str, resumable: bool = False) -> None:
        self.filename = filename
        self.resumable = resumable


class _FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str]] = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((filename, bucket, key))


class _FakeAnthropicMessages:
    def __init__(self, response: object | Exception) -> None:
        self._response = response

    def create(self, **kwargs: object) -> object:
        del kwargs
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def test_supabase_operational_store_uses_canonical_upsert_identity() -> None:
    client = _FakeClient()
    config = Config(operational={"provider": "supabase"})
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=config.operational,
        client=client,
    )

    store.upsert_records("news_items", [{"id": "1", "canonical_url": "https://example.com/a"}])

    assert client.calls[0]["method"] == "POST"
    assert client.calls[0]["params"] == {"on_conflict": "canonical_url"}


def test_supabase_operational_store_fetches_dataset_scoped_suppression_rules() -> None:
    client = _FakeClient()
    config = Config(operational={"provider": "supabase"})
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co",
        service_role_key="secret",
        config=config.operational,
        client=client,
    )

    rows = store.fetch_suppression_rules("news_items")

    assert len(rows) == 1
    assert client.calls[0]["params"] == {
        "select": "*",
        "dataset_name": "eq.news_items",
        "active": "eq.true",
    }


def test_sanitize_summary_one_sentence_uses_fallback_and_empty_default() -> None:
    assert sanitize_summary_one_sentence("   ", "שורת גיבוי") == "שורת גיבוי."
    assert sanitize_summary_one_sentence("", "") == "אין תקציר זמין."
    assert sanitize_summary_one_sentence("-", "גיבוי") == "-."


def test_fallback_enrichment_deduplicates_tags() -> None:
    item = build_unified_item()
    enrichment = fallback_enrichment(item)

    assert enrichment.summary_one_sentence.endswith(".")
    assert enrichment.topic_tags == ["brothel", "closure"]


def test_fallback_enrichment_includes_taxonomy_tags() -> None:
    item = build_unified_item().model_copy(
        update={
            "taxonomy_category_id": "human_trafficking",
            "taxonomy_subcategory_id": "trafficking_cross_border_prostitution",
        }
    )

    enrichment = fallback_enrichment(item)

    assert enrichment.topic_tags == [
        "human-trafficking",
        "trafficking-cross-border-prostitution",
        "brothel",
        "closure",
    ]


def test_deduplicate_strings_skips_blank_values() -> None:
    assert deduplicate_strings(["", "  ", "תל אביב", "תל אביב"]) == ["תל אביב"]


def test_news_item_enricher_parse_response_fallbacks() -> None:
    enricher = NewsItemEnricher.__new__(NewsItemEnricher)
    item = build_unified_item()

    fallback = enricher._parse_response("not-json", item)
    invalid_risk = enricher._parse_response(
        json.dumps(
            {
                "summary_one_sentence": "שתי מילים",
                "organizations_mentioned": ["משטרה", "משטרה"],
                "topic_tags": ["brothel", "brothel"],
                "privacy_risk_level": "not-a-risk",
            },
            ensure_ascii=False,
        ),
        item,
    )

    assert fallback.summary_one_sentence.endswith(".")
    assert invalid_risk.privacy_risk_level is PrivacyRisk.LOW
    assert invalid_risk.organizations_mentioned == ["משטרה"]
    assert invalid_risk.topic_tags == ["brothel"]


def test_news_item_enricher_parse_response_coerces_minor_schema_drift() -> None:
    enricher = NewsItemEnricher.__new__(NewsItemEnricher)
    item = build_unified_item()

    enriched = enricher._parse_response(
        json.dumps(
            {
                "summary_one_sentence": "סיכום עובדתי קצר",
                "geography_region": {"bad": "value"},
                "geography_city": 7,
                "organizations_mentioned": "משטרת ישראל",
                "topic_tags": {"bad": "value"},
                "privacy_risk_level": "medium",
            },
            ensure_ascii=False,
        ),
        item,
    )

    assert enriched.geography_region is None
    assert enriched.geography_city == "7"
    assert enriched.organizations_mentioned == ["משטרת ישראל"]
    assert enriched.topic_tags == []
    assert enriched.privacy_risk_level is PrivacyRisk.MEDIUM


@pytest.mark.asyncio
async def test_news_item_enricher_handles_text_blocks_and_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = build_unified_item()

    text_enricher = NewsItemEnricher.__new__(NewsItemEnricher)
    text_enricher._model = "test-model"
    text_enricher._client = SimpleNamespace(
        messages=_FakeAnthropicMessages(
            SimpleNamespace(
                content=[
                    _FakeTextBlock(
                        json.dumps(
                            {
                                "summary_one_sentence": "המשטרה סגרה דירה ששימשה כבית בושת",
                                "topic_tags": ["brothel"],
                                "privacy_risk_level": "medium",
                            },
                            ensure_ascii=False,
                        )
                    )
                ]
            )
        )
    )
    monkeypatch.setattr("denbust.news_items.enrich.TextBlock", _FakeTextBlock)

    enriched = await text_enricher.enrich(item)

    assert enriched.summary_one_sentence.endswith(".")
    assert enriched.privacy_risk_level is PrivacyRisk.MEDIUM

    failing_enricher = NewsItemEnricher.__new__(NewsItemEnricher)
    failing_enricher._model = "test-model"
    failing_enricher._client = SimpleNamespace(
        messages=_FakeAnthropicMessages(RuntimeError("boom"))
    )

    fallback = await failing_enricher.enrich(item)

    assert fallback.summary_one_sentence.endswith(".")


def test_parse_suppression_rules_skips_invalid_payload(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)

    rules = parse_suppression_rules([{"suppression_reason": "ok"}, {"bad": object()}])  # type: ignore[dict-item]

    assert len(rules) == 1
    assert "Skipping invalid suppression rule payload" in caplog.text


@pytest.mark.asyncio
async def test_build_operational_records_escalates_rule_based_privacy_risk() -> None:
    item = build_unified_item("https://example.com/minor")
    item.headline = "החשוד שידל קטינה לזנות"
    store = NullOperationalStore()
    config = Config()

    records = await build_operational_records([item], config=config, operational_store=store)

    assert records[0].privacy_risk_level is PrivacyRisk.MINOR_INVOLVED
    assert records[0].privacy_reason == "minor marker detected"
    assert records[0].publication_status is PublicationStatus.INTERNAL_ONLY


def test_infer_privacy_risk_sensitive_and_high_markers() -> None:
    assert infer_privacy_risk("הקורבן תיארה תקיפה מינית")[0] is PrivacyRisk.SENSITIVE_SEXUAL_OFFENCE
    assert infer_privacy_risk("המעצר נגע לאישה זרה וחסרת ישע")[0] is PrivacyRisk.HIGH
    assert derive_publication_status(PrivacyRisk.MEDIUM) is PublicationStatus.DRAFT
    assert derive_publication_status(PrivacyRisk.HIGH) is PublicationStatus.DRAFT


def test_apply_suppression_skips_inactive_rule_and_matches_record_id() -> None:
    record = build_record()
    unchanged = apply_suppression(
        record,
        [
            SuppressionRule(record_id=record.id, suppression_reason="inactive", active=False),
        ],
    )
    suppressed = apply_suppression(
        record,
        [
            SuppressionRule(record_id=record.id, suppression_reason="active suppression"),
        ],
    )

    assert unchanged.takedown_status is TakedownStatus.NONE
    assert suppressed.takedown_status is TakedownStatus.SUPPRESSED
    assert suppressed.suppression_reason == "active suppression"


def test_parse_operational_records_skips_invalid_rows(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)

    rows = parse_operational_records([{"id": "ok", "canonical_url": "https://example.com"}])

    assert rows == []
    assert "Skipping invalid news_items row" in caplog.text


def test_release_builder_manifest_and_description(tmp_path: Path) -> None:
    builder = NewsItemsReleaseBuilder(config=Config(job_name="release"))

    manifest = builder.build_manifest("news_items", tmp_path)

    assert manifest.dataset_name == "news_items"
    assert manifest.primary_files[0].path.name == "news_items.parquet"
    assert builder.describe() == "Builds a metadata-only public release bundle for news_items."


def test_schema_markdown_handles_dict_field_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "denbust.news_items.release._schema_json",
        lambda: {
            "properties": {"field": {"$ref": {"kind": "custom"}}},
            "required": ["field"],
        },
    )

    markdown = _schema_markdown()

    assert '{"kind": "custom"}' in markdown


def test_release_builder_raises_clear_error_when_pyarrow_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    builder = NewsItemsReleaseBuilder(config=Config(job_name="release"))
    original_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("pyarrow"):
            raise ImportError("missing pyarrow")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="pyarrow is required"):
        builder._write_parquet(
            [build_record().to_public_record(release_version="2026-03-19")],
            tmp_path / "rows.parquet",
        )


def test_find_latest_release_dir_errors_when_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No release bundles found"):
        find_latest_release_dir(tmp_path)


def test_release_version_from_dir_handles_missing_or_invalid_manifest(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert _release_version_from_dir(empty_dir) is None

    invalid_dir = tmp_path / "invalid"
    invalid_dir.mkdir()
    (invalid_dir / "MANIFEST.json").write_text(json.dumps(["not-a-dict"]), encoding="utf-8")
    assert _release_version_from_dir(invalid_dir) is None


def test_google_drive_uploader_requires_service_account() -> None:
    uploader = GoogleDriveLatestBackupUploader(service_account_json=None)

    with pytest.raises(ValueError, match="DENBUST_DRIVE_SERVICE_ACCOUNT_JSON"):
        uploader.upload(release_dir=Path("."), folder_id="folder")


def test_google_drive_uploader_updates_existing_and_creates_new_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    (release_dir / "MANIFEST.json").write_text("{}", encoding="utf-8")
    (release_dir / "news_items.csv").write_text("id\n1\n", encoding="utf-8")
    (release_dir / "nested").mkdir()

    fake_service = _FakeDriveService()
    fake_credentials = SimpleNamespace()

    google_module = ModuleType("google")
    oauth2_module = ModuleType("google.oauth2")
    service_account_module = ModuleType("google.oauth2.service_account")
    service_account_module.Credentials = SimpleNamespace(
        from_service_account_file=lambda *_args, **_kwargs: fake_credentials
    )
    discovery_module = ModuleType("googleapiclient.discovery")
    discovery_module.build = lambda *_args, **_kwargs: fake_service
    http_module = ModuleType("googleapiclient.http")
    http_module.MediaFileUpload = _FakeMediaFileUpload

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.oauth2", oauth2_module)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", service_account_module)
    monkeypatch.setitem(sys.modules, "googleapiclient.discovery", discovery_module)
    monkeypatch.setitem(sys.modules, "googleapiclient.http", http_module)

    uploaded = GoogleDriveLatestBackupUploader(service_account_json="/tmp/sa.json").upload(
        release_dir=release_dir,
        folder_id="folder-id",
    )

    assert uploaded == ["MANIFEST.json", "news_items.csv"]
    assert len(fake_service.files().update_calls) == 1
    assert len(fake_service.files().create_calls) == 1
    assert (
        "\\'" in fake_service.files().list_calls[0]["q"]
        or "MANIFEST" in fake_service.files().list_calls[0]["q"]
    )


def test_object_storage_uploader_requires_credentials(tmp_path: Path) -> None:
    uploader = ObjectStorageLatestBackupUploader(
        endpoint_url="https://r2.example",
        access_key_id=None,
        secret_access_key=None,
    )

    with pytest.raises(ValueError, match="DENBUST_OBJECT_STORE_ACCESS_KEY_ID"):
        uploader.upload(release_dir=tmp_path, bucket="bucket", prefix="latest")


def test_object_storage_uploader_uploads_release_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    (release_dir / "MANIFEST.json").write_text("{}", encoding="utf-8")
    (release_dir / "rows.csv").write_text("id\n1\n", encoding="utf-8")
    (release_dir / "subdir").mkdir()
    fake_client = _FakeS3Client()

    boto3_module = ModuleType("boto3")
    boto3_module.client = lambda *_args, **_kwargs: fake_client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)

    uploaded = ObjectStorageLatestBackupUploader(
        endpoint_url="https://r2.example",
        access_key_id="access",
        secret_access_key="secret",
    ).upload(release_dir=release_dir, bucket="bucket", prefix="")

    assert uploaded == ["MANIFEST.json", "rows.csv"]
    assert fake_client.uploads[0][1:] == ("bucket", "MANIFEST.json")


def test_execute_latest_backup_with_all_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    builder = NewsItemsReleaseBuilder(config=Config(job_name="release"))
    record = build_record()
    manifest = builder.build_release_bundle(
        publication_dir=tmp_path, rows=[record.model_dump(mode="json")]
    )

    monkeypatch.setattr(
        "denbust.news_items.backup.GoogleDriveLatestBackupUploader.upload",
        lambda _self, *, release_dir, folder_id: [f"drive:{folder_id}:{release_dir.name}"],
    )
    monkeypatch.setattr(
        "denbust.news_items.backup.ObjectStorageLatestBackupUploader.upload",
        lambda _self, *, release_dir, bucket, prefix: [f"{bucket}/{prefix}/{release_dir.name}"],
    )

    config = Config(
        job_name="backup",
        store={"publication_dir": tmp_path},
        backup={
            "google_drive": {"enabled": True, "folder_id": "drive-folder"},
            "object_storage": {"enabled": True, "bucket": "bucket", "prefix": "latest/news"},
        },
    )

    backup_manifest = execute_latest_backup(config, publication_root=tmp_path)

    assert backup_manifest.release_version == manifest.release_version
    assert [target.name for target in backup_manifest.targets] == ["google_drive", "object_storage"]


def test_kaggle_publisher_requires_credentials() -> None:
    publisher = KagglePublisher(username=None, key=None)

    with pytest.raises(ValueError, match="KAGGLE_USERNAME"):
        publisher.publish(
            release_dir=Path("."),
            manifest=ReleaseManifest(dataset_name="news_items", release_version="2026-03-19"),
            dataset_slug="owner/news-items",
        )


def test_kaggle_publisher_authenticates_and_restores_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    calls: list[tuple[str, object]] = []

    class FakeKaggleApi:
        def authenticate(self) -> None:
            calls.append(("authenticate", None))

        def dataset_create_version(self, **kwargs: object) -> None:
            calls.append(("dataset_create_version", kwargs))

    kaggle_module = ModuleType("kaggle")
    kaggle_api_module = ModuleType("kaggle.api")
    kaggle_api_extended_module = ModuleType("kaggle.api.kaggle_api_extended")
    kaggle_api_extended_module.KaggleApi = FakeKaggleApi
    monkeypatch.setitem(sys.modules, "kaggle", kaggle_module)
    monkeypatch.setitem(sys.modules, "kaggle.api", kaggle_api_module)
    monkeypatch.setitem(sys.modules, "kaggle.api.kaggle_api_extended", kaggle_api_extended_module)
    monkeypatch.setenv("KAGGLE_USERNAME", "previous-user")
    monkeypatch.setenv("KAGGLE_KEY", "previous-key")

    slug = KagglePublisher(username="user", key="key").publish(
        release_dir=release_dir,
        manifest=ReleaseManifest(dataset_name="news_items", release_version="2026-03-19"),
        dataset_slug="owner/news-items",
    )

    assert slug == "owner/news-items"
    assert calls[0][0] == "authenticate"
    assert (
        json.loads((release_dir / "dataset-metadata.json").read_text(encoding="utf-8"))["id"]
        == "owner/news-items"
    )
    assert os.environ["KAGGLE_USERNAME"] == "previous-user"
    assert os.environ["KAGGLE_KEY"] == "previous-key"


def test_kaggle_publisher_removes_temp_env_when_no_previous_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_dir = tmp_path / "release"
    release_dir.mkdir()

    class FakeKaggleApi:
        def authenticate(self) -> None:
            return None

        def dataset_create_version(self, **kwargs: object) -> None:
            del kwargs

    kaggle_module = ModuleType("kaggle")
    kaggle_api_module = ModuleType("kaggle.api")
    kaggle_api_extended_module = ModuleType("kaggle.api.kaggle_api_extended")
    kaggle_api_extended_module.KaggleApi = FakeKaggleApi
    monkeypatch.setitem(sys.modules, "kaggle", kaggle_module)
    monkeypatch.setitem(sys.modules, "kaggle.api", kaggle_api_module)
    monkeypatch.setitem(sys.modules, "kaggle.api.kaggle_api_extended", kaggle_api_extended_module)
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)

    KagglePublisher(username="user", key="key").publish(
        release_dir=release_dir,
        manifest=ReleaseManifest(dataset_name="news_items", release_version="2026-03-19"),
        dataset_slug="owner/news-items",
    )

    assert "KAGGLE_USERNAME" not in os.environ
    assert "KAGGLE_KEY" not in os.environ


def test_hugging_face_publisher_requires_token() -> None:
    publisher = HuggingFacePublisher(token=None)

    with pytest.raises(ValueError, match="HF_TOKEN"):
        publisher.publish(
            release_dir=Path("."),
            manifest=ReleaseManifest(dataset_name="news_items", release_version="2026-03-19"),
            repo_id="org/news-items",
        )


def test_hugging_face_publisher_uploads_folder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeHfApi:
        def __init__(self, token: str | None = None) -> None:
            calls.append(("init", {"token": token}))

        def create_repo(self, **kwargs: object) -> None:
            calls.append(("create_repo", dict(kwargs)))

        def upload_folder(self, **kwargs: object) -> None:
            calls.append(("upload_folder", dict(kwargs)))

    hf_module = ModuleType("huggingface_hub")
    hf_module.HfApi = FakeHfApi
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_module)

    repo_id = HuggingFacePublisher(token="hf-token").publish(
        release_dir=release_dir,
        manifest=ReleaseManifest(dataset_name="news_items", release_version="2026-03-19"),
        repo_id="org/news-items",
    )

    assert repo_id == "org/news-items"
    assert calls[1][0] == "create_repo"
    assert calls[2][0] == "upload_folder"


def test_publish_release_bundle_dispatches_all_configured_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_kaggle_publish(
        _self: object, *, release_dir: Path, manifest: ReleaseManifest, dataset_slug: str
    ) -> str:
        del release_dir, manifest
        return dataset_slug

    def fake_hf_publish(
        _self: object, *, release_dir: Path, manifest: ReleaseManifest, repo_id: str
    ) -> str:
        del release_dir, manifest
        return repo_id

    monkeypatch.setattr(
        "denbust.news_items.publication.KagglePublisher.publish",
        fake_kaggle_publish,
    )
    monkeypatch.setattr(
        "denbust.news_items.publication.HuggingFacePublisher.publish",
        fake_hf_publish,
    )
    config = Config(
        job_name="release",
        release={
            "kaggle_dataset": "owner/news-items",
            "huggingface_repo_id": "org/news-items",
        },
    )
    manifest = ReleaseManifest(dataset_name="news_items", release_version="2026-03-19")

    targets = publish_release_bundle(config=config, release_dir=tmp_path, manifest=manifest)

    assert targets == ["kaggle:owner/news-items", "huggingface:org/news-items"]


def test_operational_store_factory_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    none_config = Config(operational={"provider": "none"})
    assert isinstance(create_operational_store(none_config), NullOperationalStore)

    local_config = Config(
        dataset_name="news_items",
        operational={"provider": "local_json", "root_dir": tmp_path / "custom-ops"},
    )
    local_store = create_operational_store(local_config)
    assert isinstance(local_store, LocalJsonOperationalStore)
    assert local_store.root_dir == tmp_path / "custom-ops"

    derived_config = Config(operational={"provider": "local_json"})
    assert default_local_json_root(derived_config) == Path("data/news_items/operational")

    supabase_config = Config(
        operational={"provider": "supabase"},
        dataset_name="news_items",
    )
    with pytest.raises(ValueError, match="DENBUST_SUPABASE_URL"):
        create_operational_store(supabase_config)

    monkeypatch.setenv("DENBUST_SUPABASE_URL", "https://supabase.example")
    with pytest.raises(ValueError, match="DENBUST_SUPABASE_SERVICE_ROLE_KEY"):
        create_operational_store(supabase_config)

    monkeypatch.setenv("DENBUST_SUPABASE_SERVICE_ROLE_KEY", "service-role")
    supabase_store = create_operational_store(supabase_config)
    assert isinstance(supabase_store, SupabaseOperationalStore)

    object.__setattr__(supabase_config.operational, "provider", "bogus")
    with pytest.raises(ValueError, match="Unsupported operational store provider"):
        create_operational_store(supabase_config)


def test_local_json_operational_store_misc_branches(tmp_path: Path) -> None:
    store = LocalJsonOperationalStore(tmp_path / "ops")

    assert (
        store.suppression_rules_path("news_items")
        == tmp_path / "ops" / "news_items_suppression_rules.json"
    )
    store.upsert_records("news_items", [{"summary": "missing identity"}])
    assert store.fetch_records("news_items") == []

    first = {
        "id": "1",
        "canonical_url": "https://example.com/article",
        "publication_datetime": "2026-03-18T00:00:00Z",
        "created_at": "2026-03-18T00:00:00Z",
    }
    second = {
        "id": "1",
        "canonical_url": "https://example.com/article",
        "publication_datetime": "2026-03-19T00:00:00Z",
    }
    store.upsert_records("news_items", [first])
    store.upsert_records("news_items", [second])
    assert store.fetch_records("news_items")[0]["created_at"] == "2026-03-18T00:00:00Z"

    store.mark_publication_state("news_items", [], "published")
    store.suppression_rules_path("news_items").write_text("{}", encoding="utf-8")
    assert store.fetch_suppression_rules("news_items") == []


def test_supabase_store_write_fetch_mark_and_request_headers() -> None:
    snapshot = RunSnapshot(config_name="cfg", dataset_name="news_items", job_name="release")
    client = _FakeClient(payloads=[[], []])
    config = Config(operational={"provider": "supabase"})
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co/",
        service_role_key="secret",
        config=config.operational,
        client=client,
    )

    store.write_run_metadata(snapshot)
    store.fetch_records("news_items", limit=5)
    store.mark_publication_state("news_items", [], "published")
    store.mark_publication_state("news_items", ["one", "two"], "published")

    write_call = client.calls[0]
    assert write_call["url"].endswith("/release_runs")
    assert "items" not in write_call["json"]
    assert write_call["headers"]["Accept-Profile"] == config.operational.supabase_schema

    fetch_call = client.calls[1]
    assert fetch_call["params"]["limit"] == "5"

    patch_call = client.calls[2]
    assert patch_call["method"] == "PATCH"
    assert patch_call["params"] == {"id": "in.(one,two)"}


def test_supabase_store_noop_and_non_list_fallbacks() -> None:
    client = _FakeClient(payloads=["not-a-list", {"unexpected": "dict"}])
    config = Config(operational={"provider": "supabase"})
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co/",
        service_role_key="secret",
        config=config.operational,
        client=client,
    )

    store.upsert_records("news_items", [])
    rows = store.fetch_records("news_items")
    suppression_rows = store.fetch_suppression_rules("news_items")

    assert rows == []
    assert suppression_rows == []
    assert store._table_for_job("ingest") == config.operational.ingestion_runs_table
    assert store._table_for_job("release") == config.operational.release_runs_table
    assert store._table_for_job("backup") == config.operational.backup_runs_table


def test_supabase_store_close_closes_underlying_client() -> None:
    client = _FakeClient()
    config = Config(operational={"provider": "supabase"})
    store = SupabaseOperationalStore(
        base_url="https://example.supabase.co/",
        service_role_key="secret",
        config=config.operational,
        client=client,
    )

    store.close()

    assert client.closed is True


@pytest.mark.asyncio
async def test_release_publication_dir_and_release_job_mark_published(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    release_config = Config(job_name="release", store={"publication_dir": tmp_path / "release-pub"})
    assert release_publication_dir(release_config) == release_config.state_paths.publication_dir

    class FakeStore:
        def __init__(self) -> None:
            self.mark_calls: list[tuple[str, list[str], str]] = []

        def fetch_records(
            self, dataset_name: str, *, limit: int | None = None
        ) -> list[dict[str, Any]]:
            del dataset_name, limit
            return [{"id": "row-1"}]

        def mark_publication_state(
            self, dataset_name: str, record_ids: list[str], publication_status: str
        ) -> None:
            self.mark_calls.append((dataset_name, record_ids, publication_status))

        def write_run_metadata(self, snapshot: RunSnapshot) -> None:
            del snapshot

        def upsert_records(self, dataset_name: str, records: list[dict[str, Any]]) -> None:
            del dataset_name, records

        def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, Any]]:
            del dataset_name
            return []

    store = FakeStore()
    manifest = ReleaseManifest(dataset_name="news_items", release_version="2026-03-19", row_count=1)

    class FakeBuilder:
        def __init__(self, *, config: Config) -> None:
            del config

        def build_release_bundle(
            self, *, publication_dir: Path, rows: list[dict[str, Any]]
        ) -> ReleaseManifest:
            del publication_dir, rows
            return manifest

    fake_row = SimpleNamespace(id="row-1")
    monkeypatch.setattr("denbust.pipeline.NewsItemsReleaseBuilder", FakeBuilder)
    monkeypatch.setattr(
        "denbust.pipeline.publish_release_bundle", lambda **_kwargs: ["kaggle:owner/news"]
    )

    def fake_select_releasable_records(
        rows: list[dict[str, Any]], *, release_version: str
    ) -> list[SimpleNamespace]:
        del rows, release_version
        return [fake_row]

    monkeypatch.setattr(
        "denbust.pipeline.select_releasable_records", fake_select_releasable_records
    )

    result = await run_news_items_release_job(release_config, operational_store=store)

    assert result.result_summary == "release built for 1 public row(s)"
    assert store.mark_calls == [("news_items", ["row-1"], "published")]
