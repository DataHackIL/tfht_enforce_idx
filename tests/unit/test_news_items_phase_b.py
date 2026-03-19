"""Unit tests for Phase B news_items functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from denbust.config import Config
from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.models.policies import PrivacyRisk, PublicationStatus, ReviewStatus, TakedownStatus
from denbust.news_items.backup import execute_latest_backup
from denbust.news_items.enrich import sanitize_summary_one_sentence
from denbust.news_items.models import NewsItemEnrichment, NewsItemOperationalRecord, SuppressionRule
from denbust.news_items.normalize import build_news_item_id, canonicalize_news_url
from denbust.news_items.policy import (
    apply_suppression,
    derive_publication_status,
    derive_review_status,
    infer_privacy_risk,
    is_publicly_releasable,
)
from denbust.news_items.release import NewsItemsReleaseBuilder, select_releasable_records
from denbust.ops.storage import LocalJsonOperationalStore
from denbust.ops.supabase import SupabaseOperationalStore
from denbust.publish.release import ReleaseFormat


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


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request(self, method: str, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if url.endswith("/suppression_rules"):
            return _FakeResponse(
                [{"dataset_name": "news_items", "suppression_reason": "x", "active": True}]
            )
        return _FakeResponse([])


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
