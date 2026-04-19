"""Unit tests for news_items monthly report generation."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from denbust.data_models import Category, SubCategory
from denbust.models.policies import (
    PrivacyRisk,
    PublicationStatus,
    ReviewStatus,
    RightsClass,
    TakedownStatus,
)
from denbust.news_items.models import NewsItemOperationalRecord
from denbust.news_items.monthly_report import (
    MONTHLY_REPORT_PLACEHOLDER,
    generate_monthly_report,
    parse_month_key,
    persist_monthly_report_artifacts,
    write_report_json_copy,
)


def _record(
    record_id: str,
    published_at: datetime,
    *,
    index_relevant: bool = True,
    publication_status: PublicationStatus = PublicationStatus.APPROVED,
    review_status: ReviewStatus = ReviewStatus.NONE,
    taxonomy_category_id: str = "brothels",
    taxonomy_subcategory_id: str = "administrative_closure",
) -> NewsItemOperationalRecord:
    return NewsItemOperationalRecord(
        id=record_id,
        source_name="mako",
        source_domain="www.mako.co.il",
        url=f"https://example.com/{record_id}",
        canonical_url=f"https://example.com/{record_id}",
        publication_datetime=published_at,
        retrieval_datetime=published_at,
        title=f"Headline {record_id}",
        taxonomy_version="1",
        taxonomy_category_id=taxonomy_category_id,
        taxonomy_subcategory_id=taxonomy_subcategory_id,
        index_relevant=index_relevant,
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
        summary_one_sentence=f"Summary {record_id}",
        rights_class=RightsClass.METADATA_ONLY,
        privacy_risk_level=PrivacyRisk.LOW,
        review_status=review_status,
        publication_status=publication_status,
        takedown_status=TakedownStatus.NONE,
    )


def test_generate_monthly_report_filters_month_and_public_eligibility() -> None:
    report = generate_monthly_report(
        [
            _record("include-a", datetime(2026, 3, 20, 10, tzinfo=UTC)),
            _record("include-b", datetime(2026, 3, 1, 0, tzinfo=UTC)),
            _record("wrong-month", datetime(2026, 4, 1, 0, tzinfo=UTC)),
            _record("not-index", datetime(2026, 3, 5, 0, tzinfo=UTC), index_relevant=False),
            _record(
                "draft-only",
                datetime(2026, 3, 6, 0, tzinfo=UTC),
                publication_status=PublicationStatus.DRAFT,
            ),
        ],
        month=parse_month_key("2026-03"),
    )

    assert report.month_key == "2026-03"
    assert [case.headline for case in report.selected_cases] == [
        "Headline include-a",
        "Headline include-b",
    ]
    assert report.stats == {"administrative_closure": 2}


def test_generate_monthly_report_groups_and_limits_cases() -> None:
    records = [
        _record(
            f"row-{index}",
            datetime(2026, 3, min(index, 28), 12, tzinfo=UTC),
            taxonomy_subcategory_id="administrative_closure" if index % 2 else "client_fine",
        )
        for index in range(1, 9)
    ]

    report = generate_monthly_report(records, month=parse_month_key("2026-03"))

    assert len(report.selected_cases) == 6
    assert report.selected_cases[0].headline == "Headline row-8"
    assert report.selected_cases[-1].headline == "Headline row-3"
    assert report.stats == {
        "client_fine": 4,
        "administrative_closure": 4,
    }


def test_generate_monthly_report_renders_hq_placeholder_and_markdown() -> None:
    report = generate_monthly_report(
        [_record("row-1", datetime(2026, 3, 10, 8, tzinfo=UTC))],
        month=parse_month_key("2026-03"),
    )

    assert report.hq_activity is None
    assert MONTHLY_REPORT_PLACEHOLDER in report.rendered_markdown
    assert 'דו"ח חודשי מרץ 2026' in report.rendered_markdown
    assert "[מקור](https://example.com/row-1)" in report.rendered_markdown


def test_generate_monthly_report_renders_explicit_hq_activity() -> None:
    report = generate_monthly_report(
        [_record("row-1", datetime(2026, 3, 10, 8, tzinfo=UTC))],
        month=parse_month_key("2026-03"),
        hq_activity="המטה ליווה שני דיונים והגיש מכתב תמיכה אחד.",
    )

    assert report.hq_activity == "המטה ליווה שני דיונים והגיש מכתב תמיכה אחד."
    assert "המטה ליווה שני דיונים והגיש מכתב תמיכה אחד." in report.rendered_markdown


def test_parse_month_key_rejects_invalid_format() -> None:
    try:
        parse_month_key("2026/03")
    except ValueError as exc:
        assert "Expected YYYY-MM" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid month format")


def test_persist_monthly_report_artifacts_writes_utf8_json(tmp_path: Path) -> None:
    report = generate_monthly_report(
        [_record("row-1", datetime(2026, 3, 10, 8, tzinfo=UTC))],
        month=parse_month_key("2026-03"),
        hq_activity="המטה שלח מכתב תמיכה.",
    )

    artifacts = persist_monthly_report_artifacts(tmp_path, report)
    payload = artifacts.json_path.read_text(encoding="utf-8")

    assert "\\u05" not in payload
    assert "המטה שלח מכתב תמיכה." in payload
    assert payload.endswith("\n")


def test_write_report_json_copy_writes_utf8_json(tmp_path: Path) -> None:
    report = generate_monthly_report(
        [_record("row-1", datetime(2026, 3, 10, 8, tzinfo=UTC))],
        month=parse_month_key("2026-03"),
        hq_activity="המטה קיים פגישה.",
    )
    output_path = tmp_path / "report.json"

    write_report_json_copy(output_path, report)

    payload = output_path.read_text(encoding="utf-8")
    assert "\\u05" not in payload
    assert "המטה קיים פגישה." in payload
    assert payload.endswith("\n")
