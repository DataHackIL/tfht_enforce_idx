"""Unit tests for news_items monthly report generation."""

from __future__ import annotations

from datetime import UTC, date, datetime
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
    hq_activity_from_inputs,
    month_bounds_utc,
    parse_month_key,
    persist_monthly_report_artifacts,
    previous_month,
    render_monthly_report_markdown,
    resolve_report_month,
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


def test_previous_month_and_resolve_report_month_helpers() -> None:
    assert previous_month(date(2026, 3, 17)) == date(2026, 2, 1)
    assert previous_month(date(2026, 1, 5)) == date(2025, 12, 1)
    assert resolve_report_month(" 2026-03 ") == date(2026, 3, 1)
    assert resolve_report_month(None) == previous_month()
    assert resolve_report_month("   ") == previous_month()


def test_month_bounds_utc_handles_december_rollover() -> None:
    start, end = month_bounds_utc(date(2026, 12, 15))

    assert start == datetime(2026, 12, 1, tzinfo=UTC)
    assert end == datetime(2027, 1, 1, tzinfo=UTC)


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


def test_generate_monthly_report_normalizes_naive_datetimes() -> None:
    report = generate_monthly_report(
        [_record("row-1", datetime(2026, 3, 10, 8))],
        month=parse_month_key("2026-03"),
    )

    assert report.selected_cases[0].publication_datetime == datetime(2026, 3, 10, 8, tzinfo=UTC)


def test_render_monthly_report_markdown_handles_empty_stats_and_cases() -> None:
    empty_report = generate_monthly_report([], month=parse_month_key("2026-03"))

    rendered = render_monthly_report_markdown(empty_report)

    assert "- לא נמצאו אירועים ציבוריים רלוונטיים לחודש זה." in rendered
    assert "- לא נבחרו מקרים להצגה." in rendered
    assert MONTHLY_REPORT_PLACEHOLDER in rendered


def test_hq_activity_from_inputs_prefers_file_and_normalizes_blank_values(tmp_path: Path) -> None:
    file_path = tmp_path / "hq.txt"
    file_path.write_text("  פעילות מהקובץ  \n", encoding="utf-8")

    assert (
        hq_activity_from_inputs(hq_activity="  inline text  ", hq_activity_file=file_path)
        == "פעילות מהקובץ"
    )
    assert hq_activity_from_inputs(hq_activity="   ", hq_activity_file=None) is None
    assert hq_activity_from_inputs(hq_activity=None, hq_activity_file=None) is None

    blank_file = tmp_path / "blank.txt"
    blank_file.write_text("   \n", encoding="utf-8")
    assert hq_activity_from_inputs(hq_activity="ignored", hq_activity_file=blank_file) is None
