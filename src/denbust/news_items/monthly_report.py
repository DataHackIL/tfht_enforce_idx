"""Monthly report generation for the news_items dataset."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import BaseModel

from denbust.news_items.models import NewsItemOperationalRecord
from denbust.news_items.policy import is_publicly_releasable
from denbust.taxonomy import TaxonomyDefinition, default_taxonomy

HEBREW_MONTH_NAMES = {
    1: "ינואר",
    2: "פברואר",
    3: "מרץ",
    4: "אפריל",
    5: "מאי",
    6: "יוני",
    7: "יולי",
    8: "אוגוסט",
    9: "ספטמבר",
    10: "אוקטובר",
    11: "נובמבר",
    12: "דצמבר",
}

MONTHLY_REPORT_MARKDOWN_ENV = "DENBUST_MONTHLY_REPORT_OUTPUT"
MONTHLY_REPORT_JSON_ENV = "DENBUST_MONTHLY_REPORT_JSON_OUTPUT"
MONTHLY_REPORT_MONTH_ENV = "DENBUST_MONTHLY_REPORT_MONTH"
MONTHLY_REPORT_HQ_ACTIVITY_ENV = "DENBUST_MONTHLY_REPORT_HQ_ACTIVITY"
MONTHLY_REPORT_HQ_ACTIVITY_FILE_ENV = "DENBUST_MONTHLY_REPORT_HQ_ACTIVITY_FILE"
DEFAULT_MAX_CASES = 6
MONTHLY_REPORT_PLACEHOLDER = "טרם הוזן עדכון פעילות מטה לחודש זה."


class CaseSummary(BaseModel):
    """Single case blurb rendered into the monthly report."""

    headline: str
    narrative: str
    source_url: str
    publication_datetime: datetime
    taxonomy_category_id: str | None = None
    taxonomy_subcategory_id: str | None = None
    category: str
    sub_category: str | None = None


class MonthlyReport(BaseModel):
    """Structured monthly report payload."""

    month: date
    month_key: str
    month_label_he: str
    stats: dict[str, int]
    stats_labels_he: dict[str, str]
    selected_cases: list[CaseSummary]
    hq_activity: str | None = None
    hq_activity_placeholder: str = MONTHLY_REPORT_PLACEHOLDER
    rendered_markdown: str


@dataclass(frozen=True)
class MonthlyReportArtifacts:
    """Paths written for a generated monthly report bundle."""

    output_dir: Path
    markdown_path: Path
    json_path: Path
    readme_path: Path


def previous_month(today: date | None = None) -> date:
    """Return the first day of the previous calendar month."""
    effective = today or datetime.now(UTC).date()
    first_of_month = effective.replace(day=1)
    previous_day = first_of_month.fromordinal(first_of_month.toordinal() - 1)
    return previous_day.replace(day=1)


def parse_month_key(value: str) -> date:
    """Parse a YYYY-MM month key into the first day of that month."""
    try:
        parsed = datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise ValueError(f"Invalid month '{value}'. Expected YYYY-MM.") from exc
    return parsed.replace(day=1)


def resolve_report_month(month_value: str | None) -> date:
    """Resolve an explicit month key or default to the previous UTC month."""
    if month_value is None or not month_value.strip():
        return previous_month()
    return parse_month_key(month_value.strip())


def month_key(month: date) -> str:
    """Render the canonical YYYY-MM key for a report month."""
    return month.strftime("%Y-%m")


def month_label_he(month: date) -> str:
    """Render a Hebrew month/year label."""
    return f"{HEBREW_MONTH_NAMES[month.month]} {month.year}"


def month_bounds_utc(month: date) -> tuple[datetime, datetime]:
    """Return inclusive-exclusive UTC bounds for a report month."""
    start = datetime(month.year, month.month, 1, tzinfo=UTC)
    if month.month == 12:
        next_month = datetime(month.year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(month.year, month.month + 1, 1, tzinfo=UTC)
    return start, next_month


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def select_monthly_report_records(
    records: Sequence[NewsItemOperationalRecord],
    *,
    month: date,
) -> list[NewsItemOperationalRecord]:
    """Filter to eligible monthly-report records in reverse chronological order."""
    start, end = month_bounds_utc(month)
    eligible = [
        record
        for record in records
        if record.index_relevant
        and is_publicly_releasable(record)
        and start <= _normalize_datetime(record.publication_datetime) < end
    ]
    eligible.sort(key=lambda record: _normalize_datetime(record.publication_datetime), reverse=True)
    return eligible


def _stats_for_records(
    records: Sequence[NewsItemOperationalRecord],
    *,
    taxonomy: TaxonomyDefinition,
) -> tuple[dict[str, int], dict[str, str]]:
    counts = Counter(
        record.taxonomy_subcategory_id
        for record in records
        if record.taxonomy_subcategory_id is not None
    )
    stats: dict[str, int] = {}
    labels: dict[str, str] = {}
    for category in taxonomy.categories:
        for leaf in category.subcategories:
            count = counts.get(leaf.id, 0)
            if count > 0:
                stats[leaf.id] = count
                labels[leaf.id] = leaf.label_he
    return stats, labels


def _case_summary(record: NewsItemOperationalRecord) -> CaseSummary:
    return CaseSummary(
        headline=record.title,
        narrative=record.summary_one_sentence,
        source_url=record.canonical_url or record.url,
        publication_datetime=_normalize_datetime(record.publication_datetime),
        taxonomy_category_id=record.taxonomy_category_id,
        taxonomy_subcategory_id=record.taxonomy_subcategory_id,
        category=record.category.value,
        sub_category=record.sub_category.value if record.sub_category is not None else None,
    )


def render_monthly_report_markdown(report: MonthlyReport) -> str:
    """Render a human-readable Markdown monthly report."""
    lines = [
        "# מדד האכיפה — נלחמות בתעשיית המין",
        "",
        f'## דו"ח חודשי {report.month_label_he}',
        "",
        "### הנתונים החודשיים",
    ]
    if report.stats:
        for subcategory_id, count in report.stats.items():
            label = report.stats_labels_he.get(subcategory_id, subcategory_id)
            lines.append(f"- {count} מקרים: {label}")
    else:
        lines.append("- לא נמצאו אירועים ציבוריים רלוונטיים לחודש זה.")

    lines.extend(["", "### פירוט המקרים"])
    if report.selected_cases:
        for case in report.selected_cases:
            lines.append(f"- **{case.headline}**: {case.narrative} ([מקור]({case.source_url}))")
    else:
        lines.append("- לא נבחרו מקרים להצגה.")

    lines.extend(["", "### פעילות המטה"])
    lines.append(f"- {report.hq_activity or report.hq_activity_placeholder}")
    lines.extend(
        [
            "",
            "### הערת שימוש",
            '- הדו"ח מבוסס על רשומות ציבוריות מאושרות בלבד מתוך `news_items`.',
            "- טיוטה זו מיועדת לעריכה אנושית לפני פרסום חיצוני.",
            "",
        ]
    )
    return "\n".join(lines)


def generate_monthly_report(
    records: Sequence[NewsItemOperationalRecord],
    month: date,
    hq_activity: str | None = None,
    typology: TaxonomyDefinition | None = None,
    *,
    max_cases: int = DEFAULT_MAX_CASES,
) -> MonthlyReport:
    """Build a monthly report payload from eligible operational records."""
    taxonomy = typology or default_taxonomy()
    month_start = month.replace(day=1)
    eligible_records = select_monthly_report_records(records, month=month_start)
    stats, labels = _stats_for_records(eligible_records, taxonomy=taxonomy)
    selected_cases = [_case_summary(record) for record in eligible_records[:max_cases]]
    report = MonthlyReport(
        month=month_start,
        month_key=month_key(month_start),
        month_label_he=month_label_he(month_start),
        stats=stats,
        stats_labels_he=labels,
        selected_cases=selected_cases,
        hq_activity=hq_activity.strip() if hq_activity and hq_activity.strip() else None,
        rendered_markdown="",
    )
    report.rendered_markdown = render_monthly_report_markdown(report)
    return report


def persist_monthly_report_artifacts(
    publication_dir: Path,
    report: MonthlyReport,
) -> MonthlyReportArtifacts:
    """Write the monthly report bundle into the job publication namespace."""
    output_dir = publication_dir / report.month_key
    output_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = output_dir / "monthly_report.md"
    json_path = output_dir / "monthly_report.json"
    readme_path = output_dir / "README.md"
    markdown_path.write_text(report.rendered_markdown, encoding="utf-8")
    json_path.write_text(_report_json_text(report), encoding="utf-8")
    readme_path.write_text(
        (
            f"# news_items monthly report {report.month_key}\n\n"
            f"- Month: `{report.month_key}`\n"
            f"- Selected cases: {len(report.selected_cases)}\n"
            f"- Counted subcategories: {len(report.stats)}\n"
            f"- Source window: {report.month_key}-01 .. month end (UTC)\n"
        ),
        encoding="utf-8",
    )
    return MonthlyReportArtifacts(
        output_dir=output_dir,
        markdown_path=markdown_path,
        json_path=json_path,
        readme_path=readme_path,
    )


def write_report_copy(path: Path, content: str) -> None:
    """Write an explicit report output path outside the state bundle."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _report_json_text(report: MonthlyReport) -> str:
    """Serialize the report JSON with human-readable Hebrew text."""
    return json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n"


def write_report_json_copy(path: Path, report: MonthlyReport) -> None:
    """Write an explicit JSON output path outside the state bundle."""
    write_report_copy(path, _report_json_text(report))


def hq_activity_from_inputs(
    *,
    hq_activity: str | None = None,
    hq_activity_file: Path | None = None,
) -> str | None:
    """Resolve HQ activity text, preferring the file input when provided."""
    if hq_activity_file is not None:
        text = hq_activity_file.read_text(encoding="utf-8").strip()
        return text or None
    if hq_activity is None:
        return None
    text = hq_activity.strip()
    return text or None


def report_env_summary(
    *,
    month: date,
    markdown_path: Path,
    json_path: Path,
) -> dict[str, str]:
    """Small structured summary for run debug payloads."""
    return {
        "month": month_key(month),
        "markdown_path": str(markdown_path),
        "json_path": str(json_path),
    }
