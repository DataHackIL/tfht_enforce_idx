"""Tests for reviewed-table import into validation drafts."""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from denbust.validation.common import read_csv_rows
from denbust.validation.import_reviewed import TFHT_MANUAL_TRACKING_V1, import_reviewed_table


def _build_manual_tracking_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "מדד האכיפה"
    sheet["B2"] = 2026
    sheet["B3"] = "ינואר"
    headers = ["תאריך", "כתובת", "אירוע", "פרטים רלוונטיים", "סטטוס", "מקור מידע"]
    for index, value in enumerate(headers, start=2):
        sheet.cell(4, index, value)

    sheet["B5"] = 8
    sheet["C5"] = "בני ברק"
    sheet["D5"] = "מעצר חשודה על החזקת מקום לשם זנות"
    sheet["E5"] = "עינת הראל נעצרה; אותר בית בושת פעיל"
    sheet["F5"] = "פתוח"
    sheet["G5"] = (
        "https://www.maariv.co.il/news/law/article-1270778; "
        "https://www.mako.co.il/men-men_news/Article-326411394f9fb91026.htm?Partner=searchResults"
    )

    sheet["B6"] = 9
    sheet["C6"] = "אשקלון"
    sheet["D6"] = "סגירה מנהלית"
    sheet["E6"] = "צו מנהלי לדירה ברחוב ביאליק"
    sheet["F6"] = "סגור עד 23.2.26"
    sheet["G6"] = "https://www.kan-ashkelon.co.il/news/100735"

    sheet["B7"] = 10
    sheet["C7"] = "תל אביב"
    sheet["D7"] = "מעצר חשודה"
    sheet["E7"] = "אין מידע מספק"
    sheet["F7"] = ""
    sheet["G7"] = ""

    sheet["B8"] = 8
    sheet["C8"] = "בני ברק"
    sheet["D8"] = "מעצר חשודה על החזקת מקום לשם זנות"
    sheet["E8"] = "עינת הראל נעצרה; אותר בית בושת פעיל"
    sheet["F8"] = "פתוח"
    sheet["G8"] = "https://www.maariv.co.il/news/law/article-1270778"

    workbook.save(path)


def test_import_reviewed_table_normalizes_manual_tracking_workbook(tmp_path: Path) -> None:
    workbook_path = tmp_path / "manual_tracking.xlsx"
    _build_manual_tracking_workbook(workbook_path)

    result = import_reviewed_table(
        input_path=workbook_path,
        format_name=TFHT_MANUAL_TRACKING_V1,
    )

    rows = read_csv_rows(result.output_path)
    assert result.imported_rows == 2
    assert result.skipped_rows == 1
    assert any("duplicate" in warning for warning in result.warnings)

    first_row = rows[0]
    second_row = rows[1]
    assert first_row["review_status"] == "reviewed"
    assert first_row["taxonomy_category_id"] == "brothels"
    assert first_row["taxonomy_subcategory_id"] == "keeping_brothel"
    assert first_row["category"] == "brothel"
    assert first_row["source_name"] == "maariv"
    assert first_row["canonical_url"] == "https://maariv.co.il/news/law/article-1270778"
    assert first_row["annotation_source"] == TFHT_MANUAL_TRACKING_V1

    assert second_row["taxonomy_subcategory_id"] == "administrative_closure"
    assert second_row["index_relevant"] == "True"


def test_import_reviewed_table_rejects_unknown_format(tmp_path: Path) -> None:
    workbook_path = tmp_path / "manual_tracking.xlsx"
    _build_manual_tracking_workbook(workbook_path)

    try:
        import_reviewed_table(input_path=workbook_path, format_name="unknown")
    except ValueError as error:
        assert "Unsupported reviewed-table format" in str(error)
    else:
        raise AssertionError("Expected unsupported format to raise ValueError")
