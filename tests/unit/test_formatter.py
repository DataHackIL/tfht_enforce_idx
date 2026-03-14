"""Unit tests for output formatter module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from pydantic import HttpUrl

from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.output.formatter import (
    format_category,
    format_items,
    format_unified_item,
    get_icon,
    print_items,
)


class TestGetIcon:
    """Tests for get_icon function."""

    def test_brothel_closure_icon(self) -> None:
        """Test brothel closure icon."""
        icon = get_icon(Category.BROTHEL, SubCategory.CLOSURE)
        assert icon == "\U0001f6a8"  # 🚨

    def test_brothel_opening_icon(self) -> None:
        """Test brothel opening icon."""
        icon = get_icon(Category.BROTHEL, SubCategory.OPENING)
        assert icon == "\u26a0\ufe0f"  # ⚠️

    def test_trafficking_rescue_icon(self) -> None:
        """Test trafficking rescue icon."""
        icon = get_icon(Category.TRAFFICKING, SubCategory.RESCUE)
        assert icon == "\U0001f198"  # 🆘

    def test_fallback_icon(self) -> None:
        """Test fallback icon for unknown combination."""
        icon = get_icon(Category.ENFORCEMENT, None)
        assert icon == "\U0001f50d"  # 🔍


class TestFormatCategory:
    """Tests for format_category function."""

    def test_category_only(self) -> None:
        """Test formatting category without subcategory."""
        result = format_category(Category.BROTHEL, None)
        assert result == "בית בושת"

    def test_category_with_subcategory(self) -> None:
        """Test formatting category with subcategory."""
        result = format_category(Category.BROTHEL, SubCategory.CLOSURE)
        assert result == "בית בושת » סגירה"

    def test_pimping_arrest(self) -> None:
        """Test pimping arrest formatting."""
        result = format_category(Category.PIMPING, SubCategory.ARREST)
        assert result == "סרסור » מעצר"


class TestFormatUnifiedItem:
    """Tests for format_unified_item function."""

    def test_format_item(self) -> None:
        """Test formatting a unified item."""
        item = UnifiedItem(
            headline="פשיטה על בית בושת בתל אביב",
            summary="המשטרה פשטה על דירה בתל אביב שפעלה כבית בושת.",
            sources=[
                SourceReference(
                    source_name="ynet",
                    url=HttpUrl("https://ynet.co.il/article/1"),
                ),
            ],
            date=datetime(2026, 2, 15, tzinfo=UTC),
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
        )

        output = format_unified_item(item)

        assert "פשיטה על בית בושת בתל אביב" in output
        assert "2026-02-15" in output
        assert "בית בושת » סגירה" in output
        assert "ynet" in output
        assert "https://ynet.co.il/article/1" in output

    def test_format_multiple_sources(self) -> None:
        """Test formatting item with multiple sources."""
        item = UnifiedItem(
            headline="Test Headline",
            summary="Test summary",
            sources=[
                SourceReference(
                    source_name="ynet",
                    url=HttpUrl("https://ynet.co.il/1"),
                ),
                SourceReference(
                    source_name="walla",
                    url=HttpUrl("https://walla.co.il/1"),
                ),
                SourceReference(
                    source_name="mako",
                    url=HttpUrl("https://mako.co.il/1"),
                ),
            ],
            date=datetime(2026, 2, 15, tzinfo=UTC),
            category=Category.ENFORCEMENT,
            sub_category=SubCategory.OPERATION,
        )

        output = format_unified_item(item)

        assert "ynet" in output
        assert "walla" in output
        assert "mako" in output


class TestFormatItems:
    """Tests for format_items function."""

    def test_empty_list(self) -> None:
        """Test formatting empty list."""
        output = format_items([])
        assert "לא נמצאו כתבות רלוונטיות" in output

    def test_single_item(self) -> None:
        """Test formatting single item."""
        items = [
            UnifiedItem(
                headline="Test",
                summary="Summary",
                sources=[
                    SourceReference(
                        source_name="test",
                        url=HttpUrl("https://example.com/1"),
                    ),
                ],
                date=datetime(2026, 2, 15, tzinfo=UTC),
                category=Category.BROTHEL,
                sub_category=SubCategory.CLOSURE,
            )
        ]

        output = format_items(items)

        assert "Test" in output
        assert "לא נמצאו" not in output

    def test_multiple_items(self) -> None:
        """Test formatting multiple items."""
        items = [
            UnifiedItem(
                headline="Article One",
                summary="Summary one",
                sources=[
                    SourceReference(
                        source_name="a",
                        url=HttpUrl("https://a.com/1"),
                    ),
                ],
                date=datetime(2026, 2, 15, tzinfo=UTC),
                category=Category.BROTHEL,
                sub_category=SubCategory.CLOSURE,
            ),
            UnifiedItem(
                headline="Article Two",
                summary="Summary two",
                sources=[
                    SourceReference(
                        source_name="b",
                        url=HttpUrl("https://b.com/1"),
                    ),
                ],
                date=datetime(2026, 2, 14, tzinfo=UTC),
                category=Category.PIMPING,
                sub_category=SubCategory.ARREST,
            ),
        ]

        output = format_items(items)

        assert "Article One" in output
        assert "Article Two" in output
        # Check separator between items
        assert "─" in output


class TestPrintItems:
    """Tests for print_items."""

    @patch("builtins.print")
    def test_print_items_includes_total_for_non_empty(self, mock_print: MagicMock) -> None:
        """Non-empty item lists should print the formatted output and total."""
        items = [
            UnifiedItem(
                headline="Article One",
                summary="Summary one",
                sources=[
                    SourceReference(
                        source_name="a",
                        url=HttpUrl("https://a.com/1"),
                    ),
                ],
                date=datetime(2026, 2, 15, tzinfo=UTC),
                category=Category.BROTHEL,
                sub_category=SubCategory.CLOSURE,
            )
        ]

        print_items(items)

        assert mock_print.call_count == 3
        assert "סה״כ: 1 כתבות" in mock_print.call_args_list[-1].args[0]

    @patch("builtins.print")
    def test_print_items_skips_total_for_empty(self, mock_print: MagicMock) -> None:
        """Empty item lists should only print the empty-state message."""
        print_items([])

        mock_print.assert_called_once_with("לא נמצאו כתבות רלוונטיות.")
