"""Unit tests for run snapshot persistence."""

from datetime import UTC, datetime
from pathlib import Path

from pydantic import HttpUrl

from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.store.run_snapshots import RunSnapshot, snapshot_filename, write_run_snapshot


def build_item() -> UnifiedItem:
    """Create a sample unified item for snapshot tests."""
    return UnifiedItem(
        headline="פשיטה על בית בושת",
        summary="סיכום",
        sources=[SourceReference(source_name="ynet", url=HttpUrl("https://ynet.co.il/1"))],
        date=datetime(2026, 3, 1, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
    )


class TestRunSnapshots:
    """Tests for run snapshot helpers."""

    def test_snapshot_filename_is_git_safe(self) -> None:
        """Snapshot filenames should avoid colon characters."""
        filename = snapshot_filename(datetime(2026, 3, 15, 4, 0, 0, 123456, tzinfo=UTC))

        assert filename == "2026-03-15T04-00-00-123456Z.json"

    def test_write_run_snapshot_creates_directory_and_json(self, tmp_path: Path) -> None:
        """Snapshots should be written under the configured runs directory."""
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            config_name="enforcement-news",
            days_searched=7,
            output_formats=["cli", "email"],
            raw_article_count=3,
            unseen_article_count=2,
            relevant_article_count=1,
            unified_item_count=1,
            seen_count_before=10,
            seen_count_after=11,
            items=[build_item()],
            errors=["mako: timeout"],
        )

        path = write_run_snapshot(tmp_path / "runs", snapshot)

        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert '"config_name": "enforcement-news"' in content
        assert '"output_formats": [' in content
        assert '"errors": [' in content
