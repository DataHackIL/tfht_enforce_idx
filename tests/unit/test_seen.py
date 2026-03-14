"""Unit tests for seen URL store module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from denbust.store.seen import SeenStore, create_seen_store


class TestSeenStore:
    """Tests for SeenStore class."""

    def test_is_seen_empty(self, tmp_path: Path) -> None:
        """Test is_seen on empty store."""
        store = SeenStore(tmp_path / "seen.json")

        assert store.is_seen("https://example.com/1") is False
        assert store.count == 0

    def test_mark_seen(self, tmp_path: Path) -> None:
        """Test marking URLs as seen."""
        store = SeenStore(tmp_path / "seen.json")

        store.mark_seen(["https://example.com/1", "https://example.com/2"])

        assert store.is_seen("https://example.com/1") is True
        assert store.is_seen("https://example.com/2") is True
        assert store.is_seen("https://example.com/3") is False
        assert store.count == 2

    def test_filter_unseen(self, tmp_path: Path) -> None:
        """Test filtering unseen URLs."""
        store = SeenStore(tmp_path / "seen.json")
        store.mark_seen(["https://example.com/1"])

        urls = [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/3",
        ]

        unseen = store.filter_unseen(urls)

        assert unseen == ["https://example.com/2", "https://example.com/3"]

    def test_save_and_load(self, tmp_path: Path) -> None:
        """Test persistence."""
        path = tmp_path / "seen.json"

        # Create and save
        store1 = SeenStore(path)
        store1.mark_seen(["https://example.com/1", "https://example.com/2"])
        store1.save()

        # Load in new instance
        store2 = SeenStore(path)

        assert store2.is_seen("https://example.com/1") is True
        assert store2.is_seen("https://example.com/2") is True
        assert store2.count == 2

    def test_clear(self, tmp_path: Path) -> None:
        """Test clearing all URLs."""
        store = SeenStore(tmp_path / "seen.json")
        store.mark_seen(["https://example.com/1", "https://example.com/2"])

        store.clear()

        assert store.count == 0
        assert store.is_seen("https://example.com/1") is False

    def test_mark_seen_idempotent(self, tmp_path: Path) -> None:
        """Test that marking same URL twice doesn't create duplicates."""
        store = SeenStore(tmp_path / "seen.json")

        store.mark_seen(["https://example.com/1"])
        store.mark_seen(["https://example.com/1"])

        assert store.count == 1

    def test_creates_directory(self, tmp_path: Path) -> None:
        """Test that save creates parent directory."""
        path = tmp_path / "subdir" / "seen.json"
        store = SeenStore(path)
        store.mark_seen(["https://example.com/1"])
        store.save()

        assert path.exists()

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        """Test loading invalid JSON starts with empty store."""
        path = tmp_path / "seen.json"
        path.write_text("invalid json")

        store = SeenStore(path)

        assert store.count == 0

    def test_create_seen_store(self, tmp_path: Path) -> None:
        """Test create_seen_store factory function."""
        path = tmp_path / "seen.json"
        store = create_seen_store(path)

        assert isinstance(store, SeenStore)
        assert store.count == 0

    def test_prune_older_than_removes_old_entries(self, tmp_path: Path) -> None:
        """Pruning should remove entries older than the configured cutoff."""
        store = SeenStore(tmp_path / "seen.json")
        store._seen = {
            "https://example.com/old": "2020-01-01T00:00:00+00:00",
            "https://example.com/new": "2999-01-01T00:00:00+00:00",
        }

        removed = store.prune_older_than(30)

        assert removed == 1
        assert store.is_seen("https://example.com/new") is True
        assert store.is_seen("https://example.com/old") is False

    def test_prune_older_than_non_positive_days_is_noop(self, tmp_path: Path) -> None:
        """Non-positive day values should not alter the store."""
        store = SeenStore(tmp_path / "seen.json")
        store.mark_seen(["https://example.com/1"])

        removed = store.prune_older_than(0)

        assert removed == 0
        assert store.count == 1

    def test_parse_timestamp_invalid_returns_zero(self, tmp_path: Path) -> None:
        """Invalid timestamps should degrade to the epoch."""
        store = SeenStore(tmp_path / "seen.json")

        assert store._parse_timestamp("not-a-timestamp") == 0.0

    @patch("denbust.store.seen.logger")
    def test_save_logs_os_error(self, mock_logger: MagicMock, tmp_path: Path) -> None:
        """Save failures should be logged without raising."""
        store = SeenStore(tmp_path / "seen.json")
        store.mark_seen(["https://example.com/1"])

        with patch("builtins.open", side_effect=OSError("disk full")):
            store.save()

        mock_logger.error.assert_called_once()
