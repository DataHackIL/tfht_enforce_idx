"""Unit tests for seen URL store module."""

from pathlib import Path

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
