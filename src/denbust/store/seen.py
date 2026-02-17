"""Track seen URLs to avoid duplicate processing."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SeenStore:
    """Persistent store for tracking seen article URLs."""

    def __init__(self, path: Path) -> None:
        """Initialize seen store.

        Args:
            path: Path to JSON file for persistence.
        """
        self._path = path
        self._seen: dict[str, str] = {}  # url -> first_seen_timestamp
        self._load()

    def _load(self) -> None:
        """Load seen URLs from disk."""
        if not self._path.exists():
            self._seen = {}
            return

        try:
            with open(self._path) as f:
                data = json.load(f)
                self._seen = data.get("urls", {})
                logger.info(f"Loaded {len(self._seen)} seen URLs from {self._path}")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load seen URLs from {self._path}: {e}")
            self._seen = {}

    def save(self) -> None:
        """Save seen URLs to disk."""
        # Ensure parent directory exists
        self._path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "urls": self._seen,
            "updated_at": datetime.now(UTC).isoformat(),
        }

        try:
            with open(self._path, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(self._seen)} seen URLs to {self._path}")
        except OSError as e:
            logger.error(f"Failed to save seen URLs to {self._path}: {e}")

    def is_seen(self, url: str) -> bool:
        """Check if a URL has been seen before.

        Args:
            url: URL to check.

        Returns:
            True if URL was seen before.
        """
        return url in self._seen

    def mark_seen(self, urls: list[str]) -> None:
        """Mark URLs as seen.

        Args:
            urls: List of URLs to mark as seen.
        """
        timestamp = datetime.now(UTC).isoformat()
        for url in urls:
            if url not in self._seen:
                self._seen[url] = timestamp

    def filter_unseen(self, urls: list[str]) -> list[str]:
        """Filter out already-seen URLs.

        Args:
            urls: List of URLs to filter.

        Returns:
            List of URLs that haven't been seen before.
        """
        return [url for url in urls if not self.is_seen(url)]

    @property
    def count(self) -> int:
        """Get the number of seen URLs."""
        return len(self._seen)

    def clear(self) -> None:
        """Clear all seen URLs."""
        self._seen = {}

    def prune_older_than(self, days: int) -> int:
        """Remove URLs older than specified days.

        Args:
            days: Number of days to keep.

        Returns:
            Number of URLs removed.
        """
        if days <= 0:
            return 0

        cutoff = datetime.now(UTC).timestamp() - (days * 86400)
        old_count = len(self._seen)

        self._seen = {
            url: ts for url, ts in self._seen.items() if self._parse_timestamp(ts) > cutoff
        }

        removed = old_count - len(self._seen)
        if removed > 0:
            logger.info(f"Pruned {removed} URLs older than {days} days")
        return removed

    def _parse_timestamp(self, ts: str) -> float:
        """Parse ISO timestamp to Unix timestamp.

        Args:
            ts: ISO timestamp string.

        Returns:
            Unix timestamp.
        """
        try:
            return datetime.fromisoformat(ts).timestamp()
        except ValueError:
            return 0.0


def create_seen_store(path: Path) -> SeenStore:
    """Create a seen store instance.

    Args:
        path: Path to JSON file.

    Returns:
        SeenStore instance.
    """
    return SeenStore(path)
