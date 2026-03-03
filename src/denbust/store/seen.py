"""Track seen URLs to avoid duplicate processing."""

import contextlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from denbust.data_models import Category, ClassificationResult, SubCategory

logger = logging.getLogger(__name__)


class SeenStore:
    """Persistent store for tracking seen article URLs and their classifications."""

    def __init__(self, path: Path) -> None:
        """Initialize seen store.

        Args:
            path: Path to JSON file for persistence.
        """
        self._path = path
        # url -> {seen_at: str, classification?: {...}}
        self._seen: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        """Load seen URLs from disk."""
        if not self._path.exists():
            self._seen = {}
            return

        try:
            with open(self._path) as f:
                data = json.load(f)
                raw_urls = data.get("urls", {})
                # Migrate old format (url -> timestamp) to new format (url -> dict)
                self._seen = {}
                for url, value in raw_urls.items():
                    if isinstance(value, str):
                        # Old format: just a timestamp string
                        self._seen[url] = {"seen_at": value}
                    elif isinstance(value, dict):
                        # New format: dict with seen_at and optional classification
                        self._seen[url] = value
                    else:
                        # Unknown format, use current time
                        self._seen[url] = {"seen_at": datetime.now(UTC).isoformat()}
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
            with open(self._path, "w", encoding="utf-8") as f:
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
                self._seen[url] = {"seen_at": timestamp}

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
            url: data
            for url, data in self._seen.items()
            if self._parse_timestamp(data.get("seen_at", "")) > cutoff
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

    # Classification caching methods

    def get_classification(self, url: str) -> ClassificationResult | None:
        """Get cached classification for a URL.

        Args:
            url: URL to look up.

        Returns:
            Cached ClassificationResult or None if not cached.
        """
        if url not in self._seen:
            return None

        data = self._seen[url]
        classification_data = data.get("classification")
        if not classification_data:
            return None

        try:
            # Parse category
            category_str = classification_data.get("category", "not_relevant")
            try:
                category = Category(category_str)
            except ValueError:
                category = Category.NOT_RELEVANT

            # Parse sub_category
            sub_category_str = classification_data.get("sub_category")
            sub_category = None
            if sub_category_str:
                with contextlib.suppress(ValueError):
                    sub_category = SubCategory(sub_category_str)

            return ClassificationResult(
                relevant=bool(classification_data.get("relevant", False)),
                category=category,
                sub_category=sub_category,
                confidence=classification_data.get("confidence", "medium"),
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse cached classification for {url}: {e}")
            return None

    def set_classification(self, url: str, classification: ClassificationResult) -> None:
        """Cache a classification result for a URL.

        Args:
            url: URL to cache for.
            classification: Classification result to cache.
        """
        timestamp = datetime.now(UTC).isoformat()

        if url not in self._seen:
            self._seen[url] = {"seen_at": timestamp}

        self._seen[url]["classification"] = {
            "relevant": classification.relevant,
            "category": classification.category.value,
            "sub_category": (
                classification.sub_category.value if classification.sub_category else None
            ),
            "confidence": classification.confidence,
        }

    def has_classification(self, url: str) -> bool:
        """Check if a URL has a cached classification.

        Args:
            url: URL to check.

        Returns:
            True if classification is cached.
        """
        if url not in self._seen:
            return False
        return "classification" in self._seen[url]


def create_seen_store(path: Path) -> SeenStore:
    """Create a seen store instance.

    Args:
        path: Path to JSON file.

    Returns:
        SeenStore instance.
    """
    return SeenStore(path)
