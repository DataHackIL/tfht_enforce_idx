"""Base protocol for news sources."""

from abc import ABC, abstractmethod

from denbust.data_models import RawArticle


class Source(ABC):
    """Abstract base class for news sources."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the source name."""
        ...

    @abstractmethod
    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        """Fetch articles from this source.

        Args:
            days: Number of days back to search.
            keywords: Keywords to filter articles by.

        Returns:
            List of raw articles matching the criteria.
        """
        ...
