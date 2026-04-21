"""Base protocol for news sources."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

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

    def get_debug_state(self) -> dict[str, Any] | None:
        """Return optional structured runtime telemetry for debug logs."""
        return None


@runtime_checkable
class HistoricalSource(Protocol):
    """Optional protocol for sources that can fetch one explicit historical window."""

    @property
    def name(self) -> str:
        """Return the source name."""
        ...

    async def fetch_window(
        self,
        *,
        date_from: datetime,
        date_to: datetime,
        keywords: list[str],
    ) -> list[RawArticle]:
        """Fetch articles for one explicit historical window."""
