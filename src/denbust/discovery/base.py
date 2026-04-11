"""Base protocols and shared contexts for discovery producers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel, Field

from denbust.discovery.models import DiscoveredCandidate, DiscoveryQuery


class DiscoveryContext(BaseModel):
    """Shared execution context for a discovery-engine run."""

    run_id: str
    max_results_per_query: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceDiscoveryContext(BaseModel):
    """Shared execution context for source-native candidate producers."""

    run_id: str
    source_names: list[str] = Field(default_factory=list)
    date_from: datetime | None = None
    date_to: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryEngine(Protocol):
    """Protocol for search-engine discovery adapters."""

    name: str

    async def discover(
        self,
        queries: list[DiscoveryQuery],
        context: DiscoveryContext,
    ) -> list[DiscoveredCandidate]:
        """Return normalized candidates for one or more search queries."""


class SourceCandidateProducer(Protocol):
    """Protocol for source-native candidate producers."""

    name: str

    async def discover_candidates(
        self,
        context: SourceDiscoveryContext,
    ) -> list[DiscoveredCandidate]:
        """Return normalized candidates discovered directly from known sources."""
