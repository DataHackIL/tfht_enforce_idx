"""Persistence interfaces for the durable discovery layer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from denbust.discovery.models import (
    CandidateProvenance,
    CandidateStatus,
    DiscoveryRun,
    PersistentCandidate,
    ScrapeAttempt,
)


class DiscoveryRunStore(ABC):
    """Persistence interface for discovery-run bookkeeping."""

    @abstractmethod
    def write_run(self, run: DiscoveryRun) -> None:
        """Persist a discovery-run record."""


class CandidateStore(ABC):
    """Persistence interface for durable candidate rows."""

    @abstractmethod
    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        """Insert or update durable candidate rows."""

    @abstractmethod
    def get_candidate(self, candidate_id: str) -> PersistentCandidate | None:
        """Fetch a single durable candidate by id."""

    @abstractmethod
    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        """List durable candidates, optionally filtered by status."""


class ProvenanceStore(ABC):
    """Persistence interface for append-only candidate provenance."""

    @abstractmethod
    def append_provenance(self, events: Sequence[CandidateProvenance]) -> None:
        """Append provenance events for discovered candidates."""

    @abstractmethod
    def list_provenance(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[CandidateProvenance]:
        """Return provenance history for a candidate."""


class ScrapeAttemptStore(ABC):
    """Persistence interface for scrape-attempt history."""

    @abstractmethod
    def append_attempts(self, attempts: Sequence[ScrapeAttempt]) -> None:
        """Append scrape-attempt records."""

    @abstractmethod
    def list_attempts(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[ScrapeAttempt]:
        """Return scrape attempts for a candidate."""
