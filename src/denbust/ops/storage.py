"""Operational store abstractions for future dataset backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any

from denbust.models.runs import RunSnapshot


class OperationalStore(ABC):
    """Abstract operational persistence interface."""

    @abstractmethod
    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        """Persist run-level metadata."""

    @abstractmethod
    def upsert_records(self, dataset_name: str, records: Sequence[Mapping[str, Any]]) -> None:
        """Upsert operational records for a dataset."""

    @abstractmethod
    def fetch_records(self, dataset_name: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        """Fetch operational records for future release assembly."""

    @abstractmethod
    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        """Record future publication state transitions."""


class NullOperationalStore(OperationalStore):
    """No-op operational store used in Phase A."""

    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        del snapshot

    def upsert_records(self, dataset_name: str, records: Sequence[Mapping[str, Any]]) -> None:
        del dataset_name, records

    def fetch_records(self, dataset_name: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        del dataset_name, limit
        return []

    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        del dataset_name, record_ids, publication_status
