"""Operational store abstractions for future dataset backends."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from pathlib import Path
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


class LocalJsonOperationalStore(OperationalStore):
    """Simple local operational store for tests and optional local inspection."""

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    @property
    def run_metadata_path(self) -> Path:
        """Path to the JSONL file containing stored run metadata."""
        return self.root_dir / "run_metadata.jsonl"

    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        with open(self.run_metadata_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False))
            handle.write("\n")

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
