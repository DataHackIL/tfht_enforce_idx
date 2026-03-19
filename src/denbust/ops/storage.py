"""Operational store abstractions for dataset persistence."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
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
        """Fetch operational records for release assembly or local inspection."""

    @abstractmethod
    def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, Any]]:
        """Fetch suppression or takedown rules for a dataset."""

    @abstractmethod
    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        """Record publication state transitions."""


class NullOperationalStore(OperationalStore):
    """No-op operational store used when persistence is disabled."""

    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        del snapshot

    def upsert_records(self, dataset_name: str, records: Sequence[Mapping[str, Any]]) -> None:
        del dataset_name, records

    def fetch_records(self, dataset_name: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        del dataset_name, limit
        return []

    def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, Any]]:
        del dataset_name
        return []

    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        del dataset_name, record_ids, publication_status


class LocalJsonOperationalStore(OperationalStore):
    """Local JSON operational store for tests and local end-to-end development."""

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    @property
    def run_metadata_path(self) -> Path:
        """Path to the JSONL file containing stored run metadata."""
        return self.root_dir / "run_metadata.jsonl"

    def records_path(self, dataset_name: str) -> Path:
        """Path to the JSON file containing operational dataset rows."""
        return self.root_dir / f"{dataset_name}.json"

    def suppression_rules_path(self, dataset_name: str) -> Path:
        """Path to the JSON file containing suppression rules."""
        return self.root_dir / f"{dataset_name}_suppression_rules.json"

    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        with open(self.run_metadata_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False))
            handle.write("\n")

    def upsert_records(self, dataset_name: str, records: Sequence[Mapping[str, Any]]) -> None:
        existing = self.fetch_records(dataset_name)
        by_identity: dict[str, dict[str, Any]] = {}
        for record in existing:
            raw_key = record.get("canonical_url") or record.get("id")
            if raw_key is not None and raw_key != "":
                by_identity[str(raw_key)] = record

        for incoming_record in records:
            payload = dict(incoming_record)
            raw_key = payload.get("canonical_url") or payload.get("id")
            if raw_key is None or raw_key == "":
                continue
            key = str(raw_key)
            current = by_identity.get(key)
            if current is not None and "created_at" in current and "created_at" not in payload:
                payload["created_at"] = current["created_at"]
            by_identity[key] = payload

        rows = list(by_identity.values())
        rows.sort(key=lambda row: str(row.get("publication_datetime", "")), reverse=True)
        self._write_json(self.records_path(dataset_name), rows)

    def fetch_records(self, dataset_name: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self._read_json(self.records_path(dataset_name))
        if limit is None:
            return rows
        return rows[:limit]

    def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, Any]]:
        return self._read_json(self.suppression_rules_path(dataset_name))

    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        rows = self.fetch_records(dataset_name)
        wanted = set(record_ids)
        if not wanted:
            return
        updated_at = datetime.now(UTC).isoformat()
        changed = False
        for row in rows:
            if row.get("id") in wanted:
                row["publication_status"] = publication_status
                row["updated_at"] = updated_at
                changed = True
        if changed:
            self._write_json(self.records_path(dataset_name), rows)

    def _read_json(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def _write_json(self, path: Path, rows: list[dict[str, Any]]) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
