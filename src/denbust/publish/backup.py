"""Backup scaffolding for future remote backup targets."""

from __future__ import annotations

from abc import abstractmethod
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from denbust.publish.base import PhaseAScaffold


class BackupTarget(BaseModel):
    """Future backup target definition."""

    name: str
    kind: str
    location: str


class BackupManifest(BaseModel):
    """Metadata for a backup execution."""

    dataset_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    targets: list[BackupTarget] = Field(default_factory=list)
    notes: str | None = None


class BackupExecutor(PhaseAScaffold):
    """Abstract backup executor."""

    @abstractmethod
    def build_manifest(self, dataset_name: str, state_root: Path) -> BackupManifest:
        """Build a backup manifest for a dataset."""


class NullBackupExecutor(BackupExecutor):
    """Scaffold-only backup executor used in Phase A."""

    def build_manifest(self, dataset_name: str, state_root: Path) -> BackupManifest:
        return BackupManifest(
            dataset_name=dataset_name,
            targets=[
                BackupTarget(
                    name="phase-a-placeholder",
                    kind="none",
                    location=str(state_root),
                )
            ],
            notes="Backup uploads are scaffolded but not implemented in Phase A.",
        )

    def describe(self) -> str:
        return "Backup uploads are scaffolded but not implemented in Phase A."
