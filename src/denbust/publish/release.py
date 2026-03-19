"""Release/export models and interfaces for dataset publication."""

from __future__ import annotations

from abc import abstractmethod
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

from denbust.publish.base import PhaseAScaffold


class ReleaseFormat(StrEnum):
    """Supported release artifact formats."""

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    MARKDOWN = "markdown"
    TEXT = "text"


class ReleaseArtifact(BaseModel):
    """A single file produced by a dataset release."""

    path: Path
    format: ReleaseFormat
    row_count: int = 0
    sha256: str | None = None


class ReleaseManifest(BaseModel):
    """Metadata describing a dataset release."""

    dataset_name: str
    release_version: str
    release_datetime: datetime = Field(default_factory=lambda: datetime.now(UTC))
    schema_version: str = "phase-a"
    row_count: int = 0
    primary_files: list[ReleaseArtifact] = Field(default_factory=list)
    notes: str | None = None
    source_coverage_window: str | None = None
    rights_policy_version: str = "phase-a"
    privacy_policy_version: str = "phase-a"
    warnings: list[str] = Field(default_factory=list)


class ReleaseBuilder(PhaseAScaffold):
    """Abstract release builder interface."""

    @abstractmethod
    def build_manifest(self, dataset_name: str, publication_dir: Path) -> ReleaseManifest:
        """Build a release manifest for a dataset."""


class NullReleaseBuilder(ReleaseBuilder):
    """Scaffold-only release builder used in Phase A."""

    def build_manifest(self, dataset_name: str, publication_dir: Path) -> ReleaseManifest:
        return ReleaseManifest(
            dataset_name=dataset_name,
            release_version="phase-a-scaffold",
            primary_files=[
                ReleaseArtifact(
                    path=publication_dir / "placeholder.parquet",
                    format=ReleaseFormat.PARQUET,
                )
            ],
            notes="Release publication is scaffolded but not implemented in Phase A.",
        )

    def describe(self) -> str:
        return "Release publication is scaffolded but not implemented in Phase A."
