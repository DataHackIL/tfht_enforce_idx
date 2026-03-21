"""Run-level metadata models shared across dataset jobs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from denbust.data_models import UnifiedItem
from denbust.models.common import DatasetName, JobName


class RunSnapshot(BaseModel):
    """Summary of a dataset job execution."""

    run_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    dataset_name: DatasetName = DatasetName.NEWS_ITEMS
    job_name: JobName = JobName.INGEST
    config_name: str = "enforcement-news"
    config_path: str | None = None
    days_searched: int | None = None
    source_count: int = 0
    output_formats: list[str] = Field(default_factory=list)
    raw_article_count: int = 0
    unseen_article_count: int = 0
    relevant_article_count: int = 0
    unified_item_count: int = 0
    seen_count_before: int = 0
    seen_count_after: int = 0
    fatal: bool = False
    items: list[UnifiedItem] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    result_summary: str | None = None
    release_manifest: dict[str, Any] | None = None
    backup_manifest: dict[str, Any] | None = None
    _debug_payload: dict[str, Any] | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _normalize_timestamps(self) -> RunSnapshot:
        """Populate compatibility timestamps when omitted."""
        if self.started_at is None:
            self.started_at = self.run_timestamp
        if self.finished_at is None:
            self.finished_at = self.run_timestamp
        return self

    def finish(self, result_summary: str | None = None) -> RunSnapshot:
        """Mark the snapshot complete and optionally store a summary string."""
        self.finished_at = datetime.now(UTC)
        if result_summary is not None:
            self.result_summary = result_summary
        return self

    @property
    def debug_payload(self) -> dict[str, Any] | None:
        """Optional non-public diagnostic payload for state-repo logging."""
        return self._debug_payload

    def set_debug_payload(self, payload: dict[str, Any] | None) -> RunSnapshot:
        """Attach a non-serialized debug payload to the snapshot."""
        self._debug_payload = payload
        return self
