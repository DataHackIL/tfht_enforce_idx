"""Shared identifiers and small common models."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class DatasetName(StrEnum):
    """Supported dataset identifiers."""

    NEWS_ITEMS = "news_items"
    DOCS_METADATA = "docs_metadata"
    OPEN_DOCS_FULLTEXT = "open_docs_fulltext"
    EVENTS = "events"


class JobName(StrEnum):
    """Supported dataset job identifiers."""

    DISCOVER = "discover"
    SCRAPE_CANDIDATES = "scrape_candidates"
    INGEST = "ingest"
    MONTHLY_REPORT = "monthly_report"
    RELEASE = "release"
    BACKUP = "backup"
    BACKFILL_DISCOVER = "backfill_discover"
    BACKFILL_SCRAPE = "backfill_scrape"


def normalize_job_name(value: JobName | str | None) -> JobName:
    """Normalize legacy or string job names into canonical identifiers."""
    if value is None:
        return JobName.INGEST
    if isinstance(value, JobName):
        return value
    if value == "scan":
        return JobName.INGEST
    return JobName(value)


class JobIdentity(BaseModel):
    """Canonical dataset/job identity for a run."""

    dataset_name: DatasetName = DatasetName.NEWS_ITEMS
    job_name: JobName = JobName.INGEST
