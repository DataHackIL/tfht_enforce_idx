"""Core models for the persistent discovery and candidacy layer."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from pydantic import BaseModel, Field, HttpUrl, model_validator

from denbust.models.common import DatasetName, JobName


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _uuid_str() -> str:
    return str(uuid4())


def _domain_from_url(value: HttpUrl | None) -> str | None:
    if value is None:
        return None
    return urlparse(str(value)).netloc or None


class DiscoveryQueryKind(StrEnum):
    """Supported query families for candidate discovery."""

    BROAD = "broad"
    SOURCE_TARGETED = "source_targeted"
    TAXONOMY_TARGETED = "taxonomy_targeted"
    SOCIAL_TARGETED = "social_targeted"


class ProducerKind(StrEnum):
    """Producer families that can emit discovery candidates."""

    SEARCH_ENGINE = "search_engine"
    SOURCE_NATIVE = "source_native"
    SOCIAL_SEARCH = "social_search"


class CandidateStatus(StrEnum):
    """Lifecycle states for a persistent candidate."""

    NEW = "new"
    QUEUED = "queued"
    SCRAPE_PENDING = "scrape_pending"
    SCRAPE_IN_PROGRESS = "scrape_in_progress"
    SCRAPE_SUCCEEDED = "scrape_succeeded"
    SCRAPE_FAILED = "scrape_failed"
    PARTIALLY_SCRAPED = "partially_scraped"
    UNSUPPORTED_SOURCE = "unsupported_source"
    SUPPRESSED = "suppressed"
    CLOSED = "closed"


class ContentBasis(StrEnum):
    """Content quality basis for the current candidate state."""

    CANDIDATE_ONLY = "candidate_only"
    SEARCH_RESULT_ONLY = "search_result_only"
    PARTIAL_PAGE = "partial_page"
    FULL_ARTICLE_PAGE = "full_article_page"


class ScrapeAttemptKind(StrEnum):
    """Kinds of scrape attempt supported by the candidate layer."""

    SOURCE_ADAPTER = "source_adapter"
    GENERIC_FETCH = "generic_fetch"
    GENERIC_EXTRACT = "generic_extract"
    SELF_HEAL_RETRY = "self_heal_retry"
    MANUAL_RETRY = "manual_retry"
    BACKFILL_RETRY = "backfill_retry"


class FetchStatus(StrEnum):
    """Outcomes for a scrape attempt."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"
    BLOCKED = "blocked"
    TIMEOUT = "timeout"


class DiscoveryRunStatus(StrEnum):
    """Status values for discovery-run bookkeeping."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class DiscoveryQuery(BaseModel):
    """Normalized search query definition for a discovery engine."""

    query_text: str
    language: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    preferred_domains: list[str] = Field(default_factory=list)
    excluded_domains: list[str] = Field(default_factory=list)
    source_hint: str | None = None
    query_kind: DiscoveryQueryKind = DiscoveryQueryKind.BROAD
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_date_window(self) -> DiscoveryQuery:
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from must be earlier than or equal to date_to")
        return self


class DiscoveredCandidate(BaseModel):
    """A candidate emitted directly by a discovery producer."""

    discovery_id: str = Field(default_factory=_uuid_str)
    producer_name: str
    producer_kind: ProducerKind
    query_text: str | None = None
    candidate_url: HttpUrl
    canonical_url: HttpUrl | None = None
    title: str | None = None
    snippet: str | None = None
    discovered_at: datetime = Field(default_factory=_utc_now)
    publication_datetime_hint: datetime | None = None
    domain: str | None = None
    rank: int | None = Field(default=None, ge=1)
    producer_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    source_hint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_domain(self) -> DiscoveredCandidate:
        if self.domain is None:
            self.domain = _domain_from_url(self.canonical_url or self.candidate_url)
        return self


class PersistentCandidate(BaseModel):
    """Durable queue/history record for a candidate URL."""

    candidate_id: str = Field(default_factory=_uuid_str)
    canonical_url: HttpUrl | None = None
    current_url: HttpUrl
    domain: str | None = None
    titles: list[str] = Field(default_factory=list)
    snippets: list[str] = Field(default_factory=list)
    discovered_via: list[str] = Field(default_factory=list)
    discovery_queries: list[str] = Field(default_factory=list)
    source_hints: list[str] = Field(default_factory=list)
    first_seen_at: datetime = Field(default_factory=_utc_now)
    last_seen_at: datetime = Field(default_factory=_utc_now)
    candidate_status: CandidateStatus = CandidateStatus.NEW
    scrape_attempt_count: int = Field(default=0, ge=0)
    last_scrape_attempt_at: datetime | None = None
    next_scrape_attempt_at: datetime | None = None
    last_scrape_error_code: str | None = None
    last_scrape_error_message: str | None = None
    content_basis: ContentBasis = ContentBasis.CANDIDATE_ONLY
    retry_priority: int = 0
    needs_review: bool = False
    backfill_batch_id: str | None = None
    self_heal_eligible: bool = False
    source_discovery_only: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_candidate(self) -> PersistentCandidate:
        if self.domain is None:
            self.domain = _domain_from_url(self.canonical_url or self.current_url)
        if self.last_seen_at < self.first_seen_at:
            raise ValueError("last_seen_at must be later than or equal to first_seen_at")
        if (
            self.last_scrape_attempt_at is not None
            and self.last_scrape_attempt_at < self.first_seen_at
        ):
            raise ValueError("last_scrape_attempt_at cannot be earlier than first_seen_at")
        if (
            self.next_scrape_attempt_at is not None
            and self.last_scrape_attempt_at is not None
            and self.next_scrape_attempt_at < self.last_scrape_attempt_at
        ):
            raise ValueError(
                "next_scrape_attempt_at cannot be earlier than last_scrape_attempt_at"
            )
        return self


class CandidateProvenance(BaseModel):
    """Append-only provenance event for durable candidate discovery."""

    provenance_id: str = Field(default_factory=_uuid_str)
    run_id: str
    candidate_id: str
    producer_name: str
    producer_kind: ProducerKind
    query_text: str | None = None
    raw_url: HttpUrl
    normalized_url: HttpUrl | None = None
    title: str | None = None
    snippet: str | None = None
    publication_datetime_hint: datetime | None = None
    rank: int | None = Field(default=None, ge=1)
    domain: str | None = None
    discovered_at: datetime = Field(default_factory=_utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_domain(self) -> CandidateProvenance:
        if self.domain is None:
            self.domain = _domain_from_url(self.normalized_url or self.raw_url)
        return self


class ScrapeAttempt(BaseModel):
    """Audit record for each attempt to fetch or extract a candidate."""

    attempt_id: str = Field(default_factory=_uuid_str)
    candidate_id: str
    started_at: datetime = Field(default_factory=_utc_now)
    finished_at: datetime | None = None
    attempt_kind: ScrapeAttemptKind
    fetch_status: FetchStatus
    source_adapter_name: str | None = None
    extracted_title: str | None = None
    extracted_publication_datetime: datetime | None = None
    extracted_body_hash: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    diagnostics: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_finished_at(self) -> ScrapeAttempt:
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("finished_at must be later than or equal to started_at")
        return self


class DiscoveryRun(BaseModel):
    """Run-level bookkeeping row for durable discovery executions."""

    run_id: str = Field(default_factory=_uuid_str)
    started_at: datetime = Field(default_factory=_utc_now)
    finished_at: datetime | None = None
    dataset_name: DatasetName = DatasetName.NEWS_ITEMS
    job_name: JobName = JobName.DISCOVER
    status: DiscoveryRunStatus = DiscoveryRunStatus.PENDING
    query_count: int = Field(default=0, ge=0)
    candidate_count: int = Field(default=0, ge=0)
    merged_candidate_count: int = Field(default=0, ge=0)
    queued_for_scrape_count: int = Field(default=0, ge=0)
    errors: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_finished_at(self) -> DiscoveryRun:
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("finished_at must be later than or equal to started_at")
        return self
