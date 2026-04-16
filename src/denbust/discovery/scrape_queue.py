"""Candidate selection and scrape-attempt orchestration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import (
    CandidateStatus,
    ContentBasis,
    FetchStatus,
    PersistentCandidate,
    ScrapeAttempt,
    ScrapeAttemptKind,
)
from denbust.discovery.storage import DiscoveryPersistence
from denbust.news_items.normalize import canonicalize_news_url, deduplicate_strings
from denbust.sources.base import Source

SCRAPEABLE_CANDIDATE_STATUSES: tuple[CandidateStatus, ...] = (
    CandidateStatus.NEW,
    CandidateStatus.QUEUED,
    CandidateStatus.SCRAPE_PENDING,
    CandidateStatus.SCRAPE_FAILED,
    CandidateStatus.PARTIALLY_SCRAPED,
)


@dataclass
class CandidateScrapeBatch:
    """Results from attempting to materialize candidates into raw articles."""

    selected_candidates: list[PersistentCandidate]
    updated_candidates: list[PersistentCandidate]
    attempts: list[ScrapeAttempt]
    raw_articles: list[RawArticle]
    errors: list[str]


def select_candidates_for_scrape(
    persistence: DiscoveryPersistence,
    *,
    limit: int,
    now: datetime | None = None,
) -> list[PersistentCandidate]:
    """Return eligible durable candidates for scraping."""
    current_time = now or datetime.now(UTC)
    candidates = persistence.list_candidates(statuses=SCRAPEABLE_CANDIDATE_STATUSES)
    eligible = [
        candidate
        for candidate in candidates
        if candidate.next_scrape_attempt_at is None or candidate.next_scrape_attempt_at <= current_time
    ]
    ordered = sorted(
        eligible,
        key=lambda candidate: (
            candidate.retry_priority,
            candidate.next_scrape_attempt_at or datetime.min.replace(tzinfo=UTC),
            candidate.last_seen_at,
            candidate.candidate_id,
        ),
        reverse=True,
    )
    return ordered[:limit]


def queue_candidates_for_scrape(candidates: list[PersistentCandidate]) -> list[PersistentCandidate]:
    """Mark candidates as queued or pending for an immediate scrape pass."""
    queued: list[PersistentCandidate] = []
    for candidate in candidates:
        next_status = (
            CandidateStatus.QUEUED
            if candidate.candidate_status is CandidateStatus.NEW
            else CandidateStatus.SCRAPE_PENDING
        )
        queued.append(candidate.model_copy(update={"candidate_status": next_status}))
    return queued


def _candidate_urls(candidate: PersistentCandidate) -> set[str]:
    urls = {canonicalize_news_url(str(candidate.current_url))}
    if candidate.canonical_url is not None:
        urls.add(canonicalize_news_url(str(candidate.canonical_url)))
    return urls


def _select_source_name(
    candidate: PersistentCandidate,
    sources_by_name: dict[str, Source],
) -> str | None:
    for name in [*candidate.source_hints, *candidate.discovered_via]:
        if name in sources_by_name:
            return name
    return None


def _build_attempt(
    *,
    candidate_id: str,
    started_at: datetime,
    finished_at: datetime,
    attempt_kind: ScrapeAttemptKind,
    fetch_status: FetchStatus,
    source_adapter_name: str | None = None,
    article: RawArticle | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    diagnostics: dict[str, object] | None = None,
) -> ScrapeAttempt:
    return ScrapeAttempt(
        candidate_id=candidate_id,
        started_at=started_at,
        finished_at=finished_at,
        attempt_kind=attempt_kind,
        fetch_status=fetch_status,
        source_adapter_name=source_adapter_name,
        extracted_title=article.title if article is not None else None,
        extracted_publication_datetime=article.date if article is not None else None,
        error_code=error_code,
        error_message=error_message,
        diagnostics=diagnostics or {},
    )


def _mark_attempt_success(
    candidate: PersistentCandidate,
    attempt: ScrapeAttempt,
    article: RawArticle,
) -> PersistentCandidate:
    return candidate.model_copy(
        update={
            "candidate_status": CandidateStatus.SCRAPE_SUCCEEDED,
            "scrape_attempt_count": candidate.scrape_attempt_count + 1,
            "last_scrape_attempt_at": attempt.finished_at,
            "next_scrape_attempt_at": None,
            "last_scrape_error_code": None,
            "last_scrape_error_message": None,
            "content_basis": ContentBasis.PARTIAL_PAGE,
            "titles": deduplicate_strings([*candidate.titles, article.title]),
            "snippets": deduplicate_strings([*candidate.snippets, article.snippet]),
            "metadata": {
                **candidate.metadata,
                "last_scraped_source_name": article.source_name,
                "last_scraped_candidate_url": str(candidate.current_url),
            },
        }
    )


def _mark_attempt_failure(
    candidate: PersistentCandidate,
    *,
    attempts: list[ScrapeAttempt],
    status: CandidateStatus,
    error_code: str,
    error_message: str,
    config: Config,
) -> PersistentCandidate:
    final_attempt = attempts[-1]
    finished_at = final_attempt.finished_at or final_attempt.started_at
    retry_at = None
    if (
        config.candidates.allow_retry_on_fetch_failure
        and status is not CandidateStatus.UNSUPPORTED_SOURCE
    ):
        retry_at = finished_at + timedelta(
            hours=config.candidates.default_retry_backoff_hours
        )
    return candidate.model_copy(
        update={
            "candidate_status": status,
            "scrape_attempt_count": candidate.scrape_attempt_count + len(attempts),
            "last_scrape_attempt_at": finished_at,
            "next_scrape_attempt_at": retry_at,
            "last_scrape_error_code": error_code,
            "last_scrape_error_message": error_message,
            "needs_review": candidate.needs_review or status is CandidateStatus.UNSUPPORTED_SOURCE,
            "self_heal_eligible": status is CandidateStatus.SCRAPE_FAILED,
        }
    )


async def scrape_candidates(
    *,
    config: Config,
    persistence: DiscoveryPersistence,
    candidates: list[PersistentCandidate],
    sources: list[Source],
    preloaded_source_articles: dict[str, list[RawArticle]] | None = None,
) -> CandidateScrapeBatch:
    """Attempt to materialize durable candidates into raw articles."""
    if not candidates:
        return CandidateScrapeBatch([], [], [], [], [])

    queued_candidates = queue_candidates_for_scrape(candidates)
    persistence.upsert_candidates(queued_candidates)

    source_articles_cache = dict(preloaded_source_articles or {})
    sources_by_name = {source.name: source for source in sources}
    updated_candidates: list[PersistentCandidate] = []
    attempts: list[ScrapeAttempt] = []
    raw_articles: list[RawArticle] = []
    errors: list[str] = []

    for queued_candidate in queued_candidates:
        in_progress = queued_candidate.model_copy(
            update={"candidate_status": CandidateStatus.SCRAPE_IN_PROGRESS}
        )
        persistence.upsert_candidates([in_progress])

        source_name = _select_source_name(in_progress, sources_by_name)
        started_at = datetime.now(UTC)
        article: RawArticle | None = None
        candidate_attempts: list[ScrapeAttempt] = []

        if source_name is not None:
            source = sources_by_name[source_name]
            articles = source_articles_cache.get(source_name)
            if articles is None:
                articles = await source.fetch(days=config.days, keywords=config.keywords)
                source_articles_cache[source_name] = articles
            candidate_urls = _candidate_urls(in_progress)
            article = next(
                (
                    candidate_article
                    for candidate_article in articles
                    if canonicalize_news_url(str(candidate_article.url)) in candidate_urls
                ),
                None,
            )
            finished_at = datetime.now(UTC)
            if article is not None:
                candidate_attempts.append(
                    _build_attempt(
                        candidate_id=in_progress.candidate_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                        fetch_status=FetchStatus.SUCCESS,
                        source_adapter_name=source_name,
                        article=article,
                        diagnostics={"matched_candidate_urls": sorted(candidate_urls)},
                    )
                )
                updated_candidates.append(
                    _mark_attempt_success(in_progress, candidate_attempts[-1], article)
                )
                raw_articles.append(article)
                attempts.extend(candidate_attempts)
                continue

            candidate_attempts.append(
                _build_attempt(
                    candidate_id=in_progress.candidate_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                    fetch_status=FetchStatus.FAILED,
                    source_adapter_name=source_name,
                    error_code="candidate_not_found",
                    error_message=f"{source_name} adapter did not return the candidate URL",
                )
            )
        else:
            finished_at = datetime.now(UTC)
            candidate_attempts.append(
                _build_attempt(
                    candidate_id=in_progress.candidate_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                    fetch_status=FetchStatus.UNSUPPORTED,
                    error_code="source_adapter_unavailable",
                    error_message="No source adapter available for candidate source hints",
                )
            )

        generic_started_at = datetime.now(UTC)
        generic_finished_at = datetime.now(UTC)
        generic_error_message = "generic fetch fallback not implemented yet"
        candidate_attempts.append(
            _build_attempt(
                candidate_id=in_progress.candidate_id,
                started_at=generic_started_at,
                finished_at=generic_finished_at,
                attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                fetch_status=FetchStatus.UNSUPPORTED,
                error_code="generic_fetch_not_implemented",
                error_message=generic_error_message,
            )
        )

        if source_name is None:
            status = CandidateStatus.UNSUPPORTED_SOURCE
            error_code = "unsupported_source"
            error_message = "No supported source adapter or generic fetch path is available"
        else:
            status = CandidateStatus.SCRAPE_FAILED
            error_code = "generic_fetch_not_implemented"
            error_message = generic_error_message

        updated_candidates.append(
            _mark_attempt_failure(
                in_progress,
                attempts=candidate_attempts,
                status=status,
                error_code=error_code,
                error_message=error_message,
                config=config,
            )
        )
        attempts.extend(candidate_attempts)
        errors.append(f"{in_progress.candidate_id}: {error_message}")

    persistence.append_attempts(attempts)
    persistence.upsert_candidates(updated_candidates)
    return CandidateScrapeBatch(
        selected_candidates=queued_candidates,
        updated_candidates=updated_candidates,
        attempts=attempts,
        raw_articles=raw_articles,
        errors=errors,
    )
