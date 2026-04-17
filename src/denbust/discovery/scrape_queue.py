"""Candidate selection and scrape-attempt orchestration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

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
    fallback_candidates: list[PersistentCandidate]
    attempts: list[ScrapeAttempt]
    raw_articles: list[RawArticle]
    errors: list[str]


@dataclass(frozen=True)
class GenericFetchResult:
    """Minimal metadata recovered from a candidate page fetch."""

    fetch_status: FetchStatus
    title: str | None = None
    snippet: str | None = None
    publication_datetime: datetime | None = None
    final_url: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    diagnostics: dict[str, object] | None = None


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
        if candidate.next_scrape_attempt_at is None
        or candidate.next_scrape_attempt_at <= current_time
    ]
    max_datetime = datetime.max.replace(tzinfo=UTC)
    ordered = sorted(
        eligible,
        key=lambda candidate: (
            -candidate.retry_priority,
            0 if candidate.next_scrape_attempt_at is None else 1,
            candidate.next_scrape_attempt_at or max_datetime,
            -candidate.last_seen_at.timestamp(),
            candidate.candidate_id,
        ),
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
            "content_basis": ContentBasis.FULL_ARTICLE_PAGE,
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
    content_basis: ContentBasis | None = None,
    needs_review: bool | None = None,
    metadata_updates: dict[str, object] | None = None,
) -> PersistentCandidate:
    final_attempt = attempts[-1]
    finished_at = final_attempt.finished_at or final_attempt.started_at
    retry_at = None
    if (
        config.candidates.allow_retry_on_fetch_failure
        and status is not CandidateStatus.UNSUPPORTED_SOURCE
    ):
        retry_at = finished_at + timedelta(hours=config.candidates.default_retry_backoff_hours)
    return candidate.model_copy(
        update={
            "candidate_status": status,
            "scrape_attempt_count": candidate.scrape_attempt_count + len(attempts),
            "last_scrape_attempt_at": finished_at,
            "next_scrape_attempt_at": retry_at,
            "last_scrape_error_code": error_code,
            "last_scrape_error_message": error_message,
            "content_basis": content_basis or candidate.content_basis,
            "needs_review": (
                candidate.needs_review
                or status is CandidateStatus.UNSUPPORTED_SOURCE
                or bool(needs_review)
            ),
            "self_heal_eligible": status is CandidateStatus.SCRAPE_FAILED,
            "metadata": {
                **candidate.metadata,
                **(metadata_updates or {}),
            },
        }
    )


def _mark_partial_recovery(
    candidate: PersistentCandidate,
    *,
    attempts: list[ScrapeAttempt],
    attempt: ScrapeAttempt,
    fetch_result: GenericFetchResult,
    source_name: str | None,
) -> PersistentCandidate:
    return candidate.model_copy(
        update={
            "candidate_status": CandidateStatus.PARTIALLY_SCRAPED,
            "scrape_attempt_count": candidate.scrape_attempt_count + len(attempts),
            "last_scrape_attempt_at": attempt.finished_at,
            "next_scrape_attempt_at": None,
            "last_scrape_error_code": None,
            "last_scrape_error_message": None,
            "content_basis": ContentBasis.PARTIAL_PAGE,
            "needs_review": True,
            "self_heal_eligible": False,
            "titles": deduplicate_strings([*candidate.titles, fetch_result.title or ""]),
            "snippets": deduplicate_strings([*candidate.snippets, fetch_result.snippet or ""]),
            "metadata": {
                **candidate.metadata,
                **_fallback_metadata(
                    candidate=candidate,
                    content_basis=ContentBasis.PARTIAL_PAGE,
                    title=fetch_result.title,
                    snippet=fetch_result.snippet,
                    publication_datetime=fetch_result.publication_datetime,
                    source_name=source_name,
                    fetch_result=fetch_result,
                ),
            },
        }
    )


def _fallback_metadata(
    *,
    candidate: PersistentCandidate,
    content_basis: ContentBasis,
    title: str | None,
    snippet: str | None,
    publication_datetime: datetime | None,
    source_name: str | None,
    fetch_result: GenericFetchResult,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "fallback_title": title or next(iter(candidate.titles), None),
        "fallback_snippet": snippet or next(iter(candidate.snippets), None),
        "fallback_source_name": source_name
        or next(iter(candidate.source_hints), None)
        or next(iter(candidate.discovered_via), None),
        "fallback_content_basis": content_basis.value,
    }
    if publication_datetime is not None:
        payload["fallback_publication_datetime"] = publication_datetime.isoformat()
    if fetch_result.final_url is not None:
        payload["fallback_final_url"] = fetch_result.final_url
    if fetch_result.diagnostics:
        payload["fallback_diagnostics"] = dict(fetch_result.diagnostics)
    return payload


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
        return CandidateScrapeBatch([], [], [], [], [], [])

    queued_candidates = queue_candidates_for_scrape(candidates)
    persistence.upsert_candidates(queued_candidates)

    source_articles_cache = dict(preloaded_source_articles or {})
    sources_by_name = {source.name: source for source in sources}
    updated_candidates: list[PersistentCandidate] = []
    fallback_candidates: list[PersistentCandidate] = []
    attempts: list[ScrapeAttempt] = []
    raw_articles: list[RawArticle] = []
    errors: list[str] = []

    headers = {"User-Agent": "denbust-discovery/1.0"}
    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers=headers,
    ) as client:
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
                try:
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
                except Exception as exc:
                    finished_at = datetime.now(UTC)
                    error_message = f"{source_name} adapter failed: {type(exc).__name__}: {exc}"
                    candidate_attempts.append(
                        _build_attempt(
                            candidate_id=in_progress.candidate_id,
                            started_at=started_at,
                            finished_at=finished_at,
                            attempt_kind=ScrapeAttemptKind.SOURCE_ADAPTER,
                            fetch_status=FetchStatus.FAILED,
                            source_adapter_name=source_name,
                            error_code="source_adapter_error",
                            error_message=error_message,
                        )
                    )
                    updated_candidates.append(
                        _mark_attempt_failure(
                            in_progress,
                            attempts=candidate_attempts,
                            status=CandidateStatus.SCRAPE_FAILED,
                            error_code="source_adapter_error",
                            error_message=error_message,
                            config=config,
                        )
                    )
                    attempts.extend(candidate_attempts)
                    errors.append(f"{in_progress.candidate_id}: {error_message}")
                    continue

            generic_started_at = datetime.now(UTC)
            fetch_result = await _fetch_partial_page(in_progress, client=client)
            generic_finished_at = datetime.now(UTC)
            candidate_attempts.append(
                _build_attempt(
                    candidate_id=in_progress.candidate_id,
                    started_at=generic_started_at,
                    finished_at=generic_finished_at,
                    attempt_kind=ScrapeAttemptKind.GENERIC_FETCH,
                    fetch_status=fetch_result.fetch_status,
                    error_code=fetch_result.error_code,
                    error_message=fetch_result.error_message,
                    diagnostics=fetch_result.diagnostics,
                )
            )

            if fetch_result.fetch_status is FetchStatus.PARTIAL and (
                fetch_result.title or fetch_result.snippet
            ):
                partial_candidate = _mark_partial_recovery(
                    in_progress,
                    attempts=candidate_attempts,
                    attempt=candidate_attempts[-1],
                    fetch_result=fetch_result,
                    source_name=source_name,
                )
                updated_candidates.append(partial_candidate)
                fallback_candidates.append(partial_candidate)
                attempts.extend(candidate_attempts)
                continue

            fallback_candidate = _mark_attempt_failure(
                in_progress,
                attempts=candidate_attempts,
                status=CandidateStatus.SCRAPE_FAILED,
                error_code=fetch_result.error_code or "generic_fetch_failed",
                error_message=fetch_result.error_message or "Generic fetch failed",
                config=config,
                content_basis=ContentBasis.SEARCH_RESULT_ONLY,
                needs_review=True,
                metadata_updates=_fallback_metadata(
                    candidate=in_progress,
                    content_basis=ContentBasis.SEARCH_RESULT_ONLY,
                    title=fetch_result.title,
                    snippet=fetch_result.snippet,
                    publication_datetime=fetch_result.publication_datetime,
                    source_name=source_name,
                    fetch_result=fetch_result,
                ),
            )
            updated_candidates.append(fallback_candidate)
            fallback_candidates.append(fallback_candidate)
            attempts.extend(candidate_attempts)
            errors.append(
                f"{in_progress.candidate_id}: "
                f"{fetch_result.error_message or 'Generic fetch failed; retained search-result-only fallback'}"
            )

    persistence.append_attempts(attempts)
    persistence.upsert_candidates(updated_candidates)
    return CandidateScrapeBatch(
        selected_candidates=queued_candidates,
        updated_candidates=updated_candidates,
        fallback_candidates=fallback_candidates,
        attempts=attempts,
        raw_articles=raw_articles,
        errors=errors,
    )


async def _fetch_partial_page(
    candidate: PersistentCandidate,
    *,
    client: httpx.AsyncClient,
) -> GenericFetchResult:
    try:
        response = await client.get(str(candidate.current_url))
        response.raise_for_status()
    except httpx.TimeoutException as exc:
        return GenericFetchResult(
            fetch_status=FetchStatus.TIMEOUT,
            error_code="generic_fetch_timeout",
            error_message=f"Generic fetch timed out: {exc}",
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        return GenericFetchResult(
            fetch_status=FetchStatus.BLOCKED
            if status_code in {401, 403, 429}
            else FetchStatus.FAILED,
            error_code=f"generic_fetch_http_{status_code}",
            error_message=f"Generic fetch returned HTTP {status_code}",
        )
    except httpx.HTTPError as exc:
        return GenericFetchResult(
            fetch_status=FetchStatus.FAILED,
            error_code="generic_fetch_error",
            error_message=f"Generic fetch failed: {type(exc).__name__}: {exc}",
        )

    parsed = _extract_partial_page_metadata(response.text)
    if parsed["title"] or parsed["snippet"]:
        return GenericFetchResult(
            fetch_status=FetchStatus.PARTIAL,
            title=parsed["title"],
            snippet=parsed["snippet"],
            publication_datetime=parsed["publication_datetime"],
            final_url=str(response.url),
            diagnostics={"content_type": response.headers.get("content-type", "")},
        )
    return GenericFetchResult(
        fetch_status=FetchStatus.FAILED,
        error_code="generic_fetch_no_metadata",
        error_message="Generic fetch returned a page without usable metadata",
        final_url=str(response.url),
        diagnostics={"content_type": response.headers.get("content-type", "")},
    )


def _extract_partial_page_metadata(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _first_non_empty(
        soup.title.string if soup.title and soup.title.string else None,
        _meta_content(soup, property_name="og:title"),
        _meta_content(soup, name="twitter:title"),
    )
    snippet = _first_non_empty(
        _meta_content(soup, name="description"),
        _meta_content(soup, property_name="og:description"),
        _meta_content(soup, name="twitter:description"),
    )
    publication_datetime = _parse_publication_datetime(
        _first_non_empty(
            _meta_content(soup, property_name="article:published_time"),
            _meta_content(soup, name="article:published_time"),
            _meta_content(soup, name="pubdate"),
            _meta_content(soup, name="publish-date"),
            _meta_content(soup, name="date"),
        )
    )
    return {
        "title": title,
        "snippet": snippet,
        "publication_datetime": publication_datetime,
    }


def _meta_content(
    soup: BeautifulSoup,
    *,
    name: str | None = None,
    property_name: str | None = None,
) -> str | None:
    attrs: dict[str, str] = {}
    if name is not None:
        attrs["name"] = name
    if property_name is not None:
        attrs["property"] = property_name
    tag = soup.find("meta", attrs=attrs)
    if not isinstance(tag, Tag):
        return None
    value = tag.get("content")
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    return normalized or None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value is None:
            continue
        normalized = " ".join(value.split()).strip()
        if normalized:
            return normalized
    return None


def _parse_publication_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
