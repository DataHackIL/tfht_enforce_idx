"""Source-native candidacy adapters and persistence helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.discovery.base import SourceCandidateProducer, SourceDiscoveryContext
from denbust.discovery.models import (
    CandidateProvenance,
    CandidateStatus,
    ContentBasis,
    DiscoveredCandidate,
    DiscoveryRun,
    DiscoveryRunStatus,
    PersistentCandidate,
    ProducerKind,
)
from denbust.discovery.storage import DiscoveryPersistence
from denbust.news_items.normalize import canonicalize_news_url, deduplicate_strings
from denbust.sources.base import HistoricalSource, Source

_SOCIAL_DISCOVERY_DOMAINS = {"www.facebook.com"}


def _normalize_discovered_candidate(discovered: DiscoveredCandidate) -> DiscoveredCandidate:
    """Normalize search-engine social results into the social-search producer family."""
    query_kind = discovered.metadata.get("query_kind")
    if query_kind == "social_targeted" and discovered.producer_kind is ProducerKind.SEARCH_ENGINE:
        return discovered.model_copy(update={"producer_kind": ProducerKind.SOCIAL_SEARCH})
    return discovered


def _is_social_candidate(discovered: DiscoveredCandidate) -> bool:
    return discovered.producer_kind is ProducerKind.SOCIAL_SEARCH


def build_candidate_id(identity_url: str) -> str:
    """Build a deterministic candidate identifier from a stable URL identity."""
    digest = hashlib.sha256(identity_url.encode("utf-8")).hexdigest()
    return f"candidate_{digest[:24]}"


def raw_article_to_discovered_candidate(
    article: RawArticle,
    *,
    discovered_at: datetime | None = None,
) -> DiscoveredCandidate:
    """Normalize a fetched source article into a source-native candidate."""
    canonical_url = canonicalize_news_url(str(article.url))
    return DiscoveredCandidate(
        producer_name=article.source_name,
        producer_kind=ProducerKind.SOURCE_NATIVE,
        candidate_url=article.url,
        canonical_url=cast(HttpUrl, canonical_url),
        title=article.title,
        snippet=article.snippet,
        discovered_at=discovered_at or datetime.now(UTC),
        publication_datetime_hint=article.date,
        source_hint=article.source_name,
        metadata={"source_name": article.source_name},
    )


class SourceDiscoveryAdapter(SourceCandidateProducer):
    """Adapt an existing `Source` into the discovery producer interface."""

    def __init__(self, source: Source) -> None:
        self._source = source

    @property
    def name(self) -> str:
        return self._source.name

    async def discover_candidates(
        self,
        context: SourceDiscoveryContext,
    ) -> list[DiscoveredCandidate]:
        if context.days is None:
            raise ValueError("SourceDiscoveryContext.days is required for source-native discovery")
        articles = await self._source.fetch(days=context.days, keywords=context.keywords)
        return [
            raw_article_to_discovered_candidate(
                article,
                discovered_at=context.metadata.get("discovered_at"),
            )
            for article in articles
        ]

    @property
    def supports_historical_window(self) -> bool:
        """Return whether the wrapped source supports explicit historical windows."""
        return isinstance(self._source, HistoricalSource)

    async def discover_candidates_for_window(
        self,
        context: SourceDiscoveryContext,
    ) -> list[DiscoveredCandidate]:
        """Discover candidates for one explicit historical window when supported."""
        if not self.supports_historical_window:
            raise ValueError(f"{self.name} does not support historical windows")
        if context.date_from is None or context.date_to is None:
            raise ValueError("SourceDiscoveryContext.date_from/date_to are required")
        historical_source = cast(HistoricalSource, self._source)
        articles = await historical_source.fetch_window(
            date_from=context.date_from,
            date_to=context.date_to,
            keywords=context.keywords,
        )
        return [
            raw_article_to_discovered_candidate(
                article,
                discovered_at=context.metadata.get("discovered_at"),
            )
            for article in articles
        ]


def merge_discovered_candidate(
    discovered: DiscoveredCandidate,
    existing: PersistentCandidate | None,
) -> PersistentCandidate:
    """Merge a discovered candidate into a durable candidate row."""
    identity_url = str(discovered.canonical_url or discovered.candidate_url)
    existing_metadata = dict(existing.metadata) if existing is not None else {}
    if discovered.publication_datetime_hint is not None:
        existing_metadata["latest_publication_datetime_hint"] = (
            discovered.publication_datetime_hint.isoformat()
        )
    backfill_batch_id = None
    if "backfill_batch_id" in discovered.metadata:
        backfill_batch_id = str(discovered.metadata["backfill_batch_id"])
    for key in ("backfill_window_index", "backfill_window_start", "backfill_window_end"):
        if key in discovered.metadata:
            existing_metadata[key] = discovered.metadata[key]
    if discovered.metadata:
        existing_metadata["latest_discovery_metadata"] = discovered.metadata
    is_social = _is_social_candidate(discovered)
    candidate_status = existing.candidate_status if existing else CandidateStatus.NEW
    if is_social and existing is None:
        candidate_status = CandidateStatus.UNSUPPORTED_SOURCE

    return PersistentCandidate(
        candidate_id=existing.candidate_id
        if existing is not None
        else build_candidate_id(identity_url),
        canonical_url=discovered.canonical_url or (existing.canonical_url if existing else None),
        current_url=discovered.candidate_url,
        domain=discovered.domain or (existing.domain if existing else None),
        titles=deduplicate_strings(
            [*(existing.titles if existing else []), discovered.title or ""]
        ),
        snippets=deduplicate_strings(
            [*(existing.snippets if existing else []), discovered.snippet or ""]
        ),
        discovered_via=deduplicate_strings(
            [*(existing.discovered_via if existing else []), discovered.producer_name]
        ),
        discovery_queries=deduplicate_strings(
            [*(existing.discovery_queries if existing else []), discovered.query_text or ""]
        ),
        source_hints=deduplicate_strings(
            [
                *(existing.source_hints if existing else []),
                discovered.source_hint or discovered.producer_name,
            ]
        ),
        first_seen_at=min(
            [
                value
                for value in [
                    existing.first_seen_at if existing else None,
                    discovered.discovered_at,
                ]
                if value is not None
            ]
        ),
        last_seen_at=max(
            [
                value
                for value in [existing.last_seen_at if existing else None, discovered.discovered_at]
                if value is not None
            ]
        ),
        candidate_status=candidate_status,
        scrape_attempt_count=existing.scrape_attempt_count if existing else 0,
        last_scrape_attempt_at=existing.last_scrape_attempt_at if existing else None,
        next_scrape_attempt_at=existing.next_scrape_attempt_at if existing else None,
        last_scrape_error_code=existing.last_scrape_error_code if existing else None,
        last_scrape_error_message=existing.last_scrape_error_message if existing else None,
        content_basis=existing.content_basis if existing else ContentBasis.CANDIDATE_ONLY,
        retry_priority=existing.retry_priority if existing else 0,
        needs_review=(existing.needs_review if existing else False) or is_social,
        backfill_batch_id=backfill_batch_id or (existing.backfill_batch_id if existing else None),
        self_heal_eligible=existing.self_heal_eligible if existing else False,
        source_discovery_only=(
            (existing.source_discovery_only if existing else True)
            and discovered.producer_kind is ProducerKind.SOURCE_NATIVE
        ),
        metadata=existing_metadata,
    )


@dataclass
class PersistedSourceDiscovery:
    """Result bundle from persisting source-native discovery output."""

    run: DiscoveryRun
    candidates: list[PersistentCandidate]
    provenance: list[CandidateProvenance]
    warnings: list[str] | None = None


def persist_discovered_candidates(
    *,
    run: DiscoveryRun,
    discovered_candidates: list[DiscoveredCandidate],
    persistence: DiscoveryPersistence,
) -> PersistedSourceDiscovery:
    """Merge, persist, and provenance-track discovered candidates."""
    merged_candidates: dict[str, PersistentCandidate] = {}
    identity_map: dict[str, str] = {}
    provenance_events: list[CandidateProvenance] = []

    for discovered in discovered_candidates:
        discovered = _normalize_discovered_candidate(discovered)
        canonical_url = (
            str(discovered.canonical_url) if discovered.canonical_url is not None else None
        )
        current_url = str(discovered.candidate_url)
        identity_keys = [value for value in [canonical_url, current_url] if value]
        existing: PersistentCandidate | None = None
        for identity_key in identity_keys:
            candidate_id = identity_map.get(identity_key)
            if candidate_id is not None:
                existing = merged_candidates[candidate_id]
                break
        if existing is None:
            existing = persistence.find_candidate_by_urls(
                canonical_url=canonical_url,
                current_url=current_url,
            )

        merged = merge_discovered_candidate(discovered, existing)
        merged_candidates[merged.candidate_id] = merged
        for identity_key in identity_keys:
            identity_map[identity_key] = merged.candidate_id

        provenance_events.append(
            CandidateProvenance(
                run_id=run.run_id,
                candidate_id=merged.candidate_id,
                producer_name=discovered.producer_name,
                producer_kind=discovered.producer_kind,
                query_text=discovered.query_text,
                raw_url=discovered.candidate_url,
                normalized_url=discovered.canonical_url,
                title=discovered.title,
                snippet=discovered.snippet,
                publication_datetime_hint=discovered.publication_datetime_hint,
                rank=discovered.rank,
                domain=discovered.domain,
                discovered_at=discovered.discovered_at,
                metadata=discovered.metadata,
            )
        )

    persisted_candidates = list(merged_candidates.values())
    run.candidate_count = len(discovered_candidates)
    run.merged_candidate_count = len(persisted_candidates)
    run.queued_for_scrape_count = 0
    try:
        persistence.upsert_candidates(persisted_candidates)
        persistence.append_provenance(provenance_events)
    except Exception as exc:
        run.errors.append(f"persistence: {type(exc).__name__}: {exc}")
        run.status = DiscoveryRunStatus.FAILED
        run.finished_at = datetime.now(UTC)
        persistence.write_run(run)
        raise

    if run.errors:
        run.status = (
            DiscoveryRunStatus.FAILED if not discovered_candidates else DiscoveryRunStatus.PARTIAL
        )
    else:
        run.status = DiscoveryRunStatus.SUCCEEDED
    run.finished_at = datetime.now(UTC)
    persistence.write_run(run)
    return PersistedSourceDiscovery(
        run=run,
        candidates=persisted_candidates,
        provenance=provenance_events,
    )
