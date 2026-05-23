"""Brave Search discovery adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import httpx
from pydantic import HttpUrl

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.models import (
    DiscoveredCandidate,
    DiscoveryQuery,
    DiscoveryQueryKind,
    ProducerKind,
)
from denbust.news_items.normalize import canonicalize_news_url

# Brave returns HTTP 422 when the query string grows too long from many -site:
# operators.  We cap the number of excluded domains and prioritise the highest-
# traffic ones so the cap never silently drops the most impactful exclusions.
_BRAVE_MAX_EXCLUDED_DOMAINS: int = 15

# Domains sorted from most to least important to exclude.  Any domain that
# appears here is promoted to the front of the exclusion list before capping;
# the remainder are appended alphabetically.
_BRAVE_EXCLUDED_DOMAIN_PRIORITY: tuple[str, ...] = (
    "he.wikipedia.org",
    "themarker.com",
    "globes.co.il",
    "calcalist.co.il",
    "kikar.co.il",
    "srugim.co.il",
    "sport1.maariv.co.il",
    "collab.mako.co.il",
    "nevo.co.il",
    "bizportal.co.il",
    "he.wikiquote.org",
    "kolzchut.org.il",
)


def _brave_excluded_domains(domains: list[str]) -> list[str]:
    """Return *domains* sorted by exclusion priority, capped at the Brave limit."""
    priority_index = {d: i for i, d in enumerate(_BRAVE_EXCLUDED_DOMAIN_PRIORITY)}
    sorted_domains = sorted(
        domains,
        key=lambda d: (priority_index.get(d, len(_BRAVE_EXCLUDED_DOMAIN_PRIORITY)), d),
    )
    return sorted_domains[:_BRAVE_MAX_EXCLUDED_DOMAINS]


class BraveSearchEngine:
    """Brave Search API adapter for discovery candidates."""

    def __init__(
        self,
        *,
        api_key: str,
        max_results_per_query: int = 20,
        client: httpx.AsyncClient | None = None,
        base_url: str = "https://api.search.brave.com/res/v1/web/search",
    ) -> None:
        self._api_key = api_key
        self._max_results_per_query = max_results_per_query
        self._base_url = base_url
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None

    @property
    def name(self) -> str:
        return "brave"

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def discover(
        self,
        queries: list[DiscoveryQuery],
        context: DiscoveryContext,
    ) -> list[DiscoveredCandidate]:
        candidates: list[DiscoveredCandidate] = []
        for query in queries:
            response = await self._client.get(
                self._base_url,
                headers={
                    "Accept": "application/json",
                    "X-Subscription-Token": self._api_key,
                },
                params={
                    "q": self._render_query(query),
                    "count": min(
                        context.max_results_per_query or self._max_results_per_query,
                        self._max_results_per_query,
                    ),
                    "search_lang": query.language or "he",
                    **_freshness_params(query),
                },
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                continue
            web = payload.get("web", {})
            if not isinstance(web, dict):
                continue
            results = web.get("results", [])
            if not isinstance(results, list):
                continue
            for index, result in enumerate(results, start=1):
                candidate = self._result_to_candidate(result, query=query, rank=index)
                if candidate is not None:
                    candidates.append(candidate)
        return candidates

    def _render_query(self, query: DiscoveryQuery) -> str:
        parts: list[str] = []
        if query.preferred_domains:
            site_filters = " OR ".join(f"site:{domain}" for domain in query.preferred_domains)
            parts.append(f"({site_filters})")
        parts.append(query.query_text)
        # Brave has no native excludeDomains parameter; use -site: operators
        # instead.  Only applied when the query is not already scoped to a
        # preferred domain (source-targeted queries don't need it).
        if query.excluded_domains and not query.preferred_domains:
            for domain in _brave_excluded_domains(query.excluded_domains):
                parts.append(f"-site:{domain}")
        return " ".join(parts)

    def _result_to_candidate(
        self,
        result: Any,
        *,
        query: DiscoveryQuery,
        rank: int,
    ) -> DiscoveredCandidate | None:
        if not isinstance(result, dict):
            return None
        url = result.get("url")
        if not isinstance(url, str) or not url:
            return None
        if not _matches_preferred_domains(url, query.preferred_domains):
            return None
        publication_datetime_hint: datetime | None = None
        page_age = result.get("page_age")
        if isinstance(page_age, str):
            normalized = page_age.replace("Z", "+00:00")
            try:
                publication_datetime_hint = _normalize_datetime(datetime.fromisoformat(normalized))
            except ValueError:
                publication_datetime_hint = None
        if not _matches_query_date_window(publication_datetime_hint, query):
            return None
        try:
            parsed_url = HttpUrl(url)
            canonical_url = HttpUrl(canonicalize_news_url(url))
            return DiscoveredCandidate(
                producer_name=self.name,
                producer_kind=ProducerKind.SEARCH_ENGINE,
                query_text=query.query_text,
                candidate_url=parsed_url,
                canonical_url=canonical_url,
                title=result.get("title"),
                snippet=result.get("description"),
                publication_datetime_hint=publication_datetime_hint,
                rank=rank,
                source_hint=query.source_hint,
                metadata={
                    "engine": self.name,
                    "query_kind": query.query_kind.value,
                    "query_tags": query.tags,
                    "source_targeted_taxonomy": (
                        query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
                        and "taxonomy" in query.tags
                    ),
                    "preferred_domains": query.preferred_domains,
                    "result_url": url,
                    "result_title": result.get("title"),
                    "result_description": result.get("description"),
                    "result_page_age": result.get("page_age"),
                    "result_age": result.get("age"),
                },
            )
        except ValueError:
            return None


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _freshness_params(query: DiscoveryQuery) -> dict[str, str]:
    if not _is_backfill_query(query) or query.date_from is None or query.date_to is None:
        return {}
    date_from = _normalize_datetime(query.date_from).date().isoformat()
    date_to = _normalize_datetime(query.date_to).date().isoformat()
    return {"freshness": f"{date_from}to{date_to}"}


def _matches_query_date_window(
    publication_datetime_hint: datetime | None,
    query: DiscoveryQuery,
) -> bool:
    if publication_datetime_hint is None or not _is_backfill_query(query):
        return True
    hint = _normalize_datetime(publication_datetime_hint)
    if query.date_from is not None and hint < _normalize_datetime(query.date_from):
        return False
    return not (query.date_to is not None and hint > _normalize_datetime(query.date_to))


def _is_backfill_query(query: DiscoveryQuery) -> bool:
    return "backfill" in query.tags


def _matches_preferred_domains(url: str, preferred_domains: list[str]) -> bool:
    if not preferred_domains:
        return True
    result_domain = _normalize_domain(urlparse(url).netloc)
    if not result_domain:
        return False
    return any(
        result_domain == preferred_domain or result_domain.endswith(f".{preferred_domain}")
        for preferred_domain in {_normalize_domain(domain) for domain in preferred_domains}
    )


def _normalize_domain(domain: str) -> str:
    normalized = domain.lower().strip().removeprefix("www.")
    return normalized
