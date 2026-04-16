"""Google Custom Search JSON API discovery adapter."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

import httpx
from pydantic import HttpUrl

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.models import DiscoveredCandidate, DiscoveryQuery, ProducerKind
from denbust.news_items.normalize import canonicalize_news_url


class GoogleCseSearchEngine:
    """Google Custom Search JSON API adapter for discovery candidates."""

    def __init__(
        self,
        *,
        api_key: str,
        cse_id: str,
        max_results_per_query: int = 10,
        client: httpx.AsyncClient | None = None,
        base_url: str = "https://customsearch.googleapis.com/customsearch/v1",
    ) -> None:
        self._api_key = api_key
        self._cse_id = cse_id
        self._max_results_per_query = max_results_per_query
        self._base_url = base_url
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None

    @property
    def name(self) -> str:
        return "google_cse"

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
                params=self._build_params(query, context),
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                continue
            items = payload.get("items", [])
            if not isinstance(items, list):
                continue
            search_information = payload.get("searchInformation")
            total_results = None
            if isinstance(search_information, dict):
                total_results_value = search_information.get("totalResults")
                if isinstance(total_results_value, str):
                    total_results = total_results_value
            for index, item in enumerate(items, start=1):
                candidate = self._result_to_candidate(
                    item,
                    query=query,
                    rank=index,
                    total_results=total_results,
                )
                if candidate is not None:
                    candidates.append(candidate)
        return candidates

    def _build_params(
        self,
        query: DiscoveryQuery,
        context: DiscoveryContext,
    ) -> Mapping[str, str | int]:
        params: dict[str, str | int] = {
            "key": self._api_key,
            "cx": self._cse_id,
            "q": query.query_text,
            "num": min(
                context.max_results_per_query or self._max_results_per_query,
                self._max_results_per_query,
            ),
        }
        if query.language == "he":
            params["lr"] = "lang_he"
        if len(query.preferred_domains) == 1:
            params["siteSearch"] = query.preferred_domains[0]
            params["siteSearchFilter"] = "i"
        return params

    def _result_to_candidate(
        self,
        result: Any,
        *,
        query: DiscoveryQuery,
        rank: int,
        total_results: str | None,
    ) -> DiscoveredCandidate | None:
        if not isinstance(result, dict):
            return None
        url = result.get("link")
        if not isinstance(url, str) or not url:
            return None
        try:
            parsed_url = HttpUrl(url)
            canonical_url = HttpUrl(canonicalize_news_url(url))
            title = result.get("title")
            snippet = result.get("snippet")
            publication_datetime_hint = _extract_google_publication_datetime(result)
            return DiscoveredCandidate(
                producer_name=self.name,
                producer_kind=ProducerKind.SEARCH_ENGINE,
                query_text=query.query_text,
                candidate_url=parsed_url,
                canonical_url=canonical_url,
                title=title if isinstance(title, str) else None,
                snippet=snippet if isinstance(snippet, str) else None,
                publication_datetime_hint=publication_datetime_hint,
                rank=rank,
                source_hint=query.source_hint,
                metadata={
                    "engine": self.name,
                    "query_kind": query.query_kind.value,
                    "preferred_domains": query.preferred_domains,
                    "result_url": url,
                    "result_title": title if isinstance(title, str) else None,
                    "result_snippet": snippet if isinstance(snippet, str) else None,
                    "result_display_link": (
                        result.get("displayLink")
                        if isinstance(result.get("displayLink"), str)
                        else None
                    ),
                    "result_cache_id": (
                        result.get("cacheId") if isinstance(result.get("cacheId"), str) else None
                    ),
                    "total_results": total_results,
                },
            )
        except ValueError:
            return None


def _extract_google_publication_datetime(result: dict[str, Any]) -> datetime | None:
    """Extract a best-effort publication hint from Google result metadata."""
    pagemap = result.get("pagemap")
    if not isinstance(pagemap, dict):
        return None
    metatags = pagemap.get("metatags")
    if not isinstance(metatags, list):
        return None
    for metatag in metatags:
        if not isinstance(metatag, dict):
            continue
        for key in (
            "article:published_time",
            "og:published_time",
            "article:modified_time",
            "og:updated_time",
        ):
            value = metatag.get(key)
            if not isinstance(value, str):
                continue
            normalized = value.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(normalized)
            except ValueError:
                continue
    return None
