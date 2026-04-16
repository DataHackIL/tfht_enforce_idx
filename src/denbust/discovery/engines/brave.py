"""Brave Search discovery adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
from pydantic import HttpUrl

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.models import DiscoveredCandidate, DiscoveryQuery, ProducerKind


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
                },
            )
            response.raise_for_status()
            payload = response.json()
            results = payload.get("web", {}).get("results", [])
            if not isinstance(results, list):
                continue
            for index, result in enumerate(results, start=1):
                candidate = self._result_to_candidate(result, query=query, rank=index)
                if candidate is not None:
                    candidates.append(candidate)
        return candidates

    def _render_query(self, query: DiscoveryQuery) -> str:
        if not query.preferred_domains:
            return query.query_text
        site_filters = " OR ".join(f"site:{domain}" for domain in query.preferred_domains)
        return f"({site_filters}) {query.query_text}"

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
        publication_datetime_hint: datetime | None = None
        page_age = result.get("page_age")
        if isinstance(page_age, str):
            normalized = page_age.replace("Z", "+00:00")
            try:
                publication_datetime_hint = datetime.fromisoformat(normalized)
            except ValueError:
                publication_datetime_hint = None
        parsed_url = HttpUrl(url)
        return DiscoveredCandidate(
            producer_name=self.name,
            producer_kind=ProducerKind.SEARCH_ENGINE,
            query_text=query.query_text,
            candidate_url=parsed_url,
            canonical_url=parsed_url,
            title=result.get("title"),
            snippet=result.get("description"),
            publication_datetime_hint=publication_datetime_hint,
            rank=rank,
            source_hint=query.source_hint,
            metadata={
                "engine": self.name,
                "query_kind": query.query_kind.value,
                "preferred_domains": query.preferred_domains,
                "raw_result": result,
            },
        )
