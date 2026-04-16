"""Exa search discovery adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
from pydantic import HttpUrl

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.models import DiscoveredCandidate, DiscoveryQuery, ProducerKind
from denbust.news_items.normalize import canonicalize_news_url


class ExaSearchEngine:
    """Exa Search API adapter for discovery candidates."""

    def __init__(
        self,
        *,
        api_key: str,
        max_results_per_query: int = 20,
        client: httpx.AsyncClient | None = None,
        base_url: str = "https://api.exa.ai/search",
    ) -> None:
        self._api_key = api_key
        self._max_results_per_query = max_results_per_query
        self._base_url = base_url
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None

    @property
    def name(self) -> str:
        return "exa"

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
            response = await self._client.post(
                self._base_url,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "x-api-key": self._api_key,
                },
                json=self._build_payload(query, context),
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                continue
            results = payload.get("results", [])
            if not isinstance(results, list):
                continue
            request_id = payload.get("requestId")
            for index, result in enumerate(results, start=1):
                candidate = self._result_to_candidate(
                    result,
                    query=query,
                    rank=index,
                    request_id=request_id if isinstance(request_id, str) else None,
                )
                if candidate is not None:
                    candidates.append(candidate)
        return candidates

    def _build_payload(self, query: DiscoveryQuery, context: DiscoveryContext) -> dict[str, object]:
        payload: dict[str, object] = {
            "query": query.query_text,
            "type": "auto",
            "numResults": min(
                context.max_results_per_query or self._max_results_per_query,
                self._max_results_per_query,
            ),
        }
        if query.date_from is not None:
            payload["startPublishedDate"] = query.date_from.date().isoformat()
        if query.date_to is not None:
            payload["endPublishedDate"] = query.date_to.date().isoformat()
        if query.preferred_domains:
            payload["includeDomains"] = query.preferred_domains
        if query.excluded_domains:
            payload["excludeDomains"] = query.excluded_domains
        return payload

    def _result_to_candidate(
        self,
        result: Any,
        *,
        query: DiscoveryQuery,
        rank: int,
        request_id: str | None,
    ) -> DiscoveredCandidate | None:
        if not isinstance(result, dict):
            return None
        url = result.get("url")
        if not isinstance(url, str) or not url:
            return None
        publication_datetime_hint: datetime | None = None
        published_date = result.get("publishedDate")
        if isinstance(published_date, str):
            normalized = published_date.replace("Z", "+00:00")
            try:
                publication_datetime_hint = datetime.fromisoformat(normalized)
            except ValueError:
                publication_datetime_hint = None
        try:
            parsed_url = HttpUrl(url)
            canonical_url = HttpUrl(canonicalize_news_url(url))
            title = result.get("title")
            snippet = result.get("text")
            if not isinstance(snippet, str) or not snippet:
                snippet = None
            if not snippet:
                highlights = result.get("highlights")
                if isinstance(highlights, list):
                    snippet = next(
                        (value for value in highlights if isinstance(value, str) and value),
                        None,
                    )
            return DiscoveredCandidate(
                producer_name=self.name,
                producer_kind=ProducerKind.SEARCH_ENGINE,
                query_text=query.query_text,
                candidate_url=parsed_url,
                canonical_url=canonical_url,
                title=title if isinstance(title, str) else None,
                snippet=snippet,
                publication_datetime_hint=publication_datetime_hint,
                rank=rank,
                source_hint=query.source_hint,
                metadata={
                    "engine": self.name,
                    "query_kind": query.query_kind.value,
                    "preferred_domains": query.preferred_domains,
                    "request_id": request_id,
                    "result_id": result.get("id"),
                    "result_url": url,
                    "result_title": title if isinstance(title, str) else None,
                    "result_published_date": published_date
                    if isinstance(published_date, str)
                    else None,
                    "result_author": result.get("author")
                    if isinstance(result.get("author"), str)
                    else None,
                },
            )
        except ValueError:
            return None
