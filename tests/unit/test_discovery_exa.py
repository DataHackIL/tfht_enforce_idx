"""Unit tests for the Exa discovery adapter."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.engines.exa import ExaSearchEngine
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind, ProducerKind


@pytest.mark.asyncio
async def test_exa_search_engine_normalizes_results_and_renders_payload() -> None:
    """Exa responses should become normalized discovery candidates."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = str(request.url)
        captured["token"] = request.headers["x-api-key"]
        captured["payload"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "requestId": "request-123",
                "results": [
                    {
                        "id": "result-1",
                        "url": "https://www.ynet.co.il/news/article/abc?utm_source=exa",
                        "title": "פשיטה על בית בושת",
                        "publishedDate": "2026-04-15T08:00:00Z",
                        "author": "Reporter",
                        "highlights": ["המשטרה פשטה על המקום."],
                    }
                ],
            },
        )

    engine = ExaSearchEngine(
        api_key="exa-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        max_results_per_query=20,
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(
                query_text="בית בושת",
                query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                preferred_domains=["www.ynet.co.il"],
                source_hint="ynet",
                language="he",
                date_from=datetime(2026, 4, 10, tzinfo=UTC),
                date_to=datetime(2026, 4, 16, tzinfo=UTC),
            )
        ],
        DiscoveryContext(run_id="run-1", max_results_per_query=5),
    )
    await engine.aclose()

    assert captured["path"] == "https://api.exa.ai/search"
    assert captured["token"] == "exa-key"
    assert captured["payload"] == {
        "query": "בית בושת",
        "type": "auto",
        "numResults": 5,
        "startPublishedDate": "2026-04-10",
        "endPublishedDate": "2026-04-16",
        "includeDomains": ["www.ynet.co.il"],
    }
    assert len(candidates) == 1
    assert candidates[0].producer_name == "exa"
    assert candidates[0].producer_kind is ProducerKind.SEARCH_ENGINE
    assert candidates[0].source_hint == "ynet"
    assert candidates[0].rank == 1
    assert str(candidates[0].candidate_url) == "https://www.ynet.co.il/news/article/abc?utm_source=exa"
    assert str(candidates[0].canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidates[0].snippet == "המשטרה פשטה על המקום."
    assert candidates[0].publication_datetime_hint == datetime(2026, 4, 15, 8, 0, tzinfo=UTC)
    assert candidates[0].metadata == {
        "engine": "exa",
        "query_kind": "source_targeted",
        "preferred_domains": ["www.ynet.co.il"],
        "request_id": "request-123",
        "result_id": "result-1",
        "result_url": "https://www.ynet.co.il/news/article/abc?utm_source=exa",
        "result_title": "פשיטה על בית בושת",
        "result_published_date": "2026-04-15T08:00:00Z",
        "result_author": "Reporter",
    }


@pytest.mark.asyncio
async def test_exa_search_engine_aclose_closes_owned_client() -> None:
    """Owned async clients should be closed by `aclose()`."""
    engine = ExaSearchEngine(api_key="exa-key")

    await engine.aclose()

    assert engine._client.is_closed is True


@pytest.mark.asyncio
async def test_exa_search_engine_skips_malformed_payloads_and_invalid_rows() -> None:
    """Malformed payloads and invalid rows should be ignored without failing the whole call."""
    responses = iter(
        [
            httpx.Response(200, json=[]),
            httpx.Response(200, json={"results": "not-a-list"}),
            httpx.Response(
                200,
                json={
                    "results": [
                        "bad-row",
                        {"title": "missing url"},
                        {"url": "not-a-url", "title": "bad"},
                        {"url": "https://www.mako.co.il/news/article/xyz", "title": "ok"},
                    ]
                },
            ),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    engine = ExaSearchEngine(
        api_key="exa-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
        ],
        DiscoveryContext(run_id="run-2"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == [
        "https://www.mako.co.il/news/article/xyz"
    ]


@pytest.mark.asyncio
async def test_exa_search_engine_uses_text_snippet_and_ignores_invalid_published_date() -> None:
    """Text should be used as snippet when present and bad published dates should be ignored."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "url": "https://www.maariv.co.il/news/article-1",
                        "title": "כותרת",
                        "publishedDate": "not-a-datetime",
                        "text": "תקציר מלא",
                    }
                ]
            },
        )

    engine = ExaSearchEngine(
        api_key="exa-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD)],
        DiscoveryContext(run_id="run-3"),
    )
    await engine.aclose()

    assert len(candidates) == 1
    assert candidates[0].snippet == "תקציר מלא"
    assert candidates[0].publication_datetime_hint is None
