"""Unit tests for the Brave discovery adapter."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.engines.brave import BraveSearchEngine
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind, ProducerKind


@pytest.mark.asyncio
async def test_brave_search_engine_normalizes_results_and_renders_site_query() -> None:
    """Brave responses should become normalized discovery candidates."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = request.url.params["q"]
        captured["count"] = request.url.params["count"]
        captured["token"] = request.headers["X-Subscription-Token"]
        return httpx.Response(
            200,
            text=json.dumps(
                {
                    "web": {
                        "results": [
                            {
                                "url": "https://www.ynet.co.il/news/article/abc",
                                "title": "פשיטה על בית בושת",
                                "description": "המשטרה פשטה על המקום.",
                                "page_age": "2026-04-15T08:00:00Z",
                            }
                        ]
                    }
                }
            ),
        )

    engine = BraveSearchEngine(
        api_key="brave-key",
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
            )
        ],
        DiscoveryContext(run_id="run-1", max_results_per_query=5),
    )
    await engine.aclose()

    assert captured == {
        "query": "(site:www.ynet.co.il) בית בושת",
        "count": "5",
        "token": "brave-key",
    }
    assert len(candidates) == 1
    assert candidates[0].producer_name == "brave"
    assert candidates[0].producer_kind is ProducerKind.SEARCH_ENGINE
    assert candidates[0].source_hint == "ynet"
    assert candidates[0].rank == 1
    assert str(candidates[0].candidate_url) == "https://www.ynet.co.il/news/article/abc"
    assert str(candidates[0].canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidates[0].publication_datetime_hint == datetime(2026, 4, 15, 8, 0, tzinfo=UTC)
    assert candidates[0].metadata["query_kind"] == "source_targeted"
    assert candidates[0].metadata["result_url"] == "https://www.ynet.co.il/news/article/abc"
    assert candidates[0].metadata["result_title"] == "פשיטה על בית בושת"
    assert candidates[0].metadata["result_description"] == "המשטרה פשטה על המקום."
    assert candidates[0].metadata["result_page_age"] == "2026-04-15T08:00:00Z"
    assert "raw_result" not in candidates[0].metadata


@pytest.mark.asyncio
async def test_brave_search_engine_aclose_closes_owned_client() -> None:
    """Owned async clients should be closed by `aclose()`."""
    engine = BraveSearchEngine(api_key="brave-key")

    await engine.aclose()

    assert engine._client.is_closed is True


@pytest.mark.asyncio
async def test_brave_search_engine_skips_invalid_results() -> None:
    """Malformed Brave payload rows should be ignored."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "web": {
                    "results": [
                        {"title": "missing url"},
                        {"url": ""},
                        {"url": "https://www.mako.co.il/news/article/xyz", "title": "ok"},
                    ]
                }
            },
        )

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD)],
        DiscoveryContext(run_id="run-2"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == [
        "https://www.mako.co.il/news/article/xyz"
    ]


@pytest.mark.asyncio
async def test_brave_search_engine_skips_non_object_payloads_and_invalid_urls() -> None:
    """Malformed payloads and bad URLs should be ignored without failing the whole discovery call."""
    responses = iter(
        [
            httpx.Response(200, json=[]),
            httpx.Response(200, json={"web": []}),
            httpx.Response(
                200,
                json={
                    "web": {
                        "results": [
                            {"url": "not-a-url", "title": "bad"},
                            {"url": "https://www.walla.co.il/item?utm_source=test", "title": "ok"},
                        ]
                    }
                },
            ),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
        ],
        DiscoveryContext(run_id="run-3"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == [
        "https://www.walla.co.il/item?utm_source=test"
    ]
    assert [str(candidate.canonical_url) for candidate in candidates] == [
        "https://walla.co.il/item"
    ]


@pytest.mark.asyncio
async def test_brave_search_engine_skips_non_list_results_and_non_dict_rows() -> None:
    """Unexpected result container shapes should be skipped safely."""
    responses = iter(
        [
            httpx.Response(200, json={"web": {"results": "not-a-list"}}),
            httpx.Response(200, json={"web": {"results": ["bad-row", {"url": "https://ice.co.il/a"}]}}),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
            DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD),
        ],
        DiscoveryContext(run_id="run-4"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == ["https://ice.co.il/a"]


@pytest.mark.asyncio
async def test_brave_search_engine_ignores_invalid_page_age() -> None:
    """Unparseable page-age values should produce a candidate with no publication hint."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "web": {
                    "results": [
                        {
                            "url": "https://www.mako.co.il/news/article/xyz",
                            "title": "ok",
                            "page_age": "not-a-datetime",
                        }
                    ]
                }
            },
        )

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD)],
        DiscoveryContext(run_id="run-5"),
    )
    await engine.aclose()

    assert len(candidates) == 1
    assert candidates[0].publication_datetime_hint is None
