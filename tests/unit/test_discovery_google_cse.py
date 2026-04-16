"""Unit tests for the Google CSE discovery adapter."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from denbust.discovery.base import DiscoveryContext
from denbust.discovery.engines.google_cse import GoogleCseSearchEngine
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind, ProducerKind


@pytest.mark.asyncio
async def test_google_cse_search_engine_normalizes_results_and_renders_params() -> None:
    """Google CSE responses should become normalized discovery candidates."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "searchInformation": {"totalResults": "123"},
                "items": [
                    {
                        "link": "https://www.ynet.co.il/news/article/abc?utm_source=google",
                        "title": "פשיטה על בית בושת",
                        "snippet": "המשטרה פשטה על המקום.",
                        "displayLink": "www.ynet.co.il",
                        "cacheId": "cache-1",
                        "pagemap": {
                            "metatags": [
                                {
                                    "article:published_time": "2026-04-15T08:00:00Z",
                                }
                            ]
                        },
                    }
                ],
            },
        )

    engine = GoogleCseSearchEngine(
        api_key="google-key",
        cse_id="search-engine-id",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        max_results_per_query=10,
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

    assert captured["query"] == {
        "key": "google-key",
        "cx": "search-engine-id",
        "q": "בית בושת",
        "num": "5",
        "lr": "lang_he",
        "siteSearch": "www.ynet.co.il",
        "siteSearchFilter": "i",
    }
    assert len(candidates) == 1
    assert candidates[0].producer_name == "google_cse"
    assert candidates[0].producer_kind is ProducerKind.SEARCH_ENGINE
    assert str(candidates[0].candidate_url) == (
        "https://www.ynet.co.il/news/article/abc?utm_source=google"
    )
    assert str(candidates[0].canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidates[0].publication_datetime_hint == datetime(2026, 4, 15, 8, 0, tzinfo=UTC)
    assert candidates[0].metadata == {
        "engine": "google_cse",
        "query_kind": "source_targeted",
        "preferred_domains": ["www.ynet.co.il"],
        "result_url": "https://www.ynet.co.il/news/article/abc?utm_source=google",
        "result_title": "פשיטה על בית בושת",
        "result_snippet": "המשטרה פשטה על המקום.",
        "result_display_link": "www.ynet.co.il",
        "result_cache_id": "cache-1",
        "total_results": "123",
    }


@pytest.mark.asyncio
async def test_google_cse_search_engine_aclose_closes_owned_client() -> None:
    """Owned async clients should be closed by `aclose()`."""
    engine = GoogleCseSearchEngine(api_key="google-key", cse_id="search-engine-id")

    await engine.aclose()

    assert engine._client.is_closed is True


@pytest.mark.asyncio
async def test_google_cse_search_engine_skips_malformed_payloads_and_invalid_rows() -> None:
    """Malformed payloads and invalid rows should be ignored without failing the whole call."""
    responses = iter(
        [
            httpx.Response(200, json=[]),
            httpx.Response(200, json={"items": "not-a-list"}),
            httpx.Response(
                200,
                json={
                    "items": [
                        "bad-row",
                        {"title": "missing link"},
                        {"link": "not-a-url", "title": "bad"},
                        {"link": "https://www.mako.co.il/news/article/xyz", "title": "ok"},
                    ]
                },
            ),
        ]
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return next(responses)

    engine = GoogleCseSearchEngine(
        api_key="google-key",
        cse_id="search-engine-id",
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
async def test_google_cse_search_engine_ignores_invalid_publication_hints() -> None:
    """Bad publication metadata should not prevent candidate creation."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "link": "https://www.maariv.co.il/news/article-1",
                        "title": "כותרת",
                        "snippet": "תקציר",
                        "pagemap": {"metatags": [{"article:published_time": "not-a-datetime"}]},
                    }
                ]
            },
        )

    engine = GoogleCseSearchEngine(
        api_key="google-key",
        cse_id="search-engine-id",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD)],
        DiscoveryContext(run_id="run-3"),
    )
    await engine.aclose()

    assert len(candidates) == 1
    assert candidates[0].publication_datetime_hint is None


@pytest.mark.asyncio
async def test_google_cse_search_engine_skips_non_dict_metatags_before_valid_publication_time() -> None:
    """Mixed metatag lists should ignore invalid entries and still extract publication time."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "link": "https://www.ynet.co.il/news/article/def",
                        "title": "כותרת",
                        "snippet": "תקציר",
                        "pagemap": {
                            "metatags": [
                                "not-a-dict",
                                {"article:published_time": "2026-04-15T10:30:00Z"},
                            ]
                        },
                    }
                ]
            },
        )

    engine = GoogleCseSearchEngine(
        api_key="google-key",
        cse_id="search-engine-id",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [DiscoveryQuery(query_text="זנות", query_kind=DiscoveryQueryKind.BROAD)],
        DiscoveryContext(run_id="run-4"),
    )
    await engine.aclose()

    assert len(candidates) == 1
    assert candidates[0].publication_datetime_hint == datetime(2026, 4, 15, 10, 30, tzinfo=UTC)
