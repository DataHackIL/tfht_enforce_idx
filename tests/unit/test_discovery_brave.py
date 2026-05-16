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
        captured["freshness"] = request.url.params["freshness"]
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
                date_from=datetime(2026, 4, 15, 0, 0, tzinfo=UTC),
                date_to=datetime(2026, 4, 15, 23, 59, tzinfo=UTC),
                preferred_domains=["www.ynet.co.il"],
                source_hint="ynet",
                language="he",
                tags=["backfill", "ynet", "taxonomy", "category:brothels"],
            )
        ],
        DiscoveryContext(run_id="run-1", max_results_per_query=5),
    )
    await engine.aclose()

    assert captured == {
        "query": "(site:www.ynet.co.il) בית בושת",
        "count": "5",
        "token": "brave-key",
        "freshness": "2026-04-15to2026-04-15",
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
    assert candidates[0].metadata["query_tags"] == [
        "backfill",
        "ynet",
        "taxonomy",
        "category:brothels",
    ]
    assert candidates[0].metadata["source_targeted_taxonomy"] is True
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
            httpx.Response(
                200, json={"web": {"results": ["bad-row", {"url": "https://ice.co.il/a"}]}}
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


@pytest.mark.asyncio
async def test_brave_search_engine_filters_dated_results_outside_query_window() -> None:
    """Historical backfill queries should not retain dated Brave results outside their window."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["freshness"] == "2026-01-01to2026-01-07"
        return httpx.Response(
            200,
            json={
                "web": {
                    "results": [
                        {
                            "url": "https://www.ynet.co.il/news/article/old",
                            "title": "old",
                            "page_age": "2025-12-31T23:59:00Z",
                        },
                        {
                            "url": "https://www.ynet.co.il/news/article/in-window",
                            "title": "in window",
                            "page_age": "2026-01-03T09:00:00Z",
                        },
                        {
                            "url": "https://www.ynet.co.il/news/article/undated",
                            "title": "undated",
                        },
                    ]
                }
            },
        )

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(
                query_text="זנות",
                query_kind=DiscoveryQueryKind.BROAD,
                date_from=datetime(2026, 1, 1, tzinfo=UTC),
                date_to=datetime(2026, 1, 7, 23, 59, tzinfo=UTC),
                tags=["backfill"],
            )
        ],
        DiscoveryContext(run_id="run-6"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == [
        "https://www.ynet.co.il/news/article/in-window",
        "https://www.ynet.co.il/news/article/undated",
    ]


@pytest.mark.asyncio
async def test_brave_search_engine_appends_site_exclusions_for_broad_queries() -> None:
    """Broad queries with excluded_domains should append -site: operators to the query string."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = request.url.params["q"]
        return httpx.Response(200, json={"web": {"results": []}})

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    await engine.discover(
        [
            DiscoveryQuery(
                query_text="זנות",
                query_kind=DiscoveryQueryKind.BROAD,
                excluded_domains=["sport1.maariv.co.il"],
            )
        ],
        DiscoveryContext(run_id="run-excl-1"),
    )
    await engine.aclose()

    assert "-site:sport1.maariv.co.il" in str(captured["query"])
    assert str(captured["query"]).startswith("זנות")


@pytest.mark.asyncio
async def test_brave_search_engine_omits_site_exclusions_for_source_targeted_queries() -> None:
    """Source-targeted queries with preferred_domains must not append -site: exclusions."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = request.url.params["q"]
        return httpx.Response(200, json={"web": {"results": []}})

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    await engine.discover(
        [
            DiscoveryQuery(
                query_text="זנות",
                query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                preferred_domains=["www.maariv.co.il"],
                excluded_domains=["sport1.maariv.co.il"],
            )
        ],
        DiscoveryContext(run_id="run-excl-2"),
    )
    await engine.aclose()

    assert "-site:" not in str(captured["query"])
    assert str(captured["query"]) == "(site:www.maariv.co.il) זנות"


@pytest.mark.asyncio
async def test_brave_search_engine_filters_off_domain_source_targeted_results() -> None:
    """Provider results must still honor preferred-domain contracts locally."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "web": {
                    "results": [
                        {
                            "url": "https://he.wikipedia.org/wiki/%D7%96%D7%A0%D7%95%D7%AA",
                            "title": "off domain",
                        },
                        {
                            "url": "https://news.walla.co.il/item/3806949",
                            "title": "on domain",
                        },
                    ]
                }
            },
        )

    engine = BraveSearchEngine(
        api_key="brave-key",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    candidates = await engine.discover(
        [
            DiscoveryQuery(
                query_text="זנות",
                query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                preferred_domains=["news.walla.co.il"],
            )
        ],
        DiscoveryContext(run_id="run-7"),
    )
    await engine.aclose()

    assert [str(candidate.candidate_url) for candidate in candidates] == [
        "https://news.walla.co.il/item/3806949"
    ]
