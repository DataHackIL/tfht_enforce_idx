"""Fixture-backed regression coverage for Ynet search-backed recall."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

import denbust.pipeline as pipeline_module
from denbust.config import Config, SourceConfig, SourceType
from denbust.data_models import Category, ClassificationResult, ClassifiedArticle, RawArticle
from denbust.discovery.base import DiscoveryContext
from denbust.discovery.engines.brave import BraveSearchEngine
from denbust.discovery.models import (
    CandidateStatus,
    ContentBasis,
    DiscoveryQuery,
    DiscoveryQueryKind,
    DiscoveryRun,
    DiscoveryRunStatus,
    FetchStatus,
    ProducerKind,
)
from denbust.discovery.queries import build_discovery_queries
from denbust.discovery.scrape_queue import scrape_candidates
from denbust.discovery.source_native import persist_discovered_candidates
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName

YNET_RECALL_URL = "https://www.ynet.co.il/news/article/bkcarhip11g"
YNET_RECALL_CANONICAL_URL = "https://ynet.co.il/news/article/bkcarhip11g"
YNET_RECALL_TITLE = 'כלאו נשים בתנאי עבדות: "הוא אנס, היא סחטה" | קשה לקריאה'
YNET_RECALL_SNIPPET = "בתי בושת, סרסרות, זנות וסחר בבני אדם."
YNET_RECALL_PUBLISHED_AT = datetime(2026, 2, 12, 8, 0, tzinfo=UTC)


class FixtureYnetSource:
    """In-memory Ynet source fixture used instead of live RSS/search pages."""

    name = "ynet"

    def __init__(self, articles: list[RawArticle]) -> None:
        self._articles = articles

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        return self._articles


class CapturingClassifier:
    """Classifier stub that records the pre-classification article batch."""

    def __init__(self) -> None:
        self.inputs: list[RawArticle] = []

    async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
        self.inputs = list(articles)
        return [
            ClassifiedArticle(
                article=article,
                classification=ClassificationResult(
                    relevant=False,
                    category=Category.NOT_RELEVANT,
                    confidence="high",
                ),
            )
            for article in articles
        ]


def _ynet_source_targeted_taxonomy_query(queries: list[DiscoveryQuery]) -> DiscoveryQuery:
    matching = [
        query
        for query in queries
        if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
        and query.source_hint == "ynet"
        and "taxonomy" in query.tags
    ]
    assert len(matching) == 1
    return matching[0]


@pytest.mark.asyncio
async def test_ynet_source_targeted_taxonomy_search_result_fallback_reaches_classifier_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    respx_mock: object,
) -> None:
    """Protect #66: Ynet recall via source-targeted taxonomy search, not live RSS/search pages."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: [("סחר", ["taxonomy", "category:human_trafficking"])],
    )
    config = Config(
        keywords=[],
        sources=[
            SourceConfig(
                name="ynet",
                type=SourceType.RSS,
                url="https://www.ynet.co.il/Integration/StoryRss2.xml",
            )
        ],
        discovery={"default_query_kinds": ["source_targeted", "taxonomy_targeted"]},
        store={"state_root": tmp_path},
    )

    query = _ynet_source_targeted_taxonomy_query(build_discovery_queries(config, days=90))
    assert query.query_text == "סחר"
    assert query.preferred_domains == ["www.ynet.co.il"]
    assert {"ynet", "taxonomy", "category:human_trafficking"}.issubset(set(query.tags))

    captured: dict[str, object] = {}

    def search_handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = request.url.params["q"]
        return httpx.Response(
            200,
            text=json.dumps(
                {
                    "web": {
                        "results": [
                            {
                                "url": YNET_RECALL_URL,
                                "title": YNET_RECALL_TITLE,
                                "description": YNET_RECALL_SNIPPET,
                                "page_age": YNET_RECALL_PUBLISHED_AT.isoformat(),
                            }
                        ]
                    }
                }
            ),
        )

    search_client = httpx.AsyncClient(transport=httpx.MockTransport(search_handler))
    engine = BraveSearchEngine(api_key="fixture-key", client=search_client)
    try:
        discovered = await engine.discover(
            [query],
            DiscoveryContext(run_id="run-ynet-66", max_results_per_query=1),
        )
    finally:
        await engine.aclose()
        await search_client.aclose()

    assert captured["query"] == "(site:www.ynet.co.il) סחר"
    assert len(discovered) == 1
    discovered_candidate = discovered[0]
    assert str(discovered_candidate.candidate_url) == YNET_RECALL_URL
    assert str(discovered_candidate.canonical_url) == YNET_RECALL_CANONICAL_URL
    assert discovered_candidate.title == YNET_RECALL_TITLE
    assert discovered_candidate.publication_datetime_hint == YNET_RECALL_PUBLISHED_AT
    assert discovered_candidate.source_hint == "ynet"
    assert discovered_candidate.metadata["query_kind"] == "source_targeted"
    assert discovered_candidate.metadata["query_tags"] == [
        "ynet",
        "taxonomy",
        "category:human_trafficking",
    ]
    assert discovered_candidate.metadata["source_targeted_taxonomy"] is True
    assert discovered_candidate.metadata["preferred_domains"] == ["www.ynet.co.il"]

    persistence = StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    )
    persisted = persist_discovered_candidates(
        run=DiscoveryRun(
            run_id="run-ynet-66",
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.DISCOVER,
            status=DiscoveryRunStatus.RUNNING,
            query_count=1,
        ),
        discovered_candidates=discovered,
        persistence=persistence,
    )
    candidate = persisted.candidates[0]
    provenance = persisted.provenance[0]
    assert len(persisted.provenance) == 1
    assert provenance.candidate_id == candidate.candidate_id
    assert provenance.producer_name == "brave"
    assert provenance.producer_kind is ProducerKind.SEARCH_ENGINE
    assert provenance.query_text == "סחר"
    assert str(provenance.raw_url) == YNET_RECALL_URL
    assert str(provenance.normalized_url) == YNET_RECALL_CANONICAL_URL
    assert provenance.title == YNET_RECALL_TITLE
    assert provenance.publication_datetime_hint == YNET_RECALL_PUBLISHED_AT
    assert provenance.metadata["query_tags"] == [
        "ynet",
        "taxonomy",
        "category:human_trafficking",
    ]
    assert provenance.metadata["preferred_domains"] == ["www.ynet.co.il"]
    assert provenance.metadata["source_targeted_taxonomy"] is True
    assert persistence.list_provenance(candidate.candidate_id) == persisted.provenance
    assert candidate.candidate_status is CandidateStatus.NEW
    assert str(candidate.current_url) == YNET_RECALL_URL
    assert str(candidate.canonical_url) == YNET_RECALL_CANONICAL_URL
    assert candidate.titles == [YNET_RECALL_TITLE]
    assert candidate.source_hints == ["ynet"]
    assert candidate.discovered_via == ["brave"]
    assert candidate.discovery_queries == ["סחר"]
    assert candidate.metadata["latest_publication_datetime_hint"] == (
        YNET_RECALL_PUBLISHED_AT.isoformat()
    )
    assert candidate.metadata["latest_discovery_metadata"]["source_targeted_taxonomy"] is True

    article_fixture_html = f"""
    <!doctype html>
    <html lang="he">
      <head>
        <title>{YNET_RECALL_TITLE}</title>
        <meta name="description" content="{YNET_RECALL_SNIPPET}">
        <meta property="article:published_time" content="{YNET_RECALL_PUBLISHED_AT.isoformat()}">
      </head>
      <body></body>
    </html>
    """
    article_route = respx_mock.get(YNET_RECALL_URL).mock(
        return_value=httpx.Response(
            200,
            text=article_fixture_html,
            headers={"content-type": "text/html; charset=utf-8"},
        )
    )
    ynet_source = FixtureYnetSource([])
    scrape_batch = await scrape_candidates(
        config=config,
        persistence=persistence,
        candidates=[candidate],
        sources=[ynet_source],
    )

    assert scrape_batch.errors == []
    assert scrape_batch.raw_articles == []
    assert len(scrape_batch.fallback_candidates) == 1
    fallback_candidate = scrape_batch.fallback_candidates[0]
    assert fallback_candidate.candidate_status is CandidateStatus.PARTIALLY_SCRAPED
    assert fallback_candidate.content_basis is ContentBasis.PARTIAL_PAGE
    assert fallback_candidate.metadata["fallback_title"] == YNET_RECALL_TITLE
    assert fallback_candidate.metadata["fallback_snippet"] == YNET_RECALL_SNIPPET
    assert fallback_candidate.metadata["fallback_source_name"] == "ynet"
    assert fallback_candidate.metadata["fallback_publication_datetime"] == (
        YNET_RECALL_PUBLISHED_AT.isoformat()
    )
    assert fallback_candidate.metadata["fallback_final_url"] == YNET_RECALL_URL
    assert article_route.called
    assert [attempt.fetch_status for attempt in scrape_batch.attempts] == [
        FetchStatus.FAILED,
        FetchStatus.PARTIAL,
    ]
    assert scrape_batch.attempts[0].source_adapter_name == "ynet"
    assert scrape_batch.attempts[0].error_code == "candidate_not_found"
    assert scrape_batch.attempts[1].source_adapter_name is None

    classifier = CapturingClassifier()
    fallback_records = await pipeline_module._build_fallback_operational_records(
        candidates=scrape_batch.fallback_candidates,
        classifier=classifier,
    )

    assert fallback_records == []
    assert len(classifier.inputs) == 1
    fallback_input = classifier.inputs[0]
    assert str(fallback_input.url) == YNET_RECALL_CANONICAL_URL
    assert fallback_input.title == YNET_RECALL_TITLE
    assert fallback_input.snippet == YNET_RECALL_SNIPPET
    assert fallback_input.date == YNET_RECALL_PUBLISHED_AT
    assert fallback_input.source_name == "ynet"
