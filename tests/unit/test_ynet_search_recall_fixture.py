"""Fixture-backed regression coverage for Ynet search-backed recall."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import HttpUrl

import denbust.pipeline as pipeline_module
from denbust.config import Config, SourceConfig, SourceType
from denbust.data_models import Category, ClassificationResult, ClassifiedArticle, RawArticle
from denbust.discovery.base import DiscoveryContext
from denbust.discovery.engines.brave import BraveSearchEngine
from denbust.discovery.models import (
    CandidateStatus,
    DiscoveryQuery,
    DiscoveryQueryKind,
    DiscoveryRun,
    DiscoveryRunStatus,
    FetchStatus,
)
from denbust.discovery.queries import build_discovery_queries
from denbust.discovery.scrape_queue import scrape_candidates
from denbust.discovery.source_native import persist_discovered_candidates
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName
from denbust.store.seen import SeenStore

YNET_RECALL_URL = "https://www.ynet.co.il/news/article/bkcarhip11g"
YNET_RECALL_CANONICAL_URL = "https://ynet.co.il/news/article/bkcarhip11g"
YNET_RECALL_TITLE = 'כלאו נשים בתנאי עבדות: "הוא אנס, היא סחטה" | קשה לקריאה'
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
async def test_ynet_source_targeted_taxonomy_search_result_reaches_classifier_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
                                "description": "בתי בושת, סרסרות, זנות וסחר בבני אדם.",
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

    fixture_article = RawArticle(
        url=HttpUrl(YNET_RECALL_URL),
        title=YNET_RECALL_TITLE,
        snippet="בתי בושת, סרסרות, זנות וסחר בבני אדם.",
        date=YNET_RECALL_PUBLISHED_AT,
        source_name="ynet",
    )
    ynet_source = FixtureYnetSource([fixture_article])
    scrape_batch = await scrape_candidates(
        config=config,
        persistence=persistence,
        candidates=[candidate],
        sources=[ynet_source],
        preloaded_source_articles={"ynet": [fixture_article]},
    )

    assert scrape_batch.errors == []
    assert scrape_batch.raw_articles == [fixture_article]
    assert scrape_batch.updated_candidates[0].candidate_status is CandidateStatus.SCRAPE_SUCCEEDED
    assert scrape_batch.attempts[0].fetch_status is FetchStatus.SUCCESS
    assert scrape_batch.attempts[0].source_adapter_name == "ynet"

    classifier = CapturingClassifier()
    seen_store = SeenStore(tmp_path / "seen.json")
    result = pipeline_module._build_run_snapshot(config, config_path=None, days=90)
    result.source_count = 1
    result.raw_article_count = len(scrape_batch.raw_articles)
    processed = await pipeline_module._process_ingest_articles(
        config=config,
        result=result,
        source_names=["ynet"],
        sources=[ynet_source],
        all_articles=scrape_batch.raw_articles,
        seen_store=seen_store,
        classifier=classifier,
        deduplicator=object(),
        operational_store=None,
    )

    assert classifier.inputs == [fixture_article]
    assert processed.raw_article_count == 1
    assert processed.unseen_article_count == 1
    assert processed.debug_payload is not None
    assert processed.debug_payload["counts"]["raw_article_count"] == 1
    assert processed.debug_payload["classifier_summary"]["unseen_article_count"] == 1
    assert processed.debug_payload["classifier_summary"]["classified_article_count"] == 1
    assert processed.debug_payload["unseen_articles"] == [
        {
            "source_name": "ynet",
            "url": YNET_RECALL_URL,
            "canonical_url": YNET_RECALL_CANONICAL_URL,
            "title": YNET_RECALL_TITLE,
            "snippet": "בתי בושת, סרסרות, זנות וסחר בבני אדם.",
            "publication_datetime": YNET_RECALL_PUBLISHED_AT.isoformat(),
        }
    ]
