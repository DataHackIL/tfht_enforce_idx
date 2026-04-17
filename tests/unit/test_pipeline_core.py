"""Unit tests for pipeline orchestration helpers."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from pydantic import HttpUrl

import denbust.pipeline as pipeline_module
from denbust.config import Config, OutputConfig, OutputFormat, SourceConfig, SourceType
from denbust.data_models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SourceReference,
    SubCategory,
    UnifiedItem,
)
from denbust.discovery.models import (
    CandidateStatus,
    DiscoveryRun,
    DiscoveryRunStatus,
    PersistentCandidate,
)
from denbust.discovery.scrape_queue import CandidateScrapeBatch
from denbust.discovery.source_native import PersistedSourceDiscovery
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName
from denbust.models.policies import PrivacyRisk
from denbust.ops.storage import LocalJsonOperationalStore
from denbust.pipeline import (
    _build_classifier_summary,
    _build_problem_summary,
    _build_source_summaries,
    _build_suspicions,
    _persist_source_native_candidates,
    _run_brave_discovery,
    _run_exa_discovery,
    _run_google_cse_discovery,
    _run_job_from_config,
    _run_source_native_discovery,
    _source_name_from_error,
    classify_articles,
    create_sources,
    deduplicate_articles,
    fetch_all_sources,
    filter_seen,
    mark_seen,
    run_backup,
    run_job,
    run_job_async,
    run_news_discover_job,
    run_news_ingest_job,
    run_news_scrape_candidates_job,
    run_pipeline,
    run_pipeline_async,
    run_release,
    setup_logging,
)
from denbust.store.run_snapshots import RunSnapshot


def build_raw_article(url: str = "https://example.com/article") -> RawArticle:
    """Create a sample raw article."""
    return RawArticle(
        url=HttpUrl(url),
        title="פשיטה על בית בושת",
        snippet="המשטרה ביצעה פשיטה.",
        date=datetime(2026, 3, 1, tzinfo=UTC),
        source_name="test",
    )


def build_classified_article(
    url: str = "https://example.com/article",
    *,
    relevant: bool = True,
) -> ClassifiedArticle:
    """Create a sample classified article."""
    return ClassifiedArticle(
        article=build_raw_article(url),
        classification=ClassificationResult(
            relevant=relevant,
            category=Category.BROTHEL if relevant else Category.NOT_RELEVANT,
            sub_category=SubCategory.CLOSURE if relevant else None,
            confidence="high" if relevant else "low",
        ),
    )


def build_unified_item(url: str = "https://example.com/article") -> UnifiedItem:
    """Create a sample unified item."""
    return UnifiedItem(
        headline="פשיטה על בית בושת",
        summary="סיכום",
        sources=[SourceReference(source_name="test", url=HttpUrl(url))],
        date=datetime(2026, 3, 1, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
    )


def build_persistent_candidate(
    candidate_id: str,
    *,
    current_url: str,
) -> PersistentCandidate:
    """Create a sample persistent candidate."""
    return PersistentCandidate(
        candidate_id=candidate_id,
        current_url=HttpUrl(current_url),
        canonical_url=HttpUrl(current_url),
        titles=["פשיטה על בית בושת"],
        snippets=["המשטרה ביצעה פשיטה."],
        discovered_via=["source_native"],
        discovery_queries=["בית בושת"],
        source_hints=["test"],
        first_seen_at=datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
    )


class FakeSource:
    """Simple async source stub."""

    def __init__(
        self,
        name: str,
        fetch_result: list[RawArticle] | Exception,
        *,
        debug_state: dict[str, object] | None = None,
    ) -> None:
        self.name = name
        self._fetch_result = fetch_result
        self._debug_state = debug_state

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        if isinstance(self._fetch_result, Exception):
            raise self._fetch_result
        return self._fetch_result

    def get_debug_state(self) -> dict[str, object] | None:
        return self._debug_state


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_setup_logging_defaults_to_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default setup should use INFO level."""
        captured: dict[str, object] = {}

        def fake_basic_config(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

        setup_logging()

        assert captured["level"] == logging.INFO

    def test_setup_logging_verbose_uses_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verbose setup should use DEBUG level."""
        captured: dict[str, object] = {}

        def fake_basic_config(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

        setup_logging(verbose=True)

        assert captured["level"] == logging.DEBUG


class TestCreateSourcesWarnings:
    """Tests for create_sources warning branches."""

    def test_create_sources_warns_on_missing_rss_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """RSS sources without URLs should be skipped with a warning."""
        config = Config(sources=[SourceConfig(name="ynet", type=SourceType.RSS)])
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        sources = create_sources(config)

        assert sources == []
        mock_logger.warning.assert_called_once()

    def test_create_sources_warns_on_unknown_scraper(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown scraper names should be skipped with a warning."""
        config = Config(sources=[SourceConfig(name="unknown", type=SourceType.SCRAPER)])
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        sources = create_sources(config)

        assert sources == []
        mock_logger.warning.assert_called_once()

    def test_create_sources_skips_disabled_and_builds_known_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Enabled RSS and scraper sources should be instantiated; disabled ones skipped."""
        config = Config(
            sources=[
                SourceConfig(name="disabled", type=SourceType.RSS, enabled=False),
                SourceConfig(name="ynet", type=SourceType.RSS, url="https://example.com/feed.xml"),
                SourceConfig(name="walla", type=SourceType.SCRAPER),
                SourceConfig(name="mako", type=SourceType.SCRAPER),
                SourceConfig(name="maariv", type=SourceType.SCRAPER),
                SourceConfig(name="haaretz", type=SourceType.SCRAPER),
            ]
        )

        monkeypatch.setattr("denbust.pipeline.create_walla_source", lambda: FakeSource("walla", []))
        monkeypatch.setattr("denbust.pipeline.create_mako_source", lambda: FakeSource("mako", []))
        monkeypatch.setattr(
            "denbust.pipeline.create_maariv_source", lambda: FakeSource("maariv", [])
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_haaretz_source", lambda: FakeSource("haaretz", [])
        )

        sources = create_sources(config)

        assert [source.name for source in sources] == ["ynet", "walla", "mako", "maariv", "haaretz"]


class TestFetchAndClassifyHelpers:
    """Tests for fetch and classification helpers."""

    @pytest.mark.asyncio
    async def test_fetch_all_sources_continues_after_source_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One failing source should not prevent collecting later articles."""
        articles = [build_raw_article("https://example.com/ok")]
        sources = [
            FakeSource("bad", RuntimeError("boom")),
            FakeSource("good", articles),
        ]
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        found, errors = await fetch_all_sources(sources, days=3, keywords=["זנות"])

        assert found == articles
        assert errors == ["bad: boom"]
        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_classify_articles_filters_non_relevant(self) -> None:
        """Classifier helper should drop non-relevant articles."""
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[
                build_classified_article("https://example.com/1", relevant=True),
                build_classified_article("https://example.com/2", relevant=False),
            ]
        )

        relevant = await classify_articles([build_raw_article()], classifier)

        assert len(relevant) == 1
        assert relevant[0].classification.relevant is True

    def test_source_name_from_error_returns_none_without_separator(self) -> None:
        """Malformed source errors should be ignored by source summaries."""
        assert _source_name_from_error("timeout without source prefix") is None

    def test_machine_debug_helpers_flag_source_errors_and_zero_results(self) -> None:
        """Machine-oriented summaries should expose source errors and zero-result sources."""
        source_summaries = _build_source_summaries(
            sources=[
                FakeSource(
                    "mako",
                    [],
                    debug_state={"searches": [{"keyword": "בית בושת", "status": "ok"}]},
                ),
                FakeSource("haaretz", []),
                FakeSource("ice", []),
            ],
            source_names=["mako", "haaretz", "ice"],
            raw_articles=[build_raw_article("https://example.com/one")],
            errors=["mako: timeout", "plain warning without source prefix"],
        )
        classifier_summary = _build_classifier_summary(
            unseen_articles=[build_raw_article("https://example.com/u1")],
            classified_articles=[],
        )
        result = RunSnapshot(
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            unseen_article_count=1,
            relevant_article_count=0,
        )

        problems = _build_problem_summary(
            source_summaries=source_summaries,
            classifier_summary=classifier_summary,
            result=result,
        )
        suspicions = _build_suspicions(
            source_summaries=source_summaries,
            classifier_summary=classifier_summary,
            result=result,
        )

        assert source_summaries[0]["source_name"] == "mako"
        assert source_summaries[0]["had_error"] is True
        assert source_summaries[0]["runtime_debug"] == {
            "searches": [{"keyword": "בית בושת", "status": "ok"}]
        }
        assert source_summaries[1]["source_name"] == "haaretz"
        assert source_summaries[1]["returned_zero_results"] is True
        assert source_summaries[2]["source_name"] == "ice"
        assert source_summaries[2]["returned_zero_results"] is True
        assert problems["source_errors"] == ["mako"]
        assert problems["zero_result_sources"] == ["haaretz", "ice"]
        assert problems["classification_output_anomaly"] is True
        assert "source_errors_present" in suspicions
        assert "source_error_rate_high" in suspicions
        assert "sources_returned_zero_results" in suspicions

    def test_mark_seen_collects_all_source_urls(self, tmp_path: Path) -> None:
        """mark_seen should persist every source URL from unified items."""
        from denbust.store.seen import SeenStore

        store = SeenStore(tmp_path / "seen.json")
        item = UnifiedItem(
            headline="Headline",
            summary="Summary",
            sources=[
                SourceReference(source_name="a", url=HttpUrl("https://a.com/1")),
                SourceReference(source_name="b", url=HttpUrl("https://b.com/1")),
            ],
            date=datetime(2026, 3, 1, tzinfo=UTC),
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
        )

        mark_seen([item], store)

        assert store.is_seen("https://a.com/1")
        assert store.is_seen("https://b.com/1")

    def test_filter_seen_filters_logged_urls(self, tmp_path: Path) -> None:
        """filter_seen should drop URLs already present in the seen store."""
        from denbust.news_items.normalize import canonicalize_news_url
        from denbust.store.seen import SeenStore

        store = SeenStore(tmp_path / "seen.json")
        seen = build_raw_article("https://example.com/seen")
        unseen = build_raw_article("https://example.com/unseen")
        store.mark_seen([canonicalize_news_url(str(seen.url))])

        filtered = filter_seen([seen, unseen], store)

        assert filtered == [unseen]

    def test_filter_seen_emits_debug_log_with_reason_code_for_seen_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """filter_seen must emit a DEBUG log with reason=seen for every dropped article."""
        from denbust.news_items.normalize import canonicalize_news_url
        from denbust.store.seen import SeenStore

        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        store = SeenStore(tmp_path / "seen.json")
        article = build_raw_article("https://example.com/news/1")
        canonical = canonicalize_news_url(str(article.url))
        store.mark_seen([canonical])

        result = filter_seen([article], store)

        assert result == []
        assert mock_logger.debug.call_args_list == [
            call("skip url=%s reason=seen source=%s", canonical, article.source_name)
        ]

    def test_filter_seen_does_not_log_skip_for_unseen_articles(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """filter_seen must not emit reason=seen for articles not in the seen store."""
        from denbust.store.seen import SeenStore

        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        store = SeenStore(tmp_path / "seen.json")
        article = build_raw_article("https://example.com/fresh/1")

        result = filter_seen([article], store)

        assert result == [article]
        assert mock_logger.debug.call_args_list == []

    def test_deduplicate_articles_logs_result_count(self) -> None:
        """deduplicate_articles should return the deduplicator result unchanged."""
        deduplicator = MagicMock()
        expected = [build_unified_item()]
        deduplicator.deduplicate.return_value = expected

        items = deduplicate_articles([build_classified_article()], deduplicator)

        assert items == expected


class TestRunPipelineAsync:
    """Tests for async pipeline control flow."""

    @pytest.mark.asyncio
    async def test_run_brave_discovery_persists_empty_when_no_queries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Brave discovery should still persist an empty run when query building yields nothing."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_brave_discovery(config=Config(), run_id="run-brave", days=4)

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).query_count == 0
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_brave_discovery_records_missing_api_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Brave discovery should persist an error when enabled without an API key."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_brave_discovery(
            config=Config(discovery={"enabled": True, "engines": {"brave": {"enabled": True}}}),
            run_id="run-brave",
            days=4,
        )

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).errors == [
            "brave: missing DENBUST_BRAVE_SEARCH_API_KEY"
        ]
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_brave_discovery_catches_engine_errors_and_closes_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Engine failures should be recorded on the run and still close both engine and store."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["persistence_closed"] = True

        class FakeEngine:
            def __init__(self, **kwargs: object) -> None:
                captured["engine_init"] = kwargs

            async def discover(self, queries, context):
                captured["queries"] = queries
                captured["context"] = context
                raise RuntimeError("boom")

            async def aclose(self) -> None:
                captured["engine_closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(
            Config,
            "brave_search_api_key",
            property(lambda _self: "brave-key"),
        )
        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr("denbust.pipeline.BraveSearchEngine", FakeEngine)
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_brave_discovery(
            config=Config(discovery={"enabled": True, "engines": {"brave": {"enabled": True}}}),
            run_id="run-brave",
            days=6,
        )

        assert persisted.candidates == []
        assert cast(DiscoveryRun, captured["run"]).errors == ["brave: RuntimeError: boom"]
        assert captured["engine_closed"] is True
        assert captured["persistence_closed"] is True
        assert captured["engine_init"] == {"api_key": "brave-key", "max_results_per_query": 20}
        assert captured["context"].metadata == {"days": 6, "engine": "brave"}

    @pytest.mark.asyncio
    async def test_run_exa_discovery_persists_empty_when_no_queries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exa discovery should still persist an empty run when query building yields nothing."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr("denbust.pipeline.build_discovery_queries", lambda *_a, **_k: [])
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_exa_discovery(config=Config(), run_id="run-exa", days=4)

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).query_count == 0
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_exa_discovery_records_missing_api_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exa discovery should persist an error when enabled without an API key."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_exa_discovery(
            config=Config(discovery={"enabled": True, "engines": {"exa": {"enabled": True}}}),
            run_id="run-exa",
            days=4,
        )

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).errors == ["exa: missing DENBUST_EXA_API_KEY"]
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_exa_discovery_catches_engine_errors_and_closes_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exa engine failures should be recorded on the run and still close both engine and store."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["persistence_closed"] = True

        class FakeEngine:
            def __init__(self, **kwargs: object) -> None:
                captured["engine_init"] = kwargs

            async def discover(self, queries, context):
                captured["queries"] = queries
                captured["context"] = context
                raise RuntimeError("boom")

            async def aclose(self) -> None:
                captured["engine_closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(Config, "exa_api_key", property(lambda _self: "exa-key"))
        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr("denbust.pipeline.ExaSearchEngine", FakeEngine)
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_exa_discovery(
            config=Config(discovery={"enabled": True, "engines": {"exa": {"enabled": True}}}),
            run_id="run-exa",
            days=6,
        )

        assert persisted.candidates == []
        assert cast(DiscoveryRun, captured["run"]).errors == ["exa: RuntimeError: boom"]
        assert captured["engine_closed"] is True
        assert captured["persistence_closed"] is True
        assert captured["engine_init"] == {"api_key": "exa-key", "max_results_per_query": 20}
        assert captured["context"].metadata == {
            "days": 6,
            "engine": "exa",
            "allow_find_similar": True,
        }

    @pytest.mark.asyncio
    async def test_run_google_cse_discovery_persists_empty_when_no_queries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Google CSE discovery should still persist an empty run when query building yields nothing."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr("denbust.pipeline.build_discovery_queries", lambda *_a, **_k: [])
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_google_cse_discovery(config=Config(), run_id="run-google", days=4)

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).query_count == 0
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_google_cse_discovery_records_missing_credentials(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Google CSE discovery should persist an error when enabled without required credentials."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_google_cse_discovery(
            config=Config(
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}}
            ),
            run_id="run-google",
            days=4,
        )

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).errors == [
            "google_cse: missing DENBUST_GOOGLE_CSE_API_KEY"
        ]
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_google_cse_discovery_records_missing_cse_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Google CSE discovery should persist an error when the search engine id is missing."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(Config, "google_cse_api_key", property(lambda _self: "google-key"))
        monkeypatch.setattr(Config, "google_cse_id", property(lambda _self: None))
        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_google_cse_discovery(
            config=Config(
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}}
            ),
            run_id="run-google",
            days=4,
        )

        assert persisted.candidates == []
        assert captured["candidates"] == []
        assert cast(DiscoveryRun, captured["run"]).errors == [
            "google_cse: missing DENBUST_GOOGLE_CSE_ID"
        ]
        assert captured["closed"] is True

    @pytest.mark.asyncio
    async def test_run_google_cse_discovery_catches_engine_errors_and_closes_resources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Google CSE engine failures should be recorded on the run and still close both engine and store."""
        captured: dict[str, object] = {}

        class FakePersistence:
            def close(self) -> None:
                captured["persistence_closed"] = True

        class FakeEngine:
            def __init__(self, **kwargs: object) -> None:
                captured["engine_init"] = kwargs

            async def discover(self, queries, context):
                captured["queries"] = queries
                captured["context"] = context
                raise RuntimeError("boom")

            async def aclose(self) -> None:
                captured["engine_closed"] = True

        def fake_persist_discovered_candidates(*, run, discovered_candidates, persistence):
            del persistence
            captured["run"] = run
            captured["candidates"] = discovered_candidates
            return PersistedSourceDiscovery(run=run, candidates=[], provenance=[])

        monkeypatch.setattr(Config, "google_cse_api_key", property(lambda _self: "google-key"))
        monkeypatch.setattr(Config, "google_cse_id", property(lambda _self: "search-engine-id"))
        monkeypatch.setattr(
            "denbust.pipeline.build_discovery_queries",
            lambda *_args, **_kwargs: [MagicMock()],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: FakePersistence(),
        )
        monkeypatch.setattr("denbust.pipeline.GoogleCseSearchEngine", FakeEngine)
        monkeypatch.setattr(
            "denbust.pipeline.persist_discovered_candidates",
            fake_persist_discovered_candidates,
        )

        persisted = await _run_google_cse_discovery(
            config=Config(
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}}
            ),
            run_id="run-google",
            days=6,
        )

        assert persisted.candidates == []
        assert cast(DiscoveryRun, captured["run"]).errors == ["google_cse: RuntimeError: boom"]
        assert captured["engine_closed"] is True
        assert captured["persistence_closed"] is True
        assert captured["engine_init"] == {
            "api_key": "google-key",
            "cse_id": "search-engine-id",
            "max_results_per_query": 10,
        }
        assert captured["context"].metadata == {"days": 6, "engine": "google_cse"}

    @pytest.mark.asyncio
    async def test_run_pipeline_async_requires_api_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing API key should short-circuit before any work starts."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

        result = await run_pipeline_async(Config(), days=3)

        assert result.items == []
        assert result.fatal is True
        assert result.errors == ["ANTHROPIC_API_KEY not set"]

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_missing_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No configured sources should return early."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

        result = await run_pipeline_async(Config(), days=3)

        assert result.items == []
        assert result.fatal is True
        assert result.errors == ["No sources configured"]

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_no_articles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No fetched articles should return early."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        seen_store = MagicMock(count=4)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr("denbust.pipeline.fetch_all_sources", AsyncMock(return_value=([], [])))

        result = await run_pipeline_async(Config(), days=3)

        assert result.items == []
        assert result.raw_article_count == 0
        assert result.seen_count_before == 4
        assert result.seen_count_after == 4

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_all_seen(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Already-seen articles should stop before classification."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        article = build_raw_article()
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        seen_store = MagicMock(count=9)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([article], [])),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda _articles, _seen_store: [])

        result = await run_pipeline_async(Config(), days=3)

        assert result.items == []
        assert result.raw_article_count == 1
        assert result.unseen_article_count == 0
        assert result.seen_count_after == 9

    @pytest.mark.asyncio
    async def test_run_pipeline_async_warns_on_max_articles_and_handles_no_relevant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Large unseen batches should warn, then return early if nothing is relevant."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        articles = [build_raw_article(f"https://example.com/{i}") for i in range(2)]
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(return_value=[])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        seen_store = MagicMock(count=2)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=(articles, [])),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _seen_store: articles)

        result = await run_pipeline_async(Config(max_articles=1), days=3)

        assert result.items == []
        assert result.raw_article_count == 2
        assert result.unseen_article_count == 2
        assert result.relevant_article_count == 0
        mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_async_records_debug_payload_for_rejected_articles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ingest runs should retain rejected unseen items in the debug payload."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        unseen_article = build_raw_article("https://example.com/rejected")
        seen_store = MagicMock(count=4)
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[build_classified_article("https://example.com/rejected", relevant=False)]
        )

        class SourceStub:
            name = "test"

            def get_debug_state(self) -> dict[str, str]:
                return {"phase": "fetch", "status": "ok"}

        source = SourceStub()
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [source])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([unseen_article], [])),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _seen_store: articles)

        result = await run_pipeline_async(Config(max_articles=5), days=3)

        assert result.result_summary == "no relevant articles found"
        assert result.debug_payload is not None
        assert result.debug_payload["schema_version"] == "news_items.ingest.debug.v1"
        assert result.debug_payload["counts"]["unseen_article_count"] == 1
        assert result.debug_payload["source_runtime_debug"] == {
            "test": {"phase": "fetch", "status": "ok"}
        }
        assert result.debug_payload["classifier_summary"]["rejected_article_count"] == 1
        assert result.debug_payload["problems"]["all_unseen_rejected"] is True
        assert "all_unseen_rejected" in result.debug_payload["suspicions"]
        assert result.debug_payload["source_summaries"][0]["source_name"] == "test"
        assert result.debug_payload["source_summaries"][0]["raw_article_count"] == 1
        assert result.debug_payload["source_summaries"][0]["runtime_debug"] == {
            "phase": "fetch",
            "status": "ok",
        }
        rejected = result.debug_payload["rejected_articles"]
        assert len(rejected) == 1
        assert rejected[0]["canonical_url"] == "https://example.com/rejected"
        assert rejected[0]["relevant"] is False

    @pytest.mark.asyncio
    async def test_run_pipeline_async_calls_get_debug_state_once_per_payload_site(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per payload site, debug state should be captured once and reused."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        unseen_article = build_raw_article("https://example.com/rejected")
        seen_store = MagicMock(count=4)
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[build_classified_article("https://example.com/rejected", relevant=False)]
        )

        class SourceStub:
            name = "test"

            def __init__(self) -> None:
                self.calls = 0

            def get_debug_state(self) -> dict[str, int]:
                self.calls += 1
                return {"call": self.calls}

        source = SourceStub()
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [source])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([unseen_article], [])),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _seen_store: articles)

        result = await run_pipeline_async(Config(max_articles=5), days=3)

        assert result.debug_payload is not None
        assert source.calls == 2
        assert result.debug_payload["source_runtime_debug"] == {"test": {"call": 1}}
        assert result.debug_payload["source_summaries"][0]["runtime_debug"] == {"call": 2}

    def test_machine_debug_helpers_do_not_flag_all_unseen_rejected_on_classification_anomaly(
        self,
    ) -> None:
        """Incomplete classifier output should not emit the all-unseen-rejected signal."""
        source_summaries = _build_source_summaries(
            sources=[FakeSource("mako", [])],
            source_names=["mako"],
            raw_articles=[],
            errors=[],
        )
        classifier_summary = _build_classifier_summary(
            unseen_articles=[build_raw_article("https://example.com/u1")],
            classified_articles=[],
        )
        result = RunSnapshot(
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            unseen_article_count=1,
            relevant_article_count=0,
        )

        problems = _build_problem_summary(
            source_summaries=source_summaries,
            classifier_summary=classifier_summary,
            result=result,
        )
        suspicions = _build_suspicions(
            source_summaries=source_summaries,
            classifier_summary=classifier_summary,
            result=result,
        )

        assert problems["classification_output_anomaly"] is True
        assert problems["all_unseen_rejected"] is False
        assert "all_unseen_rejected" not in suspicions
        assert "classification_output_anomaly" in suspicions

    @pytest.mark.asyncio
    async def test_run_pipeline_async_happy_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful runs should return unified items after marking them seen."""

        def fake_create_classifier(
            *,
            api_key: str,
            model: str,
            system_prompt: str | None = None,
            user_prompt_template: str | None = None,
        ) -> MagicMock:
            del api_key, model, system_prompt, user_prompt_template
            return classifier

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return deduplicator

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        article = build_raw_article()
        classified = [build_classified_article()]
        unified = [build_unified_item()]
        seen_store = MagicMock()
        classifier = MagicMock()
        deduplicator = MagicMock()
        mark_seen_mock = MagicMock()

        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", fake_create_classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        seen_store.count = 11
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([article], ["mako: timeout"])),
        )
        monkeypatch.setattr(
            "denbust.pipeline.filter_seen",
            lambda articles, _current_seen_store: articles,
        )
        classifier.classify_batch = AsyncMock(return_value=classified)
        monkeypatch.setattr("denbust.pipeline.deduplicate_articles", lambda _articles, _d: unified)
        monkeypatch.setattr("denbust.pipeline.mark_seen", mark_seen_mock)

        result = await run_pipeline_async(Config(), days=3)

        assert result.items == unified
        assert result.raw_article_count == 1
        assert result.unseen_article_count == 1
        assert result.relevant_article_count == 1
        assert result.unified_item_count == 1
        assert result.errors == ["mako: timeout"]
        assert result.seen_count_before == 11
        assert result.seen_count_after == 11
        mark_seen_mock.assert_called_once_with(unified, seen_store)

    @pytest.mark.asyncio
    async def test_run_news_ingest_job_records_privacy_mix_warning(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Successful ingest runs should summarize the privacy-risk mix."""

        class FakeStore:
            def __init__(self) -> None:
                self.upserts: list[tuple[str, list[dict[str, object]]]] = []

            def upsert_records(self, dataset_name: str, records: list[dict[str, object]]) -> None:
                self.upserts.append((dataset_name, records))

        class FakeOperationalRecord:
            privacy_risk_level = PrivacyRisk.MEDIUM

            def model_dump(self, mode: str = "json") -> dict[str, str]:
                del mode
                return {"id": "row-1"}

        fake_store = FakeStore()
        raw_article = build_raw_article()
        unified_item = build_unified_item()
        seen_store = MagicMock(count=2)
        operational_record = FakeOperationalRecord()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(return_value=[build_classified_article()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([raw_article], [])),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _store: articles)
        monkeypatch.setattr(
            "denbust.pipeline.deduplicate_articles",
            lambda _articles, _deduplicator: [unified_item],
        )
        monkeypatch.setattr(
            "denbust.pipeline.build_operational_records",
            AsyncMock(return_value=[operational_record]),
        )

        def fake_mark_seen(items: list[UnifiedItem], _seen_store: object) -> None:
            del items
            seen_store.count = 3

        monkeypatch.setattr("denbust.pipeline.mark_seen", fake_mark_seen)

        result = await run_news_ingest_job(
            Config(
                output=OutputConfig(formats=[OutputFormat.CLI]),
                store={"state_root": tmp_path},
            ),
            operational_store=fake_store,
        )

        assert result.unified_item_count == 1
        assert "privacy_risk_distribution=medium:1" in result.warnings
        assert result.seen_count_after == 3
        assert fake_store.upserts == [("news_items", [{"id": "row-1"}])]

    @pytest.mark.asyncio
    async def test_persist_source_native_candidates_skips_disabled_sources(
        self, tmp_path: Path
    ) -> None:
        """Per-source source_discovery toggles should suppress disabled sources."""
        config = Config(
            source_discovery={"sources": {"ynet": False}},
            store={"state_root": tmp_path},
        )

        persisted = await _persist_source_native_candidates(
            config=config,
            raw_articles=[
                RawArticle(
                    url=HttpUrl("https://www.ynet.co.il/item"),
                    title="כתבה מיינט",
                    snippet="תקציר",
                    date=datetime(2026, 4, 11, tzinfo=UTC),
                    source_name="ynet",
                ),
                RawArticle(
                    url=HttpUrl("https://www.walla.co.il/item"),
                    title="פשיטה נוספת",
                    snippet="תקציר",
                    date=datetime(2026, 4, 11, tzinfo=UTC),
                    source_name="walla",
                ),
            ],
            run_id="run-disabled-source",
        )

        assert len(persisted.candidates) == 1
        assert persisted.candidates[0].source_hints == ["walla"]

    @pytest.mark.asyncio
    async def test_run_source_native_discovery_records_source_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Discover should keep good source candidates and record failing sources."""

        class FakePersistence:
            def close(self) -> None:
                pass

            def write_run(self, run: DiscoveryRun) -> None:
                self.run = run

            def upsert_candidates(self, candidates: list[object]) -> None:
                self.candidates = candidates

            def append_provenance(self, provenance: list[object]) -> None:
                self.provenance = provenance

            def find_candidate_by_urls(
                self, *, canonical_url: str | None, current_url: str | None
            ) -> None:
                del canonical_url, current_url
                return None

        async def fake_discover_candidates(self: object, _context: object) -> list[object]:
            name = self._source.name
            if name == "bad":
                raise RuntimeError("boom")
            from denbust.discovery.source_native import raw_article_to_discovered_candidate

            return [
                raw_article_to_discovered_candidate(build_raw_article("https://example.com/good"))
            ]

        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence", lambda _config: FakePersistence()
        )
        monkeypatch.setattr(
            "denbust.pipeline.SourceDiscoveryAdapter.discover_candidates",
            fake_discover_candidates,
        )

        persisted = await _run_source_native_discovery(
            config=Config(),
            sources=[FakeSource("bad", []), FakeSource("good", [])],
            run_id="run-source-errors",
            days=3,
        )

        assert persisted.run.errors == ["bad: boom"]
        assert persisted.run.status is DiscoveryRunStatus.PARTIAL
        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_news_ingest_job_warns_when_source_native_persistence_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ingest should degrade to warnings if source-native persistence side effects fail."""
        raw_article = build_raw_article()
        seen_store = MagicMock(count=5)

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([raw_article], [])),
        )
        monkeypatch.setattr(
            "denbust.pipeline._persist_source_native_candidates",
            AsyncMock(side_effect=RuntimeError("boom")),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda _articles, _seen_store: [])

        result = await run_news_ingest_job(Config(), operational_store=MagicMock())

        assert "source_native_candidate_persistence_failed=RuntimeError: boom" in result.warnings

    @pytest.mark.asyncio
    async def test_run_news_ingest_job_uses_candidate_scrape_results_and_passthrough_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ingest should use candidate-driven results while preserving disabled-source passthrough."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        seen_store = MagicMock(count=1)
        source_scraped = RawArticle(
            url=HttpUrl("https://www.ynet.co.il/news/article/1"),
            title="כותרת ינט",
            snippet="תקציר ינט",
            date=datetime(2026, 4, 11, tzinfo=UTC),
            source_name="ynet",
        )
        disabled_passthrough = RawArticle(
            url=HttpUrl("https://www.walla.co.il/item/1"),
            title="כותרת וואלה",
            snippet="תקציר וואלה",
            date=datetime(2026, 4, 11, tzinfo=UTC),
            source_name="walla",
        )
        persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-ingest",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.INGEST,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[],
            provenance=[],
        )
        scrape_batch = CandidateScrapeBatch(
            selected_candidates=[],
            updated_candidates=[],
            attempts=[],
            raw_articles=[source_scraped],
            errors=[],
        )
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            side_effect=lambda articles: [
                build_classified_article(str(article.url), relevant=True) for article in articles
            ]
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources",
            lambda _config: [FakeSource("ynet", []), FakeSource("walla", [])],
        )
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([source_scraped, disabled_passthrough], [])),
        )
        monkeypatch.setattr(
            "denbust.pipeline._persist_source_native_candidates",
            AsyncMock(return_value=persisted),
        )
        monkeypatch.setattr(
            "denbust.pipeline._scrape_candidate_batch",
            AsyncMock(return_value=scrape_batch),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _store: articles)
        monkeypatch.setattr(
            "denbust.pipeline.deduplicate_articles",
            lambda articles, _deduplicator: [
                build_unified_item(str(article.article.url)) for article in articles
            ],
        )
        monkeypatch.setattr(
            "denbust.pipeline.build_operational_records",
            AsyncMock(return_value=[]),
        )
        mark_seen_mock = MagicMock()
        monkeypatch.setattr("denbust.pipeline.mark_seen", mark_seen_mock)
        operational_store = MagicMock()

        result = await run_news_ingest_job(
            Config(source_discovery={"sources": {"walla": False}}),
            operational_store=operational_store,
        )

        assert result.raw_article_count == 2
        classifier.classify_batch.assert_awaited_once()
        classified_input = classifier.classify_batch.await_args.args[0]
        assert [article.source_name for article in classified_input] == ["ynet", "walla"]
        mark_seen_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_news_scrape_candidates_job_processes_scraped_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The scrape-candidates job should feed successful scraped articles into ingest processing."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        seen_store = MagicMock(count=0)
        raw_article = build_raw_article("https://example.com/queued")
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[build_classified_article("https://example.com/queued", relevant=True)]
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [FakeSource("test", [])]
        )
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline._run_candidate_scrape_job",
            AsyncMock(
                return_value=CandidateScrapeBatch(
                    selected_candidates=[MagicMock()],
                    updated_candidates=[],
                    attempts=[],
                    raw_articles=[raw_article],
                    errors=["candidate-1: generic fetch fallback not implemented yet"],
                )
            ),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _store: articles)
        monkeypatch.setattr(
            "denbust.pipeline.deduplicate_articles",
            lambda _articles, _deduplicator: [build_unified_item("https://example.com/queued")],
        )
        monkeypatch.setattr(
            "denbust.pipeline.build_operational_records",
            AsyncMock(return_value=[]),
        )
        operational_store = MagicMock()
        mark_seen_mock = MagicMock()
        monkeypatch.setattr("denbust.pipeline.mark_seen", mark_seen_mock)

        result = await run_news_scrape_candidates_job(
            Config(job_name=JobName.SCRAPE_CANDIDATES),
            operational_store=operational_store,
        )

        assert result.raw_article_count == 1
        assert result.errors == ["candidate-1: generic fetch fallback not implemented yet"]
        assert "candidate_scrape_failures=1" in result.warnings
        assert result.unified_item_count == 1
        mark_seen_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_candidate_scrape_job_selects_candidates_and_closes_persistence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Queued candidate selection should close persistence before scraping."""

        class FakePersistence:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        config = Config()
        persistence = FakePersistence()
        selected_candidates = [MagicMock(name="candidate-1"), MagicMock(name="candidate-2")]
        scrape_mock = AsyncMock(
            return_value=CandidateScrapeBatch(
                selected_candidates=selected_candidates,
                updated_candidates=[],
                attempts=[],
                raw_articles=[],
                errors=[],
            )
        )
        select_mock = MagicMock(return_value=selected_candidates)
        monkeypatch.setattr(
            "denbust.pipeline.create_discovery_persistence",
            lambda _config: persistence,
        )
        monkeypatch.setattr("denbust.pipeline.select_candidates_for_scrape", select_mock)
        monkeypatch.setattr("denbust.pipeline._scrape_candidate_batch", scrape_mock)

        sources = [FakeSource("ynet", [])]
        result = await pipeline_module._run_candidate_scrape_job(
            config=config,
            sources=sources,
            limit=3,
        )

        assert result.selected_candidates == selected_candidates
        assert persistence.closed is True
        select_mock.assert_called_once_with(persistence, limit=3)
        scrape_mock.assert_awaited_once_with(
            config=config,
            candidates=selected_candidates,
            sources=sources,
        )

    @pytest.mark.asyncio
    async def test_run_news_ingest_job_warns_when_candidate_scrape_layer_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ingest should continue and warn when candidate scrape orchestration raises."""
        source_article = build_raw_article("https://example.com/source-layer")
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[build_classified_article("https://example.com/source-layer")]
        )
        persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-ingest",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[MagicMock(name="candidate-1")],
            provenance=[],
        )
        seen_store = MagicMock(count=0)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.pipeline.create_sources",
            lambda _config: [FakeSource("ynet", [source_article])],
        )
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline.fetch_all_sources",
            AsyncMock(return_value=([source_article], [])),
        )
        monkeypatch.setattr(
            "denbust.pipeline._persist_source_native_candidates",
            AsyncMock(return_value=persisted),
        )
        monkeypatch.setattr(
            "denbust.pipeline._scrape_candidate_batch",
            AsyncMock(side_effect=RuntimeError("scrape boom")),
        )
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _store: articles)
        monkeypatch.setattr(
            "denbust.pipeline.deduplicate_articles",
            lambda articles, _deduplicator: [
                build_unified_item(str(article.article.url)) for article in articles
            ],
        )
        monkeypatch.setattr(
            "denbust.pipeline.build_operational_records",
            AsyncMock(return_value=[]),
        )
        monkeypatch.setattr("denbust.pipeline.mark_seen", MagicMock())

        result = await run_news_ingest_job(Config())

        assert "candidate_scrape_layer_failed=RuntimeError: scrape boom" in result.warnings
        assert result.raw_article_count == 1
        assert result.unified_item_count == 1

    @pytest.mark.asyncio
    async def test_run_news_scrape_candidates_job_rejects_missing_api_key(self) -> None:
        """The scrape-candidates job should fail clearly without an API key."""
        result = await run_news_scrape_candidates_job(Config(job_name=JobName.SCRAPE_CANDIDATES))

        assert result.fatal is True
        assert result.errors == ["ANTHROPIC_API_KEY not set"]
        assert result.result_summary == "fatal: missing anthropic api key"

    @pytest.mark.asyncio
    async def test_run_news_scrape_candidates_job_rejects_missing_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The scrape-candidates job should fail clearly when no sources are configured."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

        result = await run_news_scrape_candidates_job(Config(job_name=JobName.SCRAPE_CANDIDATES))

        assert result.fatal is True
        assert result.errors == ["No sources configured"]
        assert result.result_summary == "fatal: no sources configured"

    @pytest.mark.asyncio
    async def test_run_news_scrape_candidates_job_finishes_when_queue_is_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The scrape-candidates job should finish cleanly when nothing is eligible."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [FakeSource("test", [])]
        )
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))
        monkeypatch.setattr(
            "denbust.pipeline._run_candidate_scrape_job",
            AsyncMock(
                return_value=CandidateScrapeBatch(
                    selected_candidates=[],
                    updated_candidates=[],
                    attempts=[],
                    raw_articles=[],
                    errors=[],
                )
            ),
        )

        result = await run_news_scrape_candidates_job(Config(job_name=JobName.SCRAPE_CANDIDATES))

        assert result.fatal is False
        assert result.raw_article_count == 0
        assert result.result_summary == "no queued candidates eligible for scrape"

    @pytest.mark.asyncio
    async def test_run_news_scrape_candidates_job_passes_days_override_to_scrape_layer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The scrape layer should receive the effective days override, not the base config days."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(return_value=[])
        seen_store = MagicMock(count=0)
        captured: dict[str, object] = {}

        async def fake_run_candidate_scrape_job(
            *,
            config: Config,
            sources: list[FakeSource],
            limit: int,
        ) -> CandidateScrapeBatch:
            captured["days"] = config.days
            captured["limit"] = limit
            captured["sources"] = [source.name for source in sources]
            return CandidateScrapeBatch(
                selected_candidates=[],
                updated_candidates=[],
                attempts=[],
                raw_articles=[],
                errors=[],
            )

        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [FakeSource("test", [])]
        )
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr(
            "denbust.pipeline._run_candidate_scrape_job",
            fake_run_candidate_scrape_job,
        )

        result = await run_news_scrape_candidates_job(
            Config(job_name=JobName.SCRAPE_CANDIDATES, days=3, max_articles=7),
            days_override=11,
        )

        assert result.result_summary == "no queued candidates eligible for scrape"
        assert captured == {"days": 11, "limit": 7, "sources": ["test"]}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("config", "error", "summary"),
        [
            (
                Config(source_discovery={"enabled": False}),
                "source_discovery.enabled is false",
                "fatal: source-native discovery disabled",
            ),
            (
                Config(source_discovery={"persist_candidates": False}),
                "source_discovery.persist_candidates is false",
                "fatal: source-native candidate persistence disabled",
            ),
            (
                Config(),
                "No sources configured",
                "fatal: no sources configured",
            ),
        ],
    )
    async def test_run_news_discover_job_rejects_invalid_configuration(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config: Config,
        error: str,
        summary: str,
    ) -> None:
        """Discover should fail early for disabled persistence or missing sources."""
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

        result = await run_news_discover_job(config)

        assert result.fatal is True
        assert result.errors == [error]
        assert result.result_summary == summary

    @pytest.mark.asyncio
    async def test_run_news_discover_job_marks_failed_and_partial_runs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Discover should surface failed runs as fatal and partial runs as warnings."""
        persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.PARTIAL,
                candidate_count=2,
                merged_candidate_count=1,
            ),
            candidates=[],
            provenance=[],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_source_native_discovery",
            AsyncMock(return_value=persisted),
        )

        partial_result = await run_news_discover_job(Config(store={"state_root": tmp_path}))

        assert partial_result.fatal is False
        assert (
            "source-native discovery completed with partial source failures"
            in partial_result.warnings
        )

        persisted.run.status = DiscoveryRunStatus.FAILED
        failed_result = await run_news_discover_job(Config(store={"state_root": tmp_path}))

        assert failed_result.fatal is True

    @pytest.mark.asyncio
    async def test_run_news_discover_job_supports_brave_only_and_writes_metrics(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Brave discovery should run without configured sources and persist engine metrics."""
        brave_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=3,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-brave-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                ),
                build_persistent_candidate(
                    "candidate-brave-2",
                    current_url="https://www.mako.co.il/news/article/2",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 3
        assert result.unified_item_count == 2
        metrics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
        ).read_text(encoding="utf-8")
        diagnostics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "discovery_diagnostics_latest.json"
        ).read_text(encoding="utf-8")
        assert '"brave": 2' in metrics_payload
        assert '"source_native": 0' in metrics_payload
        assert '"exa": 0' in metrics_payload
        assert '"dataset_name": "news_items"' in diagnostics_payload

    @pytest.mark.asyncio
    async def test_run_news_discover_job_supports_exa_only_and_writes_metrics(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Exa discovery should run without configured sources and persist engine metrics."""
        exa_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:exa",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-exa-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                ),
                build_persistent_candidate(
                    "candidate-exa-2",
                    current_url="https://www.mako.co.il/news/article/2",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_exa_discovery",
            AsyncMock(return_value=exa_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"exa": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 2
        assert result.unified_item_count == 2
        metrics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
        ).read_text(encoding="utf-8")
        assert '"exa": 2' in metrics_payload
        assert '"source_native": 0' in metrics_payload
        assert '"brave": 0' in metrics_payload
        assert '"google_cse": 0' in metrics_payload

    @pytest.mark.asyncio
    async def test_run_news_discover_job_supports_google_cse_only_and_writes_metrics(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Google CSE discovery should run without configured sources and persist engine metrics."""
        google_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:google_cse",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-google-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                ),
                build_persistent_candidate(
                    "candidate-google-2",
                    current_url="https://www.mako.co.il/news/article/2",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_google_cse_discovery",
            AsyncMock(return_value=google_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 2
        assert result.unified_item_count == 2
        metrics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
        ).read_text(encoding="utf-8")
        assert '"google_cse": 2' in metrics_payload
        assert '"source_native": 0' in metrics_payload
        assert '"brave": 0' in metrics_payload
        assert '"exa": 0' in metrics_payload

    @pytest.mark.asyncio
    async def test_run_news_discover_job_aggregates_brave_and_source_native_without_double_counting(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Merged candidate counts should be deduplicated across discovery producers."""
        source_native = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:source_native",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared",
                    current_url="https://www.ynet.co.il/news/article/shared",
                ),
                build_persistent_candidate(
                    "candidate-source-only",
                    current_url="https://www.walla.co.il/news/article/source-only",
                ),
            ],
            provenance=[],
        )
        brave = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=3,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared",
                    current_url="https://www.ynet.co.il/news/article/shared",
                ),
                build_persistent_candidate(
                    "candidate-brave-only",
                    current_url="https://www.mako.co.il/news/article/brave-only",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_source_native_discovery",
            AsyncMock(return_value=source_native),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 5
        assert result.unified_item_count == 3

    @pytest.mark.asyncio
    async def test_run_news_discover_job_aggregates_brave_exa_and_source_native_overlaps(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Merged candidate counts and overlap metrics should include Exa alongside Brave."""
        source_native = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:source_native",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                )
            ],
            provenance=[],
        )
        brave = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                ),
                build_persistent_candidate(
                    "candidate-brave-only",
                    current_url="https://www.mako.co.il/news/article/brave-only",
                ),
            ],
            provenance=[],
        )
        exa = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:exa",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                ),
                build_persistent_candidate(
                    "candidate-exa-only",
                    current_url="https://www.maariv.co.il/news/article/exa-only",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_source_native_discovery",
            AsyncMock(return_value=source_native),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_exa_discovery",
            AsyncMock(return_value=exa),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                discovery={
                    "enabled": True,
                    "engines": {"brave": {"enabled": True}, "exa": {"enabled": True}},
                },
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 5
        assert result.unified_item_count == 3
        metrics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
        ).read_text(encoding="utf-8")
        assert '"exa": 2' in metrics_payload
        assert '"source_native_brave_shared": 1' in metrics_payload
        assert '"source_native_exa_shared": 1' in metrics_payload
        assert '"brave_exa_shared": 1' in metrics_payload
        assert '"shared_all_candidates": 0' in metrics_payload

    @pytest.mark.asyncio
    async def test_run_news_discover_job_aggregates_all_engines_and_source_native_overlaps(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Merged candidate counts and overlap metrics should include Google CSE too."""
        source_native = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:source_native",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                )
            ],
            provenance=[],
        )
        brave = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                ),
                build_persistent_candidate(
                    "candidate-brave-only",
                    current_url="https://www.mako.co.il/news/article/brave-only",
                ),
            ],
            provenance=[],
        )
        exa = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:exa",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                ),
                build_persistent_candidate(
                    "candidate-exa-only",
                    current_url="https://www.maariv.co.il/news/article/exa-only",
                ),
            ],
            provenance=[],
        )
        google_cse = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:google_cse",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=2,
                merged_candidate_count=2,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-shared-all",
                    current_url="https://www.ynet.co.il/news/article/shared-all",
                ),
                build_persistent_candidate(
                    "candidate-google-only",
                    current_url="https://www.walla.co.il/news/article/google-only",
                ),
            ],
            provenance=[],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_source_native_discovery",
            AsyncMock(return_value=source_native),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_exa_discovery",
            AsyncMock(return_value=exa),
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_google_cse_discovery",
            AsyncMock(return_value=google_cse),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                discovery={
                    "enabled": True,
                    "engines": {
                        "brave": {"enabled": True},
                        "exa": {"enabled": True},
                        "google_cse": {"enabled": True},
                    },
                },
            )
        )

        assert result.fatal is False
        assert result.raw_article_count == 7
        assert result.unified_item_count == 4
        metrics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
        ).read_text(encoding="utf-8")
        diagnostics_payload = (
            tmp_path / "news_items" / "discover" / "metrics" / "discovery_diagnostics_latest.json"
        ).read_text(encoding="utf-8")
        assert '"google_cse": 2' in metrics_payload
        assert '"source_native_google_cse_shared": 1' in metrics_payload
        assert '"brave_google_cse_shared": 1' in metrics_payload
        assert '"exa_google_cse_shared": 1' in metrics_payload
        assert '"shared_all_candidates": 1' in metrics_payload
        assert '"shared_candidates": 1' in diagnostics_payload

    @pytest.mark.asyncio
    async def test_run_news_discover_job_uses_durable_diagnostics_and_run_overlap_metrics(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Diagnostics should come from durable state while overlap stays scoped to this run."""
        config = Config(
            store={"state_root": tmp_path},
            operational={
                "provider": "local_json",
                "root_dir": tmp_path / "operational",
            },
            discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
        )
        persistence = StateRepoDiscoveryPersistence(config.discovery_state_paths)
        persistence.upsert_candidates(
            [
                build_persistent_candidate(
                    "candidate-shared",
                    current_url="https://www.ynet.co.il/news/article/shared",
                ).model_copy(update={"discovered_via": ["exa"]}),
            ]
        )
        LocalJsonOperationalStore(tmp_path / "operational").upsert_records(
            "news_items",
            [
                {
                    "id": "news-item-1",
                    "canonical_url": "https://www.ynet.co.il/news/article/shared",
                    "publication_datetime": datetime.now(UTC).isoformat(),
                }
            ],
        )

        async def fake_run_source_native_discovery(
            *,
            config: Config,
            sources: list[object],
            run_id: str,
            days: int,
        ) -> PersistedSourceDiscovery:
            del config, sources, days
            candidate = build_persistent_candidate(
                "candidate-shared",
                current_url="https://www.ynet.co.il/news/article/shared",
            ).model_copy(update={"discovered_via": ["exa", "source_native"]})
            persistence.upsert_candidates([candidate])
            return PersistedSourceDiscovery(
                run=DiscoveryRun(
                    run_id=run_id,
                    dataset_name=DatasetName.NEWS_ITEMS,
                    job_name=JobName.DISCOVER,
                    status=DiscoveryRunStatus.SUCCEEDED,
                    candidate_count=1,
                    merged_candidate_count=1,
                ),
                candidates=[candidate],
                provenance=[],
            )

        async def fake_run_brave_discovery(
            *,
            config: Config,
            run_id: str,
            days: int,
        ) -> PersistedSourceDiscovery:
            del config, days
            candidate = build_persistent_candidate(
                "candidate-brave-only",
                current_url="https://www.mako.co.il/news/article/brave-only",
            ).model_copy(update={"discovered_via": ["brave"]})
            persistence.upsert_candidates([candidate])
            return PersistedSourceDiscovery(
                run=DiscoveryRun(
                    run_id=run_id,
                    dataset_name=DatasetName.NEWS_ITEMS,
                    job_name=JobName.DISCOVER,
                    status=DiscoveryRunStatus.SUCCEEDED,
                    candidate_count=1,
                    merged_candidate_count=1,
                ),
                candidates=[candidate],
                provenance=[],
            )

        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_source_native_discovery",
            fake_run_source_native_discovery,
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            fake_run_brave_discovery,
        )

        result = await run_news_discover_job(config)

        assert result.fatal is False
        metrics_payload = json.loads(
            (
                tmp_path / "news_items" / "discover" / "metrics" / "engine_overlap_latest.json"
            ).read_text(encoding="utf-8")
        )
        diagnostics_payload = json.loads(
            (
                tmp_path
                / "news_items"
                / "discover"
                / "metrics"
                / "discovery_diagnostics_latest.json"
            ).read_text(encoding="utf-8")
        )
        per_producer = {
            entry["producer_name"]: entry for entry in diagnostics_payload["candidate_conversion"]["per_producer"]
        }

        assert metrics_payload["exa"] == 0
        assert metrics_payload["source_native"] == 1
        assert metrics_payload["brave"] == 1
        assert per_producer["exa"]["candidate_count"] == 1
        assert diagnostics_payload["operational_records_available"] is False
        assert any(
            "Operational record matching was skipped" in note
            for note in diagnostics_payload["notes"]
        )

    @pytest.mark.asyncio
    async def test_run_news_discover_job_marks_brave_failure_fatal_when_it_is_the_only_engine(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A failed Brave-only discovery run should surface as fatal."""
        brave_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.FAILED,
                candidate_count=0,
                merged_candidate_count=0,
                errors=["brave: missing DENBUST_BRAVE_SEARCH_API_KEY"],
            ),
            candidates=[],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert result.fatal is True
        assert result.errors == ["brave: missing DENBUST_BRAVE_SEARCH_API_KEY"]

    @pytest.mark.asyncio
    async def test_run_news_discover_job_marks_exa_failure_fatal_when_it_is_the_only_engine(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A failed Exa-only discovery run should surface as fatal."""
        exa_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:exa",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.FAILED,
                candidate_count=0,
                merged_candidate_count=0,
                errors=["exa: missing DENBUST_EXA_API_KEY"],
            ),
            candidates=[],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_exa_discovery",
            AsyncMock(return_value=exa_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"exa": {"enabled": True}}},
            )
        )

        assert result.fatal is True
        assert result.errors == ["exa: missing DENBUST_EXA_API_KEY"]

    @pytest.mark.asyncio
    async def test_run_news_discover_job_marks_google_cse_failure_fatal_when_it_is_the_only_engine(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A failed Google CSE-only discovery run should surface as fatal."""
        google_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:google_cse",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.FAILED,
                candidate_count=0,
                merged_candidate_count=0,
                errors=["google_cse: missing DENBUST_GOOGLE_CSE_API_KEY"],
            ),
            candidates=[],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_google_cse_discovery",
            AsyncMock(return_value=google_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}},
            )
        )

        assert result.fatal is True
        assert result.errors == ["google_cse: missing DENBUST_GOOGLE_CSE_API_KEY"]

    @pytest.mark.asyncio
    async def test_run_news_discover_job_fails_when_engine_persistence_disabled_and_no_source_native(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Engine-backed discover should fail early when engine persistence is disabled."""
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

        result = await run_news_discover_job(
            Config(
                source_discovery={"enabled": True, "persist_candidates": True},
                discovery={
                    "enabled": True,
                    "persist_candidates": False,
                    "engines": {"brave": {"enabled": True}},
                },
            )
        )

        assert result.fatal is True
        assert result.errors == ["discovery.persist_candidates is false"]
        assert result.result_summary == "fatal: engine candidate persistence disabled"

    @pytest.mark.asyncio
    async def test_run_news_discover_job_warns_when_source_native_skipped_for_no_sources_but_brave_runs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Source-native no-source skips should degrade to a warning when Brave remains active."""
        brave_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-brave-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                )
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert (
            "source-native discovery skipped because no sources are configured" in result.warnings
        )

    @pytest.mark.asyncio
    async def test_run_news_discover_job_warns_when_source_native_persistence_disabled_but_brave_runs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Source-native persistence-disabled skips should degrade to a warning when Brave runs."""
        brave_persisted = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-brave-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                )
            ],
            provenance=[],
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources", lambda _config: [MagicMock(name="ynet")]
        )
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave_persisted),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"persist_candidates": False},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert result.fatal is False
        assert "source-native discovery skipped because persistence is disabled" in result.warnings

    @pytest.mark.asyncio
    async def test_run_news_discover_job_warns_on_brave_partial(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Brave partial runs should surface as warnings and still write metrics."""
        brave_partial = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:brave",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.PARTIAL,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-brave-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                )
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_brave_discovery",
            AsyncMock(return_value=brave_partial),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"brave": {"enabled": True}}},
            )
        )

        assert "brave discovery completed with partial engine failures" in result.warnings

    @pytest.mark.asyncio
    async def test_run_news_discover_job_warns_on_exa_partial(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Exa partial runs should surface as warnings and still write metrics."""
        exa_partial = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:exa",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.PARTIAL,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-exa-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                )
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_exa_discovery",
            AsyncMock(return_value=exa_partial),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"exa": {"enabled": True}}},
            )
        )

        assert "exa discovery completed with partial engine failures" in result.warnings

    @pytest.mark.asyncio
    async def test_run_news_discover_job_warns_on_google_cse_partial(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Google CSE partial runs should surface as warnings and still write metrics."""
        google_partial = PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-discover:google_cse",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.DISCOVER,
                status=DiscoveryRunStatus.PARTIAL,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[
                build_persistent_candidate(
                    "candidate-google-1",
                    current_url="https://www.ynet.co.il/news/article/1",
                )
            ],
            provenance=[],
        )
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
        monkeypatch.setattr(
            "denbust.pipeline._run_google_cse_discovery",
            AsyncMock(return_value=google_partial),
        )

        result = await run_news_discover_job(
            Config(
                store={"state_root": tmp_path},
                source_discovery={"enabled": False},
                discovery={"enabled": True, "engines": {"google_cse": {"enabled": True}}},
            )
        )

        assert "google_cse discovery completed with partial engine failures" in result.warnings


class TestRunPipeline:
    """Tests for the sync run_pipeline wrapper."""

    def test_run_pipeline_exits_on_missing_config(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Missing config files should print a helpful message and exit 1."""
        missing = tmp_path / "missing.yaml"

        with pytest.raises(SystemExit) as exc_info:
            run_pipeline(missing)

        assert exc_info.value.code == 1
        assert f"Error: Config file not found: {missing}" in capsys.readouterr().out

    def test_run_job_from_config_writes_ingest_debug_log(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """CLI job runs should persist ingest debug logs under the namespaced logs directory."""
        config = Config(
            dataset_name="news_items",
            job_name="ingest",
            store={"runs_dir": tmp_path / "runs", "state_root": tmp_path / "state"},
            output=OutputConfig(formats=[OutputFormat.CLI]),
        )
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            config_name=config.name,
        ).finish("no relevant articles found")
        snapshot.set_debug_payload(
            {
                "schema_version": "news_items.ingest.debug.v1",
                "run_timestamp": "2026-03-15T04:00:00Z",
                "dataset_name": "news_items",
                "job_name": "ingest",
                "config_name": config.name,
                "result_summary": "no relevant articles found",
                "counts": {"unseen_article_count": 1},
                "workflow": {},
                "source_summaries": [],
                "classifier_summary": {"rejected_article_count": 1},
                "problems": {"all_unseen_rejected": True},
                "suspicions": ["all_unseen_rejected"],
                "warnings": [],
                "errors": [],
                "rejected_articles": [{"title": "כתבה", "relevant": False}],
            }
        )

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", AsyncMock(return_value=snapshot))
        monkeypatch.setattr("denbust.pipeline.output_items", MagicMock(return_value=[]))

        result = _run_job_from_config(
            config_path=Path("agents/news/github.yaml"),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
        )

        assert result is snapshot
        debug_path = config.state_paths.logs_dir / "2026-03-15T04-00-00-000000Z.json"
        summary_path = config.state_paths.logs_dir / "2026-03-15T04-00-00-000000Z.summary.json"
        assert debug_path.exists()
        assert summary_path.exists()
        content = debug_path.read_text(encoding="utf-8")
        assert '"rejected_articles": [' in content
        summary_content = summary_path.read_text(encoding="utf-8")
        assert '"suspicions": [' in summary_content

    def test_run_job_from_config_best_effort_debug_write_still_persists_snapshot(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Debug artifact write failures should not prevent the normal run snapshot."""
        config = Config(
            dataset_name="news_items",
            job_name="ingest",
            store={"runs_dir": tmp_path / "runs", "state_root": tmp_path / "state"},
            output=OutputConfig(formats=[OutputFormat.CLI]),
        )
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            config_name=config.name,
        ).finish("no relevant articles found")
        snapshot.set_debug_payload({"schema_version": "news_items.ingest.debug.v1"})

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", AsyncMock(return_value=snapshot))
        monkeypatch.setattr("denbust.pipeline.output_items", MagicMock(return_value=[]))
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)
        monkeypatch.setattr(
            "denbust.pipeline.write_run_debug_log",
            MagicMock(side_effect=OSError("disk full")),
        )
        monkeypatch.setattr(
            "denbust.pipeline.write_run_debug_summary",
            MagicMock(side_effect=TypeError("not serializable")),
        )

        result = _run_job_from_config(
            config_path=Path("agents/news/github.yaml"),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
        )

        assert result is snapshot
        assert result.errors == [
            "Failed to write run debug log: disk full",
            "Failed to write run debug summary: not serializable",
        ]
        snapshot_path = config.state_paths.runs_dir / "2026-03-15T04-00-00-000000Z.json"
        assert snapshot_path.exists()
        warning_messages = [call.args[0] for call in mock_logger.warning.call_args_list]
        assert "Failed to write run debug log: %s" in warning_messages
        assert "Failed to write run debug summary: %s" in warning_messages

    def test_run_pipeline_exits_on_invalid_config(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Other config load failures should also print and exit 1."""
        config_path = tmp_path / "broken.yaml"
        monkeypatch.setattr(
            "denbust.pipeline.load_config",
            MagicMock(side_effect=ValueError("bad yaml")),
        )

        with pytest.raises(SystemExit) as exc_info:
            run_pipeline(config_path)

        assert exc_info.value.code == 1
        assert "Error loading config: bad yaml" in capsys.readouterr().out

    def test_run_pipeline_uses_days_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """run_pipeline should prefer explicit day overrides over config defaults."""
        config = Config(days=3, output=OutputConfig(formats=[OutputFormat.CLI]))
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            config_name=config.name,
            days_searched=7,
            output_formats=["cli"],
            items=[build_unified_item()],
        )
        run_job_async_mock = AsyncMock(return_value=snapshot)
        output_items_mock = MagicMock(return_value=["telegram: not implemented"])
        write_snapshot_mock = MagicMock()

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", run_job_async_mock)
        monkeypatch.setattr("denbust.pipeline.output_items", output_items_mock)
        monkeypatch.setattr("denbust.pipeline.write_run_snapshot", write_snapshot_mock)

        run_pipeline(Path("agents/news.yaml"), days_override=7)

        run_job_async_mock.assert_awaited_once_with(
            config, config_path=Path("agents/news.yaml"), days_override=7
        )
        output_items_mock.assert_called_once_with(snapshot.items, config)
        assert snapshot.errors == ["telegram: not implemented"]
        write_snapshot_mock.assert_called_once_with(config.state_paths.runs_dir, snapshot)

    def test_run_pipeline_exits_after_writing_fatal_snapshot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fatal pipeline results should still write a snapshot, then exit 1."""
        config = Config(days=3, output=OutputConfig(formats=[OutputFormat.CLI]))
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            config_name=config.name,
            days_searched=3,
            output_formats=["cli"],
            fatal=True,
            errors=["ANTHROPIC_API_KEY not set"],
        )
        write_snapshot_mock = MagicMock()

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", AsyncMock(return_value=snapshot))
        output_items_mock = MagicMock(return_value=[])
        monkeypatch.setattr("denbust.pipeline.output_items", output_items_mock)
        monkeypatch.setattr("denbust.pipeline.write_run_snapshot", write_snapshot_mock)

        with pytest.raises(SystemExit) as exc_info:
            run_pipeline(Path("agents/news.yaml"))

        assert exc_info.value.code == 1
        output_items_mock.assert_not_called()
        write_snapshot_mock.assert_called_once_with(config.state_paths.runs_dir, snapshot)

    def test_run_pipeline_writes_snapshot_for_zero_item_runs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Even zero-item runs should emit a run snapshot file."""
        config = Config(
            output=OutputConfig(formats=[OutputFormat.CLI]),
            store={"seen_path": tmp_path / "seen.json", "runs_dir": tmp_path / "runs"},
        )
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            config_name=config.name,
            days_searched=3,
            output_formats=["cli"],
        )

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", AsyncMock(return_value=snapshot))
        monkeypatch.setattr("denbust.pipeline.output_items", MagicMock(return_value=[]))

        run_pipeline(Path("agents/news.yaml"))

        written = list((tmp_path / "runs").glob("*.json"))
        assert len(written) == 1

    @pytest.mark.asyncio
    async def test_run_job_async_rejects_unknown_dataset_job(self) -> None:
        """Unregistered dataset/job combinations should fail clearly."""
        config = Config(dataset_name="events", job_name="release")

        with pytest.raises(ValueError, match="Unsupported dataset/job combination"):
            await run_job_async(config)

    @pytest.mark.asyncio
    async def test_run_job_async_runs_discover_without_operational_store(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Discover jobs should persist candidates without requiring the operational store."""
        config = Config(
            dataset_name="news_items",
            job_name="discover",
            store={"state_root": tmp_path},
        )
        monkeypatch.setattr(
            "denbust.pipeline.create_sources",
            lambda _config: [
                FakeSource("ynet", [build_raw_article("https://www.ynet.co.il/item")])
            ],
        )

        def fail_if_called(_config: Config) -> None:
            raise AssertionError("create_operational_store should not be called for discover jobs")

        monkeypatch.setattr("denbust.pipeline.create_operational_store", fail_if_called)

        result = await run_job_async(config)

        assert result.job_name == "discover"
        assert result.raw_article_count == 1
        assert result.unified_item_count == 1
        assert result.fatal is False
        candidate_lines = (
            config.discovery_state_paths.latest_candidates_path.read_text(encoding="utf-8")
            .strip()
            .splitlines()
        )
        assert len(candidate_lines) == 1

    @pytest.mark.asyncio
    async def test_run_job_async_writes_run_metadata_via_operational_store(
        self, tmp_path: Path
    ) -> None:
        """run_job_async should write run metadata through the operational store boundary."""
        config = Config(dataset_name="news_items", job_name="release")
        operational_store = LocalJsonOperationalStore(tmp_path / "ops")

        result = await run_job_async(config, operational_store=operational_store)

        metadata_path = tmp_path / "ops" / "run_metadata.jsonl"
        assert result.job_name == "release"
        assert metadata_path.exists()
        assert '"job_name": "release"' in metadata_path.read_text(encoding="utf-8")

    @pytest.mark.asyncio
    async def test_run_job_async_warns_when_run_metadata_write_fails_and_closes_store(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Operational metadata failures should not suppress the returned snapshot."""

        class FakeStore:
            def __init__(self) -> None:
                self.closed = False

            def write_run_metadata(self, snapshot: RunSnapshot) -> None:
                del snapshot
                raise RuntimeError("supabase unavailable")

            def upsert_records(self, dataset_name: str, records: list[dict[str, object]]) -> None:
                del dataset_name, records

            def fetch_records(
                self, dataset_name: str, *, limit: int | None = None
            ) -> list[dict[str, object]]:
                del dataset_name, limit
                return []

            def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, object]]:
                del dataset_name
                return []

            def mark_publication_state(
                self, dataset_name: str, record_ids: list[str], publication_status: str
            ) -> None:
                del dataset_name, record_ids, publication_status

            def close(self) -> None:
                self.closed = True

        async def fake_handler(
            config: Config,
            config_path: Path | None,
            days_override: int | None,
            operational_store: object,
        ) -> RunSnapshot:
            del config_path, days_override, operational_store
            return RunSnapshot(
                config_name=config.name,
                dataset_name=config.dataset_name,
                job_name=config.job_name,
            ).finish("ok")

        fake_store = FakeStore()
        config = Config(dataset_name="news_items", job_name="release")
        monkeypatch.setattr("denbust.pipeline.ensure_default_jobs_registered", lambda: None)
        monkeypatch.setattr("denbust.pipeline.require_job_handler", lambda *_args: fake_handler)
        monkeypatch.setattr("denbust.pipeline.create_operational_store", lambda _config: fake_store)

        result = await run_job_async(config)

        assert result.result_summary == "ok"
        assert any(
            "operational_run_metadata_write_failed=RuntimeError: supabase unavailable" in warning
            for warning in result.warnings
        )
        assert fake_store.closed is True

    @pytest.mark.asyncio
    async def test_run_job_async_warns_when_store_close_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Store-close errors should become warnings instead of aborting the run."""

        class FakeStore:
            def write_run_metadata(self, snapshot: RunSnapshot) -> None:
                del snapshot

            def upsert_records(self, dataset_name: str, records: list[dict[str, object]]) -> None:
                del dataset_name, records

            def fetch_records(
                self, dataset_name: str, *, limit: int | None = None
            ) -> list[dict[str, object]]:
                del dataset_name, limit
                return []

            def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, object]]:
                del dataset_name
                return []

            def mark_publication_state(
                self, dataset_name: str, record_ids: list[str], publication_status: str
            ) -> None:
                del dataset_name, record_ids, publication_status

            def close(self) -> None:
                raise RuntimeError("close boom")

        async def fake_handler(
            config: Config,
            config_path: Path | None,
            days_override: int | None,
            operational_store: object,
        ) -> RunSnapshot:
            del config_path, days_override, operational_store
            return RunSnapshot(
                config_name=config.name,
                dataset_name=config.dataset_name,
                job_name=config.job_name,
            ).finish("ok")

        config = Config(dataset_name="news_items", job_name="release")
        monkeypatch.setattr("denbust.pipeline.ensure_default_jobs_registered", lambda: None)
        monkeypatch.setattr("denbust.pipeline.require_job_handler", lambda *_args: fake_handler)
        monkeypatch.setattr(
            "denbust.pipeline.create_operational_store", lambda _config: FakeStore()
        )

        result = await run_job_async(config)

        assert result.result_summary == "ok"
        assert any(
            "operational_store_close_failed=RuntimeError: close boom" in warning
            for warning in result.warnings
        )

    def test_scaffolded_release_and_backup_write_snapshots(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Release and backup wrappers should emit scaffold summaries and snapshots."""
        config = Config(
            dataset_name="news_items",
            job_name="ingest",
            store={"runs_dir": tmp_path / "runs"},
        )

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))

        run_release(config_path=Path("agents/release/news_items.yaml"), dataset_name="news_items")
        run_backup(config_path=Path("agents/backup/news_items.yaml"), dataset_name="news_items")

        out = capsys.readouterr().out
        assert "release built for 0 public row(s)" in out
        assert "backup completed for 0 target(s)" in out
        assert len(list((tmp_path / "runs").glob("*.json"))) == 2

    def test_run_job_wrapper_delegates_to_generic_runner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """run_job should forward its arguments to the shared config runner."""
        delegated: dict[str, object] = {}

        def fake_run_job_from_config(**kwargs: object) -> RunSnapshot:
            delegated.update(kwargs)
            return RunSnapshot(config_name="test-config")

        monkeypatch.setattr("denbust.pipeline._run_job_from_config", fake_run_job_from_config)

        run_job(
            config_path=Path("agents/news/local.yaml"),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            days_override=5,
        )

        assert delegated == {
            "config_path": Path("agents/news/local.yaml"),
            "dataset_name": DatasetName.NEWS_ITEMS,
            "job_name": JobName.INGEST,
            "days_override": 5,
            "operational_store": None,
        }

    def test_run_job_from_config_passes_operational_store_to_async_runner(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Supplying an operational store should be forwarded to run_job_async."""
        config = Config(days=4, output=OutputConfig(formats=[OutputFormat.CLI]))
        snapshot = RunSnapshot(config_name=config.name)
        operational_store = LocalJsonOperationalStore(Path("/tmp/ops"))
        run_job_async_mock = AsyncMock(return_value=snapshot)

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", run_job_async_mock)
        monkeypatch.setattr("denbust.pipeline.write_run_snapshot", MagicMock())
        monkeypatch.setattr("denbust.pipeline.output_items", MagicMock(return_value=[]))

        result = _run_job_from_config(
            config_path=Path("agents/news/local.yaml"),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            days_override=6,
            operational_store=operational_store,
        )

        assert result is snapshot
        run_job_async_mock.assert_awaited_once_with(
            config,
            config_path=Path("agents/news/local.yaml"),
            days_override=6,
            operational_store=operational_store,
        )

    def test_run_job_from_config_exits_on_runner_value_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Runner-level value errors should print a clear error and exit 1."""
        config = Config()
        run_job_async_mock = AsyncMock(side_effect=ValueError("unsupported job"))

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_job_async", run_job_async_mock)

        with pytest.raises(SystemExit) as exc_info:
            _run_job_from_config(
                config_path=Path("agents/news/local.yaml"),
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.INGEST,
            )

        assert exc_info.value.code == 1
        assert "Error: unsupported job" in capsys.readouterr().out
