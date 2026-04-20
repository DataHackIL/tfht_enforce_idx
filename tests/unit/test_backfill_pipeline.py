"""Unit tests for backfill pipeline jobs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import HttpUrl

import denbust.pipeline as pipeline_module
from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.backfill import BackfillWindow
from denbust.discovery.models import (
    BackfillBatch,
    BackfillBatchStatus,
    CandidateStatus,
    DiscoveredCandidate,
    DiscoveryRun,
    DiscoveryRunStatus,
    PersistentCandidate,
)
from denbust.discovery.scrape_queue import CandidateScrapeBatch
from denbust.discovery.source_native import PersistedSourceDiscovery
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot
from denbust.pipeline import run_news_backfill_discover_job, run_news_backfill_scrape_job


class FakeSource:
    """Minimal source stub for backfill pipeline tests."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        return []


class HistoricalFakeSource(FakeSource):
    """Backfill-capable source stub."""

    def __init__(self, name: str, articles: list[RawArticle] | None = None) -> None:
        super().__init__(name)
        self.articles = articles or []

    async def fetch_window(
        self,
        *,
        date_from: datetime,
        date_to: datetime,
        keywords: list[str],
    ) -> list[RawArticle]:
        del date_from, date_to, keywords
        return self.articles


def build_candidate(batch_id: str) -> PersistentCandidate:
    """Create one batch-linked candidate fixture."""
    return PersistentCandidate(
        candidate_id="candidate-1",
        current_url=HttpUrl("https://example.com/article"),
        canonical_url=HttpUrl("https://example.com/article"),
        titles=["title"],
        snippets=["snippet"],
        discovered_via=["brave"],
        discovery_queries=["בית בושת"],
        source_hints=["ynet"],
        first_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
        last_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
        backfill_batch_id=batch_id,
        metadata={"backfill_window_start": datetime(2026, 1, 1, tzinfo=UTC).isoformat()},
    )


def build_window(index: int = 0) -> BackfillWindow:
    """Create one backfill window fixture."""
    return BackfillWindow(
        index=index,
        date_from=datetime(2026, 1, 1 + index, tzinfo=UTC),
        date_to=datetime(2026, 1, 2 + index, tzinfo=UTC),
    )


def build_batch(batch_id: str = "batch-1", *, status: BackfillBatchStatus) -> BackfillBatch:
    """Create one backfill batch fixture."""
    return BackfillBatch(
        batch_id=batch_id,
        created_at=datetime(2026, 1, 10, tzinfo=UTC),
        updated_at=datetime(2026, 1, 10, tzinfo=UTC),
        started_at=datetime(2026, 1, 10, tzinfo=UTC),
        requested_date_from=datetime(2026, 1, 1, tzinfo=UTC),
        requested_date_to=datetime(2026, 1, 3, tzinfo=UTC),
        status=status,
        window_count=2,
    )


def build_discovered_candidate() -> DiscoveredCandidate:
    """Create one discovered candidate fixture."""
    return DiscoveredCandidate(
        producer_name="ynet",
        producer_kind="source_native",
        candidate_url=HttpUrl("https://example.com/article"),
        canonical_url=HttpUrl("https://example.com/article"),
        title="title",
        snippet="snippet",
        discovered_at=datetime(2026, 1, 2, tzinfo=UTC),
        source_hint="ynet",
        metadata={"existing": "value"},
    )


def build_persisted_discovery(
    *,
    query_count: int = 1,
    candidate_count: int = 1,
    merged_candidate_count: int = 1,
    errors: list[str] | None = None,
    warnings: list[str] | None = None,
    batch_id: str = "batch-1",
) -> PersistedSourceDiscovery:
    """Create a persisted discovery bundle fixture."""
    candidate = build_candidate(batch_id)
    return PersistedSourceDiscovery(
        run=DiscoveryRun(
            run_id="run-1",
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.BACKFILL_DISCOVER,
            status=DiscoveryRunStatus.SUCCEEDED,
            query_count=query_count,
            candidate_count=candidate_count,
            merged_candidate_count=merged_candidate_count,
            errors=errors or [],
        ),
        candidates=[candidate] if merged_candidate_count else [],
        provenance=[],
        warnings=warnings or [],
    )


def build_scrape_batch(
    *,
    batch_id: str = "batch-1",
    raw_articles: list[RawArticle] | None = None,
    fallback_candidates: list[PersistentCandidate] | None = None,
    errors: list[str] | None = None,
) -> CandidateScrapeBatch:
    """Create a candidate scrape batch fixture."""
    selected = [build_candidate(batch_id)]
    return CandidateScrapeBatch(
        selected_candidates=selected,
        updated_candidates=selected,
        fallback_candidates=fallback_candidates or [],
        attempts=[],
        raw_articles=raw_articles or [],
        errors=errors or [],
    )


def build_store(tmp_path: Path) -> StateRepoDiscoveryPersistence:
    """Create a state-repo discovery store."""
    return StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    )


def build_config(tmp_path: Path, **updates: object) -> Config:
    """Create a minimal backfill config fixture."""
    return Config(
        job_name=JobName.BACKFILL_DISCOVER,
        store={"state_root": tmp_path},
        **updates,
    )


def select_stub(
    persistence: StateRepoDiscoveryPersistence,
    *,
    limit: int,
    batch_id: str | None = None,
) -> list[PersistentCandidate]:
    """Return candidates from one store while accepting the full selector signature."""
    del batch_id
    return persistence.list_candidates(limit=limit)


@pytest.mark.asyncio
async def test_tag_backfill_discovered_candidates_adds_metadata() -> None:
    """Backfill tagging should merge existing metadata with batch window metadata."""
    tagged = pipeline_module._tag_backfill_discovered_candidates(
        [build_discovered_candidate()],
        batch_id="batch-1",
        window=build_window(2),
    )

    assert tagged[0].metadata["existing"] == "value"
    assert tagged[0].metadata["backfill_batch_id"] == "batch-1"
    assert tagged[0].metadata["backfill_window_index"] == 2


def test_update_backfill_batch_state_refreshes_counts(tmp_path: Path) -> None:
    """Batch updates should recompute merged and queued candidate counts before persisting."""
    store = build_store(tmp_path)
    store.upsert_candidates(
        [
            build_candidate("batch-1"),
            build_candidate("batch-1").model_copy(
                update={
                    "candidate_id": "done",
                    "candidate_status": CandidateStatus.SCRAPE_SUCCEEDED,
                }
            ),
            build_candidate("other-batch").model_copy(update={"candidate_id": "other"}),
        ]
    )
    batch = build_batch(status=BackfillBatchStatus.RUNNING)

    updated = pipeline_module._update_backfill_batch_state(
        store,
        batch=batch,
        status=BackfillBatchStatus.DISCOVERED,
        query_count=4,
        candidate_count=3,
        warnings=["warning"],
        errors=["error"],
        finished=True,
    )

    assert updated.status is BackfillBatchStatus.DISCOVERED
    assert updated.query_count == 4
    assert updated.candidate_count == 3
    assert updated.merged_candidate_count == 2
    assert updated.queued_for_scrape_count == 1
    assert updated.finished_at is not None
    assert store.get_backfill_batch("batch-1") is not None


def test_batch_candidate_counts_uses_batch_filter(tmp_path: Path) -> None:
    """Batch counting should only consider candidates from the requested backfill batch."""
    store = build_store(tmp_path)
    store.upsert_candidates(
        [
            build_candidate("batch-1"),
            build_candidate("batch-1").model_copy(
                update={
                    "candidate_id": "future",
                    "candidate_status": CandidateStatus.SCRAPE_FAILED,
                    "next_scrape_attempt_at": datetime(2026, 1, 20, tzinfo=UTC),
                }
            ),
            build_candidate("batch-2").model_copy(update={"candidate_id": "other"}),
        ]
    )

    merged_count, queued_count = pipeline_module._batch_candidate_counts(store, batch_id="batch-1")

    assert merged_count == 2
    assert queued_count == 2


@pytest.mark.asyncio
async def test_run_source_native_backfill_discovery_handles_unsupported_and_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unsupported sources should warn and failing historical sources should be recorded as errors."""
    store = build_store(tmp_path)
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr(
        "denbust.pipeline.persist_discovered_candidates",
        lambda **kwargs: PersistedSourceDiscovery(
            run=kwargs["run"],
            candidates=[],
            provenance=[],
            warnings=[],
        ),
    )

    class FailingHistoricalSource(HistoricalFakeSource):
        async def fetch_window(
            self,
            *,
            date_from: datetime,
            date_to: datetime,
            keywords: list[str],
        ) -> list[RawArticle]:
            del date_from, date_to, keywords
            raise RuntimeError("boom")

    result = await pipeline_module._run_source_native_backfill_discovery(
        config=Config(
            job_name=JobName.BACKFILL_DISCOVER,
            store={"state_root": tmp_path},
            source_discovery={"enabled": True, "persist_candidates": True},
        ),
        sources=[FakeSource("walla"), FailingHistoricalSource("ynet")],
        run_id="run-1",
        window=build_window(),
        batch_id="batch-1",
    )

    assert result.warnings == ["walla: historical window discovery is unsupported"]
    assert result.run.errors == ["ynet: boom"]


@pytest.mark.asyncio
async def test_run_backfill_engine_discovery_handles_empty_queries_and_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill engine discovery should handle empty queries, missing keys, errors, and bad engines."""
    store = build_store(tmp_path)
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.build_backfill_queries", lambda *_args, **_kwargs: [])

    empty = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={"enabled": True, "persist_candidates": True, "engines": {"brave": {}}},
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="brave",
    )
    assert empty.run.candidate_count == 0

    monkeypatch.setattr("denbust.pipeline.build_backfill_queries", lambda *_args, **_kwargs: ["q"])
    missing_brave = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={"enabled": True, "persist_candidates": True, "engines": {"brave": {}}},
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="brave",
    )
    assert missing_brave.run.errors == ["brave: missing DENBUST_BRAVE_SEARCH_API_KEY"]

    missing = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={"enabled": True, "persist_candidates": True, "engines": {"exa": {}}},
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="exa",
    )
    assert missing.run.errors == ["exa: missing DENBUST_EXA_API_KEY"]

    discover = AsyncMock(side_effect=RuntimeError("kaput"))
    fake_engine = MagicMock(discover=discover, aclose=AsyncMock())
    monkeypatch.setattr("denbust.pipeline.BraveSearchEngine", lambda **_kwargs: fake_engine)
    monkeypatch.setenv("DENBUST_BRAVE_SEARCH_API_KEY", "key")
    errored = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {
                    "brave": {
                        "api_key_env": "DENBUST_BRAVE_SEARCH_API_KEY",
                        "max_results_per_query": 5,
                    }
                },
            },
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="brave",
    )
    assert errored.run.errors == ["brave: RuntimeError: kaput"]
    fake_engine.aclose.assert_awaited_once()

    with pytest.raises(ValueError, match="Unsupported backfill engine"):
        await pipeline_module._run_backfill_engine_discovery(
            config=build_config(tmp_path),
            run_id="run-1",
            batch_id="batch-1",
            window=build_window(),
            engine_name="bing",
        )


@pytest.mark.asyncio
async def test_run_backfill_engine_discovery_covers_exa_and_google_variants(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill engine discovery should build Exa and Google contexts and handle missing Google config."""
    store = build_store(tmp_path)
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.build_backfill_queries", lambda *_args, **_kwargs: ["q"])
    monkeypatch.setattr(
        "denbust.pipeline.persist_discovered_candidates",
        lambda **kwargs: PersistedSourceDiscovery(
            run=kwargs["run"],
            candidates=[],
            provenance=[],
            warnings=[],
        ),
    )

    exa_engine = MagicMock(discover=AsyncMock(return_value=[]), aclose=AsyncMock())
    monkeypatch.setattr("denbust.pipeline.ExaSearchEngine", lambda **_kwargs: exa_engine)
    monkeypatch.setenv("DENBUST_EXA_API_KEY", "exa-key")
    exa_result = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {
                    "exa": {
                        "api_key_env": "DENBUST_EXA_API_KEY",
                        "max_results_per_query": 7,
                        "allow_find_similar": True,
                    }
                },
            },
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="exa",
    )
    exa_context = exa_engine.discover.await_args.kwargs["context"]
    assert exa_result.run.errors == []
    assert exa_context.metadata["engine"] == "exa"
    assert exa_context.metadata["allow_find_similar"] is True

    missing_google_key = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={"enabled": True, "persist_candidates": True, "engines": {"google_cse": {}}},
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="google_cse",
    )
    assert missing_google_key.run.errors == ["google_cse: missing DENBUST_GOOGLE_CSE_API_KEY"]

    monkeypatch.setenv("DENBUST_GOOGLE_CSE_API_KEY", "google-key")
    missing_google_id = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {
                    "google_cse": {
                        "api_key_env": "DENBUST_GOOGLE_CSE_API_KEY",
                        "cse_id_env": "DENBUST_GOOGLE_CSE_ID",
                    }
                },
            },
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="google_cse",
    )
    assert missing_google_id.run.errors == ["google_cse: missing DENBUST_GOOGLE_CSE_ID"]

    google_engine = MagicMock(discover=AsyncMock(return_value=[]), aclose=AsyncMock())
    monkeypatch.setattr("denbust.pipeline.GoogleCseSearchEngine", lambda **_kwargs: google_engine)
    monkeypatch.setenv("DENBUST_GOOGLE_CSE_ID", "cx-1")
    google_result = await pipeline_module._run_backfill_engine_discovery(
        config=build_config(
            tmp_path,
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {
                    "google_cse": {
                        "api_key_env": "DENBUST_GOOGLE_CSE_API_KEY",
                        "cse_id_env": "DENBUST_GOOGLE_CSE_ID",
                        "max_results_per_query": 11,
                    }
                },
            },
        ),
        run_id="run-1",
        batch_id="batch-1",
        window=build_window(),
        engine_name="google_cse",
    )
    google_context = google_engine.discover.await_args.kwargs["context"]
    assert google_result.run.errors == []
    assert google_context.metadata["engine"] == "google_cse"


@pytest.mark.asyncio
async def test_run_backfill_candidate_scrape_job_uses_batch_selector(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill scrape selection should use the dedicated selector and backfill mode."""
    selected = [build_candidate("batch-1")]
    monkeypatch.setattr(
        "denbust.pipeline.create_discovery_persistence", lambda _config: build_store(tmp_path)
    )
    monkeypatch.setattr(
        "denbust.pipeline.select_backfill_candidates_for_scrape",
        lambda *_args, **_kwargs: selected,
    )
    scrape = AsyncMock(return_value=build_scrape_batch())
    monkeypatch.setattr("denbust.pipeline._scrape_candidate_batch", scrape)

    await pipeline_module._run_backfill_candidate_scrape_job(
        config=Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path}),
        sources=[FakeSource("ynet")],
        limit=3,
        batch_id="batch-1",
    )

    assert scrape.await_args.kwargs["candidates"] == selected
    assert scrape.await_args.kwargs["backfill_mode"] is True


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_succeeds_without_operational_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill discover should succeed with discovery persistence only."""
    monkeypatch.setenv("DENBUST_BACKFILL_DATE_FROM", "2026-01-01T00:00:00+00:00")
    monkeypatch.setenv("DENBUST_BACKFILL_DATE_TO", "2026-01-03T00:00:00+00:00")
    store = StateRepoDiscoveryPersistence(
        resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    )

    async def fake_engine(
        *,
        config: Config,
        run_id: str,
        batch_id: str,
        window,
        engine_name: str,
    ) -> PersistedSourceDiscovery:
        del config, run_id, window, engine_name
        candidate = build_candidate(batch_id)
        store.upsert_candidates([candidate])
        return PersistedSourceDiscovery(
            run=DiscoveryRun(
                run_id="run-1",
                dataset_name=DatasetName.NEWS_ITEMS,
                job_name=JobName.BACKFILL_DISCOVER,
                status=DiscoveryRunStatus.SUCCEEDED,
                query_count=1,
                candidate_count=1,
                merged_candidate_count=1,
            ),
            candidates=[candidate],
            provenance=[],
            warnings=[],
        )

    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
    monkeypatch.setattr("denbust.pipeline._run_backfill_engine_discovery", fake_engine)

    result = await run_news_backfill_discover_job(
        Config(
            job_name=JobName.BACKFILL_DISCOVER,
            store={"state_root": tmp_path},
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {"brave": {"enabled": True}},
            },
        )
    )

    assert result.fatal is False
    assert "backfill discovery persisted 1 candidate(s)" in (result.result_summary or "")
    assert store.list_backfill_batches(limit=1)[0].merged_candidate_count == 1


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_finishes_when_queue_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill scrape should finish cleanly when no historical candidates are eligible."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [FakeSource("ynet")])
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))
    monkeypatch.setattr(
        "denbust.pipeline.create_discovery_persistence",
        lambda _config: StateRepoDiscoveryPersistence(
            resolve_discovery_state_paths(
                state_root=tmp_path,
                dataset_name=DatasetName.NEWS_ITEMS,
            )
        ),
    )

    result = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )

    assert result.fatal is False
    assert result.result_summary == "no queued backfill candidates eligible for scrape"


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_fails_on_invalid_request_window(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Invalid requested backfill windows should terminate the run as fatal."""
    monkeypatch.setattr(
        "denbust.pipeline.resolve_backfill_request_window",
        lambda: (_ for _ in ()).throw(ValueError("bad window")),
    )

    result = await run_news_backfill_discover_job(build_config(tmp_path))

    assert result.fatal is True
    assert result.errors == ["bad window"]
    assert result.result_summary == "fatal: invalid backfill request window"


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_requires_producers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill discovery should fail fast when neither sources nor engines are configured."""
    monkeypatch.setattr(
        "denbust.pipeline.resolve_backfill_request_window",
        lambda: (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)),
    )
    monkeypatch.setattr(
        "denbust.pipeline.plan_backfill_windows", lambda **_kwargs: [build_window()]
    )
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

    result = await run_news_backfill_discover_job(build_config(tmp_path))

    assert result.fatal is True
    assert result.errors == ["No backfill discovery producers configured"]
    assert result.result_summary == "fatal: no backfill discovery producers configured"


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_marks_partial_and_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill discovery should distinguish partial success from full failure."""
    monkeypatch.setattr(
        "denbust.pipeline.resolve_backfill_request_window",
        lambda: (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC)),
    )
    monkeypatch.setenv("DENBUST_BACKFILL_BATCH_ID", "batch-1")
    monkeypatch.setattr(
        "denbust.pipeline.plan_backfill_windows",
        lambda **_kwargs: [build_window(0), build_window(1)],
    )
    monkeypatch.setattr(
        "denbust.pipeline.create_sources", lambda _config: [HistoricalFakeSource("ynet")]
    )
    store = build_store(tmp_path)
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)

    source_calls = iter(
        [
            build_persisted_discovery(query_count=1, candidate_count=1, merged_candidate_count=1),
            build_persisted_discovery(
                query_count=1,
                candidate_count=0,
                merged_candidate_count=0,
                errors=["ynet: boom"],
                warnings=["ynet: warning"],
            ),
        ]
    )
    engine_calls = iter(
        [
            build_persisted_discovery(
                query_count=1,
                candidate_count=1,
                merged_candidate_count=1,
                errors=["brave: partial"],
            ),
            build_persisted_discovery(
                query_count=1,
                candidate_count=0,
                merged_candidate_count=0,
                errors=["brave: fatal"],
            ),
        ]
    )

    async def fake_source_native(**_kwargs: object) -> PersistedSourceDiscovery:
        persisted = next(source_calls)
        if persisted.candidates:
            store.upsert_candidates(
                [
                    candidate.model_copy(
                        update={
                            "candidate_id": f"source-{candidate.candidate_id}-{len(store.list_candidates())}"
                        }
                    )
                    for candidate in persisted.candidates
                ]
            )
        return persisted

    async def fake_engine(**_kwargs: object) -> PersistedSourceDiscovery:
        persisted = next(engine_calls)
        if persisted.candidates:
            store.upsert_candidates(
                [
                    candidate.model_copy(
                        update={
                            "candidate_id": f"engine-{candidate.candidate_id}-{len(store.list_candidates())}"
                        }
                    )
                    for candidate in persisted.candidates
                ]
            )
        return persisted

    monkeypatch.setattr(
        "denbust.pipeline._run_source_native_backfill_discovery", fake_source_native
    )
    monkeypatch.setattr("denbust.pipeline._run_backfill_engine_discovery", fake_engine)

    partial = await run_news_backfill_discover_job(
        build_config(
            tmp_path,
            source_discovery={"enabled": True, "persist_candidates": True},
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {"brave": {"enabled": True}},
            },
        )
    )

    batch = store.list_backfill_batches(limit=1)[0]
    assert partial.fatal is False
    assert batch.status is BackfillBatchStatus.PARTIAL
    assert "backfill discovery persisted 2 candidate(s)" in (partial.result_summary or "")
    assert "ynet: warning" in partial.warnings

    fail_store = build_store(tmp_path / "failed")
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: fail_store)
    monkeypatch.setattr(
        "denbust.pipeline._run_source_native_backfill_discovery",
        AsyncMock(
            return_value=build_persisted_discovery(
                merged_candidate_count=0, errors=["source: boom"]
            )
        ),
    )
    monkeypatch.setattr(
        "denbust.pipeline._run_backfill_engine_discovery",
        AsyncMock(
            return_value=build_persisted_discovery(merged_candidate_count=0, errors=["brave: boom"])
        ),
    )

    failed = await run_news_backfill_discover_job(
        build_config(
            tmp_path / "failed",
            source_discovery={"enabled": True, "persist_candidates": True},
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {"brave": {"enabled": True}},
            },
        )
    )

    assert failed.fatal is True
    assert failed.result_summary is not None
    assert failed.result_summary.startswith("fatal: backfill discovery failed for batch ")
    assert fail_store.list_backfill_batches(limit=1)[0].status is BackfillBatchStatus.FAILED


@pytest.mark.asyncio
async def test_run_news_backfill_discover_job_runs_exa_and_google_engines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill discovery should invoke each enabled engine family within the window loop."""
    monkeypatch.setattr(
        "denbust.pipeline.resolve_backfill_request_window",
        lambda: (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)),
    )
    monkeypatch.setenv("DENBUST_BACKFILL_BATCH_ID", "batch-1")
    monkeypatch.setattr(
        "denbust.pipeline.plan_backfill_windows", lambda **_kwargs: [build_window(0)]
    )
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
    store = build_store(tmp_path)
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    engine_names: list[str] = []

    async def fake_engine(**kwargs: object) -> PersistedSourceDiscovery:
        engine_names.append(str(kwargs["engine_name"]))
        return build_persisted_discovery(merged_candidate_count=0)

    monkeypatch.setattr("denbust.pipeline._run_backfill_engine_discovery", fake_engine)

    result = await run_news_backfill_discover_job(
        build_config(
            tmp_path,
            discovery={
                "enabled": True,
                "persist_candidates": True,
                "engines": {
                    "exa": {"enabled": True},
                    "google_cse": {"enabled": True},
                },
            },
        )
    )

    assert result.fatal is False
    assert engine_names == ["exa", "google_cse"]


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_handles_missing_requirements_and_batch_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill scrape should fail fast on missing API key, sources, or batch metadata."""
    missing_key = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )
    assert missing_key.fatal is True
    assert missing_key.result_summary == "fatal: missing anthropic api key"

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])
    no_sources = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )
    assert no_sources.fatal is True
    assert no_sources.result_summary == "fatal: no sources configured"

    store = build_store(tmp_path / "missing-batch")
    store.upsert_candidates([build_candidate("batch-missing")])
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [FakeSource("ynet")])
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr(
        "denbust.pipeline.select_backfill_candidates_for_scrape",
        select_stub,
    )
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))

    missing_batch = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path / "missing-batch"})
    )

    assert missing_batch.fatal is True
    assert missing_batch.result_summary == "fatal: missing backfill batch metadata"


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_returns_when_selected_batch_drains_to_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A scrape pass with no selected candidates after dispatch should return the empty-queue summary."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    store = build_store(tmp_path)
    store.upsert_backfill_batches([build_batch(status=BackfillBatchStatus.DISCOVERED)])
    store.upsert_candidates([build_candidate("batch-1")])
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [FakeSource("ynet")])
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))
    monkeypatch.setattr(
        "denbust.pipeline._run_backfill_candidate_scrape_job",
        AsyncMock(
            return_value=CandidateScrapeBatch(
                selected_candidates=[],
                updated_candidates=[],
                fallback_candidates=[],
                attempts=[],
                raw_articles=[],
                errors=[],
            )
        ),
    )

    result = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )

    assert result.result_summary == "no queued backfill candidates eligible for scrape"


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_keeps_batch_open_for_future_retries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Future retry candidates should keep the backfill batch from being marked completed."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    store = build_store(tmp_path)
    store.upsert_backfill_batches([build_batch(status=BackfillBatchStatus.DISCOVERED)])
    store.upsert_candidates([build_candidate("batch-1")])
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [FakeSource("ynet")])
    monkeypatch.setattr("denbust.pipeline.create_discovery_persistence", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))

    async def fake_scrape_job(**_kwargs: object) -> CandidateScrapeBatch:
        store.upsert_candidates(
            [
                build_candidate("batch-1").model_copy(
                    update={
                        "candidate_id": "candidate-1",
                        "candidate_status": CandidateStatus.SCRAPE_FAILED,
                        "next_scrape_attempt_at": datetime(2026, 1, 20, tzinfo=UTC),
                    }
                )
            ]
        )
        return build_scrape_batch(raw_articles=[], fallback_candidates=[], errors=["retry later"])

    monkeypatch.setattr("denbust.pipeline._run_backfill_candidate_scrape_job", fake_scrape_job)
    monkeypatch.setattr(
        "denbust.pipeline._build_fallback_operational_records",
        AsyncMock(return_value=[]),
    )

    result = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path})
    )

    assert result.result_summary == (
        "fallback retention completed with 0 provisional row(s) for backfill batch batch-1"
    )
    assert store.get_backfill_batch("batch-1").status is BackfillBatchStatus.PARTIAL


@pytest.mark.asyncio
async def test_run_news_backfill_scrape_job_handles_fallback_and_processed_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backfill scrape should support fallback-only and ingest-processing paths with final batch updates."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    sources = [FakeSource("ynet")]
    monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: sources)
    monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_deduplicator", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock(count=0))

    fallback_store = build_store(tmp_path / "fallback")
    fallback_store.upsert_backfill_batches([build_batch(status=BackfillBatchStatus.DISCOVERED)])
    fallback_store.upsert_candidates([build_candidate("batch-1")])
    monkeypatch.setattr(
        "denbust.pipeline.create_discovery_persistence",
        lambda _config: fallback_store,
    )
    monkeypatch.setattr(
        "denbust.pipeline._run_backfill_candidate_scrape_job",
        AsyncMock(
            return_value=build_scrape_batch(
                fallback_candidates=[build_candidate("batch-1")],
                raw_articles=[],
                errors=["candidate failed"],
            )
        ),
    )
    monkeypatch.setattr(
        "denbust.pipeline._build_fallback_operational_records",
        AsyncMock(return_value=[MagicMock(model_dump=lambda **_kwargs: {"id": "row-1"})]),
    )
    store = MagicMock()
    monkeypatch.setattr("denbust.pipeline.create_operational_store", lambda _config: store)

    fallback = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path / "fallback"})
    )

    assert fallback.fatal is False
    assert fallback.result_summary == (
        "fallback retention completed with 1 provisional row(s) for backfill batch batch-1"
    )
    assert "fallback_operational_records=1" in fallback.warnings
    assert fallback_store.get_backfill_batch("batch-1") is not None

    processed_store = build_store(tmp_path / "processed")
    processed_store.upsert_backfill_batches([build_batch(status=BackfillBatchStatus.DISCOVERED)])
    processed_store.upsert_candidates([build_candidate("batch-1")])
    monkeypatch.setattr(
        "denbust.pipeline.create_discovery_persistence",
        lambda _config: processed_store,
    )
    monkeypatch.setattr(
        "denbust.pipeline._run_backfill_candidate_scrape_job",
        AsyncMock(
            return_value=build_scrape_batch(
                raw_articles=[
                    RawArticle(
                        url=HttpUrl("https://example.com/raw"),
                        title="title",
                        snippet="snippet",
                        date=datetime(2026, 1, 2, tzinfo=UTC),
                        source_name="ynet",
                    )
                ]
            )
        ),
    )
    monkeypatch.setattr(
        "denbust.pipeline._build_fallback_operational_records",
        AsyncMock(return_value=[]),
    )
    processed_snapshot = RunSnapshot(job_name=JobName.BACKFILL_SCRAPE).finish("processed 1 article")
    monkeypatch.setattr(
        "denbust.pipeline._process_ingest_articles",
        AsyncMock(return_value=processed_snapshot),
    )

    processed = await run_news_backfill_scrape_job(
        Config(job_name=JobName.BACKFILL_SCRAPE, store={"state_root": tmp_path / "processed"})
    )

    assert processed.result_summary == "backfill batch batch-1: processed 1 article"
    assert processed.debug_payload == {"batch_id": "batch-1"}
    assert processed_store.get_backfill_batch("batch-1") is not None
