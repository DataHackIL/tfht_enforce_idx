"""Unit tests for cross-run backfill query execution tracking."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from denbust.discovery.models import (
    DiscoveryQuery,
    DiscoveryQueryKind,
    ExecutedBackfillQuery,
)
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence, _executed_query_key
from denbust.models.common import DatasetName
from denbust.pipeline import _backfill_query_execution_key

_T0 = datetime(2024, 1, 1, tzinfo=UTC)
_T1 = datetime(2024, 1, 7, 23, 59, 59, tzinfo=UTC)


def _make_executed(
    engine: str = "brave",
    keyword: str = "זנות",
    query_kind: DiscoveryQueryKind = DiscoveryQueryKind.BROAD,
    source_hint: str | None = None,
    date_from: datetime = _T0,
    date_to: datetime = _T1,
    batch_id: str = "batch-1",
) -> ExecutedBackfillQuery:
    return ExecutedBackfillQuery(
        engine=engine,
        query_kind=query_kind,
        query_text=keyword,
        source_hint=source_hint,
        date_from=date_from,
        date_to=date_to,
        batch_id=batch_id,
    )


def _make_query(
    keyword: str = "זנות",
    query_kind: DiscoveryQueryKind = DiscoveryQueryKind.BROAD,
    source_hint: str | None = None,
    date_from: datetime = _T0,
    date_to: datetime = _T1,
) -> DiscoveryQuery:
    return DiscoveryQuery(
        query_text=keyword,
        query_kind=query_kind,
        source_hint=source_hint,
        date_from=date_from,
        date_to=date_to,
        language="he",
    )


class TestExecutedQueryKey:
    def test_storage_key_matches_pipeline_key(self) -> None:
        record = _make_executed()
        query = _make_query()
        assert _executed_query_key(record) == _backfill_query_execution_key("brave", query)

    def test_keys_differ_by_engine(self) -> None:
        query = _make_query()
        assert _backfill_query_execution_key("brave", query) != _backfill_query_execution_key(
            "exa", query
        )

    def test_keys_differ_by_keyword(self) -> None:
        q1 = _make_query(keyword="זנות")
        q2 = _make_query(keyword="ליווי")
        assert _backfill_query_execution_key("brave", q1) != _backfill_query_execution_key(
            "brave", q2
        )

    def test_keys_differ_by_source_hint(self) -> None:
        q_none = _make_query(source_hint=None)
        q_ynet = _make_query(source_hint="ynet")
        assert _backfill_query_execution_key("brave", q_none) != _backfill_query_execution_key(
            "brave", q_ynet
        )

    def test_keys_differ_by_date_window(self) -> None:
        q1 = _make_query(date_from=_T0, date_to=_T1)
        q2 = _make_query(
            date_from=datetime(2024, 1, 8, tzinfo=UTC),
            date_to=datetime(2024, 1, 14, tzinfo=UTC),
        )
        assert _backfill_query_execution_key("brave", q1) != _backfill_query_execution_key(
            "brave", q2
        )

    def test_query_with_none_dates_never_matches_executed_record(self) -> None:
        q_no_dates = DiscoveryQuery(query_text="זנות", language="he")
        record = _make_executed()
        assert _backfill_query_execution_key("brave", q_no_dates) != _executed_query_key(record)


class TestStateRepoExecutedQueryPersistence:
    def test_load_returns_empty_frozenset_when_no_file(self, tmp_path: Path) -> None:
        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        store = StateRepoDiscoveryPersistence(paths)
        assert store.load_executed_backfill_query_keys() == frozenset()

    def test_append_then_load_roundtrip(self, tmp_path: Path) -> None:
        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        store = StateRepoDiscoveryPersistence(paths)
        record = _make_executed()
        store.append_executed_backfill_queries([record])

        keys = store.load_executed_backfill_query_keys()
        assert _executed_query_key(record) in keys

    def test_appends_accumulate_across_calls(self, tmp_path: Path) -> None:
        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        store = StateRepoDiscoveryPersistence(paths)
        r1 = _make_executed(keyword="זנות")
        r2 = _make_executed(keyword="ליווי")
        store.append_executed_backfill_queries([r1])
        store.append_executed_backfill_queries([r2])

        keys = store.load_executed_backfill_query_keys()
        assert _executed_query_key(r1) in keys
        assert _executed_query_key(r2) in keys

    def test_different_engines_tracked_independently(self, tmp_path: Path) -> None:
        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        store = StateRepoDiscoveryPersistence(paths)
        brave_record = _make_executed(engine="brave")
        store.append_executed_backfill_queries([brave_record])

        keys = store.load_executed_backfill_query_keys()
        exa_query = _make_query()
        assert _backfill_query_execution_key("exa", exa_query) not in keys
        assert _backfill_query_execution_key("brave", exa_query) in keys

    def test_executed_queries_path_is_in_backfill_batches_dir(self, tmp_path: Path) -> None:
        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        assert paths.backfill_executed_queries_path == (
            tmp_path / "news_items" / "discover" / "backfill_batches" / "executed_queries.jsonl.gz"
        )
