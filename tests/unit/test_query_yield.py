"""Unit tests for yield-weighted query prioritization."""

from __future__ import annotations

from pathlib import Path

from denbust.discovery.query_yield import QueryYieldStore, compute_query_yield


def test_compute_query_yield_credits_contributing_queries() -> None:
    """Each query that fed an index-relevant record earns a point per record."""
    records = [
        {"index_relevant": True, "event_candidate_ids": ["c1", "c2"]},
        {"index_relevant": True, "event_candidate_ids": ["c3"]},
        {"index_relevant": False, "event_candidate_ids": ["c4"]},  # ignored
    ]
    candidate_queries = {
        "c1": ["סחר בבני אדם", "בית בושת"],
        "c2": ["סחר בבני אדם"],  # same record → "סחר" counted once for record 1
        "c3": ["בית בושת"],
        "c4": ["ליווי"],  # only on a non-relevant record
    }

    result = compute_query_yield(records, candidate_queries)

    assert result["סחר בבני אדם"] == 1  # one distinct relevant record
    assert result["בית בושת"] == 2  # record 1 and record 3
    assert "ליווי" not in result  # never on a relevant record


def test_compute_query_yield_dedupes_within_record() -> None:
    """A query on multiple candidates of the same record counts once for it."""
    records = [{"index_relevant": True, "event_candidate_ids": ["a", "b", "c"]}]
    candidate_queries = {"a": ["k"], "b": ["k"], "c": ["k"]}
    assert compute_query_yield(records, candidate_queries) == {"k": 1}


def test_compute_query_yield_empty() -> None:
    """No relevant records → empty map."""
    assert compute_query_yield([], {}) == {}
    assert compute_query_yield([{"index_relevant": False}], {}) == {}


def test_query_yield_store_round_trip(tmp_path: Path) -> None:
    """The cache writes and reads back the yield map."""
    store = QueryYieldStore(tmp_path / "query_yield.json")
    assert store.load() == {}
    store.save({"בית בושת": 2, "סחר בבני אדם": 1})
    assert QueryYieldStore(tmp_path / "query_yield.json").load() == {
        "בית בושת": 2,
        "סחר בבני אדם": 1,
    }


def test_select_run_queries_prioritizes_high_yield() -> None:
    """A capped run keeps high-yield query texts first, over kind priority."""
    from denbust.config import Config, SourceConfig, SourceType
    from denbust.discovery.queries import build_discovery_queries, select_run_queries

    config = Config(
        keywords=["זנות", "בית בושת", "סרסור"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad"]},
    )
    queries = build_discovery_queries(config, days=3)
    assert len(queries) == 3

    yield_map = {"סרסור": 5}  # the last keyword is the proven one

    capped = select_run_queries(queries, 1, yield_of=lambda q: yield_map.get(q.query_text, 0))
    assert [q.query_text for q in capped] == ["סרסור"]
