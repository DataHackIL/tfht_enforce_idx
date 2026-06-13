"""Unit tests for the search-budget ledger and guard."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from denbust.discovery.search_budget import (
    ENGINE_REQUEST_USD,
    SearchBudgetLedger,
    affordable_query_count,
    billed_cost_usd,
    engine_request_usd,
    month_to_date_summary,
)


def test_billed_cost_usd_after_free_allowance() -> None:
    """Real cost bills only queries beyond the free allowance."""
    assert billed_cost_usd("brave", queries=900, monthly_free_queries=1000) == 0.0
    assert billed_cost_usd("brave", queries=1200, monthly_free_queries=1000) == round(
        200 * 0.005, 6
    )
    assert billed_cost_usd("exa", queries=1100, monthly_free_queries=1000) == round(100 * 0.007, 6)


def test_record_and_month_spend_round_trip(tmp_path: Path) -> None:
    """Recorded spend is summed per month and per engine."""
    ledger = SearchBudgetLedger(tmp_path / "budget.jsonl")
    ledger.record(engine="brave", queries=100, run_id="r1", now=datetime(2026, 6, 1, tzinfo=UTC))
    ledger.record(engine="brave", queries=50, run_id="r2", now=datetime(2026, 6, 15, tzinfo=UTC))
    ledger.record(engine="exa", queries=10, run_id="r3", now=datetime(2026, 6, 2, tzinfo=UTC))
    # Different month — must not count toward June.
    ledger.record(engine="brave", queries=999, run_id="r4", now=datetime(2026, 5, 1, tzinfo=UTC))

    q, usd = ledger.month_spend(year_month="2026-06", engine="brave")
    assert q == 150
    assert usd == round(150 * ENGINE_REQUEST_USD["brave"], 6)

    q_exa, usd_exa = ledger.month_spend(year_month="2026-06", engine="exa")
    assert q_exa == 10
    assert usd_exa == round(10 * ENGINE_REQUEST_USD["exa"], 6)

    # Engine-agnostic total for the month.
    q_all, _ = ledger.month_spend(year_month="2026-06")
    assert q_all == 160


def test_affordable_query_count() -> None:
    """The guard spends the free allowance first, then the paid budget."""
    # No free, brave $0.005/q; $1 budget, 100 spent ($0.50) → $0.50 left → 100 more.
    assert (
        affordable_query_count(
            engine="brave", requested=435, queries_spent=100, monthly_budget_usd=1.00
        )
        == 100
    )
    # Budget exhausted (200 spent = $1.00 of $1.00) → 0.
    assert (
        affordable_query_count(
            engine="brave", requested=435, queries_spent=200, monthly_budget_usd=1.00
        )
        == 0
    )
    # Neither free nor budget → unchanged.
    assert (
        affordable_query_count(
            engine="brave", requested=435, queries_spent=999, monthly_budget_usd=None
        )
        == 435
    )


def test_affordable_query_count_free_allowance() -> None:
    """The free monthly allowance is spent before any paid budget."""
    # 1,000 free, no paid budget: 600 used → 400 free left.
    assert (
        affordable_query_count(
            engine="brave",
            requested=435,
            queries_spent=600,
            monthly_budget_usd=None,
            monthly_free_queries=1000,
        )
        == 400
    )
    # Free exhausted, no paid budget → 0.
    assert (
        affordable_query_count(
            engine="exa",
            requested=67,
            queries_spent=1000,
            monthly_budget_usd=None,
            monthly_free_queries=1000,
        )
        == 0
    )
    # 1,000 free + $5 budget on exa ($0.007/q → 714 paid): 1,000 used → 0 free + 714 paid.
    assert (
        affordable_query_count(
            engine="exa",
            requested=999,
            queries_spent=1000,
            monthly_budget_usd=5.00,
            monthly_free_queries=1000,
        )
        == 714
    )
    # Well within free → full request.
    assert (
        affordable_query_count(
            engine="brave",
            requested=35,
            queries_spent=100,
            monthly_budget_usd=1.00,
            monthly_free_queries=1000,
        )
        == 35
    )


def test_engine_request_usd_defaults() -> None:
    """Known engines use their price; unknown engines fall back to a default."""
    assert engine_request_usd("brave") == 0.005
    assert engine_request_usd("exa") == 0.007
    assert engine_request_usd("mystery") == 0.005


def test_month_to_date_summary(tmp_path: Path) -> None:
    """The summary returns (queries, usd) per requested engine."""
    ledger = SearchBudgetLedger(tmp_path / "b.jsonl")
    ledger.record(engine="brave", queries=40, run_id="r", now=datetime(2026, 6, 3, tzinfo=UTC))
    summary = month_to_date_summary(ledger, year_month="2026-06", engines=("brave", "exa"))
    assert summary["brave"] == (40, round(40 * 0.005, 6))
    assert summary["exa"] == (0, 0.0)


def test_ledger_missing_file_is_empty(tmp_path: Path) -> None:
    """A ledger with no file reads as empty rather than erroring."""
    ledger = SearchBudgetLedger(tmp_path / "missing.jsonl")
    assert ledger.load() == []
    assert ledger.month_spend(year_month="2026-06") == (0, 0.0)


def test_searched_on_detects_a_same_day_search(tmp_path: Path) -> None:
    """searched_on backs the GitHub search backstop: only days with a real search count."""
    ledger = SearchBudgetLedger(tmp_path / "b.jsonl")
    assert ledger.searched_on(day=date(2026, 6, 13)) is False  # empty ledger

    ledger.record(engine="brave", queries=10, run_id="r1", now=datetime(2026, 6, 13, 9, tzinfo=UTC))
    # A budget-skipped run that recorded 0 queries does not count as "searched".
    ledger.record(engine="exa", queries=0, run_id="r2", now=datetime(2026, 6, 13, 10, tzinfo=UTC))

    assert ledger.searched_on(day=date(2026, 6, 13)) is True
    assert ledger.searched_on(day=date(2026, 6, 12)) is False  # a different day
    assert ledger.searched_on(day=date(2026, 6, 13), engine="brave") is True
    assert ledger.searched_on(day=date(2026, 6, 13), engine="exa") is False  # exa logged 0


def test_searched_on_compares_in_utc(tmp_path: Path) -> None:
    """A timestamp is bucketed by its UTC calendar day."""
    ledger = SearchBudgetLedger(tmp_path / "b.jsonl")
    # 2026-06-13T23:30-05:00 is 2026-06-14T04:30Z.
    ledger.record(
        engine="brave",
        queries=5,
        run_id="r",
        now=datetime.fromisoformat("2026-06-13T23:30:00-05:00"),
    )
    assert ledger.searched_on(day=date(2026, 6, 14)) is True
    assert ledger.searched_on(day=date(2026, 6, 13)) is False
