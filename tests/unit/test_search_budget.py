"""Unit tests for the search-budget ledger and guard."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from denbust.discovery.search_budget import (
    ENGINE_REQUEST_USD,
    SearchBudgetLedger,
    affordable_query_count,
    engine_request_usd,
    month_to_date_summary,
)


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
    """The guard caps requested queries to what the remaining budget affords."""
    # Brave $0.005/q; $1 budget, $0.50 spent → $0.50 left → 100 queries.
    assert (
        affordable_query_count(
            engine="brave", requested=435, spent_usd=0.50, monthly_budget_usd=1.00
        )
        == 100
    )
    # Budget already exhausted → 0.
    assert (
        affordable_query_count(
            engine="brave", requested=435, spent_usd=1.00, monthly_budget_usd=1.00
        )
        == 0
    )
    # Fewer requested than affordable → unchanged.
    assert (
        affordable_query_count(engine="exa", requested=20, spent_usd=0.0, monthly_budget_usd=100.0)
        == 20
    )
    # No budget set → unchanged.
    assert (
        affordable_query_count(
            engine="brave", requested=435, spent_usd=999.0, monthly_budget_usd=None
        )
        == 435
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
