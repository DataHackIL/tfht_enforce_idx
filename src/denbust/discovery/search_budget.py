"""Search-engine budget ledger and guard.

Brave and Exa charge per request and each give only ~1,000 free queries/month.
A naive discovery run issued 435 queries/engine and exhausted the month's budget
in 2-3 runs — surfacing as ``402 Payment Required`` mid-run with no warning.

This module is the accounting + safety layer:

* ``SearchBudgetLedger`` — an append-only JSONL log of per-engine, per-run
  search spend under the discovery state dir.
* ``month_spend`` — month-to-date queries + estimated USD for an engine.
* ``affordable_query_count`` — how many of a run's planned queries fit the
  remaining monthly budget (the guard truncates to this, keeping the
  highest-priority queries, instead of overspending into a 402).

Pricing matches Brave ($5/1k) and Exa ($7/1k). The cheaper engine (Brave) gets
more queries through the same dollar budget, which is the routing preference.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel

logger = logging.getLogger(__name__)

#: Estimated USD per search request, per engine.
ENGINE_REQUEST_USD: dict[str, float] = {
    "brave": 0.005,  # $5 / 1k
    "exa": 0.007,  # $7 / 1k
    "google_cse": 0.005,  # $5 / 1k (CSE billable tier)
}
_DEFAULT_REQUEST_USD = 0.005


def engine_request_usd(engine: str) -> float:
    """Estimated USD cost of one search request on *engine*."""
    return ENGINE_REQUEST_USD.get(engine, _DEFAULT_REQUEST_USD)


class SearchSpendRecord(BaseModel):
    """One run's search spend on one engine."""

    run_id: str
    engine: str
    queries: int
    estimated_cost_usd: float
    recorded_at: datetime


class SearchBudgetLedger:
    """Append-only JSONL log of per-engine search spend."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> list[SearchSpendRecord]:
        if not self.path.exists():
            return []
        records: list[SearchSpendRecord] = []
        with open(self.path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(SearchSpendRecord.model_validate_json(line))
        return records

    def record(self, *, engine: str, queries: int, run_id: str, now: datetime) -> SearchSpendRecord:
        """Append a spend record for *queries* issued on *engine*."""
        record = SearchSpendRecord(
            run_id=run_id,
            engine=engine,
            queries=queries,
            estimated_cost_usd=round(queries * engine_request_usd(engine), 6),
            recorded_at=now,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(record.model_dump_json())
            handle.write("\n")
        return record

    def searched_since(self, *, since: datetime, engine: str | None = None) -> bool:
        """True if a search (queries > 0) was recorded at or after *since*.

        Backs the GitHub search backstop: GH issues open-web queries only when no
        run — local or CI — searched within the trailing window (the discover job
        uses the last 24h). A rolling window (rather than a calendar day) means GH
        defers to a recent local search regardless of clock-time ordering: as long
        as local runs at least daily, GH always skips and only searches once local
        has been idle for longer than the window. ``since`` is compared in an aware
        fashion; a tz-naive ``recorded_at`` is treated as UTC.
        """
        for record in self.load():
            if record.queries <= 0:
                continue
            if engine is not None and record.engine != engine:
                continue
            recorded_at = record.recorded_at
            if recorded_at.tzinfo is None:
                recorded_at = recorded_at.replace(tzinfo=UTC)
            if recorded_at >= since:
                return True
        return False

    def month_spend(self, *, year_month: str, engine: str | None = None) -> tuple[int, float]:
        """Return ``(queries, usd)`` spent in *year_month* (``YYYY-MM``)."""
        queries = 0
        usd = 0.0
        for record in self.load():
            if record.recorded_at.strftime("%Y-%m") != year_month:
                continue
            if engine is not None and record.engine != engine:
                continue
            queries += record.queries
            usd += record.estimated_cost_usd
        return queries, round(usd, 6)


def billed_cost_usd(engine: str, *, queries: int, monthly_free_queries: int = 0) -> float:
    """Real USD for *queries* this month after the free allowance is consumed."""
    paid = max(0, queries - monthly_free_queries)
    return round(paid * engine_request_usd(engine), 6)


def affordable_query_count(
    *,
    engine: str,
    requested: int,
    queries_spent: int,
    monthly_budget_usd: float | None,
    monthly_free_queries: int = 0,
) -> int:
    """How many of *requested* queries fit the remaining monthly allowance.

    Spends the free monthly allowance first, then the paid *monthly_budget_usd*.
    Returns *requested* unchanged when neither a free allowance nor a budget is
    set. Never negative.
    """
    free_remaining = max(0, monthly_free_queries - queries_spent)
    if monthly_budget_usd is None:
        if monthly_free_queries <= 0:
            return requested
        return min(requested, free_remaining)
    price = engine_request_usd(engine)
    if price <= 0:
        return requested
    paid_spent = max(0, queries_spent - monthly_free_queries)
    paid_budget_remaining = max(0.0, monthly_budget_usd - paid_spent * price)
    paid_affordable = int(paid_budget_remaining / price)
    return max(0, min(requested, free_remaining + paid_affordable))


def month_to_date_summary(
    ledger: SearchBudgetLedger, *, year_month: str, engines: Sequence[str]
) -> dict[str, tuple[int, float]]:
    """Return ``{engine: (queries, usd)}`` spent this month for each engine."""
    return {engine: ledger.month_spend(year_month=year_month, engine=engine) for engine in engines}
