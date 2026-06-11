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
from datetime import datetime
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


def affordable_query_count(
    *,
    engine: str,
    requested: int,
    spent_usd: float,
    monthly_budget_usd: float | None,
) -> int:
    """How many of *requested* queries fit the remaining monthly budget.

    Returns *requested* unchanged when no budget is set. Never negative.
    """
    if monthly_budget_usd is None:
        return requested
    remaining = max(0.0, monthly_budget_usd - spent_usd)
    price = engine_request_usd(engine)
    if price <= 0:
        return requested
    return max(0, min(requested, int(remaining / price)))


def month_to_date_summary(
    ledger: SearchBudgetLedger, *, year_month: str, engines: Sequence[str]
) -> dict[str, tuple[int, float]]:
    """Return ``{engine: (queries, usd)}`` spent this month for each engine."""
    return {engine: ledger.month_spend(year_month=year_month, engine=engine) for engine in engines}
