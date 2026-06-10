"""Unit tests for frequency-weighted, source-balanced scrape batch planning."""

from __future__ import annotations

import collections
from datetime import UTC, datetime

from pydantic import HttpUrl

from denbust.discovery.balanced_selection import (
    candidate_month,
    candidate_source_key,
    largest_remainder_allocation,
    plan_balanced_scrape_batch,
    select_within_month,
)
from denbust.discovery.models import CandidateStatus, PersistentCandidate


def make_candidate(
    candidate_id: str,
    *,
    domain: str,
    month: str,
    last_seen: datetime | None = None,
    source_hints: list[str] | None = None,
) -> PersistentCandidate:
    """Build a candidate with a publication month and domain."""
    pub = f"{month}-15T08:00:00+00:00"
    return PersistentCandidate(
        candidate_id=candidate_id,
        canonical_url=HttpUrl(f"https://{domain}/article/{candidate_id}"),
        current_url=HttpUrl(f"https://{domain}/article/{candidate_id}"),
        domain=domain,
        titles=["t"],
        snippets=["s"],
        discovered_via=["brave"],
        discovery_queries=["q"],
        source_hints=source_hints if source_hints is not None else ["brave"],
        first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_seen_at=last_seen or datetime(2026, 1, 1, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
        metadata={"latest_publication_datetime_hint": pub},
    )


def test_candidate_source_key_collapses_subdomains() -> None:
    """Subdomains and engine hints resolve to the publisher family."""
    assert candidate_source_key(make_candidate("a", domain="www.ynet.co.il", month="2026-01")) == (
        "ynet"
    )
    assert (
        candidate_source_key(make_candidate("b", domain="sport1.maariv.co.il", month="2026-01"))
        == "maariv"
    )
    assert (
        candidate_source_key(make_candidate("c", domain="canary.haaretz.co.il", month="2026-01"))
        == "haaretz"
    )
    # Unknown domain falls back to the normalized domain, not the engine hint.
    assert (
        candidate_source_key(
            make_candidate("d", domain="example.org", month="2026-01", source_hints=["brave"])
        )
        == "example.org"
    )


def test_candidate_month_reads_publication_hint() -> None:
    """Publication month derives from the latest_publication_datetime_hint."""
    assert candidate_month(make_candidate("a", domain="ynet.co.il", month="2026-03")) == "2026-03"


def test_largest_remainder_allocation_sums_to_total() -> None:
    """Allocation is proportional and sums exactly to the requested total."""
    weights = {"2026-01": 621, "2026-02": 505, "2026-03": 425, "2026-04": 86, "2026-05": 171}
    alloc = largest_remainder_allocation(weights, 60)
    assert sum(alloc.values()) == 60
    # Busiest month gets the most; quietest gets the least.
    assert alloc["2026-01"] == max(alloc.values())
    assert alloc["2026-04"] == min(alloc.values())


def test_largest_remainder_handles_zero_and_empty() -> None:
    """Zero total or zero weights yield an all-zero allocation."""
    assert largest_remainder_allocation({"a": 5}, 0) == {"a": 0}
    assert largest_remainder_allocation({"a": 0, "b": 0}, 10) == {"a": 0, "b": 0}


def test_select_within_month_spreads_across_sources() -> None:
    """Within a month, selection round-robins across sources before draining one."""
    cands = (
        [make_candidate(f"y{i}", domain="ynet.co.il", month="2026-01") for i in range(10)]
        + [make_candidate(f"m{i}", domain="mako.co.il", month="2026-01") for i in range(10)]
        + [make_candidate(f"h{i}", domain="haaretz.co.il", month="2026-01") for i in range(10)]
    )
    picked = select_within_month(cands, 6)
    keys = collections.Counter(candidate_source_key(c) for c in picked)
    # Six slots across three equally-sized sources → two each, none monopolised.
    assert keys == {"ynet": 2, "mako": 2, "haaretz": 2}


def test_plan_balanced_scrape_batch_weights_months_and_balances_sources() -> None:
    """The batch is frequency-weighted across months and source-balanced within."""
    cands: list[PersistentCandidate] = []
    # Jan: 40 candidates (busy), Feb: 20, Mar: 4 (quiet) — two sources each.
    for month, total in [("2026-01", 40), ("2026-02", 20), ("2026-03", 4)]:
        for i in range(total):
            domain = "ynet.co.il" if i % 2 == 0 else "mako.co.il"
            cands.append(make_candidate(f"{month}-{i}", domain=domain, month=month))

    batch = plan_balanced_scrape_batch(cands, batch_size=16)
    assert len(batch) == 16
    by_month = collections.Counter(candidate_month(c) for c in batch)
    # 40:20:4 over 16 slots → 10:5:1 (largest remainder).
    assert by_month["2026-01"] > by_month["2026-02"] > by_month["2026-03"]
    assert sum(by_month.values()) == 16
    # Within the busy month both sources appear.
    jan_sources = collections.Counter(
        candidate_source_key(c) for c in batch if candidate_month(c) == "2026-01"
    )
    assert set(jan_sources) == {"ynet", "mako"}


def test_plan_balanced_scrape_batch_caps_at_pool_size() -> None:
    """Requesting more than available returns the whole pool, no duplicates."""
    cands = [make_candidate(f"a{i}", domain="ynet.co.il", month="2026-01") for i in range(5)]
    batch = plan_balanced_scrape_batch(cands, batch_size=60)
    assert len(batch) == 5
    assert len({c.candidate_id for c in batch}) == 5


def test_plan_balanced_scrape_batch_tops_up_when_month_underfills() -> None:
    """A month allocated more than it can supply is topped up from other months."""
    # Month A is heavily weighted but only has 1 candidate; month B has plenty.
    cands = [make_candidate("a0", domain="ynet.co.il", month="2026-01")]
    cands += [make_candidate(f"b{i}", domain="mako.co.il", month="2026-02") for i in range(30)]
    batch = plan_balanced_scrape_batch(cands, batch_size=10)
    assert len(batch) == 10
    assert len({c.candidate_id for c in batch}) == 10


def test_plan_balanced_scrape_batch_excludes_undated() -> None:
    """Candidates with no publication month are excluded from balanced selection."""
    dated = make_candidate("d", domain="ynet.co.il", month="2026-01")
    undated = PersistentCandidate(
        candidate_id="u",
        canonical_url=HttpUrl("https://ynet.co.il/article/u"),
        current_url=HttpUrl("https://ynet.co.il/article/u"),
        domain="ynet.co.il",
        titles=["t"],
        snippets=["s"],
        discovered_via=["brave"],
        discovery_queries=["q"],
        source_hints=["brave"],
        first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
        metadata={},
    )
    batch = plan_balanced_scrape_batch([dated, undated], batch_size=10)
    assert [c.candidate_id for c in batch] == ["d"]


def test_plan_balanced_scrape_batch_zero_size_is_empty() -> None:
    """A non-positive batch size yields no selection."""
    cands = [make_candidate("a", domain="ynet.co.il", month="2026-01")]
    assert plan_balanced_scrape_batch(cands, batch_size=0) == []
