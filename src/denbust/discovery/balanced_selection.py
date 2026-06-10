"""Frequency-weighted, source-balanced scrape-batch planning.

Given a pool of scrape-eligible candidates this module selects a fixed-size
batch that is:

1. **Frequency-weighted across months** — months with more eligible candidates
   receive proportionally more of the batch (largest-remainder apportionment),
   so a quiet month is not over-represented relative to a busy one.
2. **Source-balanced within each month** — a month's allocation is spread
   round-robin across the distinct publication sources/source families present,
   ordered by source scrape priority, so one prolific site cannot monopolise a
   month's slots.

The publication source is derived from the candidate *domain* (not
``source_hints``, which for search-discovered candidates records the discovery
engine — ``exa`` / ``brave`` — rather than the publisher).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from denbust.discovery.candidate_filters import normalize_domain
from denbust.discovery.models import PersistentCandidate
from denbust.discovery.scrape_queue import (
    _SOURCE_SCRAPE_PRIORITY,
    _backfill_publication_datetime,
)

# Discovery-engine markers that may appear in ``source_hints``; never a publisher.
_ENGINE_SOURCE_HINTS: frozenset[str] = frozenset({"brave", "exa", "google_cse", "test"})

# Registrable domain → canonical source-family key.  Subdomains collapse onto
# the family (e.g. ``sport1.maariv.co.il`` and ``maariv.co.il`` both map to
# ``maariv``) so the within-month balancer treats them as one source.
_DOMAIN_SOURCE_FAMILIES: dict[str, str] = {
    "ynet.co.il": "ynet",
    "mako.co.il": "mako",
    "ice.co.il": "ice",
    "haaretz.co.il": "haaretz",
    "maariv.co.il": "maariv",
    "walla.co.il": "walla",
    "globes.co.il": "globes",
    "themarker.com": "themarker",
    "israelhayom.co.il": "israelhayom",
    "kan.org.il": "kan",
    "news1.co.il": "news1",
    "calcalist.co.il": "calcalist",
}

#: Canonical family keys for the curated/known outlets. These are always exempt
#: from the domain-frequency gate (they are trusted regardless of recurrence).
KNOWN_SOURCE_FAMILIES: frozenset[str] = frozenset(_DOMAIN_SOURCE_FAMILIES.values())


def candidate_source_key(candidate: PersistentCandidate) -> str:
    """Return the canonical publication-source key for *candidate*.

    Resolution order: domain → known family; else the normalized domain; else a
    non-engine ``source_hint``; else ``"unknown"``.
    """
    domain = normalize_domain(candidate.domain) if candidate.domain else None
    if domain:
        for base, family in _DOMAIN_SOURCE_FAMILIES.items():
            if domain == base or domain.endswith(f".{base}"):
                return family
        return domain
    for hint in candidate.source_hints:
        if hint and hint not in _ENGINE_SOURCE_HINTS:
            return hint
    return "unknown"


def candidate_month(candidate: PersistentCandidate) -> str | None:
    """Return the candidate's publication month as ``YYYY-MM`` or ``None``."""
    published = _backfill_publication_datetime(candidate)
    return published.strftime("%Y-%m") if published is not None else None


def _source_key_priority(source_key: str) -> int:
    """Scrape priority for a resolved source-family key (0 when unknown)."""
    return _SOURCE_SCRAPE_PRIORITY.get(source_key, 0)


def domain_frequencies(candidates: list[PersistentCandidate]) -> dict[str, int]:
    """Count how many candidates share each resolved source key.

    Computed over a broad set (ideally the whole candidate store) so the count
    reflects how often a domain has *recurred* across discovery, which is the
    junk signal the frequency gate relies on: real outlets recur, one-off spam
    appears once.
    """
    counts: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        counts[candidate_source_key(candidate)] += 1
    return dict(counts)


def filter_by_domain_frequency(
    candidates: list[PersistentCandidate],
    *,
    min_frequency: int,
    frequencies: dict[str, int],
    exempt_known_families: bool = True,
) -> list[PersistentCandidate]:
    """Hold back candidates on domains seen fewer than *min_frequency* times.

    A candidate passes the gate when its domain is a curated/known outlet
    (always exempt when *exempt_known_families*) or its domain's recurrence
    count in *frequencies* is at least *min_frequency*.  Nothing is mutated or
    deleted — held-back candidates simply stay out of this batch and become
    eligible automatically once their domain recurs.

    ``min_frequency <= 1`` is a no-op (every domain trivially clears it).
    """
    if min_frequency <= 1:
        return list(candidates)
    kept: list[PersistentCandidate] = []
    for candidate in candidates:
        key = candidate_source_key(candidate)
        is_known = exempt_known_families and key in KNOWN_SOURCE_FAMILIES
        if is_known or frequencies.get(key, 0) >= min_frequency:
            kept.append(candidate)
    return kept


def largest_remainder_allocation(weights: dict[str, int], total: int) -> dict[str, int]:
    """Apportion *total* across keys proportionally to *weights*.

    Uses the largest-remainder (Hamilton) method so the allocations sum exactly
    to ``min(total, sum(weights))`` worth of demand while staying as close as
    possible to the proportional ideal.
    """
    weight_sum = sum(weights.values())
    if weight_sum <= 0 or total <= 0:
        return dict.fromkeys(weights, 0)
    exact = {key: total * value / weight_sum for key, value in weights.items()}
    allocation = {key: int(value) for key, value in exact.items()}
    remaining = total - sum(allocation.values())
    if remaining > 0:
        order = sorted(
            weights,
            key=lambda key: (exact[key] - allocation[key], weights[key], key),
            reverse=True,
        )
        for key in order[:remaining]:
            allocation[key] += 1
    return allocation


def _order_within_source(candidates: list[PersistentCandidate]) -> list[PersistentCandidate]:
    """Order one source's candidates by priority, then recency, then id."""
    return sorted(
        candidates,
        key=lambda c: (
            -_source_key_priority(candidate_source_key(c)),
            -c.last_seen_at.timestamp(),
            c.candidate_id,
        ),
    )


def select_within_month(
    candidates: list[PersistentCandidate],
    count: int,
) -> list[PersistentCandidate]:
    """Pick *count* candidates from one month, round-robin across sources.

    Sources are visited in descending scrape-priority order each round, so
    higher-signal publishers get first pick while every source is represented
    before any source is drained.
    """
    if count <= 0 or not candidates:
        return []
    by_source: dict[str, list[PersistentCandidate]] = defaultdict(list)
    for candidate in candidates:
        by_source[candidate_source_key(candidate)].append(candidate)
    queues = {key: _order_within_source(group) for key, group in by_source.items()}
    source_order = sorted(
        queues,
        key=lambda key: (-_source_key_priority(key), -len(queues[key]), key),
    )
    selected: list[PersistentCandidate] = []
    while len(selected) < count and any(queues[key] for key in source_order):
        for key in source_order:
            if queues[key]:
                selected.append(queues[key].pop(0))
                if len(selected) >= count:
                    break
    return selected


def plan_balanced_scrape_batch(
    candidates: list[PersistentCandidate],
    *,
    batch_size: int,
    now: datetime | None = None,
) -> list[PersistentCandidate]:
    """Select a month-frequency-weighted, source-balanced batch.

    *candidates* should already be the scrape-eligible (and prefilter-passing)
    pool.  Candidates with no resolvable publication month are excluded from
    balanced selection.  Returns at most *batch_size* candidates.
    """
    del now  # reserved for future recency weighting; selection is order-stable
    if batch_size <= 0:
        return []
    by_month: dict[str, list[PersistentCandidate]] = defaultdict(list)
    for candidate in candidates:
        month = candidate_month(candidate)
        if month is not None:
            by_month[month].append(candidate)
    if not by_month:
        return []

    available = sum(len(group) for group in by_month.values())
    target = min(batch_size, available)
    weights = {month: len(group) for month, group in by_month.items()}
    allocation = largest_remainder_allocation(weights, target)

    selected: list[PersistentCandidate] = []
    selected_ids: set[str] = set()
    for month, group in by_month.items():
        picked = select_within_month(group, allocation.get(month, 0))
        selected.extend(picked)
        selected_ids.update(c.candidate_id for c in picked)

    # Top-up: a month allocated more than it could supply leaves the batch
    # short; backfill from the remaining pool by global scrape priority.
    if len(selected) < target:
        leftovers = [c for c in candidates if c.candidate_id not in selected_ids]
        leftovers = [c for c in leftovers if candidate_month(c) is not None]
        for candidate in _order_within_source(leftovers):
            if len(selected) >= target:
                break
            selected.append(candidate)
            selected_ids.add(candidate.candidate_id)

    return selected[:batch_size]
