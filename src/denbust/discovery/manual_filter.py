"""Stage B2 — manual LLM/agent pre-scrape filtering of selected candidates.

Stage B (the NaiveBayes prefilter) is statistical and lets through a long tail
of keyword-rich spam — escort-listing sites, massage-ad pages, SEO-bait domains
— whose title/snippet contain the enforcement lexicon but which will never
yield a real enforcement event.  **Stage B2** is the judgment pass that closes
that gap: before any scrape budget is spent on a planned batch, the operating
agent (LLM) reviews each candidate's title, snippet, and domain and *suppresses*
the ones that are clearly junk.

Suppression is terminal: a Stage-B2-suppressed candidate moves to
``CandidateStatus.SUPPRESSED`` and therefore leaves the scrapeable pool
permanently, so it never re-enters a future balanced batch.  This module is the
durable side-effect layer for that decision; the *judgment* itself lives with
the agent (and the protocol documented in ``docs/batch_scraping_protocol.md``).

Two entry points:

* ``suppress_candidates_b2`` — suppress an explicit list of candidate ids
  (one-off junk on otherwise-legitimate domains).
* ``suppress_candidates_b2_by_domain`` — suppress every still-scrapeable
  candidate on one or more spam domains (use together with adding the domain to
  ``_IRRELEVANT_CONTENT_DOMAINS`` so future discovery is blocked too).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from denbust.discovery.candidate_filters import normalize_domain
from denbust.discovery.models import CandidateStatus, PersistentCandidate
from denbust.discovery.scrape_queue import SCRAPEABLE_CANDIDATE_STATUSES
from denbust.discovery.storage import DiscoveryPersistence

#: Metadata marker + reason recorded on every Stage-B2-suppressed candidate.
B2_FILTER_METADATA_KEY = "b2_manual_filter"
B2_SUPPRESSION_REASON = "b2_manual_llm_filter"


def _b2_suppressed(candidate: PersistentCandidate, *, note: str | None) -> PersistentCandidate:
    """Return *candidate* transitioned to SUPPRESSED with Stage-B2 provenance."""
    metadata = {
        **candidate.metadata,
        B2_FILTER_METADATA_KEY: True,
        "b2_reason": B2_SUPPRESSION_REASON,
    }
    if note:
        metadata["b2_note"] = note
    return candidate.model_copy(
        update={
            "candidate_status": CandidateStatus.SUPPRESSED,
            "needs_review": False,
            "self_heal_eligible": False,
            "next_scrape_attempt_at": None,
            "metadata": metadata,
        }
    )


def suppress_candidates_b2(
    persistence: DiscoveryPersistence,
    candidate_ids: Iterable[str],
    *,
    note: str | None = None,
) -> list[PersistentCandidate]:
    """Stage B2: suppress candidates by id; return the rows actually suppressed.

    Already-suppressed and missing ids are skipped.  The change is persisted via
    ``upsert_candidates`` so the candidates leave the scrapeable pool.
    """
    suppressed: list[PersistentCandidate] = []
    for candidate_id in dict.fromkeys(candidate_ids):  # de-dupe, preserve order
        candidate = persistence.get_candidate(candidate_id)
        if candidate is None or candidate.candidate_status is CandidateStatus.SUPPRESSED:
            continue
        suppressed.append(_b2_suppressed(candidate, note=note))
    if suppressed:
        persistence.upsert_candidates(suppressed)
    return suppressed


def _domain_matches(candidate_domain: str | None, targets: frozenset[str]) -> bool:
    normalized = normalize_domain(candidate_domain) if candidate_domain else None
    if not normalized:
        return False
    return any(normalized == base or normalized.endswith(f".{base}") for base in targets)


def suppress_candidates_b2_by_domain(
    persistence: DiscoveryPersistence,
    domains: Sequence[str],
    *,
    note: str | None = None,
) -> list[PersistentCandidate]:
    """Stage B2: suppress every still-scrapeable candidate on *domains*.

    Subdomains are matched (``sport1.maariv.co.il`` matches target
    ``maariv.co.il``).  Pair with adding the domain to
    ``_IRRELEVANT_CONTENT_DOMAINS`` to block future discovery as well.
    """
    targets = frozenset(normalize_domain(d) or d for d in domains)
    if not targets:
        return []
    scrapeable = persistence.list_candidates(statuses=SCRAPEABLE_CANDIDATE_STATUSES)
    suppressed = [
        _b2_suppressed(candidate, note=note)
        for candidate in scrapeable
        if _domain_matches(candidate.domain, targets)
    ]
    if suppressed:
        persistence.upsert_candidates(suppressed)
    return suppressed
