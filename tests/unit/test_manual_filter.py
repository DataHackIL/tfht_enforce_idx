"""Unit tests for Stage B2 manual-filter suppression."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pydantic import HttpUrl

from denbust.discovery.manual_filter import (
    B2_FILTER_METADATA_KEY,
    B2_SUPPRESSION_REASON,
    suppress_candidates_b2,
    suppress_candidates_b2_by_domain,
)
from denbust.discovery.models import CandidateStatus, PersistentCandidate
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName


def _candidate(
    candidate_id: str,
    *,
    domain: str,
    status: CandidateStatus = CandidateStatus.NEW,
) -> PersistentCandidate:
    return PersistentCandidate(
        candidate_id=candidate_id,
        canonical_url=HttpUrl(f"https://{domain}/a/{candidate_id}"),
        current_url=HttpUrl(f"https://{domain}/a/{candidate_id}"),
        domain=domain,
        titles=["t"],
        snippets=["s"],
        discovered_via=["brave"],
        discovery_queries=["q"],
        source_hints=["brave"],
        first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        candidate_status=status,
    )


def _store(tmp_path: Path) -> StateRepoDiscoveryPersistence:
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    return StateRepoDiscoveryPersistence(paths)


def test_suppress_by_ids_marks_terminal_and_records_provenance(tmp_path: Path) -> None:
    """Suppressing by id moves rows to SUPPRESSED with Stage-B2 metadata."""
    store = _store(tmp_path)
    store.upsert_candidates(
        [_candidate("a", domain="ynet.co.il"), _candidate("b", domain="spam.example")]
    )

    suppressed = suppress_candidates_b2(store, ["b"], note="B2: junk")

    assert [c.candidate_id for c in suppressed] == ["b"]
    reloaded = store.get_candidate("b")
    assert reloaded is not None
    assert reloaded.candidate_status is CandidateStatus.SUPPRESSED
    assert reloaded.metadata[B2_FILTER_METADATA_KEY] is True
    assert reloaded.metadata["b2_reason"] == B2_SUPPRESSION_REASON
    assert reloaded.metadata["b2_note"] == "B2: junk"
    # Untouched candidate stays scrapeable.
    assert store.get_candidate("a").candidate_status is CandidateStatus.NEW  # type: ignore[union-attr]


def test_suppress_by_ids_skips_missing_and_already_suppressed(tmp_path: Path) -> None:
    """Missing ids and already-suppressed rows are no-ops."""
    store = _store(tmp_path)
    store.upsert_candidates(
        [_candidate("done", domain="x.example", status=CandidateStatus.SUPPRESSED)]
    )
    suppressed = suppress_candidates_b2(store, ["done", "missing"])
    assert suppressed == []


def test_suppress_by_domain_matches_subdomains(tmp_path: Path) -> None:
    """Domain suppression collapses subdomains and only touches scrapeable rows."""
    store = _store(tmp_path)
    store.upsert_candidates(
        [
            _candidate("keep", domain="ynet.co.il"),
            _candidate("spam1", domain="escort.example"),
            _candidate("spam2", domain="sub.escort.example"),
            _candidate("already", domain="escort.example", status=CandidateStatus.SCRAPE_SUCCEEDED),
        ]
    )

    suppressed = suppress_candidates_b2_by_domain(store, ["escort.example"], note="B2: spam")

    assert {c.candidate_id for c in suppressed} == {"spam1", "spam2"}
    assert store.get_candidate("spam1").candidate_status is CandidateStatus.SUPPRESSED  # type: ignore[union-attr]
    assert store.get_candidate("spam2").candidate_status is CandidateStatus.SUPPRESSED  # type: ignore[union-attr]
    # Non-scrapeable status is left as-is; legitimate domain untouched.
    assert store.get_candidate("already").candidate_status is CandidateStatus.SCRAPE_SUCCEEDED  # type: ignore[union-attr]
    assert store.get_candidate("keep").candidate_status is CandidateStatus.NEW  # type: ignore[union-attr]


def test_suppress_by_domain_empty_input_is_noop(tmp_path: Path) -> None:
    """No domains → nothing suppressed."""
    store = _store(tmp_path)
    store.upsert_candidates([_candidate("a", domain="ynet.co.il")])
    assert suppress_candidates_b2_by_domain(store, []) == []
