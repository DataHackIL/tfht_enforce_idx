"""Unit tests for the automated per-domain LLM verdict gate."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pydantic import HttpUrl

from denbust.discovery.domain_verdicts import (
    DomainClassifierProtocol,
    DomainVerdict,
    DomainVerdictStore,
    _parse_verdict,
    blocked_domains,
    classify_pool_domains,
    filter_by_domain_verdict,
)
from denbust.discovery.models import CandidateStatus, PersistentCandidate


def _cand(candidate_id: str, *, domain: str, title: str = "t") -> PersistentCandidate:
    return PersistentCandidate(
        candidate_id=candidate_id,
        canonical_url=HttpUrl(f"https://{domain}/a/{candidate_id}"),
        current_url=HttpUrl(f"https://{domain}/a/{candidate_id}"),
        domain=domain,
        titles=[title],
        snippets=["s"],
        discovered_via=["brave"],
        discovery_queries=["q"],
        source_hints=["brave"],
        first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        candidate_status=CandidateStatus.NEW,
    )


class FakeClassifier(DomainClassifierProtocol):
    """Verdicts domains by a fixed allow/block lookup; records calls."""

    def __init__(self, decisions: dict[str, str]) -> None:
        self.decisions = decisions
        self.calls: list[str] = []

    def classify(self, domain: str, sample_titles: list[str]) -> DomainVerdict | None:
        self.calls.append(domain)
        decision = self.decisions.get(domain)
        if decision is None:
            return None
        return DomainVerdict(
            domain=domain, decision=decision, reason="t", sample_titles=sample_titles
        )


def test_verdict_store_round_trips(tmp_path: Path) -> None:
    """Verdicts persist to JSONL and reload, keyed by domain."""
    store = DomainVerdictStore(tmp_path / "verdicts.jsonl")
    store.upsert(
        [
            DomainVerdict(domain="newsru.co.il", decision="allow", reason="news"),
            DomainVerdict(domain="xmassage.example", decision="block", reason="escort"),
        ]
    )
    fresh = DomainVerdictStore(tmp_path / "verdicts.jsonl")
    loaded = fresh.load()
    assert loaded["newsru.co.il"].decision == "allow"
    assert loaded["xmassage.example"].decision == "block"
    assert blocked_domains(fresh) == ["xmassage.example"]


def test_verdict_store_upsert_overwrites(tmp_path: Path) -> None:
    """Re-judging a domain replaces the prior verdict."""
    store = DomainVerdictStore(tmp_path / "v.jsonl")
    store.upsert([DomainVerdict(domain="d.example", decision="block", reason="x")])
    store.upsert([DomainVerdict(domain="d.example", decision="allow", reason="y")])
    assert DomainVerdictStore(tmp_path / "v.jsonl").get("d.example").decision == "allow"  # type: ignore[union-attr]


def test_parse_verdict_extracts_json() -> None:
    """The parser tolerates surrounding prose and validates the decision."""
    v = _parse_verdict(
        "d.example",
        'sure: {"decision": "block", "reason": "escort"} done',
        model="m",
        sample_titles=[],
    )
    assert v is not None and v.decision == "block"
    assert _parse_verdict("d.example", "no json here", model="m", sample_titles=[]) is None
    assert _parse_verdict("d.example", '{"decision": "maybe"}', model="m", sample_titles=[]) is None


def test_classify_pool_domains_skips_known_static_and_cached(tmp_path: Path) -> None:
    """Only unjudged, non-known, non-blocklisted domains get classified."""
    store = DomainVerdictStore(tmp_path / "v.jsonl")
    store.upsert([DomainVerdict(domain="already.example", decision="allow", reason="cached")])
    pool = [
        _cand("k", domain="ynet.co.il"),  # known family → skip
        _cand("s", domain="static.example"),  # in static blocklist → skip
        _cand("c", domain="already.example"),  # cached → skip
        _cand("n1", domain="fresh.example", title="escort ad"),  # new → classify
        _cand("n2", domain="news.example", title="police raid"),  # new → classify
    ]
    classifier = FakeClassifier({"fresh.example": "block", "news.example": "allow"})

    new = classify_pool_domains(
        pool,
        store=store,
        classifier=classifier,
        static_blocklist=frozenset({"static.example"}),
    )

    assert set(classifier.calls) == {"fresh.example", "news.example"}
    assert {v.domain: v.decision for v in new} == {
        "fresh.example": "block",
        "news.example": "allow",
    }
    # Persisted and merged with the pre-existing cached verdict.
    assert set(DomainVerdictStore(tmp_path / "v.jsonl").load()) == {
        "already.example",
        "fresh.example",
        "news.example",
    }


def test_classify_pool_domains_limit(tmp_path: Path) -> None:
    """The limit caps how many new domains are classified per run."""
    store = DomainVerdictStore(tmp_path / "v.jsonl")
    pool = [_cand(f"c{i}", domain=f"d{i}.example") for i in range(5)]
    classifier = FakeClassifier({f"d{i}.example": "block" for i in range(5)})
    new = classify_pool_domains(pool, store=store, classifier=classifier, limit=2)
    assert len(new) == 2
    assert len(classifier.calls) == 2


def test_filter_by_domain_verdict_blocks_and_exempts() -> None:
    """Block verdicts are held; allow + known + unjudged pass by default."""
    verdicts = {
        "escort.example": DomainVerdict(domain="escort.example", decision="block", reason="x"),
        "news.example": DomainVerdict(domain="news.example", decision="allow", reason="y"),
    }
    pool = [
        _cand("a", domain="escort.example"),  # block → drop
        _cand("b", domain="news.example"),  # allow → keep
        _cand("c", domain="ynet.co.il"),  # known → keep
        _cand("d", domain="unjudged.example"),  # unjudged → keep (default)
    ]
    kept = {c.candidate_id for c in filter_by_domain_verdict(pool, verdicts=verdicts)}
    assert kept == {"b", "c", "d"}

    strict = {
        c.candidate_id
        for c in filter_by_domain_verdict(pool, verdicts=verdicts, block_unjudged=True)
    }
    assert strict == {"b", "c"}  # unjudged now held too
