"""Automated per-domain LLM verdict gate.

Open-web discovery on prostitution/escort/massage keywords drags in an
unbounded tail of off-topic and spam domains. The domain blocklist
(``_IRRELEVANT_CONTENT_DOMAINS``) and Stage B2 handle this manually, one
blocklist PR per batch. This module automates that judgment: each *new* domain
is classified once by an LLM ("is this a plausible Israeli enforcement-news
source, or junk?"), the verdict is cached durably, and the gate then holds back
candidates on ``block`` domains — with zero per-batch manual work.

Pieces:

* ``DomainVerdict`` — the cached decision for one domain.
* ``DomainVerdictStore`` — JSONL-backed cache under the discovery state dir.
* ``DomainClassifier`` — Anthropic-backed; ``classify(domain, titles)`` returns
  a verdict (or ``None`` on provider error, so a transient failure never blocks).
* ``classify_pool_domains`` — orchestration: pick unjudged domains from the
  candidate pool, classify them, upsert the cache. Takes an injected classifier
  so it is testable without live calls.
* ``filter_by_domain_verdict`` — the gate used at scrape-selection time.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, Field

from denbust.discovery.balanced_selection import KNOWN_SOURCE_FAMILIES, candidate_source_key
from denbust.discovery.models import PersistentCandidate

logger = logging.getLogger(__name__)

DomainDecision = Literal["allow", "block"]

#: How many sample titles to show the classifier per domain.
_DEFAULT_SAMPLE_SIZE = 5


class DomainVerdict(BaseModel):
    """A cached allow/block decision for one publication domain/source family."""

    domain: str
    decision: DomainDecision
    reason: str = ""
    model: str = ""
    sample_titles: list[str] = Field(default_factory=list)


class DomainVerdictStore:
    """JSONL-backed cache of per-domain verdicts under the discovery state dir."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._cache: dict[str, DomainVerdict] = {}
        self._loaded = False

    def load(self) -> dict[str, DomainVerdict]:
        """Return all verdicts keyed by domain (lazily read once)."""
        if self._loaded:
            return self._cache
        if self.path.exists():
            with open(self.path, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        verdict = DomainVerdict.model_validate_json(line)
                        self._cache[verdict.domain] = verdict
        self._loaded = True
        return self._cache

    def get(self, domain: str) -> DomainVerdict | None:
        return self.load().get(domain)

    def upsert(self, verdicts: Sequence[DomainVerdict]) -> None:
        """Merge *verdicts* into the cache and rewrite the JSONL file."""
        if not verdicts:
            return
        self.load()
        for verdict in verdicts:
            self._cache[verdict.domain] = verdict
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(self._cache.values(), key=lambda v: v.domain)
        with open(self.path, "w", encoding="utf-8") as handle:
            for verdict in ordered:
                handle.write(verdict.model_dump_json())
                handle.write("\n")


class DomainClassifierProtocol(Protocol):
    """Anything that can verdict a domain from a few sample titles."""

    def classify(self, domain: str, sample_titles: list[str]) -> DomainVerdict | None: ...


_SYSTEM_PROMPT = (
    "You decide whether a web domain is a plausible SOURCE of Israeli "
    "law-enforcement news about sex trafficking and prostitution.\n"
    'Reply with ONLY a JSON object: {"decision": "allow" | "block", "reason": "<short>"}.\n'
    "allow = an Israeli news outlet or official source that could report enforcement "
    "events such as raids, indictments, arrests, brothel closures, or client fines — "
    "including niche, local, or Russian/English-language Israeli outlets.\n"
    "block = escort/massage/webcam listings, SEO/ad/marketing pages, real-estate, cars, "
    "finance, gadgets, dictionaries, academic papers, blogs/forums, social media, "
    "religious-study sites, or any FOREIGN (non-Israeli) outlet.\n"
    "When unsure, choose block (precision over recall)."
)


class DomainClassifier:
    """Anthropic-backed domain classifier."""

    def __init__(self, *, api_key: str, model: str = "claude-sonnet-4-20250514") -> None:
        import anthropic

        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    def classify(self, domain: str, sample_titles: list[str]) -> DomainVerdict | None:
        import anthropic
        from anthropic.types import TextBlock

        titles = "\n".join(f"- {t[:120]}" for t in sample_titles[:_DEFAULT_SAMPLE_SIZE] if t)
        prompt = (
            f"Domain: {domain}\nSample candidate titles from this domain:\n{titles or '(none)'}"
        )
        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=128,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIError as error:
            logger.warning("Domain classify failed for %s: %s", domain, type(error).__name__)
            return None
        text = ""
        if response.content and isinstance(response.content[0], TextBlock):
            text = response.content[0].text
        return _parse_verdict(domain, text, model=self._model, sample_titles=sample_titles)


def _parse_verdict(
    domain: str, text: str, *, model: str, sample_titles: list[str]
) -> DomainVerdict | None:
    """Parse a ``{"decision","reason"}`` JSON object from the model text."""
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None
    decision = payload.get("decision")
    if decision not in ("allow", "block"):
        return None
    return DomainVerdict(
        domain=domain,
        decision=decision,
        reason=str(payload.get("reason", ""))[:200],
        model=model,
        sample_titles=[t for t in sample_titles[:_DEFAULT_SAMPLE_SIZE] if t],
    )


def _domain_samples(
    candidates: Sequence[PersistentCandidate],
    *,
    skip: frozenset[str],
    sample_size: int,
) -> dict[str, list[str]]:
    """Group candidate titles by source key, skipping exempt/known domains."""
    samples: dict[str, list[str]] = defaultdict(list)
    for candidate in candidates:
        key = candidate_source_key(candidate)
        if key in skip or key in KNOWN_SOURCE_FAMILIES:
            continue
        if len(samples[key]) < sample_size and candidate.titles:
            samples[key].append(candidate.titles[0])
    return samples


def classify_pool_domains(
    candidates: Sequence[PersistentCandidate],
    *,
    store: DomainVerdictStore,
    classifier: DomainClassifierProtocol,
    static_blocklist: frozenset[str] = frozenset(),
    limit: int | None = None,
    sample_size: int = _DEFAULT_SAMPLE_SIZE,
) -> list[DomainVerdict]:
    """Classify and cache verdicts for not-yet-judged domains in the pool.

    Domains that are known families, already in *static_blocklist*, or already
    cached are skipped. Returns the newly written verdicts.
    """
    already = set(store.load()) | set(static_blocklist)
    samples = _domain_samples(candidates, skip=frozenset(already), sample_size=sample_size)
    domains = sorted(samples, key=lambda d: (-len(samples[d]), d))
    if limit is not None:
        domains = domains[:limit]
    new_verdicts: list[DomainVerdict] = []
    for domain in domains:
        verdict = classifier.classify(domain, samples[domain])
        if verdict is not None:
            new_verdicts.append(verdict)
    store.upsert(new_verdicts)
    return new_verdicts


def blocked_domains(store: DomainVerdictStore) -> list[str]:
    """Return all domains the cache has decided to block."""
    return [d for d, v in store.load().items() if v.decision == "block"]


def filter_by_domain_verdict(
    candidates: list[PersistentCandidate],
    *,
    verdicts: dict[str, DomainVerdict],
    exempt_known_families: bool = True,
    block_unjudged: bool = False,
) -> list[PersistentCandidate]:
    """Hold back candidates whose domain verdict is ``block``.

    Known families (when *exempt_known_families*) and domains with an ``allow``
    verdict pass. Unjudged domains pass unless *block_unjudged* is set.
    """
    kept: list[PersistentCandidate] = []
    for candidate in candidates:
        key = candidate_source_key(candidate)
        if exempt_known_families and key in KNOWN_SOURCE_FAMILIES:
            kept.append(candidate)
            continue
        verdict = verdicts.get(key)
        if verdict is None:
            if not block_unjudged:
                kept.append(candidate)
        elif verdict.decision == "allow":
            kept.append(candidate)
    return kept
