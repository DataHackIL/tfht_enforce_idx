"""Core data models for the local pre-classification filter cascade."""

from __future__ import annotations

import dataclasses
from typing import Literal, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Verdict = Literal["pass", "drop"]
StageName = Literal["A", "B", "C", "D"]
StoppedAt = Literal["A", "B", "C", "D", "passed_all"]
PassKind = Literal["thin", "thick"]


# ---------------------------------------------------------------------------
# CandidateView — minimal read-only protocol consumed by cascade stages
# ---------------------------------------------------------------------------


@runtime_checkable
class CandidateView(Protocol):
    """Read-only view of a candidate exposed to cascade stages.

    Implementors must provide all five properties.  The concrete
    ``PersistentCandidate`` model satisfies this protocol via an adapter;
    see ``cascade.py``.
    """

    @property
    def candidate_id(self) -> str:
        """Stable unique identifier for the candidate."""
        ...

    @property
    def domain(self) -> str | None:
        """Normalized eTLD+1 host, or ``None`` if unavailable."""
        ...

    @property
    def title(self) -> str | None:
        """First (or only) title string, or ``None``."""
        ...

    @property
    def snippet(self) -> str | None:
        """First (or only) snippet string, or ``None``."""
        ...

    @property
    def url(self) -> str | None:
        """Canonical or current URL as a plain string, or ``None``."""
        ...


# ---------------------------------------------------------------------------
# StageScore — per-stage probability score
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class StageScore:
    """Probability output from a single cascade stage.

    Attributes
    ----------
    stage:
        Which stage produced this score (``"A"``, ``"B"``, ``"C"``, or ``"D"``).
    p_negative:
        Estimated probability that the candidate is a true negative.
        Ranges ``[0.0, 1.0]``.
    threshold:
        The configured drop threshold for this stage.  If
        ``p_negative >= threshold`` the stage would drop the candidate.
    dropped:
        Whether this stage's score exceeded the threshold.
    reason:
        Human-readable explanation, e.g. ``"domain_reputation:globes.co.il=0.99"``.
    model_version:
        Identifier of the artifact (lexicon hash, sklearn model path, etc.)
        that produced this score.  ``""`` for stub stages.
    """

    stage: StageName
    p_negative: float
    threshold: float
    dropped: bool
    reason: str
    model_version: str


# ---------------------------------------------------------------------------
# PrefilterDecision — full cascade verdict for one candidate
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PrefilterDecision:
    """Result of running the cascade over one candidate.

    Attributes
    ----------
    candidate_id:
        Matches the ``PersistentCandidate.candidate_id`` that was evaluated.
    pass_kind:
        ``"thin"`` for the pre-scrape pass (Stages A+B only) or ``"thick"``
        for the post-scrape pass (Stages A–D).
    verdict:
        ``"pass"`` — candidate proceeds to the next pipeline step.
        ``"drop"`` — cascade is confident this is a true negative; skip it.
    stopped_at_stage:
        Which stage emitted the drop verdict, or ``"passed_all"`` when no
        stage dropped the candidate.
    stage_scores:
        Ordered tuple of scores from each stage that ran.  Empty when
        the cascade is in ``off`` mode or all stages are stubs.
    decided_at:
        ISO-8601 UTC timestamp of when the decision was produced.
    config_hash:
        SHA-1 of the ``PrefilterConfig`` that was active when the decision
        was made, for audit traceability.
    """

    candidate_id: str
    pass_kind: PassKind
    verdict: Verdict
    stopped_at_stage: StoppedAt
    stage_scores: tuple[StageScore, ...]
    decided_at: str
    config_hash: str
