"""Cascade orchestrator — wires all stages and emits PrefilterDecision records."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.models import (
    CandidateView,
    PassKind,
    PrefilterDecision,
    StageScore,
    StoppedAt,
    Verdict,
)
from denbust.prefilter.stage_a import StageAScorer
from denbust.prefilter.stage_b import StageBScorer
from denbust.prefilter.stage_c import StageCScorer
from denbust.prefilter.stage_d import StageDScorer
from denbust.prefilter.telemetry import PrefilterDecisionWriter


class CascadeOrchestrator:
    """Evaluates candidates through the active cascade stages.

    Modes
    -----
    OFF (or ``enabled=False``):
        Returns a pass decision immediately without running any stage or
        writing telemetry.  The pipeline is completely unchanged.
    SHADOW:
        Runs all configured stages and records decisions, but downgrades any
        ``"drop"`` verdict to ``"pass"`` so no candidates are removed.  The
        ``stopped_at_stage`` field still records where the cascade *would*
        have stopped, preserving data for recall analysis.  Use this for the
        validation period before switching to ENFORCE.
    ENFORCE:
        Runs all configured stages and respects ``"drop"`` verdicts.  Only
        safe after a shadow period with confirmed recall ≥ the configured
        floor.

    Stage instantiation
    -------------------
    Only *enabled* stages are instantiated.  Real implementations (LPF-PR-03+)
    load embedding models and SLM weights at construction time; disabled
    stages must never pay that cost.

    Stages A (LPF-PR-03), B (LPF-PR-04), C (LPF-PR-06), and D (LPF-PR-07)
    are all fully implemented.  When artifacts are absent or optional extras
    are missing the scorer returns ``None`` (pass-through) for that stage.
    """

    def __init__(
        self,
        config: PrefilterConfig,
        decisions_dir: Path,
        models_dir: Path | None = None,
    ) -> None:
        self._config = config
        self._config_hash = _hash_config(config)
        self._writer = PrefilterDecisionWriter(decisions_dir)
        # Only instantiate enabled stages — real implementations load models.
        self._stage_a: StageAScorer | None = (
            StageAScorer(models_dir=models_dir, threshold=config.stages.a.threshold)
            if config.stages.a.enabled
            else None
        )
        self._stage_b: StageBScorer | None = (
            StageBScorer(models_dir=models_dir, threshold=config.stages.b.threshold)
            if config.stages.b.enabled
            else None
        )
        self._stage_c: StageCScorer | None = StageCScorer() if config.stages.c.enabled else None
        self._stage_d: StageDScorer | None = StageDScorer() if config.stages.d.enabled else None

    # ------------------------------------------------------------------
    # Public evaluation interface
    # ------------------------------------------------------------------

    def evaluate_thin(self, candidate: CandidateView) -> PrefilterDecision:
        """Run the thin (pre-scrape) pass: Stages A and B only.

        In OFF mode (or when ``enabled=False``) returns a pass decision
        immediately without running any stage or writing telemetry.

        Parameters
        ----------
        candidate:
            A :class:`CandidateView`-compatible object.
        """
        if not self._config.enabled or self._config.mode == PrefilterMode.OFF:
            return self._noop_pass(candidate, "thin")

        scores: list[StageScore] = []

        # Stage A — lexicon + domain reputation + URL heuristics
        if self._stage_a is not None:
            score: StageScore | None = self._stage_a.evaluate(candidate, "thin")
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thin", "drop", "A", candidate, tuple(scores))

        # Stage B — thin uses title + snippet only (no body)
        if self._stage_b is not None:
            score = self._stage_b.evaluate(candidate, "thin")
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thin", "drop", "B", candidate, tuple(scores))

        return self._conclude("thin", "pass", "passed_all", candidate, tuple(scores))

    def evaluate_thick(self, candidate: CandidateView, body: str) -> PrefilterDecision:
        """Run the thick (post-scrape) pass: Stages A through D.

        In OFF mode (or when ``enabled=False``) returns a pass decision
        immediately without running any stage or writing telemetry.

        Parameters
        ----------
        candidate:
            A :class:`CandidateView`-compatible object.
        body:
            Full article body text (truncation handled per stage).

        Note
        ----
        Stage A re-executes in this pass so that the canonical scraped URL
        is evaluated.  When a thin pass has already run on the same candidate
        Stage A will execute twice; a future PR may add a thin-result
        parameter to avoid the redundant work.
        """
        if not self._config.enabled or self._config.mode == PrefilterMode.OFF:
            return self._noop_pass(candidate, "thick")

        scores: list[StageScore] = []

        # Stage A — domain recheck on the canonical scraped URL
        if self._stage_a is not None:
            score: StageScore | None = self._stage_a.evaluate(candidate, "thick")
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thick", "drop", "A", candidate, tuple(scores))

        # Stage B — thick pass uses full article body
        if self._stage_b is not None:
            score = self._stage_b.evaluate(candidate, "thick", body)
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thick", "drop", "B", candidate, tuple(scores))

        # Stage C — embedding similarity, thick pass only by default
        if self._stage_c is not None:
            score = self._stage_c.evaluate(candidate, "thick", body)
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thick", "drop", "C", candidate, tuple(scores))

        # Stage D — SLM logprob judge, thick pass only
        if self._stage_d is not None:
            score = self._stage_d.evaluate(candidate, "thick", body)
            if score is not None:
                scores.append(score)
                if score.dropped:
                    return self._conclude("thick", "drop", "D", candidate, tuple(scores))

        return self._conclude("thick", "pass", "passed_all", candidate, tuple(scores))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _noop_pass(
        self,
        candidate: CandidateView,
        pass_kind: PassKind,
    ) -> PrefilterDecision:
        """Return a pass decision without running any stage or writing telemetry.

        Used when ``config.enabled is False`` or ``config.mode == OFF``.
        """
        return PrefilterDecision(
            candidate_id=candidate.candidate_id,
            pass_kind=pass_kind,
            verdict="pass",
            stopped_at_stage="passed_all",
            stage_scores=(),
            decided_at=datetime.now(UTC),
            config_hash=self._config_hash,
        )

    def _conclude(
        self,
        pass_kind: PassKind,
        verdict: Verdict,
        stopped_at: StoppedAt,
        candidate: CandidateView,
        scores: tuple[StageScore, ...],
    ) -> PrefilterDecision:
        """Apply mode policy, build a decision, write telemetry, and return it.

        In SHADOW mode a ``"drop"`` verdict is downgraded to ``"pass"`` so no
        candidates are removed during the validation period.  The
        ``stopped_at_stage`` field still reflects where the cascade would
        have stopped in ENFORCE mode.
        """
        effective_verdict: Verdict = (
            "pass" if self._config.mode == PrefilterMode.SHADOW and verdict == "drop" else verdict
        )
        decision = PrefilterDecision(
            candidate_id=candidate.candidate_id,
            pass_kind=pass_kind,
            verdict=effective_verdict,
            stopped_at_stage=stopped_at,
            stage_scores=scores,
            decided_at=datetime.now(UTC),
            config_hash=self._config_hash,
        )
        self._writer.append(decision)
        return decision


def _hash_config(config: PrefilterConfig) -> str:
    """Return a SHA-1 hex digest of the serialised config."""
    blob = config.model_dump_json().encode()
    return hashlib.sha1(blob).hexdigest()  # noqa: S324 — non-crypto use
