"""Cascade orchestrator — wires all stages and emits PrefilterDecision records."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from denbust.prefilter.config import PrefilterConfig
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
from denbust.prefilter.stage_d import StageDJudge
from denbust.prefilter.telemetry import PrefilterDecisionWriter


class CascadeOrchestrator:
    """Evaluates candidates through the active cascade stages.

    In LPF-PR-01 all stages are stubs that return ``None``, so every call
    produces ``verdict="pass"`` with an empty ``stage_scores`` tuple.  The
    full stage implementations are added in LPF-PR-03 through LPF-PR-07.

    Parameters
    ----------
    config:
        Pre-filter configuration for this dataset.
    decisions_dir:
        Directory into which ``PrefilterDecision`` records are appended as
        JSONL.  Created on first write.
    """

    def __init__(
        self,
        config: PrefilterConfig,
        decisions_dir: Path,
    ) -> None:
        self._config = config
        self._config_hash = _hash_config(config)
        self._writer = PrefilterDecisionWriter(decisions_dir)
        self._stage_a = StageAScorer()
        self._stage_b = StageBScorer()
        self._stage_c = StageCScorer()
        self._stage_d = StageDJudge()

    # ------------------------------------------------------------------
    # Public evaluation interface
    # ------------------------------------------------------------------

    def evaluate_thin(self, candidate: CandidateView) -> PrefilterDecision:
        """Run the thin (pre-scrape) pass: Stages A and B only.

        Always returns ``verdict="pass"`` until LPF-PR-03/04 replace the
        stub scorers.  Telemetry is always written regardless of mode.

        Parameters
        ----------
        candidate:
            A ``CandidateView``-compatible object (title, snippet, domain, url).
        """
        scores: list[StageScore] = []

        # Stage A
        if self._config.stages.a.enabled:
            score_a = self._stage_a.evaluate(candidate)
            if score_a is not None:
                scores.append(score_a)
                if score_a.dropped:
                    return self._record("thin", "drop", "A", candidate, tuple(scores))

        # Stage B — thin uses title + snippet (no body)
        if self._config.stages.b.enabled:
            score_b = self._stage_b.evaluate(candidate, "thin")
            if score_b is not None:
                scores.append(score_b)
                if score_b.dropped:
                    return self._record("thin", "drop", "B", candidate, tuple(scores))

        return self._record("thin", "pass", "passed_all", candidate, tuple(scores))

    def evaluate_thick(self, candidate: CandidateView, body: str) -> PrefilterDecision:
        """Run the thick (post-scrape) pass: Stages A through D.

        Always returns ``verdict="pass"`` until the stub scorers are replaced.
        Telemetry is always written regardless of mode.

        Parameters
        ----------
        candidate:
            A ``CandidateView``-compatible object.
        body:
            Full article body text (truncation handled per stage).
        """
        scores: list[StageScore] = []

        # Stage A — domain recheck on canonical URL
        if self._config.stages.a.enabled:
            score_a = self._stage_a.evaluate(candidate)
            if score_a is not None:
                scores.append(score_a)
                if score_a.dropped:
                    return self._record("thick", "drop", "A", candidate, tuple(scores))

        # Stage B — thick uses full article body
        if self._config.stages.b.enabled:
            score_b = self._stage_b.evaluate(candidate, "thick", body)
            if score_b is not None:
                scores.append(score_b)
                if score_b.dropped:
                    return self._record("thick", "drop", "B", candidate, tuple(scores))

        # Stage C — thick pass only by default
        if self._config.stages.c.enabled:
            score_c = self._stage_c.evaluate(candidate, "thick", body)
            if score_c is not None:
                scores.append(score_c)
                if score_c.dropped:
                    return self._record("thick", "drop", "C", candidate, tuple(scores))

        # Stage D — thick pass only, never runs in thin
        if self._config.stages.d.enabled:
            score_d = self._stage_d.evaluate(candidate, body)
            if score_d is not None:
                scores.append(score_d)
                if score_d.dropped:
                    return self._record("thick", "drop", "D", candidate, tuple(scores))

        return self._record("thick", "pass", "passed_all", candidate, tuple(scores))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record(
        self,
        pass_kind: PassKind,
        verdict: Verdict,
        stopped_at: StoppedAt,
        candidate: CandidateView,
        scores: tuple[StageScore, ...],
    ) -> PrefilterDecision:
        decision = PrefilterDecision(
            candidate_id=candidate.candidate_id,
            pass_kind=pass_kind,
            verdict=verdict,
            stopped_at_stage=stopped_at,
            stage_scores=scores,
            decided_at=datetime.now(UTC).isoformat(),
            config_hash=self._config_hash,
        )
        self._writer.append(decision)
        return decision


def _hash_config(config: PrefilterConfig) -> str:
    """Return a short SHA-1 hex digest of the serialised config."""
    blob = config.model_dump_json().encode()
    return hashlib.sha1(blob).hexdigest()  # noqa: S324 — non-crypto use


# Narrow Literal aliases re-exported so stage modules can annotate
# ``pass_kind`` parameters without importing from models directly.
ThinOrThick = Literal["thin", "thick"]
