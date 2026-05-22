"""Integration tests for Stage B within the CascadeOrchestrator.

Verifies that:
- When trained artifacts are present, Stage B produces a StageScore in the
  decision's ``stage_scores`` tuple for both thin and thick passes.
- When no artifacts are present (models_dir=None), Stage B is silently skipped
  and the cascade continues without a B score.
- SHADOW mode correctly downgrades a B-driven drop to a pass verdict.
- ENFORCE mode correctly propagates a B-driven drop as a drop verdict.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig
from denbust.prefilter.models import CandidateView, PassKind, StageScore
from tests.unit.prefilter._helpers import FakeCandidate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _shadow_orchestrator(
    decisions_dir: Path,
    models_dir: Path | None = None,
    *,
    disable_a: bool = True,
    disable_c: bool = True,
    disable_d: bool = True,
) -> CascadeOrchestrator:
    """Build a SHADOW-mode orchestrator with only Stage B enabled by default."""
    cfg = PrefilterConfig.model_validate(
        {
            "enabled": True,
            "mode": "shadow",
            "stages": {
                "a": {"enabled": not disable_a},
                "b": {"enabled": True},
                "c": {"enabled": not disable_c},
                "d": {"enabled": not disable_d},
            },
        }
    )
    return CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir, models_dir=models_dir)


def _enforce_orchestrator(
    decisions_dir: Path,
    models_dir: Path | None = None,
    *,
    disable_a: bool = True,
    disable_c: bool = True,
    disable_d: bool = True,
) -> CascadeOrchestrator:
    """Build an ENFORCE-mode orchestrator with only Stage B enabled by default."""
    cfg = PrefilterConfig.model_validate(
        {
            "enabled": True,
            "mode": "enforce",
            "stages": {
                "a": {"enabled": not disable_a},
                "b": {"enabled": True},
                "c": {"enabled": not disable_c},
                "d": {"enabled": not disable_d},
            },
        }
    )
    return CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir, models_dir=models_dir)


# ---------------------------------------------------------------------------
# Stub behavior — no trained artifacts
# ---------------------------------------------------------------------------


class TestStageBStubInCascade:
    """With no trained artifacts, Stage B is silently skipped."""

    def test_thin_pass_no_b_score_when_no_artifacts(self, tmp_path: Path) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=None)
        decision = orch.evaluate_thin(FakeCandidate())
        b_scores = [s for s in decision.stage_scores if s.stage == "B"]
        assert b_scores == []

    def test_thick_pass_no_b_score_when_no_artifacts(self, tmp_path: Path) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=None)
        decision = orch.evaluate_thick(FakeCandidate(), body="some article body")
        b_scores = [s for s in decision.stage_scores if s.stage == "B"]
        assert b_scores == []

    def test_cascade_still_passes_without_b_artifacts(self, tmp_path: Path) -> None:
        orch = _enforce_orchestrator(tmp_path / "decisions", models_dir=None)
        decision = orch.evaluate_thin(FakeCandidate())
        assert decision.verdict == "pass"
        assert decision.stopped_at_stage == "passed_all"


# ---------------------------------------------------------------------------
# Loaded model — Stage B score appears in stage_scores
# ---------------------------------------------------------------------------


class TestStageBScoreInCascade:
    """With trained artifacts, Stage B emits a StageScore in the decision."""

    def test_thin_pass_emits_b_score(self, trained_stage_b_dir: Path, tmp_path: Path) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thin(FakeCandidate())
        b_scores = [s for s in decision.stage_scores if s.stage == "B"]
        assert len(b_scores) == 1

    def test_thick_pass_emits_b_score(self, trained_stage_b_dir: Path, tmp_path: Path) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thick(
            FakeCandidate(), body="החשוד נעצר לאחר חקירה ממושכת של המשטרה"
        )
        b_scores = [s for s in decision.stage_scores if s.stage == "B"]
        assert len(b_scores) == 1

    def test_b_score_p_negative_in_unit_interval(
        self, trained_stage_b_dir: Path, tmp_path: Path
    ) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thin(FakeCandidate())
        b_score = next(s for s in decision.stage_scores if s.stage == "B")
        assert 0.0 <= b_score.p_negative <= 1.0

    def test_b_score_model_version_nonempty(
        self, trained_stage_b_dir: Path, tmp_path: Path
    ) -> None:
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thin(FakeCandidate())
        b_score = next(s for s in decision.stage_scores if s.stage == "B")
        assert len(b_score.model_version) > 0


# ---------------------------------------------------------------------------
# Mode semantics — SHADOW vs ENFORCE
# ---------------------------------------------------------------------------


class TestStageBModeSemanticsInCascade:
    """SHADOW downgrades B drops; ENFORCE respects them."""

    def test_shadow_downgrades_b_drop_to_pass(
        self,
        trained_stage_b_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When Stage B would drop in ENFORCE, SHADOW must record pass instead."""
        import denbust.prefilter.stage_b as _stage_b_mod

        def _always_drop(
            _self: Any,
            _candidate: CandidateView,
            _pass_kind: PassKind,
            _body: str | None = None,
        ) -> StageScore:
            return StageScore(
                stage="B",
                p_negative=0.99,
                threshold=0.95,
                dropped=True,
                reason="test-inject",
                model_version="test",
            )

        monkeypatch.setattr(_stage_b_mod.StageBScorer, "evaluate", _always_drop)

        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thin(FakeCandidate())
        assert decision.verdict == "pass"
        # stopped_at_stage records where the cascade would have stopped.
        assert decision.stopped_at_stage == "B"

    def test_enforce_respects_b_drop(
        self,
        trained_stage_b_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In ENFORCE mode a B-driven drop must propagate as verdict='drop'."""
        import denbust.prefilter.stage_b as _stage_b_mod

        def _always_drop(
            _self: Any,
            _candidate: CandidateView,
            _pass_kind: PassKind,
            _body: str | None = None,
        ) -> StageScore:
            return StageScore(
                stage="B",
                p_negative=0.99,
                threshold=0.95,
                dropped=True,
                reason="test-inject",
                model_version="test",
            )

        monkeypatch.setattr(_stage_b_mod.StageBScorer, "evaluate", _always_drop)

        orch = _enforce_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        decision = orch.evaluate_thin(FakeCandidate())
        assert decision.verdict == "drop"
        assert decision.stopped_at_stage == "B"

    def test_telemetry_written_in_shadow_after_b_evaluates(
        self, trained_stage_b_dir: Path, tmp_path: Path
    ) -> None:
        """Stage B running must cause a telemetry record to be written."""
        decisions_dir = tmp_path / "decisions"
        orch = _shadow_orchestrator(decisions_dir, models_dir=trained_stage_b_dir)
        orch.evaluate_thin(FakeCandidate())
        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1

    def test_thick_pass_uses_body_for_b(self, trained_stage_b_dir: Path, tmp_path: Path) -> None:
        """evaluate_thick must forward body to Stage B so the thick model runs."""
        orch = _shadow_orchestrator(tmp_path / "decisions", models_dir=trained_stage_b_dir)
        # Positive title+snippet, but strongly negative body → thick model score should
        # be higher (more negative) than thin model score.
        cand = FakeCandidate(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        body = "משחקי כדורגל וטניס וכדורסל הם ספורט פנאי"

        decision_thin = orch.evaluate_thin(cand)
        decision_thick = orch.evaluate_thick(cand, body=body)

        b_thin = next(s for s in decision_thin.stage_scores if s.stage == "B")
        b_thick = next(s for s in decision_thick.stage_scores if s.stage == "B")

        # The thick model uses the body (strongly negative sports text) while the
        # thin model uses the title+snippet (positive crime text).  They should differ.
        assert b_thin.p_negative != b_thick.p_negative
