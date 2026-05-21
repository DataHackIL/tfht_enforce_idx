"""Unit tests for prefilter.models — frozen dataclasses, validation, and protocols."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime

import pytest

from denbust.prefilter.models import (
    PassKind,
    PrefilterDecision,
    StageEvaluator,
    StageName,
    StageScore,
    StoppedAt,
    Verdict,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_score(stage: StageName = "A") -> StageScore:
    return StageScore(
        stage=stage,
        p_negative=0.97,
        threshold=0.95,
        dropped=True,
        reason="domain_reputation:example.com=0.97",
        model_version="v1",
    )


def _make_decision(
    verdict: Verdict = "drop",
    stopped_at: StoppedAt = "A",
    scores: tuple[StageScore, ...] = (),
) -> PrefilterDecision:
    return PrefilterDecision(
        candidate_id="cand-abc",
        pass_kind="thin",
        verdict=verdict,
        stopped_at_stage=stopped_at,
        stage_scores=scores,
        decided_at=datetime(2026, 5, 21, 8, 0, 0, tzinfo=UTC),
        config_hash="deadbeef",
    )


# ---------------------------------------------------------------------------
# StageScore — immutability
# ---------------------------------------------------------------------------


class TestStageScore:
    def test_frozen(self) -> None:
        score = _make_score()
        with pytest.raises(dataclasses.FrozenInstanceError):
            score.p_negative = 0.5  # type: ignore[misc]

    def test_hashable(self) -> None:
        score = _make_score()
        assert hash(score) == hash(_make_score())
        s = {score}
        assert score in s

    def test_asdict_json_serializable(self) -> None:
        score = _make_score()
        d = dataclasses.asdict(score)
        text = json.dumps(d)
        round_tripped = json.loads(text)
        assert round_tripped["stage"] == "A"
        assert round_tripped["p_negative"] == 0.97
        assert round_tripped["dropped"] is True


# ---------------------------------------------------------------------------
# StageScore — bounds validation
# ---------------------------------------------------------------------------


class TestStageScoreValidation:
    def test_p_negative_above_one_raises(self) -> None:
        with pytest.raises(ValueError, match="p_negative"):
            StageScore(
                stage="A",
                p_negative=1.01,
                threshold=0.95,
                dropped=True,
                reason="x",
                model_version="",
            )

    def test_p_negative_below_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="p_negative"):
            StageScore(
                stage="A",
                p_negative=-0.01,
                threshold=0.95,
                dropped=True,
                reason="x",
                model_version="",
            )

    def test_threshold_above_one_raises(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            StageScore(
                stage="A",
                p_negative=0.97,
                threshold=1.01,
                dropped=True,
                reason="x",
                model_version="",
            )

    def test_threshold_below_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            StageScore(
                stage="A",
                p_negative=0.97,
                threshold=-0.01,
                dropped=True,
                reason="x",
                model_version="",
            )

    def test_boundary_values_ok(self) -> None:
        # 0.0 and 1.0 are valid
        s = StageScore(
            stage="B",
            p_negative=0.0,
            threshold=1.0,
            dropped=False,
            reason="",
            model_version="",
        )
        assert s.p_negative == 0.0
        assert s.threshold == 1.0


# ---------------------------------------------------------------------------
# PrefilterDecision — immutability and serialisation
# ---------------------------------------------------------------------------


class TestPrefilterDecision:
    def test_frozen(self) -> None:
        decision = _make_decision()
        with pytest.raises(dataclasses.FrozenInstanceError):
            decision.verdict = "pass"  # type: ignore[misc]

    def test_hashable_no_scores(self) -> None:
        d1 = _make_decision()
        d2 = _make_decision()
        assert hash(d1) == hash(d2)
        s = {d1}
        assert d2 in s

    def test_hashable_with_scores(self) -> None:
        score = _make_score()
        d = _make_decision(scores=(score,))
        assert hash(d) is not None  # does not raise

    def test_decided_at_is_datetime(self) -> None:
        d = _make_decision()
        assert isinstance(d.decided_at, datetime)
        assert d.decided_at.tzinfo is not None

    def test_asdict_requires_explicit_datetime_serialization(self) -> None:
        """dataclasses.asdict includes datetime; callers must convert for JSON."""
        d = _make_decision()
        record = dataclasses.asdict(d)
        # datetime is not JSON-serialisable by default — convert explicitly.
        record["decided_at"] = d.decided_at.isoformat()
        text = json.dumps(record)
        parsed = json.loads(text)
        assert parsed["candidate_id"] == "cand-abc"
        assert parsed["verdict"] == "drop"
        assert parsed["stage_scores"] == []
        assert parsed["decided_at"] == "2026-05-21T08:00:00+00:00"

    def test_asdict_with_scores(self) -> None:
        score = _make_score()
        d = _make_decision(scores=(score,))
        record = dataclasses.asdict(d)
        record["decided_at"] = d.decided_at.isoformat()
        text = json.dumps(record)
        parsed = json.loads(text)
        assert len(parsed["stage_scores"]) == 1
        assert parsed["stage_scores"][0]["stage"] == "A"

    def test_pass_verdict(self) -> None:
        d = _make_decision(verdict="pass", stopped_at="passed_all")
        assert d.verdict == "pass"
        assert d.stopped_at_stage == "passed_all"


# ---------------------------------------------------------------------------
# StageEvaluator Protocol conformance — all four stages must satisfy it
# ---------------------------------------------------------------------------


class TestStageEvaluatorProtocol:
    def test_stage_a_satisfies_protocol(self) -> None:
        from denbust.prefilter.stage_a import StageAScorer

        assert isinstance(StageAScorer(), StageEvaluator)

    def test_stage_b_satisfies_protocol(self) -> None:
        from denbust.prefilter.stage_b import StageBScorer

        assert isinstance(StageBScorer(), StageEvaluator)

    def test_stage_c_satisfies_protocol(self) -> None:
        from denbust.prefilter.stage_c import StageCScorer

        assert isinstance(StageCScorer(), StageEvaluator)

    def test_stage_d_satisfies_protocol(self) -> None:
        from denbust.prefilter.stage_d import StageDJudge

        assert isinstance(StageDJudge(), StageEvaluator)

    def test_object_without_evaluate_does_not_satisfy(self) -> None:
        class _NoEvaluate:
            pass

        assert not isinstance(_NoEvaluate(), StageEvaluator)


# ---------------------------------------------------------------------------
# Type alias sanity
# ---------------------------------------------------------------------------


class TestTypeAliases:
    """Smoke-check that the public type aliases exist and are usable as annotations."""

    def test_verdict_values(self) -> None:
        v: Verdict = "pass"
        assert v == "pass"
        v2: Verdict = "drop"
        assert v2 == "drop"

    def test_pass_kind_values(self) -> None:
        pk: PassKind = "thin"
        assert pk == "thin"
        pk2: PassKind = "thick"
        assert pk2 == "thick"

    def test_stage_name_values(self) -> None:
        for s in ("A", "B", "C", "D"):
            sn: StageName = s  # type: ignore[assignment]
            assert sn == s

    def test_stopped_at_includes_passed_all(self) -> None:
        sa: StoppedAt = "passed_all"
        assert sa == "passed_all"
