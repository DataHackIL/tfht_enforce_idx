"""Unit tests for prefilter.models — frozen dataclasses and serialization."""

from __future__ import annotations

import dataclasses
import json

from denbust.prefilter.models import PrefilterDecision, StageScore


def _make_score(stage: str = "A") -> StageScore:
    return StageScore(
        stage=stage,  # type: ignore[arg-type]
        p_negative=0.97,
        threshold=0.95,
        dropped=True,
        reason="domain_reputation:example.com=0.97",
        model_version="v1",
    )


def _make_decision(
    verdict: str = "drop",
    stopped_at: str = "A",
    scores: tuple[StageScore, ...] = (),
) -> PrefilterDecision:
    return PrefilterDecision(
        candidate_id="cand-abc",
        pass_kind="thin",  # type: ignore[arg-type]
        verdict=verdict,  # type: ignore[arg-type]
        stopped_at_stage=stopped_at,  # type: ignore[arg-type]
        stage_scores=scores,
        decided_at="2026-05-21T08:00:00+00:00",
        config_hash="deadbeef",
    )


class TestStageScore:
    def test_frozen(self) -> None:
        score = _make_score()
        try:
            score.p_negative = 0.5  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            pass
        else:
            raise AssertionError("StageScore should be frozen")

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


class TestPrefilterDecision:
    def test_frozen(self) -> None:
        decision = _make_decision()
        try:
            decision.verdict = "pass"  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            pass
        else:
            raise AssertionError("PrefilterDecision should be frozen")

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

    def test_asdict_json_serializable_empty_scores(self) -> None:
        d = _make_decision()
        record = dataclasses.asdict(d)
        text = json.dumps(record)
        parsed = json.loads(text)
        assert parsed["candidate_id"] == "cand-abc"
        assert parsed["verdict"] == "drop"
        assert parsed["stage_scores"] == []

    def test_asdict_json_serializable_with_scores(self) -> None:
        score = _make_score()
        d = _make_decision(scores=(score,))
        record = dataclasses.asdict(d)
        text = json.dumps(record)
        parsed = json.loads(text)
        assert len(parsed["stage_scores"]) == 1
        assert parsed["stage_scores"][0]["stage"] == "A"

    def test_pass_verdict(self) -> None:
        d = _make_decision(verdict="pass", stopped_at="passed_all")
        assert d.verdict == "pass"
        assert d.stopped_at_stage == "passed_all"
