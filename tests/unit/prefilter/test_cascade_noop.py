"""Unit tests for CascadeOrchestrator — no-op stub behaviour and mode logic."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.models import (
    CandidateView,
    PassKind,
    PrefilterDecision,
    StageScore,
)

# ---------------------------------------------------------------------------
# Minimal CandidateView fixture
# ---------------------------------------------------------------------------


class _FakeCandidate:
    """Minimal object satisfying the CandidateView protocol for testing."""

    def __init__(
        self,
        candidate_id: str = "cand-test-1",
        domain: str | None = "example.co.il",
        title: str | None = "ידיעה לדוגמה",
        snippet: str | None = "קטע לדוגמה",
        url: str | None = "https://example.co.il/article/1",
    ) -> None:
        self._candidate_id = candidate_id
        self._domain = domain
        self._title = title
        self._snippet = snippet
        self._url = url

    @property
    def candidate_id(self) -> str:
        return self._candidate_id

    @property
    def domain(self) -> str | None:
        return self._domain

    @property
    def title(self) -> str | None:
        return self._title

    @property
    def snippet(self) -> str | None:
        return self._snippet

    @property
    def url(self) -> str | None:
        return self._url


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_orchestrator(tmp_path: Path, **config_overrides: Any) -> CascadeOrchestrator:
    cfg = (
        PrefilterConfig.model_validate(config_overrides) if config_overrides else PrefilterConfig()
    )
    return CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")


def _shadow_orchestrator(tmp_path: Path) -> CascadeOrchestrator:
    """Return an orchestrator in SHADOW mode (stages active, telemetry written)."""
    cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW)
    return CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_fake_candidate_satisfies_protocol() -> None:
    candidate = _FakeCandidate()
    assert isinstance(candidate, CandidateView)


# ---------------------------------------------------------------------------
# evaluate_thin — basic contract (mode=OFF / disabled → noop pass)
# ---------------------------------------------------------------------------


class TestEvaluateThin:
    def test_returns_prefilter_decision(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert isinstance(decision, PrefilterDecision)

    def test_verdict_is_always_pass(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.verdict == "pass"

    def test_stopped_at_passed_all(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.stopped_at_stage == "passed_all"

    def test_stage_scores_empty(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.stage_scores == ()

    def test_pass_kind_is_thin(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.pass_kind == "thin"

    def test_candidate_id_propagated(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate(candidate_id="my-cand"))
        assert decision.candidate_id == "my-cand"

    def test_config_hash_nonempty(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert len(decision.config_hash) > 0

    def test_decided_at_is_utc_datetime(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        assert isinstance(decision.decided_at, datetime)
        assert decision.decided_at.tzinfo is not None

    def test_off_mode_writes_no_telemetry(self, tmp_path: Path) -> None:
        """mode=OFF (default) must not write any JSONL files."""
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate())
        assert not decisions_dir.exists()

    def test_shadow_mode_writes_one_decision(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(
            config=PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW),
            decisions_dir=decisions_dir,
        )
        orch.evaluate_thin(_FakeCandidate())
        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["verdict"] == "pass"
        assert record["pass_kind"] == "thin"


# ---------------------------------------------------------------------------
# evaluate_thick — basic contract
# ---------------------------------------------------------------------------


class TestEvaluateThick:
    def test_verdict_is_always_pass(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thick(_FakeCandidate(), body="גוף המאמר")
        assert decision.verdict == "pass"

    def test_pass_kind_is_thick(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thick(_FakeCandidate(), body="some body text")
        assert decision.pass_kind == "thick"

    def test_stage_scores_empty(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thick(_FakeCandidate(), body="")
        assert decision.stage_scores == ()

    def test_off_mode_writes_no_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
        orch.evaluate_thick(_FakeCandidate(), body="article text")
        assert not decisions_dir.exists()

    def test_shadow_mode_writes_one_decision(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(
            config=PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW),
            decisions_dir=decisions_dir,
        )
        orch.evaluate_thick(_FakeCandidate(), body="article text")
        files = list(decisions_dir.glob("*.jsonl"))
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["pass_kind"] == "thick"


# ---------------------------------------------------------------------------
# Mode behaviour
# ---------------------------------------------------------------------------


class TestModes:
    def test_off_mode_passes_without_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
        d = orch.evaluate_thin(_FakeCandidate())
        assert d.verdict == "pass"
        assert not decisions_dir.exists()

    def test_disabled_flag_suppresses_telemetry_even_in_shadow(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        cfg = PrefilterConfig(enabled=False, mode=PrefilterMode.SHADOW)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate())
        assert not decisions_dir.exists()

    def test_shadow_mode_writes_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate())
        assert decisions_dir.exists()
        assert len(list(decisions_dir.glob("*.jsonl"))) == 1

    def test_enforce_mode_writes_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.ENFORCE)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate())
        assert len(list(decisions_dir.glob("*.jsonl"))) == 1

    def test_shadow_mode_downgrades_drop_to_pass(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When a stage would drop in ENFORCE, SHADOW must record pass instead."""
        import denbust.prefilter.stage_a as _stage_a_mod

        def _always_drop(
            _self: Any,
            _candidate: CandidateView,
            _pass_kind: PassKind,
            _body: str | None = None,
        ) -> StageScore:
            return StageScore(
                stage="A",
                p_negative=0.99,
                threshold=0.95,
                dropped=True,
                reason="test-inject",
                model_version="test",
            )

        monkeypatch.setattr(_stage_a_mod.StageAScorer, "evaluate", _always_drop)

        cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.verdict == "pass"
        # stopped_at_stage still records the would-be drop for recall analysis
        assert decision.stopped_at_stage == "A"

    def test_enforce_mode_respects_drop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In ENFORCE mode a stage drop must propagate as verdict='drop'."""
        import denbust.prefilter.stage_a as _stage_a_mod

        def _always_drop(
            _self: Any,
            _candidate: CandidateView,
            _pass_kind: PassKind,
            _body: str | None = None,
        ) -> StageScore:
            return StageScore(
                stage="A",
                p_negative=0.99,
                threshold=0.95,
                dropped=True,
                reason="test-inject",
                model_version="test",
            )

        monkeypatch.setattr(_stage_a_mod.StageAScorer, "evaluate", _always_drop)

        cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.ENFORCE)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")
        decision = orch.evaluate_thin(_FakeCandidate())
        assert decision.verdict == "drop"
        assert decision.stopped_at_stage == "A"


# ---------------------------------------------------------------------------
# Multiple calls
# ---------------------------------------------------------------------------


class TestMultipleCalls:
    def test_each_call_writes_one_decision(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        cfg = PrefilterConfig(enabled=True, mode=PrefilterMode.SHADOW)
        orch = CascadeOrchestrator(config=cfg, decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate("c1"))
        orch.evaluate_thin(_FakeCandidate("c2"))
        orch.evaluate_thick(_FakeCandidate("c3"), body="text")
        all_lines = []
        for f in decisions_dir.glob("*.jsonl"):
            all_lines.extend(f.read_text(encoding="utf-8").splitlines())
        assert len(all_lines) == 3

    def test_config_hash_stable_across_calls(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        d1 = orch.evaluate_thin(_FakeCandidate("c1"))
        d2 = orch.evaluate_thin(_FakeCandidate("c2"))
        assert d1.config_hash == d2.config_hash


# ---------------------------------------------------------------------------
# Stages disabled
# ---------------------------------------------------------------------------


class TestStagesDisabled:
    def test_all_stages_disabled_still_passes(self, tmp_path: Path) -> None:
        """With cascade active but all individual stages disabled, verdict is pass."""
        cfg = PrefilterConfig.model_validate(
            {
                "enabled": True,
                "mode": "shadow",
                "stages": {
                    "a": {"enabled": False},
                    "b": {"enabled": False},
                    "c": {"enabled": False},
                    "d": {"enabled": False},
                },
            }
        )
        orch = CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")
        thin = orch.evaluate_thin(_FakeCandidate())
        thick = orch.evaluate_thick(_FakeCandidate(), body="text")
        assert thin.verdict == "pass"
        assert thick.verdict == "pass"

    def test_disabled_stages_are_not_instantiated(self, tmp_path: Path) -> None:
        """Stage objects must be None when their config flag is False."""
        cfg = PrefilterConfig.model_validate(
            {
                "enabled": True,
                "mode": "shadow",
                "stages": {
                    "a": {"enabled": False},
                    "b": {"enabled": True},
                    "c": {"enabled": False},
                    "d": {"enabled": False},
                },
            }
        )
        orch = CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")
        assert orch._stage_a is None
        assert orch._stage_b is not None
        assert orch._stage_c is None
        assert orch._stage_d is None
