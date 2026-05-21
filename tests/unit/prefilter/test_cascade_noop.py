"""Unit tests for the no-op CascadeOrchestrator (LPF-PR-01 stub behaviour)."""

from __future__ import annotations

import json
from pathlib import Path

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig
from denbust.prefilter.models import CandidateView, PrefilterDecision

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


def _make_orchestrator(tmp_path: Path, **config_overrides: object) -> CascadeOrchestrator:
    cfg = (
        PrefilterConfig.model_validate(config_overrides) if config_overrides else PrefilterConfig()
    )
    return CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_fake_candidate_satisfies_protocol() -> None:
    candidate = _FakeCandidate()
    assert isinstance(candidate, CandidateView)


# ---------------------------------------------------------------------------
# evaluate_thin — always pass in stub mode
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

    def test_decided_at_is_iso8601(self, tmp_path: Path) -> None:
        orch = _make_orchestrator(tmp_path)
        decision = orch.evaluate_thin(_FakeCandidate())
        # Should be parseable as ISO-8601
        from datetime import datetime

        datetime.fromisoformat(decision.decided_at)

    def test_writes_one_decision_to_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
        orch.evaluate_thin(_FakeCandidate())
        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["verdict"] == "pass"
        assert record["pass_kind"] == "thin"


# ---------------------------------------------------------------------------
# evaluate_thick — always pass in stub mode
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

    def test_writes_one_decision_to_telemetry(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
        orch.evaluate_thick(_FakeCandidate(), body="article text")
        files = list(decisions_dir.glob("*.jsonl"))
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["pass_kind"] == "thick"


# ---------------------------------------------------------------------------
# Multiple calls
# ---------------------------------------------------------------------------


class TestMultipleCalls:
    def test_each_call_writes_one_decision(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        orch = CascadeOrchestrator(config=PrefilterConfig(), decisions_dir=decisions_dir)
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
        cfg = PrefilterConfig.model_validate(
            {
                "stages": {
                    "a": {"enabled": False},
                    "b": {"enabled": False},
                    "c": {"enabled": False},
                    "d": {"enabled": False},
                }
            }
        )
        orch = CascadeOrchestrator(config=cfg, decisions_dir=tmp_path / "decisions")
        thin = orch.evaluate_thin(_FakeCandidate())
        thick = orch.evaluate_thick(_FakeCandidate(), body="text")
        assert thin.verdict == "pass"
        assert thick.verdict == "pass"
