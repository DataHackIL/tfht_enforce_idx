"""Unit tests for StageDScorer inference in prefilter.stage_d.

All tests monkeypatch ``mlx_lm.load`` with a lightweight fake so no network
access occurs and the test suite stays fast.  ``_mlx_score`` is also patched
so no actual MLX computation runs.

Skipped entirely when ``mlx_lm`` is not installed.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

_ = pytest.importorskip("mlx_lm")

from denbust.prefilter.models import StageScore
from denbust.prefilter.stage_d import (
    _DEFAULT_BASE_MODEL_D,
    _DEFAULT_CB_THRESHOLD,
    _DEFAULT_TIMEOUT_SECONDS,
    StageDScorer,
    bake_stage_d,
)
from tests.unit.prefilter._helpers import FakeCandidate, FakeMLXTokenizer

_PATCH_MLX_LOAD = "mlx_lm.load"
_PATCH_MLX_SCORE = "denbust.prefilter.stage_d._mlx_score"

# A fixed p_negative returned by the patched _mlx_score.
_FIXED_P_NEG = 0.25


def _fake_mlx_load(*_args: Any, **_kwargs: Any) -> tuple[MagicMock, FakeMLXTokenizer]:
    """Return a fake (model, tokenizer) pair without loading any real model."""
    return MagicMock(), FakeMLXTokenizer()


def _fake_mlx_score(*_args: Any, **_kwargs: Any) -> float:
    return _FIXED_P_NEG


# ---------------------------------------------------------------------------
# Module-scoped fixture: baked artifacts (no model download)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stage_d_models_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Bake Stage D artifacts once for the whole test module."""
    base = tmp_path_factory.mktemp("stage_d_models")
    bake_stage_d(base)
    return base


# ---------------------------------------------------------------------------
# Stub behaviour — no artifacts
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageDScorerStub:
    """StageDScorer returns None when artifacts are absent."""

    def test_none_models_dir_returns_none_thick(self) -> None:
        scorer = StageDScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None

    def test_none_models_dir_returns_none_thin(self) -> None:
        scorer = StageDScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        scorer = StageDScorer(models_dir=tmp_path / "nonexistent")
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None

    def test_empty_models_dir_returns_none(self, tmp_path: Path) -> None:
        scorer = StageDScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None

    def test_thin_pass_always_returns_none_when_loaded(self, stage_d_models_dir: Path) -> None:
        """Thin pass must return None even when artifacts are loaded."""
        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            scorer = StageDScorer(models_dir=stage_d_models_dir)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_mlx_lm_not_installed_returns_none(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When mlx_lm is missing the scorer logs a warning."""
        import builtins

        bake_stage_d(tmp_path)

        real_import = builtins.__import__

        def _block(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("mlx_lm", "mlx"):
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block)
        with caplog.at_level(logging.WARNING, logger="denbust.prefilter.stage_d"):
            scorer = StageDScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None
        assert any("prefilter" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Loaded model behaviour
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageDScorerLoaded:
    """StageDScorer loaded from real artifacts (fake mlx) returns valid scores."""

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_thick_returns_stage_score(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="גוף הכתבה")
        assert isinstance(result, StageScore)

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_thick_without_body_falls_back_to_snippet(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body=None)
        assert isinstance(result, StageScore)

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_stage_score_has_stage_d(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.stage == "D"

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_p_negative_in_unit_interval(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_p_negative_matches_mock(self, stage_d_models_dir: Path) -> None:
        """p_negative should equal the value returned by the patched _mlx_score."""
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.p_negative == pytest.approx(_FIXED_P_NEG)

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_reason_format(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.reason.startswith("slm/thick=")

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_threshold_propagated(self, stage_d_models_dir: Path) -> None:
        custom = 0.7
        scorer = StageDScorer(models_dir=stage_d_models_dir, threshold=custom)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.threshold == custom

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_dropped_flag_consistent_with_threshold(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir, threshold=0.5)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.dropped == (result.p_negative >= 0.5)

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_model_version_loaded_from_meta(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        assert len(scorer.model_version) == 12
        assert all(c in "0123456789abcdef" for c in scorer.model_version)

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_model_version_propagated_to_stage_score(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.model_version == scorer.model_version

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_base_model_id_loaded_from_meta(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        assert scorer.base_model_id == _DEFAULT_BASE_MODEL_D

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_timeout_loaded_from_meta(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        assert scorer._timeout_seconds == _DEFAULT_TIMEOUT_SECONDS

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_timeout_override_respected(self, stage_d_models_dir: Path) -> None:
        """A caller-supplied timeout_seconds overrides the value from meta.json."""
        scorer = StageDScorer(models_dir=stage_d_models_dir, timeout_seconds=5.0)
        assert scorer._timeout_seconds == 5.0

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_cb_threshold_loaded_from_meta(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        assert scorer._cb_threshold == _DEFAULT_CB_THRESHOLD

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_cb_threshold_override_respected(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir, circuit_breaker_threshold=1)
        assert scorer._cb_threshold == 1


# ---------------------------------------------------------------------------
# Timeout and circuit breaker
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageDCircuitBreaker:
    """Timeout and circuit-breaker behaviour."""

    def _make_scorer(self, stage_d_models_dir: Path, cb_threshold: int = 3) -> StageDScorer:
        """Return a loaded scorer with the patched mlx_lm loader."""
        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            return StageDScorer(
                models_dir=stage_d_models_dir,
                timeout_seconds=0.001,  # nearly zero — will always time out
                circuit_breaker_threshold=cb_threshold,
            )

    def test_single_timeout_returns_none(self, stage_d_models_dir: Path) -> None:
        """A single timeout must return None but NOT open the circuit."""
        scorer = self._make_scorer(stage_d_models_dir, cb_threshold=3)
        # _mlx_score is NOT patched — the real function will block until the
        # thread pool timeout fires.  We patch it to sleep so the timeout fires.
        import time

        def _slow_score(*_args: Any, **_kwargs: Any) -> float:
            time.sleep(10)
            return 0.5

        with patch(_PATCH_MLX_SCORE, _slow_score):
            result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is None
        assert scorer._consecutive_timeouts == 1
        assert not scorer._circuit_open

    def test_threshold_timeouts_open_circuit(self, stage_d_models_dir: Path) -> None:
        """After cb_threshold consecutive timeouts the circuit breaker opens."""
        scorer = self._make_scorer(stage_d_models_dir, cb_threshold=2)

        import time

        def _slow_score(*_args: Any, **_kwargs: Any) -> float:
            time.sleep(10)
            return 0.5

        with patch(_PATCH_MLX_SCORE, _slow_score):
            scorer.evaluate(FakeCandidate(), "thick", body="body")
            scorer.evaluate(FakeCandidate(), "thick", body="body")

        assert scorer._circuit_open

    def test_open_circuit_skips_inference(self, stage_d_models_dir: Path) -> None:
        """Once the circuit is open, evaluate returns None immediately."""
        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            scorer = StageDScorer(models_dir=stage_d_models_dir)
        scorer._circuit_open = True

        # If _mlx_score were called it would return _FIXED_P_NEG and the result
        # would be a StageScore.  The fact that we get None confirms it was skipped.
        with patch(_PATCH_MLX_SCORE, _fake_mlx_score):
            result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is None

    def test_successful_inference_resets_consecutive_count(self, stage_d_models_dir: Path) -> None:
        """A successful inference resets the consecutive-timeout counter."""
        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            scorer = StageDScorer(models_dir=stage_d_models_dir, circuit_breaker_threshold=5)
        scorer._consecutive_timeouts = 3  # simulate prior timeouts

        with patch(_PATCH_MLX_SCORE, _fake_mlx_score):
            result = scorer.evaluate(FakeCandidate(), "thick", body="body")

        assert result is not None
        assert scorer._consecutive_timeouts == 0


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageDDeterminism:
    """Same input to the same scorer must produce the same p_negative."""

    @patch(_PATCH_MLX_SCORE, _fake_mlx_score)
    @patch(_PATCH_MLX_LOAD, _fake_mlx_load)
    def test_deterministic_same_input(self, stage_d_models_dir: Path) -> None:
        scorer = StageDScorer(models_dir=stage_d_models_dir)
        cand = FakeCandidate(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        r1 = scorer.evaluate(cand, "thick", body="נעצר")
        r2 = scorer.evaluate(cand, "thick", body="נעצר")
        assert r1 is not None and r2 is not None
        assert r1.p_negative == r2.p_negative


# ---------------------------------------------------------------------------
# Meta overrides (no model dir — just meta.json checks)
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageDMetaOverride:
    """meta.json values are overridden when caller supplies explicit params."""

    def test_meta_without_meta_json_uses_defaults(self, tmp_path: Path) -> None:
        """Scorer loaded from a dir with only prompt.txt uses hardcoded defaults."""
        stage_dir = tmp_path / "stage_d"
        stage_dir.mkdir()
        (stage_dir / "prompt.txt").write_text("[הוראה] {title} {body}", encoding="utf-8")

        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            scorer = StageDScorer(models_dir=tmp_path)

        assert scorer.base_model_id == _DEFAULT_BASE_MODEL_D
        assert scorer._timeout_seconds == _DEFAULT_TIMEOUT_SECONDS
        assert scorer._cb_threshold == _DEFAULT_CB_THRESHOLD

    def test_custom_meta_json_values_loaded(self, tmp_path: Path) -> None:
        """A custom meta.json written by bake_stage_d is fully loaded."""
        bake_stage_d(
            tmp_path,
            base_model_id="Qwen/Qwen2.5-7B-Instruct",
            timeout_seconds=10.0,
            circuit_breaker_threshold=1,
        )

        with patch(_PATCH_MLX_LOAD, _fake_mlx_load):
            scorer = StageDScorer(models_dir=tmp_path)

        assert scorer.base_model_id == "Qwen/Qwen2.5-7B-Instruct"
        assert scorer._timeout_seconds == 10.0
        assert scorer._cb_threshold == 1
