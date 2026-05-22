"""Unit tests for StageBSetFitScorer inference in prefilter.stage_b.

SetFit loading calls ``SetFitModel.from_pretrained`` which normally
downloads a large model from HuggingFace.  All tests here monkeypatch that
call with a lightweight fake so no network access occurs.

Skipped entirely when ``setfit`` is not installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

setfit = pytest.importorskip("setfit")  # skip whole module if setfit not installed

from denbust.prefilter.models import StageScore
from denbust.prefilter.stage_b import StageBSetFitScorer
from tests.unit.prefilter._helpers import FakeCandidate

# ---------------------------------------------------------------------------
# Fake SetFit model that covers both save_pretrained and predict_proba
# ---------------------------------------------------------------------------


class _FakeSetFitModel:
    """Lightweight SetFit model stub for predict tests."""

    def __init__(self, p_negative: float = 0.2) -> None:
        self._p_negative = p_negative

    def predict_proba(self, texts: list[str]) -> Any:
        import numpy as np

        n = len(texts)
        result = np.zeros((n, 2), dtype=np.float64)
        result[:, 0] = self._p_negative
        result[:, 1] = 1.0 - self._p_negative
        return result

    def save_pretrained(self, path: str) -> None:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        (p / "config_setfit.json").write_text(
            json.dumps({"model_type": "fake_setfit"}), encoding="utf-8"
        )
        (p / "model_head.pkl").write_bytes(b"fake")
        (p / "config.json").write_text(json.dumps({"hidden_size": 4}), encoding="utf-8")


def _fake_from_pretrained(path: str, **_kwargs: Any) -> _FakeSetFitModel:
    """Fake loader: deterministically varies p_negative based on model path."""
    # Thin model gets p_negative=0.2 (positive-leaning);
    # thick model gets p_negative=0.8 (negative-leaning).
    p_neg = 0.2 if "thin" in str(path) else 0.8
    return _FakeSetFitModel(p_negative=p_neg)


# ---------------------------------------------------------------------------
# Fixture: a minimal stage_b_setfit/ artifact directory (module-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def setfit_models_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a minimal stage_b_setfit/ directory with fake model artifacts."""
    base = tmp_path_factory.mktemp("stage_b_setfit")
    models_dir = base / "models"
    stage_dir = models_dir / "stage_b_setfit"

    thin_dir = stage_dir / "thin_model"
    thick_dir = stage_dir / "thick_model"

    fake_thin = _FakeSetFitModel(p_negative=0.2)
    fake_thick = _FakeSetFitModel(p_negative=0.8)
    fake_thin.save_pretrained(str(thin_dir))
    fake_thick.save_pretrained(str(thick_dir))

    meta = {
        "model_kind": "setfit",
        "model_version": "abc123def456",
        "trained_at": "2026-05-22T00:00:00+00:00",
        "n_train": 20,
        "n_val": 6,
        "n_thick_with_body": 20,
        "base_model_id": "intfloat/multilingual-e5-large",
    }
    (stage_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return models_dir


_PATCH_FROM_PRETRAINED = "setfit.SetFitModel.from_pretrained"


# ---------------------------------------------------------------------------
# Stub behaviour — no artifacts
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageBSetFitScorerStub:
    """StageBSetFitScorer returns None when no artifacts are present."""

    def test_none_models_dir_returns_none_thin(self) -> None:
        scorer = StageBSetFitScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_none_models_dir_returns_none_thick(self) -> None:
        scorer = StageBSetFitScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thick", body="some body") is None

    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=tmp_path / "nonexistent")
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_empty_models_dir_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "stage_b_setfit").mkdir()
        scorer = StageBSetFitScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_partial_artifacts_missing_thick_returns_none(self, tmp_path: Path) -> None:
        stage_dir = tmp_path / "stage_b_setfit"
        stage_dir.mkdir()
        (stage_dir / "thin_model").mkdir()
        scorer = StageBSetFitScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_setfit_not_installed_logs_warning_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When setfit import fails, scorer logs a warning and returns None."""
        import builtins

        stage_dir = tmp_path / "stage_b_setfit"
        (stage_dir / "thin_model").mkdir(parents=True)
        (stage_dir / "thick_model").mkdir(parents=True)

        real_import = builtins.__import__

        def _block_setfit(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "setfit":
                raise ImportError("No module named 'setfit'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block_setfit)
        import logging

        with caplog.at_level(logging.WARNING, logger="denbust.prefilter.stage_b"):
            scorer = StageBSetFitScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thin") is None
        assert any("setfit" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Loaded model behaviour
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageBSetFitScorerLoaded:
    """StageBSetFitScorer loaded from fake artifacts produces valid StageScore."""

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thin_returns_stage_score(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert isinstance(result, StageScore)

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thick_with_body_returns_stage_score(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="החשוד נעצר")
        assert isinstance(result, StageScore)

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thick_without_body_falls_back_to_thin(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body=None)
        assert isinstance(result, StageScore)

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_stage_score_has_stage_b(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert result.stage == "B"

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_p_negative_in_unit_interval_thin(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_p_negative_in_unit_interval_thick(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="החשוד נעצר")
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thin_reason_starts_with_setfit_thin(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert result.reason.startswith("setfit/thin=")

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thick_reason_starts_with_setfit_thick(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="החשוד נעצר")
        assert result is not None
        assert result.reason.startswith("setfit/thick=")

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_threshold_propagated(self, setfit_models_dir: Path) -> None:
        custom = 0.7
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir, threshold=custom)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert result.threshold == custom

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_dropped_flag_consistent_with_threshold(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir, threshold=0.95)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert result.dropped == (result.p_negative >= 0.95)

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_model_version_loaded_from_meta(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        assert scorer.model_version == "abc123def456"

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_model_version_propagated_to_stage_score(self, setfit_models_dir: Path) -> None:
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is not None
        assert result.model_version == "abc123def456"

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_deterministic_thin_same_input(self, setfit_models_dir: Path) -> None:
        """Same input to the same scorer must produce the same p_negative."""
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        cand = FakeCandidate(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        r1 = scorer.evaluate(cand, "thin")
        r2 = scorer.evaluate(cand, "thin")
        assert r1 is not None and r2 is not None
        assert r1.p_negative == r2.p_negative

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thin_and_thick_use_different_models(self, setfit_models_dir: Path) -> None:
        """Thin and thick pass with a body must use different model objects."""
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        cand = FakeCandidate()
        r_thin = scorer.evaluate(cand, "thin")
        r_thick = scorer.evaluate(cand, "thick", body="body text here")
        assert r_thin is not None and r_thick is not None
        # fake_from_pretrained returns different p_negative for thin vs thick
        assert r_thin.p_negative != r_thick.p_negative
        assert r_thin.reason.startswith("setfit/thin=")
        assert r_thick.reason.startswith("setfit/thick=")

    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thick_without_body_uses_thin_model(self, setfit_models_dir: Path) -> None:
        """When body is absent the thick pass falls back to the thin model."""
        scorer = StageBSetFitScorer(models_dir=setfit_models_dir)
        cand = FakeCandidate()
        r_thin = scorer.evaluate(cand, "thin")
        r_thick_no_body = scorer.evaluate(cand, "thick", body=None)
        assert r_thin is not None and r_thick_no_body is not None
        # Both should use the thin model (p_negative=0.2), so values should match.
        assert r_thin.p_negative == r_thick_no_body.p_negative
