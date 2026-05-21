"""Unit tests for StageBScorer inference (predict path) in prefilter.stage_b."""

from __future__ import annotations

from pathlib import Path

from denbust.prefilter.models import StageScore
from denbust.prefilter.stage_b import StageBScorer

# ---------------------------------------------------------------------------
# Minimal CandidateView for testing
# ---------------------------------------------------------------------------


class _FakeCand:
    """Minimal object satisfying the CandidateView protocol."""

    def __init__(
        self,
        candidate_id: str = "cand-test",
        domain: str | None = "example.co.il",
        title: str | None = "עצור חשוד ברצח",
        snippet: str | None = "המשטרה עצרה חשוד",
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
# Stub behaviour — no artifacts
# ---------------------------------------------------------------------------


class TestStageBScorerStub:
    """StageBScorer returns None when no trained artifacts are present."""

    def test_none_models_dir_returns_none_thin(self) -> None:
        scorer = StageBScorer(models_dir=None)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is None

    def test_none_models_dir_returns_none_thick(self) -> None:
        scorer = StageBScorer(models_dir=None)
        result = scorer.evaluate(_FakeCand(), "thick", body="some body text")
        assert result is None

    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        scorer = StageBScorer(models_dir=tmp_path / "nonexistent")
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is None

    def test_empty_models_dir_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "stage_b").mkdir(parents=True)
        scorer = StageBScorer(models_dir=tmp_path)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is None

    def test_partial_artifacts_returns_none(self, tmp_path: Path) -> None:
        """Only thin_model present (no thick_model) → stub behaviour."""
        stage_dir = tmp_path / "stage_b"
        stage_dir.mkdir(parents=True)
        (stage_dir / "thin_model.joblib").write_bytes(b"dummy")
        scorer = StageBScorer(models_dir=tmp_path)
        assert scorer.evaluate(_FakeCand(), "thin") is None


# ---------------------------------------------------------------------------
# Loaded model behaviour — uses trained_stage_b_dir fixture from conftest
# ---------------------------------------------------------------------------


class TestStageBScorerLoaded:
    """StageBScorer loaded from trained artifacts produces valid StageScore."""

    def test_thin_returns_stage_score(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert isinstance(result, StageScore)

    def test_thick_with_body_returns_stage_score(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(
            _FakeCand(), "thick", body="החשוד נעצר לאחר חקירה ממושכת של המשטרה"
        )
        assert isinstance(result, StageScore)

    def test_thick_without_body_falls_back_to_thin(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thick", body=None)
        assert isinstance(result, StageScore)

    def test_thick_empty_body_falls_back_to_thin(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thick", body="   ")
        assert isinstance(result, StageScore)

    def test_stage_score_has_stage_b(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert result.stage == "B"

    def test_p_negative_in_unit_interval_thin(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    def test_p_negative_in_unit_interval_thick(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(
            _FakeCand(), "thick", body="החשוד נעצר לאחר חקירה ממושכת של המשטרה"
        )
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    def test_threshold_propagated(self, trained_stage_b_dir: Path) -> None:
        custom_threshold = 0.7
        scorer = StageBScorer(models_dir=trained_stage_b_dir, threshold=custom_threshold)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert result.threshold == custom_threshold

    def test_dropped_flag_consistent_with_threshold(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir, threshold=0.95)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert result.dropped == (result.p_negative >= 0.95)

    def test_model_version_nonempty(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert len(result.model_version) > 0

    def test_model_version_is_hex(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result is not None
        assert all(c in "0123456789abcdef" for c in result.model_version)

    def test_deterministic_thin_same_input(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        cand = _FakeCand(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        r1 = scorer.evaluate(cand, "thin")
        r2 = scorer.evaluate(cand, "thin")
        assert r1 is not None and r2 is not None
        assert r1.p_negative == r2.p_negative

    def test_deterministic_thick_same_input(self, trained_stage_b_dir: Path) -> None:
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        cand = _FakeCand()
        body = "החשוד נעצר לאחר חקירה ממושכת של המשטרה"
        r1 = scorer.evaluate(cand, "thick", body=body)
        r2 = scorer.evaluate(cand, "thick", body=body)
        assert r1 is not None and r2 is not None
        assert r1.p_negative == r2.p_negative

    def test_different_inputs_may_differ(self, trained_stage_b_dir: Path) -> None:
        """Crime text and sports text should produce different p_negative values."""
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        crime = _FakeCand(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        sports = _FakeCand(title="ספורט ופנאי", snippet="כדורגל ושחמט")
        r_crime = scorer.evaluate(crime, "thin")
        r_sports = scorer.evaluate(sports, "thin")
        assert r_crime is not None and r_sports is not None
        # Crime is positive → lower p_negative; sports is negative → higher p_negative.
        # At 15 examples per class the model should be directionally correct.
        assert r_crime.p_negative < r_sports.p_negative

    def test_thin_and_thick_passes_use_different_models(self, trained_stage_b_dir: Path) -> None:
        """Thin and thick scorers are separate sklearn pipelines.

        With a body that differs from title+snippet the scores should differ,
        confirming that two distinct models are used.
        """
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        cand = _FakeCand(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        # Pass a *negative* body → thick model should produce higher p_negative
        # than the thin model that saw a positive title+snippet.
        r_thin = scorer.evaluate(cand, "thin")
        r_thick = scorer.evaluate(cand, "thick", body="משחקי כדורגל וטניס וכדורסל הם ספורט פנאי")
        assert r_thin is not None and r_thick is not None
        # Different models → scores must not be identical.
        assert r_thin.p_negative != r_thick.p_negative
