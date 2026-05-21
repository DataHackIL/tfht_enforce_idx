"""Unit tests verifying that Stage B calibration reduces Brier score.

The calibrated ComplementNB should assign probability estimates that are
better-calibrated than a naive constant-output baseline.  We evaluate on
held-out examples drawn from the same text distribution as the training
fixture (see conftest.py → trained_stage_b_dir).
"""

from __future__ import annotations

from pathlib import Path

from denbust.prefilter.stage_b import StageBScorer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeCand:
    """Minimal CandidateView for calibration evaluation."""

    def __init__(
        self,
        candidate_id: str,
        title: str | None,
        snippet: str | None,
    ) -> None:
        self._candidate_id = candidate_id
        self._title = title
        self._snippet = snippet

    @property
    def candidate_id(self) -> str:
        return self._candidate_id

    @property
    def domain(self) -> str | None:
        return "example.co.il"

    @property
    def title(self) -> str | None:
        return self._title

    @property
    def snippet(self) -> str | None:
        return self._snippet

    @property
    def url(self) -> str | None:
        return "https://example.co.il/article/1"


def _brier_score(p_positives: list[float], labels: list[int]) -> float:
    """Mean squared error between P(positive) and true labels (0=negative, 1=positive)."""
    assert len(p_positives) == len(labels)
    return sum((p - y) ** 2 for p, y in zip(p_positives, labels)) / len(p_positives)


# ---------------------------------------------------------------------------
# Val-split candidates (mirrors conftest._write_fixture val rows)
# The conftest fixture trains on sports (negative) and crime (positive).
# These val examples come from the same distribution.
# y = 0 → negative (sports), y = 1 → positive (crime/arrest).
# ---------------------------------------------------------------------------

_VAL_CANDIDATES: list[tuple[_FakeCand, int]] = [
    # negatives — sports
    (_FakeCand("v-neg-0", "ספורט ופנאי", "כדורגל ושחמט"), 0),
    (_FakeCand("v-neg-1", "ספורט ופנאי", "כדורגל ושחמט"), 0),
    (_FakeCand("v-neg-2", "ספורט ופנאי", "כדורגל ושחמט"), 0),
    (_FakeCand("v-neg-3", "ספורט ופנאי", "כדורגל ושחמט"), 0),
    # positives — crime/arrest
    (_FakeCand("v-pos-0", "עצור חשוד ברצח", "המשטרה עצרה חשוד"), 1),
    (_FakeCand("v-pos-1", "עצור חשוד ברצח", "המשטרה עצרה חשוד"), 1),
    (_FakeCand("v-pos-2", "עצור חשוד ברצח", "המשטרה עצרה חשוד"), 1),
    (_FakeCand("v-pos-3", "עצור חשוד ברצח", "המשטרה עצרה חשוד"), 1),
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStageBCalibration:
    """Calibrated model Brier score must beat the naive constant baseline."""

    def test_brier_better_than_constant_05_baseline_thin(self, trained_stage_b_dir: Path) -> None:
        """Calibrated thin model must beat always-predicting-0.5 baseline.

        Brier score for a constant 0.5 predictor is always 0.25 regardless of
        class balance.  The calibrated model should score below this.
        """
        scorer = StageBScorer(models_dir=trained_stage_b_dir)

        p_pos: list[float] = []
        y_true: list[int] = []
        for cand, label in _VAL_CANDIDATES:
            result = scorer.evaluate(cand, "thin")
            assert result is not None
            # p_positive = 1 − p_negative; Brier score is in terms of P(positive).
            p_pos.append(1.0 - result.p_negative)
            y_true.append(label)

        model_brier = _brier_score(p_pos, y_true)
        # Constant 0.5 → Brier = 0.25 for any label distribution.
        baseline_brier = _brier_score([0.5] * len(y_true), y_true)
        assert model_brier < baseline_brier, (
            f"Calibrated thin model Brier ({model_brier:.4f}) must beat "
            f"constant-0.5 baseline ({baseline_brier:.4f})"
        )

    def test_brier_better_than_constant_05_baseline_thick(self, trained_stage_b_dir: Path) -> None:
        """Calibrated thick model must beat always-predicting-0.5 baseline.

        Thick model falls back to title+snippet when body is absent, which
        is the val-set scenario (conftest sets body=None for non-train rows).
        """
        scorer = StageBScorer(models_dir=trained_stage_b_dir)

        p_pos: list[float] = []
        y_true: list[int] = []
        for cand, label in _VAL_CANDIDATES:
            # body=None → falls back to title+snippet in the thick model
            result = scorer.evaluate(cand, "thick", body=None)
            assert result is not None
            p_pos.append(1.0 - result.p_negative)
            y_true.append(label)

        model_brier = _brier_score(p_pos, y_true)
        baseline_brier = _brier_score([0.5] * len(y_true), y_true)
        assert model_brier < baseline_brier, (
            f"Calibrated thick model Brier ({model_brier:.4f}) must beat "
            f"constant-0.5 baseline ({baseline_brier:.4f})"
        )

    def test_all_probabilities_bounded(self, trained_stage_b_dir: Path) -> None:
        """Every p_negative value returned by evaluate must lie in [0, 1]."""
        scorer = StageBScorer(models_dir=trained_stage_b_dir)
        for cand, _ in _VAL_CANDIDATES:
            result = scorer.evaluate(cand, "thin")
            assert result is not None
            assert 0.0 <= result.p_negative <= 1.0, (
                f"p_negative={result.p_negative} out of [0, 1] for {cand.candidate_id}"
            )

    def test_negative_class_higher_p_negative_than_positive(
        self, trained_stage_b_dir: Path
    ) -> None:
        """On average, negatives should score higher p_negative than positives."""
        scorer = StageBScorer(models_dir=trained_stage_b_dir)

        neg_scores = []
        pos_scores = []
        for cand, label in _VAL_CANDIDATES:
            result = scorer.evaluate(cand, "thin")
            assert result is not None
            if label == 0:
                neg_scores.append(result.p_negative)
            else:
                pos_scores.append(result.p_negative)

        assert neg_scores and pos_scores
        avg_neg = sum(neg_scores) / len(neg_scores)
        avg_pos = sum(pos_scores) / len(pos_scores)
        assert avg_neg > avg_pos, (
            f"Mean p_negative for negatives ({avg_neg:.3f}) must exceed "
            f"mean p_negative for positives ({avg_pos:.3f})"
        )
