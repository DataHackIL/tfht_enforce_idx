"""Unit tests for _threshold_at_recall_floor and _compute_stage_b_metrics.

These are pure-Python helpers extracted from the ``evaluate`` CLI command so
that the core metric logic can be tested independently of CLI plumbing or
file I/O.  No model loading, no filesystem access.
"""

from __future__ import annotations

import pytest

from denbust.prefilter.cli import _compute_stage_b_metrics, _threshold_at_recall_floor

# ---------------------------------------------------------------------------
# _threshold_at_recall_floor
# ---------------------------------------------------------------------------


class TestThresholdAtRecallFloor:
    """Unit tests for the threshold search algorithm."""

    def test_no_positives_returns_one(self) -> None:
        """When there are no positives, any threshold is safe — return 1.0."""
        p = [0.1, 0.2, 0.9]
        y = [0, 0, 0]
        assert _threshold_at_recall_floor(p, y, recall_floor=0.99) == 1.0

    def test_floor_satisfied_at_minimum_threshold(self) -> None:
        """When the minimum unique value already satisfies the floor, return it."""
        # All positives have very low p_negative; the tightest threshold (0.1)
        # does not drop any positives.
        p = [0.1, 0.2, 0.8, 0.9]
        y = [1, 1, 0, 0]  # positives at 0.1, 0.2
        threshold = _threshold_at_recall_floor(p, y, recall_floor=0.99)
        # At threshold=0.1, false_drops = 2 (both positives); 2 > 0 → skip.
        # At threshold=0.2, false_drops = 2 (both positives); still > 0 → skip.
        # At threshold=0.8, false_drops = 0 ≤ 0 → return 0.8.
        assert threshold == 0.8

    def test_partial_floor_allows_one_false_drop(self) -> None:
        """With recall_floor=0.9 and 10 positives, one false drop is allowed."""
        # max_false_drops = int(10 * 0.1) = 1
        pos_p = [0.8, 0.9] + [0.1] * 8  # 2 high-p positives, 8 low-p positives
        neg_p = [0.95, 0.97]
        p = pos_p + neg_p
        y = [1] * 10 + [0] * 2
        threshold = _threshold_at_recall_floor(p, y, recall_floor=0.9)
        # At threshold=0.8: false_drops=2 > 1 → skip
        # At threshold=0.9: false_drops=1 ≤ 1 → return 0.9
        assert threshold == 0.9

    def test_bug_regression_first_violation_not_returned(self) -> None:
        """Key regression: violating at T must not return T+1e-9 prematurely.

        Old algorithm bailed at the first threshold that violated and returned
        ``T + 1e-9``.  If T was the minimum value in the set, ``T + 1e-9``
        still dropped every positive above T, violating the floor.

        New algorithm continues until it finds a threshold that satisfies the
        constraint, only returning above-max when none does.
        """
        # recall_floor=1.0 → max_false_drops=0 (no false drops allowed).
        # Positives are at 0.3, 0.5, 0.7; negatives at 0.1, 0.2.
        # Every candidate threshold in [0.1..0.7] drops at least one positive.
        # The only safe answer is above 0.7 (max of all p_negatives).
        p = [0.1, 0.2, 0.3, 0.5, 0.7]
        y = [0, 0, 1, 1, 1]  # positives at 0.3, 0.5, 0.7
        threshold = _threshold_at_recall_floor(p, y, recall_floor=1.0)

        # Returned threshold must be above 0.7 so no positive is dropped.
        assert threshold > 0.7
        # Verify recall is actually 1.0 at the returned threshold.
        false_drops = sum(1 for pv, yv in zip(p, y) if pv >= threshold and yv == 1)
        assert false_drops == 0

    def test_no_threshold_satisfies_returns_above_max(self) -> None:
        """When even the strictest threshold still drops a positive, return max+eps."""
        # Single positive with the highest p_negative in the dataset.
        # No threshold can avoid dropping it without dropping nothing.
        p = [0.9, 0.5, 0.3]
        y = [1, 0, 0]  # the single positive has the max p
        threshold = _threshold_at_recall_floor(p, y, recall_floor=1.0)
        assert threshold > 0.9
        # At this threshold, the positive (p=0.9) is not dropped.
        assert not (threshold <= 0.9)

    def test_single_positive_zero_drop_required(self) -> None:
        """recall_floor=1.0 with one positive; threshold must be above that positive's p."""
        p = [0.95, 0.6, 0.4]
        y = [1, 0, 0]
        threshold = _threshold_at_recall_floor(p, y, recall_floor=1.0)
        assert threshold > 0.95

    def test_all_same_probability_no_floor_violation(self) -> None:
        """When all p_negatives are identical and a false drop is allowed, drop them."""
        # max_false_drops = int(2 * 0.5) = 1; one false drop allowed.
        p = [0.7, 0.7, 0.7, 0.7]
        y = [1, 1, 0, 0]
        threshold = _threshold_at_recall_floor(p, y, recall_floor=0.5)
        # At threshold=0.7: false_drops=2 > 1 → skip.
        # No more thresholds → return max(p)+1e-9 = 0.7+1e-9.
        assert threshold > 0.7

    def test_returned_threshold_always_satisfies_floor(self) -> None:
        """Property: the returned threshold must never violate the recall floor."""
        import random

        rng = random.Random(42)
        for _ in range(50):
            n = rng.randint(5, 20)
            p = [round(rng.random(), 2) for _ in range(n)]
            y = [rng.randint(0, 1) for _ in range(n)]
            floor = rng.choice([0.8, 0.9, 0.95, 0.99, 1.0])
            threshold = _threshold_at_recall_floor(p, y, recall_floor=floor)
            n_pos = sum(y)
            if n_pos == 0:
                continue
            false_drops = sum(1 for pv, yv in zip(p, y) if pv >= threshold and yv == 1)
            recall = (n_pos - false_drops) / n_pos
            assert recall >= floor - 1e-9, (
                f"Recall {recall:.4f} < floor {floor} at threshold {threshold:.6f} (p={p}, y={y})"
            )


# ---------------------------------------------------------------------------
# _compute_stage_b_metrics
# ---------------------------------------------------------------------------


class TestComputeStageBMetrics:
    """Unit tests for the extracted metric computation helper."""

    def test_empty_scored_rows_returns_safe_defaults(self) -> None:
        result = _compute_stage_b_metrics(
            scored_p=[], scored_y=[], n_pos_total=5, n_total=10, recall_floor=0.99
        )
        assert result["recall"] == 1.0
        assert result["drop_rate"] == 0.0
        assert result["brier_score"] == 0.0
        assert result["drop_precision"] == 0.0

    def test_perfect_model_brier_near_zero(self) -> None:
        """A perfect model assigns p_negative=1.0 to negatives, 0.0 to positives."""
        scored_p = [1.0, 1.0, 0.0, 0.0]  # neg, neg, pos, pos
        scored_y = [0, 0, 1, 1]
        result = _compute_stage_b_metrics(
            scored_p=scored_p, scored_y=scored_y, n_pos_total=2, n_total=4, recall_floor=0.99
        )
        assert result["brier_score"] == pytest.approx(0.0)

    def test_worst_model_brier_near_one(self) -> None:
        """A completely inverted model assigns p_negative=0.0 to negatives, 1.0 to positives."""
        scored_p = [0.0, 0.0, 1.0, 1.0]  # neg, neg, pos, pos
        scored_y = [0, 0, 1, 1]
        result = _compute_stage_b_metrics(
            scored_p=scored_p, scored_y=scored_y, n_pos_total=2, n_total=4, recall_floor=0.99
        )
        assert result["brier_score"] == pytest.approx(1.0)

    def test_drop_rate_uses_n_total_not_n_scored(self) -> None:
        """drop_rate = dropped / n_total, not dropped / len(scored_p)."""
        # 3 rows scored out of 10 total: 2 high-scoring negatives and 1 positive
        # that the model correctly keeps (low p_negative).  The threshold lands
        # at 0.85 (first value that drops no positives), so both negatives are
        # dropped.  With n_total=10, drop_rate = 2/10 = 0.2, NOT 2/3 ≈ 0.67.
        scored_p = [0.95, 0.85, 0.1]
        scored_y = [0, 0, 1]
        result = _compute_stage_b_metrics(
            scored_p=scored_p,
            scored_y=scored_y,
            n_pos_total=1,
            n_total=10,
            recall_floor=0.99,
        )
        assert result["drop_rate"] == pytest.approx(0.2)

    def test_recall_uses_n_pos_total_not_scored_positives(self) -> None:
        """Unscored positives are NOT dropped; recall uses n_pos_total."""
        # 3 positives in total; 2 are scored (neither dropped), 1 was unscored.
        scored_p = [0.1, 0.2]  # both well below any threshold
        scored_y = [1, 1]
        result = _compute_stage_b_metrics(
            scored_p=scored_p,
            scored_y=scored_y,
            n_pos_total=3,  # one extra unscored positive
            n_total=10,
            recall_floor=0.99,
        )
        # No positives among scored rows are dropped, so false_drops=0.
        # recall = (3 - 0) / 3 = 1.0.
        assert result["recall"] == pytest.approx(1.0)

    def test_false_drop_reduces_recall(self) -> None:
        """A positive that scores above the threshold reduces recall."""
        # recall_floor=0.5 → allow up to 1 false drop among 2 positives.
        # Threshold will be chosen such that recall ≥ 0.5.
        scored_p = [0.95, 0.1]  # first is positive, second is positive
        scored_y = [1, 1]
        result = _compute_stage_b_metrics(
            scored_p=scored_p,
            scored_y=scored_y,
            n_pos_total=2,
            n_total=4,
            recall_floor=0.5,
        )
        assert result["recall"] >= 0.5

    def test_drop_precision_all_true_drops(self) -> None:
        """When every dropped candidate is truly negative, drop_precision = 1.0."""
        scored_p = [0.9, 0.8, 0.05, 0.05]
        scored_y = [0, 0, 1, 1]  # dropped items are negatives
        result = _compute_stage_b_metrics(
            scored_p=scored_p,
            scored_y=scored_y,
            n_pos_total=2,
            n_total=4,
            recall_floor=0.99,
        )
        assert result["drop_precision"] == pytest.approx(1.0)

    def test_no_positives_recall_is_one(self) -> None:
        """When the eval split has no positives, recall defaults to 1.0."""
        scored_p = [0.9, 0.8]
        scored_y = [0, 0]
        result = _compute_stage_b_metrics(
            scored_p=scored_p, scored_y=scored_y, n_pos_total=0, n_total=2, recall_floor=0.99
        )
        assert result["recall"] == pytest.approx(1.0)

    def test_threshold_satisfies_floor(self) -> None:
        """The chosen threshold must always achieve the declared recall floor."""
        scored_p = [0.95, 0.85, 0.3, 0.2, 0.1]
        scored_y = [0, 0, 1, 1, 1]
        for floor in [0.8, 0.9, 0.99, 1.0]:
            result = _compute_stage_b_metrics(
                scored_p=scored_p,
                scored_y=scored_y,
                n_pos_total=3,
                n_total=5,
                recall_floor=floor,
            )
            assert result["recall"] >= floor - 1e-9, (
                f"recall {result['recall']:.4f} < floor {floor} "
                f"at threshold {result['threshold']:.6f}"
            )
