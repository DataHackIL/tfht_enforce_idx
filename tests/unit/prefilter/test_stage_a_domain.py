"""Unit tests for DomainReputationScorer in prefilter.stage_a."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from denbust.prefilter.stage_a import DomainReputation, DomainReputationScorer, _wilson_upper_95

# ---------------------------------------------------------------------------
# Wilson-score helper
# ---------------------------------------------------------------------------


class TestWilsonUpper95:
    def test_zero_observations_returns_one(self) -> None:
        assert _wilson_upper_95(0, 0) == 1.0

    def test_all_negative_gives_high_upper_bound(self) -> None:
        # k=n → p_hat=1, upper bound should be close to 1
        assert _wilson_upper_95(100, 100) > 0.95

    def test_all_positive_gives_low_upper_bound(self) -> None:
        # k=0 → p_hat=0, upper bound should be < 0.1 for large n
        assert _wilson_upper_95(0, 100) < 0.1

    def test_output_in_unit_interval(self) -> None:
        for k, n in [(0, 10), (5, 10), (10, 10), (100, 200)]:
            result = _wilson_upper_95(k, n)
            assert 0.0 <= result <= 1.0


# ---------------------------------------------------------------------------
# DomainReputation
# ---------------------------------------------------------------------------


class TestDomainReputation:
    def test_frozen(self) -> None:
        rep = DomainReputation("example.co.il", 50, 40, 0.8, 0.9)
        with pytest.raises(dataclasses.FrozenInstanceError):
            rep.domain = "other.co.il"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DomainReputationScorer — scoring
# ---------------------------------------------------------------------------


class TestDomainReputationScorerScoring:
    def _rep(self, domain: str, n: int, k_neg: int) -> DomainReputation:
        from denbust.prefilter.stage_a import _wilson_upper_95

        p_mean = (k_neg + 1) / (n + 2)
        p_upper = _wilson_upper_95(k_neg, n)
        return DomainReputation(
            domain=domain, n=n, k_negative=k_neg, p_post_mean=p_mean, p_post_upper_95=p_upper
        )

    def test_fully_negative_domain_scores_high(self) -> None:
        """A domain with k=n negatives (k=20, n=20) should yield p_post_mean close to 1."""
        rep = self._rep("bad.co.il", n=20, k_neg=20)
        scorer = DomainReputationScorer({"bad.co.il": rep}, min_observations=20)
        assert scorer.score("bad.co.il") > 0.9

    def test_fully_positive_domain_scores_low(self) -> None:
        """A domain with k=0 negatives should yield a low score."""
        rep = self._rep("good.co.il", n=40, k_neg=0)
        scorer = DomainReputationScorer({"good.co.il": rep}, min_observations=20)
        assert scorer.score("good.co.il") < 0.1

    def test_unknown_domain_returns_zero(self) -> None:
        scorer = DomainReputationScorer({}, min_observations=20)
        assert scorer.score("unknown.co.il") == 0.0

    def test_below_min_observations_returns_zero(self) -> None:
        """Domains with n < min_observations must be treated as unknown."""
        rep = self._rep("small.co.il", n=10, k_neg=10)
        scorer = DomainReputationScorer({"small.co.il": rep}, min_observations=20)
        assert scorer.score("small.co.il") == 0.0

    def test_domain_casefold(self) -> None:
        """Domain lookup is case-insensitive via casefold."""
        rep = self._rep("example.co.il", n=30, k_neg=28)
        scorer = DomainReputationScorer({"example.co.il": rep}, min_observations=20)
        assert scorer.score("EXAMPLE.CO.IL") > 0.5

    def test_empty_domain_returns_zero(self) -> None:
        scorer = DomainReputationScorer({}, min_observations=20)
        assert scorer.score("") == 0.0

    def test_none_domain_returns_zero(self) -> None:
        scorer = DomainReputationScorer({}, min_observations=20)
        assert scorer.score(None) == 0.0  # type: ignore[arg-type]

    def test_p_post_upper_95_wider_than_mean_for_small_n(self) -> None:
        """For a domain with n just at the threshold, upper > mean."""
        rep = self._rep("mid.co.il", n=20, k_neg=10)
        assert rep.p_post_upper_95 > rep.p_post_mean


# ---------------------------------------------------------------------------
# DomainReputationScorer — file I/O
# ---------------------------------------------------------------------------


class TestDomainReputationScorerIO:
    def _make_scorer(self) -> DomainReputationScorer:
        from denbust.prefilter.stage_a import _wilson_upper_95

        reps = {
            "neg.co.il": DomainReputation(
                domain="neg.co.il",
                n=50,
                k_negative=45,
                p_post_mean=46 / 52,
                p_post_upper_95=_wilson_upper_95(45, 50),
            ),
            "pos.co.il": DomainReputation(
                domain="pos.co.il",
                n=30,
                k_negative=2,
                p_post_mean=3 / 32,
                p_post_upper_95=_wilson_upper_95(2, 30),
            ),
        }
        return DomainReputationScorer(reps, min_observations=20)

    def test_round_trip(self, tmp_path: Path) -> None:
        scorer = self._make_scorer()
        path = tmp_path / "domain_reputation.parquet"
        scorer.save(path)
        loaded = DomainReputationScorer.from_file(path, min_observations=20)
        assert abs(loaded.score("neg.co.il") - scorer.score("neg.co.il")) < 1e-6
        assert abs(loaded.score("pos.co.il") - scorer.score("pos.co.il")) < 1e-6

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        scorer = self._make_scorer()
        path = tmp_path / "nested" / "dir" / "domain_reputation.parquet"
        scorer.save(path)
        assert path.exists()

    def test_empty_table_round_trip(self, tmp_path: Path) -> None:
        scorer = DomainReputationScorer({}, min_observations=20)
        path = tmp_path / "empty.parquet"
        scorer.save(path)
        loaded = DomainReputationScorer.from_file(path, min_observations=20)
        assert loaded.score("anything.co.il") == 0.0
