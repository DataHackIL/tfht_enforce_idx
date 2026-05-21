"""Unit tests for StageAScorer blend and evaluate in prefilter.stage_a."""

from __future__ import annotations

import math
from pathlib import Path

from denbust.prefilter.models import StageScore
from denbust.prefilter.stage_a import (
    LexiconEntry,
    LexiconScorer,
    StageAScorer,
)

# ---------------------------------------------------------------------------
# Minimal CandidateView
# ---------------------------------------------------------------------------


class _FakeCand:
    def __init__(
        self,
        candidate_id: str = "cand-1",
        domain: str | None = "example.co.il",
        title: str | None = "כתבה לדוגמה",
        snippet: str | None = "קטע קצר",
        url: str | None = "https://example.co.il/article/1",
    ) -> None:
        self._id = candidate_id
        self._domain = domain
        self._title = title
        self._snippet = snippet
        self._url = url

    @property
    def candidate_id(self) -> str:
        return self._id

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
# Independence formula test
# ---------------------------------------------------------------------------


class TestBlendFormula:
    def test_independence_blend_correctness(self) -> None:
        """LexiconScorer with known log_weight produces the expected sigmoid probability."""
        p_lex = 0.7
        # sigmoid(log(0.7/0.3)) == 0.7 exactly (by construction of _sigmoid)
        log_w = math.log(0.7 / 0.3)
        entries = [LexiconEntry(term="test_term", log_weight_negative=log_w, k_neg=7, k_pos=3)]
        lex_scorer = LexiconScorer(entries)
        assert abs(lex_scorer.score("test_term", "") - p_lex) < 1e-6

    def test_zero_all_signals_gives_zero_blend(self) -> None:
        """If all sub-scorers return 0, blend is 0."""
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        # A candidate with no excluded title terms, clean URL, and no domain model
        cand = _FakeCand(
            title="עצור חשוד ברצח",
            snippet="המשטרה עצרה חשוד",
            url="https://example.co.il/article/123",
            domain="example.co.il",
        )
        result = scorer.evaluate(cand, "thin")
        # Domain scorer is empty (no_opinion=0), URL is clean, title has no excluded terms
        assert result.p_negative == 0.0

    def test_high_lexicon_signal_triggers_drop(self) -> None:
        """A candidate with an excluded title term should be dropped at default threshold."""
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(title="ספורט בישראל", snippet="", url="https://example.co.il/x")
        result = scorer.evaluate(cand, "thin")
        assert result.p_negative >= 0.95
        assert result.dropped is True

    def test_excluded_term_in_snippet_triggers_drop(self) -> None:
        """Excluded terms in the snippet also count."""
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(title="חדשות", snippet="ספורט ישראלי", url="https://example.co.il/x")
        result = scorer.evaluate(cand, "thin")
        assert result.dropped is True

    def test_evaluate_returns_stage_score(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert isinstance(result, StageScore)
        assert result.stage == "A"

    def test_stage_score_threshold_matches_config(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.80)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result.threshold == 0.80

    def test_p_negative_in_unit_interval(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        for title in ["ספורט", "נתניהו", "עצור חשוד", "", "כלכלה בישראל"]:
            result = scorer.evaluate(_FakeCand(title=title), "thin")
            assert 0.0 <= result.p_negative <= 1.0

    def test_none_title_snippet_handled(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(title=None, snippet=None, url="https://example.co.il/article/1")
        result = scorer.evaluate(cand, "thin")
        assert isinstance(result, StageScore)

    def test_none_domain_url_handled(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(domain=None, url=None)
        result = scorer.evaluate(cand, "thin")
        assert isinstance(result, StageScore)

    def test_reason_contains_lex_when_term_matches(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(title="ספורט", snippet="", url="https://example.co.il/x")
        result = scorer.evaluate(cand, "thin")
        assert "lex=" in result.reason

    def test_reason_is_no_signal_when_clean(self) -> None:
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand(title="עצור חשוד", snippet="", url="https://example.co.il/article/1")
        result = scorer.evaluate(cand, "thin")
        assert result.reason == "no_signal"

    def test_model_version_default(self) -> None:
        """With no models_dir, model_version should be 'default'."""
        scorer = StageAScorer(models_dir=None)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result.model_version == "default"

    def test_model_version_from_file(self, tmp_path: Path) -> None:
        """With a models_dir containing artifacts, model_version is a hash."""
        # Build minimal artifacts
        import math

        from denbust.prefilter.stage_a import LexiconEntry, LexiconScorer

        stage_dir = tmp_path / "stage_a"
        lex = LexiconScorer([LexiconEntry("test", math.log(2), 1, 0)])
        lex.save(stage_dir / "lexicon.json")

        scorer = StageAScorer(models_dir=tmp_path)
        result = scorer.evaluate(_FakeCand(), "thin")
        assert result.model_version != "default"
        assert len(result.model_version) == 12  # short SHA-1

    def test_body_and_pass_kind_accepted(self) -> None:
        """evaluate must accept body and pass_kind without raising."""
        scorer = StageAScorer(models_dir=None, threshold=0.95)
        cand = _FakeCand()
        result = scorer.evaluate(cand, "thick", "full article body text here")
        assert isinstance(result, StageScore)
