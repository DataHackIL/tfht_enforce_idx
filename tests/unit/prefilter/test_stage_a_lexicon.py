"""Unit tests for LexiconScorer in prefilter.stage_a."""

from __future__ import annotations

import dataclasses
import json
import math
from pathlib import Path

import pytest

from denbust.discovery.candidate_filters import globally_excluded_title_terms
from denbust.prefilter.stage_a import (
    LexiconEntry,
    LexiconScorer,
    _chi2_top_terms,
    _default_lexicon,
    _sigmoid,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry(term: str, k_neg: int, k_pos: int) -> LexiconEntry:
    log_w = math.log((k_neg + 1) / (k_pos + 1))
    return LexiconEntry(term=term, log_weight_negative=log_w, k_neg=k_neg, k_pos=k_pos)


def _high_weight_entry(term: str) -> LexiconEntry:
    """Produce a high-weight (≈0.99) entry for *term*."""
    return _entry(term, k_neg=98, k_pos=0)


# ---------------------------------------------------------------------------
# Sigmoid
# ---------------------------------------------------------------------------


class TestSigmoid:
    def test_zero_gives_half(self) -> None:
        assert abs(_sigmoid(0.0) - 0.5) < 1e-9

    def test_positive_gives_gt_half(self) -> None:
        assert _sigmoid(2.0) > 0.5

    def test_negative_gives_lt_half(self) -> None:
        assert _sigmoid(-2.0) < 0.5

    def test_large_positive_near_one(self) -> None:
        assert _sigmoid(10.0) > 0.999

    def test_large_negative_near_zero(self) -> None:
        assert _sigmoid(-10.0) < 0.001


# ---------------------------------------------------------------------------
# LexiconEntry
# ---------------------------------------------------------------------------


class TestLexiconEntry:
    def test_frozen(self) -> None:
        entry = _entry("test", 5, 2)
        with pytest.raises(dataclasses.FrozenInstanceError):
            entry.term = "other"  # type: ignore[misc]

    def test_log_weight_computed_correctly(self) -> None:
        entry = _entry("test", 9, 0)
        assert abs(entry.log_weight_negative - math.log(10)) < 1e-9


# ---------------------------------------------------------------------------
# LexiconScorer — scoring
# ---------------------------------------------------------------------------


class TestLexiconScorerScoring:
    def test_no_entries_returns_zero(self) -> None:
        scorer = LexiconScorer([])
        assert scorer.score("anything", "here") == 0.0

    def test_matching_high_weight_term_yields_high_p(self) -> None:
        scorer = LexiconScorer([_high_weight_entry("ספורט")])
        p = scorer.score("כתבת ספורט", "משחק כדורגל")
        assert p >= 0.95

    def test_non_matching_term_yields_zero(self) -> None:
        scorer = LexiconScorer([_high_weight_entry("ספורט")])
        p = scorer.score("כתבת חדשות", "עצור חשוד ברצח")
        assert p == 0.0

    def test_multiple_matching_terms_combine(self) -> None:
        scorer = LexiconScorer(
            [_entry("ספורט", k_neg=4, k_pos=1), _entry("מכבי", k_neg=4, k_pos=1)]
        )
        p_both = scorer.score("ספורט מכבי", "")
        p_one = scorer.score("ספורט", "")
        assert p_both > p_one

    def test_casefold_matching(self) -> None:
        scorer = LexiconScorer([_high_weight_entry("themarker")])
        assert scorer.score("TheMarker כתבה", "") >= 0.95
        assert scorer.score("THEMARKER", "") >= 0.95

    def test_substring_matching(self) -> None:
        """Term matching is substring-based (matches within longer words)."""
        scorer = LexiconScorer([_high_weight_entry("ספורט")])
        # "ספורטאי" contains "ספורט" as a substring
        assert scorer.score("ספורטאי ידוע", "") >= 0.95

    def test_returns_value_in_unit_interval(self) -> None:
        scorer = LexiconScorer(
            [_high_weight_entry("א"), _high_weight_entry("ב"), _high_weight_entry("ג")]
        )
        p = scorer.score("א ב ג", "")
        assert 0.0 <= p <= 1.0


# ---------------------------------------------------------------------------
# LexiconScorer — file I/O
# ---------------------------------------------------------------------------


class TestLexiconScorerValidation:
    """from_file must reject corrupt artifact data with an informative error."""

    def test_rejects_null_weight(self, tmp_path: Path) -> None:
        """null log_weight_negative (JSON null → Python None) must raise ValueError."""
        path = tmp_path / "bad.json"
        path.write_text(
            '[{"term": "test", "log_weight_negative": null, "k_neg": 1, "k_pos": 0}]',
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="log_weight_negative"):
            LexiconScorer.from_file(path)

    def test_rejects_negative_k_neg(self, tmp_path: Path) -> None:
        """Negative k_neg must raise ValueError."""
        path = tmp_path / "bad.json"
        path.write_text(
            '[{"term": "test", "log_weight_negative": 1.0, "k_neg": -1, "k_pos": 0}]',
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="k_neg"):
            LexiconScorer.from_file(path)

    def test_rejects_negative_k_pos(self, tmp_path: Path) -> None:
        """Negative k_pos must raise ValueError."""
        path = tmp_path / "bad.json"
        path.write_text(
            '[{"term": "test", "log_weight_negative": 1.0, "k_neg": 5, "k_pos": -2}]',
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="k_pos"):
            LexiconScorer.from_file(path)


class TestLexiconScorerIO:
    def test_round_trip(self, tmp_path: Path) -> None:
        entries = [_entry("ספורט", 10, 1), _entry("themarker", 20, 0)]
        scorer = LexiconScorer(entries)
        path = tmp_path / "lexicon.json"
        scorer.save(path)
        loaded = LexiconScorer.from_file(path)
        # Same score on same input
        assert abs(loaded.score("ספורט", "") - scorer.score("ספורט", "")) < 1e-6

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        scorer = LexiconScorer([_entry("test", 1, 0)])
        path = tmp_path / "nested" / "dir" / "lexicon.json"
        scorer.save(path)
        assert path.exists()

    def test_saved_json_is_valid(self, tmp_path: Path) -> None:
        scorer = LexiconScorer([_entry("מבחן", 5, 2)])
        path = tmp_path / "lexicon.json"
        scorer.save(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert data[0]["term"] == "מבחן"


# ---------------------------------------------------------------------------
# Default lexicon (no training data)
# ---------------------------------------------------------------------------


class TestDefaultLexicon:
    def test_returns_lexicon_scorer(self) -> None:
        lex = _default_lexicon()
        assert isinstance(lex, LexiconScorer)

    def test_excluded_term_scores_high(self) -> None:
        """Any term from _EXCLUDED_TITLE_TERMS should score >= 0.95."""
        lex = _default_lexicon()
        # "ספורט" is in _EXCLUDED_TITLE_TERMS
        assert lex.score("ספורט", "") >= 0.95

    def test_all_excluded_terms_score_high(self) -> None:
        """Every term in _EXCLUDED_TITLE_TERMS must produce p >= 0.95.

        This is the automated recall-floor check: the default lexicon must
        replicate the existing hard-drop behaviour for the full excluded-term
        list, not just for a single sampled term.
        """
        lex = _default_lexicon()
        for term in globally_excluded_title_terms():
            p = lex.score(term, "")
            assert p >= 0.95, f"term {term!r} scored {p:.4f} < 0.95"

    def test_unrelated_text_scores_zero(self) -> None:
        lex = _default_lexicon()
        p = lex.score("עצור חשוד ברצח אישה בתל אביב", "המשטרה עצרה חשוד ברצח")
        assert p == 0.0


# ---------------------------------------------------------------------------
# chi2 feature selection
# ---------------------------------------------------------------------------


class TestChi2TopTerms:
    def test_excludes_positive_skewed_terms(self) -> None:
        """_chi2_top_terms must never return terms where k_neg <= k_pos.

        A positive-skewed term gets log_weight < 0, so sigmoid < 0.5, meaning
        matching it would *decrease* p_negative and silently hurt recall.
        """
        # "good" appears only in positives; "bad" only in negatives.
        pos_texts = ["good article"] * 20
        neg_texts = ["bad content"] * 20

        results = _chi2_top_terms(pos_texts, neg_texts, n=100, min_df=1)
        term_names = [t for t, _, _ in results]

        assert "bad" in term_names, "negative-skewed term missing from output"
        assert "good" not in term_names, "positive-skewed term must not appear in output"

    def test_all_returned_terms_are_negative_skewed(self) -> None:
        """Every (term, k_neg, k_pos) triple must satisfy k_neg > k_pos."""
        pos_texts = ["נושא חיובי שמח ושמח"] * 30
        neg_texts = ["נושא שלילי רע ורע"] * 30 + ["נושא חיובי"] * 5

        results = _chi2_top_terms(pos_texts, neg_texts, n=50, min_df=2)
        for term, k_neg, k_pos in results:
            assert k_neg > k_pos, (
                f"term {term!r} is not negative-skewed: k_neg={k_neg}, k_pos={k_pos}"
            )
