"""Unit tests for UrlHeuristicScorer in prefilter.stage_a."""

from __future__ import annotations

import pytest

from denbust.prefilter.stage_a import UrlHeuristicScorer


@pytest.fixture()
def scorer() -> UrlHeuristicScorer:
    return UrlHeuristicScorer()


class TestUrlHeuristicScorerScoring:
    def test_clean_article_url_scores_low(self, scorer: UrlHeuristicScorer) -> None:
        """A clean article-style URL should not be flagged."""
        p = scorer.score("https://www.ynet.co.il/news/article/abc123")
        assert p < 0.3

    def test_tag_index_url_scores_high(self, scorer: UrlHeuristicScorer) -> None:
        p = scorer.score("https://www.ynet.co.il/tag/prostitution")
        assert p >= 0.5

    def test_category_url_scores_high(self, scorer: UrlHeuristicScorer) -> None:
        p = scorer.score("https://news.example.co.il/category/crime/")
        assert p >= 0.5

    def test_sitemap_url_scores_high(self, scorer: UrlHeuristicScorer) -> None:
        p = scorer.score("https://example.co.il/sitemap.xml")
        assert p >= 0.5

    def test_pdf_extension_scores_high(self, scorer: UrlHeuristicScorer) -> None:
        p = scorer.score("https://example.co.il/report.pdf")
        assert p >= 0.5

    def test_trailing_slash_scores_positive(self, scorer: UrlHeuristicScorer) -> None:
        """A non-root path ending in '/' is flagged but not necessarily high."""
        p = scorer.score("https://example.co.il/crime/news/")
        assert p > 0.0

    def test_excess_query_params_scores_positive(self, scorer: UrlHeuristicScorer) -> None:
        url = "https://example.co.il/article?a=1&b=2&c=3&d=4"
        p = scorer.score(url)
        assert p > 0.0

    def test_acceptable_query_params_scores_zero(self, scorer: UrlHeuristicScorer) -> None:
        url = "https://example.co.il/article?id=123"
        p = scorer.score(url)
        assert p == 0.0

    def test_empty_url_returns_zero(self, scorer: UrlHeuristicScorer) -> None:
        assert scorer.score("") == 0.0

    def test_root_path_not_flagged_for_trailing_slash(self, scorer: UrlHeuristicScorer) -> None:
        """The root path '/' should not trigger the trailing-slash heuristic."""
        p = scorer.score("https://example.co.il/")
        assert p == 0.0

    def test_returns_value_in_unit_interval(self, scorer: UrlHeuristicScorer) -> None:
        for url in [
            "https://example.co.il/tag/crime",
            "https://example.co.il/article/123",
            "https://example.co.il/sitemap.xml",
            "",
        ]:
            assert 0.0 <= scorer.score(url) <= 1.0

    def test_score_capped_at_0_99(self, scorer: UrlHeuristicScorer) -> None:
        """Score must never reach exactly 1.0 (per spec)."""
        # Combine multiple signals
        url = "https://example.co.il/tag/crime/?a=1&b=2&c=3&d=4"
        assert scorer.score(url) <= 0.99
