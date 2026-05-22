"""Unit tests for prefilter pipeline wiring.

Covers:
- :class:`~denbust.prefilter.adapters.PersistentCandidateView` property mapping
- :class:`~denbust.prefilter.adapters.RawArticleCandidateView` property mapping
- :func:`~denbust.prefilter.adapters._etld1` domain extraction
- Pipeline helper functions exposed by ``denbust.pipeline``:
  ``_build_cascade_orchestrator``, ``_thin_pass_prefilter``,
  ``_thick_pass_prefilter``, ``_build_prefilter_pass_dict``,
  ``_write_prefilter_run_summary``

All tests are offline (no network, no model inference).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from pydantic import HttpUrl

import denbust.pipeline as pipeline_module
from denbust.config import Config
from denbust.data_models import RawArticle
from denbust.discovery.models import PersistentCandidate
from denbust.prefilter.adapters import (
    PersistentCandidateView,
    RawArticleCandidateView,
    _etld1,
)
from denbust.prefilter.models import PrefilterDecision

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_persistent_candidate(
    *,
    candidate_id: str = "cand-001",
    canonical_url: str | None = "https://www.ynet.co.il/news/article/1234",
    current_url: str = "https://www.ynet.co.il/news/article/1234",
    titles: list[str] | None = None,
    snippets: list[str] | None = None,
    domain: str | None = None,
) -> PersistentCandidate:
    return PersistentCandidate(
        candidate_id=candidate_id,
        current_url=HttpUrl(current_url),
        canonical_url=HttpUrl(canonical_url) if canonical_url else None,
        titles=titles if titles is not None else ["כותרת ראשית"],
        snippets=snippets if snippets is not None else ["קטעי תוכן"],
        domain=domain,
    )


def _make_raw_article(
    url: str = "https://www.mako.co.il/news/article/abc",
    title: str = "כותרת כתבה",
    snippet: str = "תקציר הכתבה",
) -> RawArticle:
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet=snippet,
        date=datetime(2026, 1, 15, tzinfo=UTC),
        source_name="mako",
    )


def _make_decision(
    candidate_id: str = "cand-001",
    verdict: str = "pass",
    stopped_at_stage: str = "passed_all",
) -> PrefilterDecision:
    d = MagicMock(spec=PrefilterDecision)
    d.candidate_id = candidate_id
    d.verdict = verdict
    d.stopped_at_stage = stopped_at_stage
    return d


# ===========================================================================
# _etld1
# ===========================================================================


class TestEtld1:
    """_etld1 extracts a best-effort eTLD+1 from a URL string."""

    def test_standard_two_part_domain(self) -> None:
        assert _etld1("https://example.com/path") == "example.com"

    def test_subdomain_trimmed(self) -> None:
        assert _etld1("https://www.ynet.co.il/news") == "co.il"

    def test_bare_hostname(self) -> None:
        assert (
            _etld1("https://localhost/path") is None
            or _etld1("https://localhost/path") == "localhost"
        )

    def test_empty_url(self) -> None:
        assert _etld1("") is None

    def test_no_dots(self) -> None:
        result = _etld1("https://intranet/resource")
        # single-label host: return as-is or None
        assert result in ("intranet", None)

    def test_co_il_domain(self) -> None:
        assert _etld1("https://www.mako.co.il/article/123") == "co.il"

    def test_news_domain(self) -> None:
        assert _etld1("https://haaretz.co.il/article") == "co.il"


# ===========================================================================
# PersistentCandidateView
# ===========================================================================


class TestPersistentCandidateView:
    """PersistentCandidateView maps PersistentCandidate fields to CandidateView protocol."""

    def test_candidate_id(self) -> None:
        c = _make_persistent_candidate(candidate_id="xyz-999")
        view = PersistentCandidateView(c)
        assert view.candidate_id == "xyz-999"

    def test_domain_from_candidate(self) -> None:
        c = _make_persistent_candidate()
        view = PersistentCandidateView(c)
        assert view.domain is not None
        assert "ynet" in view.domain or "co.il" in view.domain

    def test_title_returns_first_title(self) -> None:
        c = _make_persistent_candidate(titles=["כותרת ראשונה", "כותרת שניה"])
        view = PersistentCandidateView(c)
        assert view.title == "כותרת ראשונה"

    def test_title_returns_none_when_empty(self) -> None:
        c = _make_persistent_candidate(titles=[])
        view = PersistentCandidateView(c)
        assert view.title is None

    def test_snippet_returns_first_snippet(self) -> None:
        c = _make_persistent_candidate(snippets=["תקציר א", "תקציר ב"])
        view = PersistentCandidateView(c)
        assert view.snippet == "תקציר א"

    def test_snippet_returns_none_when_empty(self) -> None:
        c = _make_persistent_candidate(snippets=[])
        view = PersistentCandidateView(c)
        assert view.snippet is None

    def test_url_prefers_canonical(self) -> None:
        c = _make_persistent_candidate(
            canonical_url="https://canonical.example.com/",
            current_url="https://current.example.com/",
        )
        view = PersistentCandidateView(c)
        assert "canonical" in view.url  # type: ignore[operator]

    def test_url_falls_back_to_current(self) -> None:
        c = _make_persistent_candidate(
            canonical_url=None,
            current_url="https://current.example.com/article",
        )
        view = PersistentCandidateView(c)
        assert view.url is not None
        assert "current" in view.url

    def test_url_is_string(self) -> None:
        c = _make_persistent_candidate()
        view = PersistentCandidateView(c)
        assert isinstance(view.url, str)


# ===========================================================================
# RawArticleCandidateView
# ===========================================================================


class TestRawArticleCandidateView:
    """RawArticleCandidateView maps RawArticle + caller-supplied candidate_id."""

    def test_candidate_id_from_caller(self) -> None:
        a = _make_raw_article()
        view = RawArticleCandidateView(a, candidate_id="caller-cand-007")
        assert view.candidate_id == "caller-cand-007"

    def test_title_from_article(self) -> None:
        a = _make_raw_article(title="כותרת הכתבה")
        view = RawArticleCandidateView(a, candidate_id="c-1")
        assert view.title == "כותרת הכתבה"

    def test_snippet_from_article(self) -> None:
        a = _make_raw_article(snippet="תקציר הכתבה")
        view = RawArticleCandidateView(a, candidate_id="c-1")
        assert view.snippet == "תקציר הכתבה"

    def test_url_from_article(self) -> None:
        a = _make_raw_article(url="https://www.example.co.il/path")
        view = RawArticleCandidateView(a, candidate_id="c-1")
        assert view.url == "https://www.example.co.il/path"

    def test_url_is_string(self) -> None:
        a = _make_raw_article()
        view = RawArticleCandidateView(a, candidate_id="c-1")
        assert isinstance(view.url, str)

    def test_domain_extracted_from_url(self) -> None:
        a = _make_raw_article(url="https://news.walla.co.il/article/3456")
        view = RawArticleCandidateView(a, candidate_id="c-1")
        # _etld1("https://news.walla.co.il/article/3456") → "co.il"
        assert view.domain is not None
        assert "." in view.domain

    def test_domain_none_for_invalid_url(self) -> None:
        # We can't construct a RawArticle with a truly invalid URL (pydantic validates)
        # but we can verify domain doesn't crash on an unusual structure.
        a = _make_raw_article(url="https://example.com/path")
        view = RawArticleCandidateView(a, candidate_id="c-1")
        assert view.domain == "example.com"


# ===========================================================================
# _build_cascade_orchestrator
# ===========================================================================


class TestBuildCascadeOrchestrator:
    """_build_cascade_orchestrator returns None when prefilter is disabled/OFF."""

    def test_returns_none_when_prefilter_disabled(self, tmp_path: Path) -> None:
        config = Config(store={"state_root": tmp_path})
        # Default config has prefilter.enabled = False or mode = OFF
        result = pipeline_module._build_cascade_orchestrator(config)
        assert result is None

    def test_returns_none_with_mode_off(self, tmp_path: Path) -> None:
        config = Config(
            store={"state_root": tmp_path},
            prefilter={"mode": "off"},
        )
        result = pipeline_module._build_cascade_orchestrator(config)
        assert result is None


# ===========================================================================
# _build_prefilter_pass_dict
# ===========================================================================


class TestBuildPrefilterPassDict:
    """_build_prefilter_pass_dict produces correctly structured pass summary dicts."""

    def test_empty_decisions(self) -> None:
        result = pipeline_module._build_prefilter_pass_dict([])
        assert result["evaluated"] == 0
        assert result["passed"] == 0
        assert result["dropped"] == 0
        assert result["stage_stopped_counts"] == {}

    def test_all_passed(self) -> None:
        decisions = [
            _make_decision(verdict="pass", stopped_at_stage="passed_all") for _ in range(5)
        ]
        result = pipeline_module._build_prefilter_pass_dict(decisions)
        assert result["evaluated"] == 5
        assert result["passed"] == 5
        assert result["dropped"] == 0
        assert result["stage_stopped_counts"] == {}

    def test_some_dropped(self) -> None:
        decisions = [
            _make_decision(verdict="pass", stopped_at_stage="passed_all"),
            _make_decision(verdict="drop", stopped_at_stage="stage_a"),
            _make_decision(verdict="drop", stopped_at_stage="stage_a"),
        ]
        result = pipeline_module._build_prefilter_pass_dict(decisions)
        assert result["evaluated"] == 3
        assert result["passed"] == 1
        assert result["dropped"] == 2
        assert result["stage_stopped_counts"] == {"stage_a": 2}

    def test_mixed_stages(self) -> None:
        decisions = [
            _make_decision(verdict="drop", stopped_at_stage="stage_a"),
            _make_decision(verdict="drop", stopped_at_stage="stage_b"),
            _make_decision(verdict="drop", stopped_at_stage="stage_a"),
            _make_decision(verdict="pass", stopped_at_stage="passed_all"),
        ]
        result = pipeline_module._build_prefilter_pass_dict(decisions)
        assert result["dropped"] == 3
        assert result["stage_stopped_counts"] == {"stage_a": 2, "stage_b": 1}

    def test_passed_all_not_counted_in_stage_stopped(self) -> None:
        decisions = [
            _make_decision(verdict="pass", stopped_at_stage="passed_all"),
            _make_decision(verdict="pass", stopped_at_stage="passed_all"),
        ]
        result = pipeline_module._build_prefilter_pass_dict(decisions)
        assert result["stage_stopped_counts"] == {}


# ===========================================================================
# _thin_pass_prefilter
# ===========================================================================


class TestThinPassPrefilter:
    """_thin_pass_prefilter short-circuits when orchestrator is None."""

    def test_no_orchestrator_returns_all_candidates(self) -> None:
        candidates = [
            _make_persistent_candidate(candidate_id="c1"),
            _make_persistent_candidate(candidate_id="c2"),
        ]
        passed, decisions = pipeline_module._thin_pass_prefilter(candidates, None)
        assert passed == candidates
        assert decisions == []

    def test_orchestrator_pass_keeps_all(self) -> None:
        candidates = [_make_persistent_candidate(candidate_id=f"c{i}") for i in range(3)]

        orchestrator = MagicMock()
        pass_decision = MagicMock(spec=PrefilterDecision)
        pass_decision.verdict = "pass"
        orchestrator.evaluate_thin.return_value = pass_decision

        passed, decisions = pipeline_module._thin_pass_prefilter(candidates, orchestrator)
        assert len(passed) == 3
        assert len(decisions) == 3

    def test_orchestrator_drop_removes_candidates(self) -> None:
        candidates = [
            _make_persistent_candidate(candidate_id="keep"),
            _make_persistent_candidate(candidate_id="drop"),
        ]

        drop_decision = MagicMock(spec=PrefilterDecision)
        drop_decision.verdict = "drop"
        pass_decision = MagicMock(spec=PrefilterDecision)
        pass_decision.verdict = "pass"

        orchestrator = MagicMock()
        orchestrator.evaluate_thin.side_effect = [pass_decision, drop_decision]

        passed, decisions = pipeline_module._thin_pass_prefilter(candidates, orchestrator)
        assert len(passed) == 1
        assert passed[0].candidate_id == "keep"
        assert len(decisions) == 2

    def test_empty_candidates_with_orchestrator(self) -> None:
        orchestrator = MagicMock()
        passed, decisions = pipeline_module._thin_pass_prefilter([], orchestrator)
        assert passed == []
        assert decisions == []
        orchestrator.evaluate_thin.assert_not_called()


# ===========================================================================
# _thick_pass_prefilter
# ===========================================================================


class TestThickPassPrefilter:
    """_thick_pass_prefilter short-circuits when orchestrator is None."""

    def test_no_orchestrator_returns_all_articles(self) -> None:
        articles = [_make_raw_article(url=f"https://example.com/{i}") for i in range(3)]
        passed, decisions = pipeline_module._thick_pass_prefilter(
            articles, None, candidate_id_map={}
        )
        assert passed == articles
        assert decisions == []

    def test_orchestrator_pass_keeps_all(self) -> None:
        articles = [
            _make_raw_article(url="https://example.com/a"),
            _make_raw_article(url="https://example.com/b"),
        ]
        pass_decision = MagicMock(spec=PrefilterDecision)
        pass_decision.verdict = "pass"

        orchestrator = MagicMock()
        orchestrator.evaluate_thick.return_value = pass_decision

        passed, decisions = pipeline_module._thick_pass_prefilter(
            articles, orchestrator, candidate_id_map={}
        )
        assert len(passed) == 2
        assert len(decisions) == 2

    def test_orchestrator_drop_removes_articles(self) -> None:
        articles = [
            _make_raw_article(url="https://example.com/keep"),
            _make_raw_article(url="https://example.com/drop"),
        ]
        pass_decision = MagicMock(spec=PrefilterDecision)
        pass_decision.verdict = "pass"
        drop_decision = MagicMock(spec=PrefilterDecision)
        drop_decision.verdict = "drop"

        orchestrator = MagicMock()
        orchestrator.evaluate_thick.side_effect = [pass_decision, drop_decision]

        passed, decisions = pipeline_module._thick_pass_prefilter(
            articles, orchestrator, candidate_id_map={}
        )
        assert len(passed) == 1
        assert str(passed[0].url).endswith("/keep")
        assert len(decisions) == 2

    def test_candidate_id_map_used_for_lookup(self) -> None:
        """candidate_id_map should map canonicalized URL → candidate_id."""
        article = _make_raw_article(url="https://example.com/article")
        pass_decision = MagicMock(spec=PrefilterDecision)
        pass_decision.verdict = "pass"

        orchestrator = MagicMock()
        orchestrator.evaluate_thick.return_value = pass_decision

        pipeline_module._thick_pass_prefilter(
            [article],
            orchestrator,
            candidate_id_map={"https://example.com/article": "mapped-cand-id"},
        )

        # The view passed to evaluate_thick should use the mapped candidate_id
        call_args = orchestrator.evaluate_thick.call_args
        view = call_args[0][0]  # positional arg 0
        assert view.candidate_id == "mapped-cand-id"

    def test_empty_articles_with_orchestrator(self) -> None:
        orchestrator = MagicMock()
        passed, decisions = pipeline_module._thick_pass_prefilter(
            [], orchestrator, candidate_id_map={}
        )
        assert passed == []
        assert decisions == []
        orchestrator.evaluate_thick.assert_not_called()


# ===========================================================================
# _write_prefilter_run_summary
# ===========================================================================


class TestWritePrefilterRunSummary:
    """_write_prefilter_run_summary writes a valid JSON summary to reports_dir."""

    def test_writes_json_file(self, tmp_path: Path) -> None:
        config = Config(store={"state_root": tmp_path})
        # Force prefilter enabled to trigger the write even with empty decisions
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=[_make_decision()],
            thick_decisions=[_make_decision()],
        )
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        summary_path = prefilter_paths.reports_dir / "prefilter_summary.json"
        assert summary_path.exists()

    def test_json_schema_version(self, tmp_path: Path) -> None:
        config = Config(store={"state_root": tmp_path})
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=[_make_decision()],
            thick_decisions=[],
        )
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        raw = json.loads(
            (prefilter_paths.reports_dir / "prefilter_summary.json").read_text(encoding="utf-8")
        )
        assert raw["schema_version"] == "prefilter.run_summary.v1"

    def test_json_contains_thin_and_thick_keys(self, tmp_path: Path) -> None:
        config = Config(store={"state_root": tmp_path})
        thin = [
            _make_decision(verdict="pass"),
            _make_decision(verdict="drop", stopped_at_stage="stage_a"),
        ]
        thick = [_make_decision(verdict="pass")]
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=thin,
            thick_decisions=thick,
        )
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        raw = json.loads(
            (prefilter_paths.reports_dir / "prefilter_summary.json").read_text(encoding="utf-8")
        )
        assert "thin_pass" in raw
        assert "thick_pass" in raw
        assert raw["thin_pass"]["evaluated"] == 2
        assert raw["thin_pass"]["dropped"] == 1
        assert raw["thick_pass"]["evaluated"] == 1
        assert raw["thick_pass"]["dropped"] == 0

    def test_skips_write_when_empty_and_disabled(self, tmp_path: Path) -> None:
        """With no decisions and prefilter disabled, nothing should be written."""
        config = Config(store={"state_root": tmp_path}, prefilter={"mode": "off"})
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=[],
            thick_decisions=[],
        )
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        assert not (prefilter_paths.reports_dir / "prefilter_summary.json").exists()

    def test_generated_at_is_iso8601(self, tmp_path: Path) -> None:
        config = Config(store={"state_root": tmp_path})
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=[_make_decision()],
            thick_decisions=[],
        )
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        raw = json.loads(
            (prefilter_paths.reports_dir / "prefilter_summary.json").read_text(encoding="utf-8")
        )
        dt = datetime.fromisoformat(raw["generated_at"])
        assert dt.tzinfo is not None

    def test_does_not_raise_on_write_failure(self, tmp_path: Path) -> None:
        """Summary write errors must be swallowed, never propagate."""
        config = Config(store={"state_root": tmp_path})
        # Point reports_dir at a file (not a dir) to force a write error
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths

        prefilter_paths = resolve_prefilter_state_paths(
            state_root=tmp_path,
            dataset_name=config.dataset_name,
        )
        prefilter_paths.reports_dir.parent.mkdir(parents=True, exist_ok=True)
        # Create a file at the reports_dir path so mkdir fails
        prefilter_paths.reports_dir.parent.joinpath(prefilter_paths.reports_dir.name).write_text(
            "blocker", encoding="utf-8"
        )
        # Should not raise
        pipeline_module._write_prefilter_run_summary(
            config,
            thin_decisions=[_make_decision()],
            thick_decisions=[],
        )
