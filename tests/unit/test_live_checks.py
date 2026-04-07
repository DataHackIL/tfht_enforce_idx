"""Unit tests for the tracked live-check runner."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import HttpUrl

from denbust.config import Config, SourceConfig, SourceType
from denbust.data_models import Category, ClassificationResult, RawArticle, SubCategory
from denbust.live_checks.runner import (
    ActualClassification,
    CaseResult,
    ExpectedClassification,
    LiveSourceArticleCaseConfig,
    _capture_live_source_payload,
    _compare_expected,
    _find_repo_root,
    _load_fixture_article,
    _normalize_fixture_datetime,
    _render_case_markdown,
    _resolve_repo_path,
    _to_actual_classification,
    load_live_check_scenario,
    render_live_check_markdown,
    run_live_check_scenario_sync,
)


class FakeClassifier:
    """Simple async classifier stub."""

    def __init__(self, result: ClassificationResult) -> None:
        self._result = result

    async def classify(self, article: RawArticle) -> ClassificationResult:
        del article
        return self._result


class FakeSource:
    """Simple live-source stub."""

    def __init__(self, name: str, articles: list[RawArticle]) -> None:
        self.name = name
        self._articles = articles

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        return self._articles


def _init_repo(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    return tmp_path


def _write_fixture(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_scenario(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _build_matching_classification() -> ClassificationResult:
    return ClassificationResult(
        relevant=True,
        enforcement_related=True,
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
        confidence="high",
    )


class TestLiveChecks:
    """Tests for the live-check framework."""

    def test_load_live_check_scenario_parses_fixture_and_live_source_cases(
        self, tmp_path: Path
    ) -> None:
        repo_root = _init_repo(tmp_path)
        scenario_path = repo_root / "agents" / "live_checks" / "scenario.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "mixed",
                "runtime_config": "agents/news/local.yaml",
                "cases": [
                    {
                        "id": "fixture-case",
                        "type": "fixture_article",
                        "fixture": "tests/fixtures/articles/case.json",
                    },
                    {
                        "id": "live-case",
                        "type": "live_source_article",
                        "source_name": "walla",
                        "match_title_contains": "בית בושת",
                        "expected": {
                            "relevant": True,
                            "enforcement_related": True,
                            "category": "brothel",
                            "sub_category": "closure",
                        },
                    },
                ],
            },
        )

        scenario = load_live_check_scenario(scenario_path)

        assert scenario.name == "mixed"
        assert len(scenario.cases) == 2
        assert isinstance(scenario.cases[1], LiveSourceArticleCaseConfig)

    def test_run_live_check_scenario_writes_bundle_for_fixture_case(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        repo_root = _init_repo(tmp_path)
        fixture_path = repo_root / "tests" / "fixtures" / "articles" / "case.json"
        _write_fixture(
            fixture_path,
            {
                "url": "https://example.com/brothel",
                "title": "פשיטה על בית בושת",
                "snippet": "המשטרה פשטה על בית בושת",
                "date": "2026-04-07T12:00:00+00:00",
                "source_name": "test",
                "expected_classification": {
                    "relevant": True,
                    "enforcement_related": True,
                    "category": "brothel",
                    "sub_category": "closure",
                },
            },
        )
        scenario_path = repo_root / "agents" / "live_checks" / "fixture.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "fixture",
                "runtime_config": "agents/news/local.yaml",
                "cases": [
                    {
                        "id": "fixture-case",
                        "type": "fixture_article",
                        "fixture": "tests/fixtures/articles/case.json",
                    }
                ],
            },
        )

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.live_checks.runner.load_config",
            lambda _path: Config(classifier={"model": "mock-model"}),
        )
        monkeypatch.setattr(
            "denbust.live_checks.runner.create_classifier",
            lambda **_kwargs: FakeClassifier(_build_matching_classification()),
        )

        report = run_live_check_scenario_sync(scenario_path)

        output_dir = Path(report.output_dir)
        assert report.overall_status == "passed"
        assert report.model_name == "mock-model"
        assert output_dir.parent.name == "fixture"
        assert (output_dir / "report.json").exists()
        assert (output_dir / "report.md").exists()
        assert (output_dir / "artifacts" / "fixture-case.fixture.json").exists()

    def test_runner_continues_after_one_case_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        repo_root = _init_repo(tmp_path)
        good_fixture_path = repo_root / "tests" / "fixtures" / "articles" / "good.json"
        _write_fixture(
            good_fixture_path,
            {
                "url": "https://example.com/good",
                "title": "פשיטה על בית בושת",
                "snippet": "המשטרה פשטה על בית בושת",
                "date": "2026-04-07T12:00:00+00:00",
                "source_name": "test",
                "expected_classification": {
                    "relevant": True,
                    "enforcement_related": True,
                    "category": "brothel",
                    "sub_category": "closure",
                },
            },
        )
        scenario_path = repo_root / "agents" / "live_checks" / "mixed.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "mixed",
                "runtime_config": "agents/news/local.yaml",
                "cases": [
                    {
                        "id": "missing-fixture",
                        "type": "fixture_article",
                        "fixture": "tests/fixtures/articles/missing.json",
                    },
                    {
                        "id": "good-fixture",
                        "type": "fixture_article",
                        "fixture": "tests/fixtures/articles/good.json",
                    },
                ],
            },
        )

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.live_checks.runner.load_config",
            lambda _path: Config(classifier={"model": "mock-model"}),
        )
        monkeypatch.setattr(
            "denbust.live_checks.runner.create_classifier",
            lambda **_kwargs: FakeClassifier(_build_matching_classification()),
        )

        report = run_live_check_scenario_sync(scenario_path)

        assert report.overall_status == "failed"
        assert len(report.case_results) == 2
        assert report.case_results[0].passed is False
        assert report.case_results[1].passed is True

    def test_missing_api_key_writes_failed_report_bundle(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        repo_root = _init_repo(tmp_path)
        fixture_path = repo_root / "tests" / "fixtures" / "articles" / "case.json"
        _write_fixture(
            fixture_path,
            {
                "url": "https://example.com/case",
                "title": "פשיטה",
                "snippet": "תקציר",
                "date": "2026-04-07T12:00:00+00:00",
                "source_name": "test",
            },
        )
        scenario_path = repo_root / "agents" / "live_checks" / "missing-key.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "missing-key",
                "runtime_config": "agents/news/local.yaml",
                "cases": [
                    {
                        "id": "fixture-case",
                        "type": "fixture_article",
                        "fixture": "tests/fixtures/articles/case.json",
                    }
                ],
            },
        )

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setattr(
            "denbust.live_checks.runner.load_config",
            lambda _path: Config(classifier={"model": "mock-model"}),
        )

        report = run_live_check_scenario_sync(scenario_path)

        output_dir = Path(report.output_dir)
        assert report.overall_status == "failed"
        assert report.errors == ["ANTHROPIC_API_KEY environment variable not set"]
        assert (output_dir / "report.json").exists()
        assert (output_dir / "report.md").exists()

    def test_live_source_case_writes_candidates_artifact_and_passes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        repo_root = _init_repo(tmp_path)
        scenario_path = repo_root / "agents" / "live_checks" / "live-source.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "live-source",
                "runtime_config": "agents/news/local.yaml",
                "cases": [
                    {
                        "id": "walla-live",
                        "type": "live_source_article",
                        "source_name": "walla",
                        "keywords": ["בית בושת"],
                        "match_title_contains": "בית בושת",
                        "expected": {
                            "relevant": True,
                            "enforcement_related": True,
                            "category": "brothel",
                            "sub_category": "closure",
                        },
                    }
                ],
            },
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/live"),
            title="פשיטה על בית בושת",
            snippet="המשטרה ביצעה פשיטה",
            date=datetime(2026, 4, 7, tzinfo=UTC),
            source_name="walla",
        )

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.live_checks.runner.load_config",
            lambda _path: Config(
                classifier={"model": "mock-model"},
                sources=[SourceConfig(name="walla", type=SourceType.SCRAPER)],
            ),
        )
        monkeypatch.setattr(
            "denbust.live_checks.runner.create_classifier",
            lambda **_kwargs: FakeClassifier(_build_matching_classification()),
        )
        monkeypatch.setattr(
            "denbust.live_checks.runner.create_sources",
            lambda _config: [FakeSource("walla", [article])],
        )

        report = run_live_check_scenario_sync(scenario_path)

        output_dir = Path(report.output_dir)
        assert report.overall_status == "passed"
        assert report.case_results[0].passed is True
        assert (output_dir / "artifacts" / "walla-live.candidates.json").exists()

    def test_gitignore_includes_live_checks_directory(self) -> None:
        repo_root = _find_repo_root(Path(__file__).resolve().parent)
        gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8")
        assert ".live_checks/" in gitignore

    def test_helper_paths_and_datetime_normalization(self, tmp_path: Path) -> None:
        repo_root = _init_repo(tmp_path)
        nested = repo_root / "agents" / "live_checks"
        nested.mkdir(parents=True)

        assert _find_repo_root(nested) == repo_root
        assert _resolve_repo_path(repo_root, "agents/live_checks/test.yaml") == (
            repo_root / "agents" / "live_checks" / "test.yaml"
        )

        absolute = tmp_path / "absolute.yaml"
        assert _resolve_repo_path(repo_root, str(absolute)) == absolute

        naive = _normalize_fixture_datetime("2026-04-07T12:00:00")
        assert naive.tzinfo == UTC
        aware = _normalize_fixture_datetime("2026-04-07T15:00:00+03:00")
        assert aware == datetime(2026, 4, 7, 12, 0, tzinfo=UTC)

    def test_find_repo_root_raises_when_missing(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            _find_repo_root(tmp_path)

    def test_load_fixture_article_without_expected_classification(self, tmp_path: Path) -> None:
        fixture = tmp_path / "case.json"
        _write_fixture(
            fixture,
            {
                "url": "https://example.com/no-expected",
                "title": "כותרת",
                "snippet": "תקציר",
                "date": "2026-04-07T12:00:00",
                "source_name": "test",
            },
        )

        article, expected, payload = _load_fixture_article(fixture)

        assert article.source_name == "test"
        assert article.date.tzinfo == UTC
        assert expected is None
        assert payload["title"] == "כותרת"

    def test_compare_expected_and_render_markdown_cover_mismatch_branches(self) -> None:
        expected = ExpectedClassification(
            relevant=True,
            enforcement_related=True,
            category="brothel",
            sub_category="closure",
        )
        actual = ActualClassification(
            relevant=False,
            enforcement_related=False,
            category="prostitution",
            sub_category=None,
            confidence="low",
        )

        passed, notes = _compare_expected(expected, actual)

        assert passed is False
        assert len(notes) == 4

        case = CaseResult(
            case_id="case-1",
            case_type="fixture_article",
            passed=False,
            source_name="walla",
            url="https://example.com/article",
            title="כותרת",
            snippet="תקציר",
            expected=expected,
            actual=actual,
            notes=notes,
            artifact_paths=["/tmp/artifact.json"],
            error="boom",
        )
        markdown = _render_case_markdown(case)
        assert "- Error: boom" in markdown
        assert "- Note: relevant expected True but got False" in markdown
        assert "- Artifact: /tmp/artifact.json" in markdown

        report = SimpleNamespace(
            scenario_name="scenario",
            runtime_config_path="agents/news/local.yaml",
            model_name="mock-model",
            output_dir="/tmp/out",
            started_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
            finished_at=datetime(2026, 4, 7, 12, 1, tzinfo=UTC),
            errors=["top-level error"],
            case_results=[case],
            overall_status="failed",
        )
        rendered = render_live_check_markdown(report)
        assert "- Errors:" in rendered
        assert "top-level error" in rendered

    @pytest.mark.asyncio
    async def test_capture_live_source_payload_and_live_source_url_selection(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        artifact_path = tmp_path / "captured.html"

        class FakeResponse:
            text = "<html>ok</html>"

            def raise_for_status(self) -> None:
                return None

        class FakeClient:
            async def __aenter__(self) -> FakeClient:
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

            async def get(self, target_url: str) -> FakeResponse:
                assert target_url == "https://example.com/raw"
                return FakeResponse()

        monkeypatch.setattr(
            "denbust.live_checks.runner.httpx.AsyncClient", lambda **_: FakeClient()
        )

        captured = await _capture_live_source_payload(
            target_url="https://example.com/raw",
            artifact_path=artifact_path,
        )
        assert Path(captured).read_text(encoding="utf-8") == "<html>ok</html>"

        article = RawArticle(
            url=HttpUrl("https://example.com/article?x=1"),
            title="פשיטה על בית בושת",
            snippet="המשטרה פשטה",
            date=datetime(2026, 4, 7, tzinfo=UTC),
            source_name="walla",
        )
        case = LiveSourceArticleCaseConfig(
            id="live-url",
            type="live_source_article",
            source_name="walla",
            expected=ExpectedClassification(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
            ),
            match_url="https://example.com/article?x=1",
            artifact_capture_url="https://example.com/raw",
            artifact_filename="../escaped.html",
        )

        result = await __import__(
            "denbust.live_checks.runner", fromlist=["_execute_live_source_article_case"]
        )._execute_live_source_article_case(  # type: ignore[attr-defined]
            case,
            classifier=FakeClassifier(_build_matching_classification()),
            sources_by_name={"walla": FakeSource("walla", [article])},
            artifacts_dir=tmp_path,
            runtime_config=Config(keywords=["זנות"]),
        )

        assert result.passed is True
        assert len(result.artifact_paths) == 2
        assert any(path.endswith("/escaped.html") for path in result.artifact_paths)
        assert not any(".." in path for path in result.artifact_paths)

    @pytest.mark.asyncio
    async def test_live_source_artifact_filename_falls_back_for_dot_segments(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        class FakeResponse:
            text = "<html>ok</html>"

            def raise_for_status(self) -> None:
                return None

        class FakeClient:
            async def __aenter__(self) -> FakeClient:
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

            async def get(self, target_url: str) -> FakeResponse:
                assert target_url == "https://example.com/raw"
                return FakeResponse()

        monkeypatch.setattr("denbust.live_checks.runner.httpx.AsyncClient", lambda **_: FakeClient())

        case = LiveSourceArticleCaseConfig(
            id="live-dot",
            type="live_source_article",
            source_name="walla",
            expected=ExpectedClassification(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
            ),
            match_title_contains="בית בושת",
            artifact_capture_url="https://example.com/raw",
            artifact_filename="..",
        )
        article = RawArticle(
            url=HttpUrl("https://example.com/live"),
            title="פשיטה על בית בושת",
            snippet="המשטרה פשטה",
            date=datetime(2026, 4, 7, tzinfo=UTC),
            source_name="walla",
        )

        result = await __import__("denbust.live_checks.runner", fromlist=["_execute_live_source_article_case"])._execute_live_source_article_case(  # type: ignore[attr-defined]
            case,
            classifier=FakeClassifier(_build_matching_classification()),
            sources_by_name={"walla": FakeSource("walla", [article])},
            artifacts_dir=tmp_path,
            runtime_config=Config(keywords=["זנות"]),
        )

        assert any(path.endswith("/live-dot.html") for path in result.artifact_paths)

    @pytest.mark.asyncio
    async def test_live_source_case_failure_branches(
        self,
        tmp_path: Path,
    ) -> None:
        runner = __import__(
            "denbust.live_checks.runner", fromlist=["_execute_live_source_article_case"]
        )
        case = LiveSourceArticleCaseConfig(
            id="missing-source",
            type="live_source_article",
            source_name="walla",
            expected=ExpectedClassification(
                relevant=True,
                enforcement_related=True,
                category="brothel",
                sub_category="closure",
            ),
            match_title_contains="בית בושת",
            notes="case note",
        )

        missing_source = await runner._execute_live_source_article_case(
            case,
            classifier=FakeClassifier(_build_matching_classification()),
            sources_by_name={},
            artifacts_dir=tmp_path,
            runtime_config=Config(keywords=["זנות"]),
        )
        assert missing_source.passed is False
        assert missing_source.error == "Unknown or disabled source: walla"

        no_match = await runner._execute_live_source_article_case(
            case,
            classifier=FakeClassifier(_build_matching_classification()),
            sources_by_name={
                "walla": FakeSource(
                    "walla",
                    [
                        RawArticle(
                            url=HttpUrl("https://example.com/other"),
                            title="לא קשור",
                            snippet="תקציר",
                            date=datetime(2026, 4, 7, tzinfo=UTC),
                            source_name="walla",
                        )
                    ],
                )
            },
            artifacts_dir=tmp_path,
            runtime_config=Config(keywords=["זנות"]),
        )
        assert no_match.passed is False
        assert no_match.error == "No live source article matched the configured selector"
        assert len(no_match.artifact_paths) == 1

    def test_run_live_check_scenario_handles_runtime_config_load_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        repo_root = _init_repo(tmp_path)
        scenario_path = repo_root / "agents" / "live_checks" / "broken-config.yaml"
        _write_scenario(
            scenario_path,
            {
                "name": "broken-config",
                "runtime_config": "agents/news/local.yaml",
                "cases": [],
            },
        )

        monkeypatch.setattr(
            "denbust.live_checks.runner.load_config",
            lambda _path: (_ for _ in ()).throw(ValueError("bad config")),
        )

        report = run_live_check_scenario_sync(scenario_path)

        assert report.overall_status == "failed"
        assert report.errors == ["Failed to load runtime config: bad config"]
        assert (Path(report.output_dir) / "report.json").exists()

    def test_to_actual_classification_exposes_string_values(self) -> None:
        actual = _to_actual_classification(_build_matching_classification())
        assert actual.category == "brothel"
        assert actual.sub_category == "closure"
