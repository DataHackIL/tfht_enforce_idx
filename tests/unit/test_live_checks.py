"""Unit tests for the tracked live-check runner."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from pydantic import HttpUrl

from denbust.config import Config, SourceConfig, SourceType
from denbust.data_models import Category, ClassificationResult, RawArticle, SubCategory
from denbust.live_checks.runner import (
    LiveSourceArticleCaseConfig,
    load_live_check_scenario,
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
        gitignore = Path(".gitignore").read_text(encoding="utf-8")
        assert ".live_checks/" in gitignore
