"""Tracked live-check runner for verification scenarios."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

import httpx
import yaml
from pydantic import BaseModel, Field, HttpUrl, TypeAdapter

from denbust.classifier.relevance import Classifier, create_classifier
from denbust.config import Config, load_config
from denbust.data_models import ClassificationResult, RawArticle
from denbust.news_items.normalize import canonicalize_news_url
from denbust.pipeline import create_sources
from denbust.sources.base import Source

REQUIRED_ENV_VARS = ["ANTHROPIC_API_KEY"]


class ExpectedClassification(BaseModel):
    """Expected labels for a live-check case."""

    relevant: bool
    enforcement_related: bool = False
    category: str | None = None
    sub_category: str | None = None
    confidence: str | None = None


class FixtureArticleCaseConfig(BaseModel):
    """A live-check case backed by a tracked article fixture."""

    id: str
    type: Literal["fixture_article"]
    fixture: str
    notes: str | None = None
    store_case_payload: bool = True


class LiveSourceArticleCaseConfig(BaseModel):
    """A live-check case that fetches live source candidates and selects one."""

    id: str
    type: Literal["live_source_article"]
    source_name: str
    expected: ExpectedClassification
    keywords: list[str] = Field(default_factory=list)
    days: int = Field(default=30, ge=1)
    match_url: str | None = None
    match_title_contains: str | None = None
    artifact_capture_url: str | None = None
    artifact_filename: str | None = None
    notes: str | None = None


CaseConfig = Annotated[
    FixtureArticleCaseConfig | LiveSourceArticleCaseConfig,
    Field(discriminator="type"),
]
CASE_CONFIG_ADAPTER: TypeAdapter[CaseConfig] = TypeAdapter(CaseConfig)


class LiveCheckScenario(BaseModel):
    """Tracked scenario configuration for a live-check run."""

    name: str
    description: str | None = None
    runtime_config: str = "agents/news/local.yaml"
    output_root: str = ".live_checks"
    cases: list[CaseConfig]


class ActualClassification(BaseModel):
    """Actual classifier output recorded in a case result."""

    relevant: bool
    enforcement_related: bool
    category: str
    sub_category: str | None = None
    confidence: str


class CaseResult(BaseModel):
    """Result of executing a single live-check case."""

    case_id: str
    case_type: str
    passed: bool
    source_name: str | None = None
    url: str | None = None
    title: str | None = None
    snippet: str | None = None
    expected: ExpectedClassification | None = None
    actual: ActualClassification | None = None
    notes: list[str] = Field(default_factory=list)
    artifact_paths: list[str] = Field(default_factory=list)
    error: str | None = None


class LiveCheckReport(BaseModel):
    """Machine-readable report for a live-check run."""

    scenario_name: str
    scenario_config_path: str
    runtime_config_path: str
    output_dir: str
    model_name: str | None
    required_env_vars: list[str]
    started_at: datetime
    finished_at: datetime
    overall_status: Literal["passed", "failed"]
    case_results: list[CaseResult]
    errors: list[str] = Field(default_factory=list)


def _find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError(f"Could not find repository root from {start}")


def _resolve_repo_path(repo_root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return repo_root / path


def _normalize_fixture_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_fixture_article(
    path: Path,
) -> tuple[RawArticle, ExpectedClassification | None, dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    article = RawArticle(
        url=HttpUrl(payload["url"]),
        title=payload["title"],
        snippet=payload["snippet"],
        date=_normalize_fixture_datetime(payload["date"]),
        source_name=payload["source_name"],
    )
    expected_payload = payload.get("expected_classification")
    expected = (
        ExpectedClassification.model_validate(expected_payload)
        if expected_payload is not None
        else None
    )
    return article, expected, payload


def load_live_check_scenario(path: Path) -> LiveCheckScenario:
    """Load a tracked scenario config from YAML."""
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "cases" in data:
        data["cases"] = [CASE_CONFIG_ADAPTER.validate_python(case) for case in data["cases"]]
    return LiveCheckScenario.model_validate(data)


def _build_output_dir(
    repo_root: Path, output_root: str, scenario_name: str, started_at: datetime
) -> Path:
    stamp = started_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return repo_root / output_root / scenario_name / stamp


def _to_actual_classification(classification: ClassificationResult) -> ActualClassification:
    return ActualClassification(
        relevant=classification.relevant,
        enforcement_related=classification.enforcement_related,
        category=classification.category.value,
        sub_category=(
            classification.sub_category.value if classification.sub_category is not None else None
        ),
        confidence=classification.confidence,
    )


def _compare_expected(
    expected: ExpectedClassification,
    actual: ActualClassification,
) -> tuple[bool, list[str]]:
    notes: list[str] = []
    if actual.relevant != expected.relevant:
        notes.append(f"relevant expected {expected.relevant} but got {actual.relevant}")
    if actual.enforcement_related != expected.enforcement_related:
        notes.append(
            "enforcement_related expected "
            f"{expected.enforcement_related} but got {actual.enforcement_related}"
        )
    if expected.category is not None and actual.category != expected.category:
        notes.append(f"category expected {expected.category} but got {actual.category}")
    if expected.sub_category != actual.sub_category:
        notes.append(
            f"sub_category expected {expected.sub_category!r} but got {actual.sub_category!r}"
        )
    return (not notes), notes


def _render_case_markdown(case: CaseResult) -> str:
    status = "PASS" if case.passed else "FAIL"
    lines = [f"### {case.case_id} [{status}]"]
    if case.title:
        lines.append(f"- Title: {case.title}")
    if case.source_name:
        lines.append(f"- Source: {case.source_name}")
    if case.url:
        lines.append(f"- URL: {case.url}")
    if case.expected is not None:
        lines.append(f"- Expected: {case.expected.model_dump()}")
    if case.actual is not None:
        lines.append(f"- Actual: {case.actual.model_dump()}")
    if case.error:
        lines.append(f"- Error: {case.error}")
    for note in case.notes:
        lines.append(f"- Note: {note}")
    for artifact in case.artifact_paths:
        lines.append(f"- Artifact: {artifact}")
    return "\n".join(lines)


def render_live_check_markdown(report: LiveCheckReport) -> str:
    """Render a compact Markdown summary for a live-check run."""
    lines = [
        f"# Live Check: {report.scenario_name}",
        "",
        f"- Overall status: {report.overall_status}",
        f"- Runtime config: {report.runtime_config_path}",
        f"- Model: {report.model_name or 'unavailable'}",
        f"- Output directory: {report.output_dir}",
        f"- Started: {report.started_at.isoformat()}",
        f"- Finished: {report.finished_at.isoformat()}",
    ]
    if report.errors:
        lines.append("- Errors:")
        lines.extend(f"  - {error}" for error in report.errors)
    for case in report.case_results:
        lines.extend(["", _render_case_markdown(case)])
    return "\n".join(lines) + "\n"


def _write_report_bundle(report: LiveCheckReport) -> None:
    output_dir = Path(report.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(report.model_dump_json(indent=2), encoding="utf-8")
    (output_dir / "report.md").write_text(render_live_check_markdown(report), encoding="utf-8")


async def _execute_fixture_article_case(
    case: FixtureArticleCaseConfig,
    *,
    repo_root: Path,
    classifier: Classifier,
    artifacts_dir: Path,
) -> CaseResult:
    fixture_path = _resolve_repo_path(repo_root, case.fixture)
    article, expected, payload = _load_fixture_article(fixture_path)
    artifact_paths: list[str] = []
    if case.store_case_payload:
        artifact_path = artifacts_dir / f"{case.id}.fixture.json"
        artifact_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        artifact_paths.append(str(artifact_path))

    classification = await classifier.classify(article)
    actual = _to_actual_classification(classification)
    notes = [case.notes] if case.notes else []
    passed = True
    if expected is not None:
        passed, compare_notes = _compare_expected(expected, actual)
        notes.extend(compare_notes)

    return CaseResult(
        case_id=case.id,
        case_type=case.type,
        passed=passed,
        source_name=article.source_name,
        url=str(article.url),
        title=article.title,
        snippet=article.snippet,
        expected=expected,
        actual=actual,
        notes=notes,
        artifact_paths=artifact_paths,
    )


async def _capture_live_source_payload(
    *,
    target_url: str,
    artifact_path: Path,
) -> str:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(target_url)
        response.raise_for_status()
        artifact_path.write_text(response.text, encoding="utf-8")
    return str(artifact_path)


async def _execute_live_source_article_case(
    case: LiveSourceArticleCaseConfig,
    *,
    classifier: Classifier,
    sources_by_name: dict[str, Source],
    artifacts_dir: Path,
    runtime_config: Config,
) -> CaseResult:
    notes = [case.notes] if case.notes else []
    source = sources_by_name.get(case.source_name)
    if source is None:
        return CaseResult(
            case_id=case.id,
            case_type=case.type,
            passed=False,
            expected=case.expected,
            notes=notes,
            error=f"Unknown or disabled source: {case.source_name}",
        )

    keywords = case.keywords or runtime_config.keywords
    articles = await source.fetch(days=case.days, keywords=keywords)
    candidates_payload = [
        {
            "url": str(article.url),
            "canonical_url": canonicalize_news_url(str(article.url)),
            "title": article.title,
            "snippet": article.snippet,
            "source_name": article.source_name,
            "date": article.date.isoformat(),
        }
        for article in articles
    ]
    artifact_paths: list[str] = []
    candidates_artifact = artifacts_dir / f"{case.id}.candidates.json"
    candidates_artifact.write_text(
        json.dumps(candidates_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    artifact_paths.append(str(candidates_artifact))

    selected: RawArticle | None = None
    if case.match_url is not None:
        target_url = canonicalize_news_url(case.match_url)
        selected = next(
            (
                article
                for article in articles
                if canonicalize_news_url(str(article.url)) == target_url
            ),
            None,
        )
    elif case.match_title_contains is not None:
        needle = case.match_title_contains.casefold()
        selected = next(
            (article for article in articles if needle in article.title.casefold()), None
        )

    if selected is None:
        return CaseResult(
            case_id=case.id,
            case_type=case.type,
            passed=False,
            expected=case.expected,
            notes=notes,
            artifact_paths=artifact_paths,
            error="No live source article matched the configured selector",
        )

    if case.artifact_capture_url is not None:
        filename = case.artifact_filename or f"{case.id}.html"
        sanitized_filename = Path(filename).name
        if sanitized_filename in {"", ".", ".."}:
            sanitized_filename = f"{case.id}.html"
        captured_path = await _capture_live_source_payload(
            target_url=case.artifact_capture_url,
            artifact_path=artifacts_dir / sanitized_filename,
        )
        artifact_paths.append(captured_path)

    classification = await classifier.classify(selected)
    actual = _to_actual_classification(classification)
    passed, compare_notes = _compare_expected(case.expected, actual)
    notes.extend(compare_notes)
    return CaseResult(
        case_id=case.id,
        case_type=case.type,
        passed=passed,
        source_name=selected.source_name,
        url=str(selected.url),
        title=selected.title,
        snippet=selected.snippet,
        expected=case.expected,
        actual=actual,
        notes=notes,
        artifact_paths=artifact_paths,
    )


async def run_live_check_scenario(
    scenario_path: Path,
    *,
    output_root: Path | None = None,
) -> LiveCheckReport:
    """Execute a live-check scenario and write a result bundle."""
    resolved_scenario_path = scenario_path.resolve()
    repo_root = _find_repo_root(resolved_scenario_path.parent)
    scenario = load_live_check_scenario(resolved_scenario_path)
    started_at = datetime.now(UTC)
    output_dir = _build_output_dir(
        repo_root=repo_root,
        output_root=str(output_root) if output_root is not None else scenario.output_root,
        scenario_name=scenario.name,
        started_at=started_at,
    )
    artifacts_dir = output_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    runtime_config_path = _resolve_repo_path(repo_root, scenario.runtime_config)
    case_results: list[CaseResult] = []
    errors: list[str] = []
    model_name: str | None = None

    try:
        runtime_config = load_config(runtime_config_path)
        model_name = runtime_config.classifier.model
    except Exception as exc:
        errors.append(f"Failed to load runtime config: {exc}")
        report = LiveCheckReport(
            scenario_name=scenario.name,
            scenario_config_path=str(resolved_scenario_path),
            runtime_config_path=str(runtime_config_path),
            output_dir=str(output_dir),
            model_name=model_name,
            required_env_vars=REQUIRED_ENV_VARS,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            overall_status="failed",
            case_results=case_results,
            errors=errors,
        )
        _write_report_bundle(report)
        return report

    if not runtime_config.anthropic_api_key:
        errors.append("ANTHROPIC_API_KEY environment variable not set")
        report = LiveCheckReport(
            scenario_name=scenario.name,
            scenario_config_path=str(resolved_scenario_path),
            runtime_config_path=str(runtime_config_path),
            output_dir=str(output_dir),
            model_name=model_name,
            required_env_vars=REQUIRED_ENV_VARS,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            overall_status="failed",
            case_results=case_results,
            errors=errors,
        )
        _write_report_bundle(report)
        return report

    classifier = create_classifier(
        api_key=runtime_config.anthropic_api_key,
        model=runtime_config.classifier.model,
        system_prompt=runtime_config.classifier.system_prompt,
        user_prompt_template=runtime_config.classifier.user_prompt_template,
    )
    needs_live_sources = any(
        isinstance(case, LiveSourceArticleCaseConfig) for case in scenario.cases
    )
    sources_by_name: dict[str, Source] = {}
    if needs_live_sources:
        sources_by_name = {source.name: source for source in create_sources(runtime_config)}

    for case in scenario.cases:
        try:
            if isinstance(case, FixtureArticleCaseConfig):
                result = await _execute_fixture_article_case(
                    case,
                    repo_root=repo_root,
                    classifier=classifier,
                    artifacts_dir=artifacts_dir,
                )
            else:
                result = await _execute_live_source_article_case(
                    case,
                    classifier=classifier,
                    sources_by_name=sources_by_name,
                    artifacts_dir=artifacts_dir,
                    runtime_config=runtime_config,
                )
        except Exception as exc:
            result = CaseResult(
                case_id=case.id,
                case_type=case.type,
                passed=False,
                notes=[case.notes] if case.notes else [],
                error=str(exc),
            )
        case_results.append(result)

    overall_status: Literal["passed", "failed"] = (
        "passed" if not errors and all(case.passed for case in case_results) else "failed"
    )
    report = LiveCheckReport(
        scenario_name=scenario.name,
        scenario_config_path=str(resolved_scenario_path),
        runtime_config_path=str(runtime_config_path),
        output_dir=str(output_dir),
        model_name=model_name,
        required_env_vars=REQUIRED_ENV_VARS,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        overall_status=overall_status,
        case_results=case_results,
        errors=errors,
    )
    _write_report_bundle(report)
    return report


def run_live_check_scenario_sync(
    scenario_path: Path,
    *,
    output_root: Path | None = None,
) -> LiveCheckReport:
    """Synchronous wrapper for the live-check runner."""
    return asyncio.run(run_live_check_scenario(scenario_path, output_root=output_root))
