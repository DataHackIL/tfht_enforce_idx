"""Unit tests for source-health diagnostics."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from denbust.config import Config
from denbust.diagnostics import source_health
from denbust.diagnostics.source_health import (
    DiagnosticStatus,
    FailureBucket,
    ProbeCheck,
    _FetchResult,
)
from denbust.sources.base import Source


def _config_with_state_root(state_root: Path) -> Config:
    return Config.model_validate(
        {
            "days": 21,
            "sources": [
                {
                    "name": "ynet",
                    "type": "rss",
                    "url": "https://www.ynet.co.il/Integration/StoryRss2.xml",
                },
                {"name": "maariv", "type": "scraper"},
                {"name": "ice", "type": "scraper"},
            ],
            "store": {"state_root": str(state_root)},
        }
    )


def _write_summary(logs_dir: Path, stem: str, payload: dict[str, object]) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"{stem}.summary.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_build_artifact_check_marks_zero_result_source_as_warn(tmp_path: Path) -> None:
    config = _config_with_state_root(tmp_path)
    logs_dir = config.state_paths.logs_dir
    summary_path = _write_summary(
        logs_dir,
        "2026-04-01T00-00-00-000000Z",
        {
            "run_timestamp": "2026-04-01T00:00:00Z",
            "source_summaries": [
                {
                    "source_name": "ynet",
                    "raw_article_count": 0,
                    "had_error": False,
                    "returned_zero_results": True,
                    "error_messages": [],
                }
            ],
            "problems": {"zero_result_sources": ["ynet"]},
            "suspicions": ["sources_returned_zero_results"],
            "warnings": [],
            "errors": [],
        },
    )

    latest_path, payload = source_health._load_latest_debug_summary(config)
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=latest_path,
        latest_summary_payload=payload,
    )

    assert latest_path == summary_path
    assert check.status == DiagnosticStatus.WARN
    assert "zero results" in check.summary


def test_build_artifact_check_returns_skip_when_summary_missing() -> None:
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=None,
        latest_summary_payload=None,
    )

    assert check.status == DiagnosticStatus.SKIP
    assert "No ingest debug summary" in check.summary


@pytest.mark.asyncio
async def test_probe_ynet_distinguishes_feed_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del url, user_agent
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    result = await source_health._probe_ynet(
        source_cfg=SimpleNamespace(url="https://example.com/rss"),
        days=21,
        sample_keywords=["זנות"],
    )

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.FEED_FETCH_FAILED


@pytest.mark.asyncio
async def test_probe_ynet_distinguishes_stale_feed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="application/rss+xml",
            text="<rss />",
        )

    def fake_parse(text: str) -> object:
        del text
        return SimpleNamespace(
            entries=[{"link": "https://example.com/1", "title": "old", "published": "Mon, 01 Jan 2024 00:00:00 GMT"}],
            bozo=False,
            bozo_exception=None,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    monkeypatch.setattr(source_health.feedparser, "parse", fake_parse)

    result = await source_health._probe_ynet(
        source_cfg=SimpleNamespace(url="https://example.com/rss"),
        days=21,
        sample_keywords=["זנות"],
    )

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.FEED_EMPTY_OR_STALE


@pytest.mark.asyncio
async def test_probe_ynet_distinguishes_keyword_zeroing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="application/rss+xml",
            text="<rss />",
        )

    recent = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    def fake_parse(text: str) -> object:
        del text
        return SimpleNamespace(
            entries=[
                {"link": "https://example.com/1", "title": "חדשות כלליות", "summary": "ללא התאמה", "published": recent}
            ],
            bozo=False,
            bozo_exception=None,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    monkeypatch.setattr(source_health.feedparser, "parse", fake_parse)

    result = await source_health._probe_ynet(
        source_cfg=SimpleNamespace(url="https://example.com/rss"),
        days=21,
        sample_keywords=["זנות"],
    )

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS


@pytest.mark.asyncio
async def test_probe_maariv_distinguishes_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del url, user_agent
        raise httpx.HTTPStatusError(
            "fail",
            request=httpx.Request("GET", "https://example.com"),
            response=httpx.Response(500),
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_maariv(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.HTTP_FETCH_FAILED


@pytest.mark.asyncio
async def test_probe_maariv_distinguishes_selector_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text="<html><body><div>nothing here</div></body></html>",
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_maariv(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.SELECTOR_DRIFT_SUSPECTED


@pytest.mark.asyncio
async def test_probe_maariv_distinguishes_keyword_zeroing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <article class="category-article">
      <a class="category-article-link" href="/news/article-123">לינק</a>
      <h2>חדשות כלליות</h2>
      <p>ללא התאמה</p>
      <time datetime="2099-01-01T00:00:00+00:00"></time>
    </article>
    """

    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_maariv(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS


@pytest.mark.asyncio
async def test_probe_ice_distinguishes_missing_results_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text="<html><body><h1>עמוד אחר</h1></body></html>",
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_ice(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.SELECTOR_DRIFT_SUSPECTED


@pytest.mark.asyncio
async def test_probe_ice_distinguishes_parse_zeroed_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <html>
      <body>
        <h1>תוצאות חיפוש</h1>
        <article><ul><li><div>missing article link</div></li></ul></article>
      </body>
    </html>
    """

    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_ice(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.PARSE_ZEROED_RESULTS


@pytest.mark.asyncio
async def test_probe_ice_reports_successful_page(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <h1>תוצאות חיפוש</h1>
        <article>
          <ul>
            <li>
              <a href="/article/123">בית בושת אותר</a>
              <span>01/01/2099 12:00</span>
            </li>
          </ul>
        </article>
      </body>
    </html>
    """

    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_ice(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


class _FakeSource(Source):
    def __init__(self, name: str, count: int) -> None:
        self._name = name
        self._count = count

    @property
    def name(self) -> str:
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[object]:
        del days, keywords
        return [object()] * self._count


def test_run_source_diagnostics_filters_requested_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
days: 21
sources:
  - name: ynet
    type: rss
    url: https://www.ynet.co.il/Integration/StoryRss2.xml
    enabled: true
  - name: maariv
    type: scraper
    enabled: true
  - name: ice
    type: scraper
    enabled: true
store:
  state_root: data
        """.strip(),
        encoding="utf-8",
    )

    async def fake_probe_source(
        *,
        source_name: str,
        source: Source | None,
        source_cfg: object,
        days: int,
        sample_keywords: list[str],
    ) -> source_health.SourceDiagnosticResult:
        del source, source_cfg, days, sample_keywords
        return source_health.SourceDiagnosticResult(
            source_name=source_name,
            status=DiagnosticStatus.OK,
            live_status=DiagnosticStatus.OK,
            checks=[ProbeCheck(name="live_probe", status=DiagnosticStatus.OK, summary="ok")],
        )

    monkeypatch.setattr(source_health, "_probe_source", fake_probe_source)
    monkeypatch.setattr(
        source_health,
        "create_sources",
        lambda config: [_FakeSource(source_cfg.name, 1) for source_cfg in config.sources if source_cfg.enabled],
    )

    report = source_health.run_source_diagnostics(
        config_path=config_path,
        source_names=["ice"],
        include_artifacts=False,
        include_live=True,
    )

    assert [result.source_name for result in report.results] == ["ice"]

