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
    SourceDiagnosticReport,
    SourceDiagnosticResult,
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
                    "url": "https://www.ynet.co.il/Integration/StoryRss190.xml",
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


def test_load_latest_debug_summary_skips_invalid_json_files(tmp_path: Path) -> None:
    config = _config_with_state_root(tmp_path)
    logs_dir = config.state_paths.logs_dir
    valid_summary_path = _write_summary(
        logs_dir,
        "2026-04-01T00-00-00-000000Z",
        {
            "run_timestamp": "2026-04-01T00:00:00Z",
            "source_summaries": [],
        },
    )
    newer_invalid_path = logs_dir / "2026-04-02T00-00-00-000000Z.summary.json"
    newer_invalid_path.write_text("{not json", encoding="utf-8")

    latest_path, payload = source_health._load_latest_debug_summary(config)

    assert latest_path == valid_summary_path
    assert payload == {
        "run_timestamp": "2026-04-01T00:00:00Z",
        "source_summaries": [],
    }


def test_build_artifact_check_returns_skip_when_summary_missing() -> None:
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=None,
        latest_summary_payload=None,
    )

    assert check.status == DiagnosticStatus.SKIP
    assert "No ingest debug summary" in check.summary


def test_build_artifact_check_returns_skip_when_source_missing(tmp_path: Path) -> None:
    summary_path = tmp_path / "latest.summary.json"
    check = source_health._build_artifact_check(
        source_name="ice",
        latest_summary_path=summary_path,
        latest_summary_payload={"source_summaries": []},
    )

    assert check.status == DiagnosticStatus.SKIP
    assert "does not include this source" in check.summary


def test_build_artifact_check_marks_had_error_as_warn(tmp_path: Path) -> None:
    summary_path = tmp_path / "latest.summary.json"
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=summary_path,
        latest_summary_payload={
            "source_summaries": [
                {
                    "source_name": "ynet",
                    "raw_article_count": 0,
                    "had_error": True,
                    "returned_zero_results": False,
                }
            ]
        },
    )

    assert check.status == DiagnosticStatus.WARN
    assert "source error" in check.summary


def test_build_artifact_check_marks_ok_when_articles_present(tmp_path: Path) -> None:
    summary_path = tmp_path / "latest.summary.json"
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=summary_path,
        latest_summary_payload={
            "source_summaries": [
                {
                    "source_name": "ynet",
                    "raw_article_count": 4,
                    "had_error": False,
                    "returned_zero_results": False,
                }
            ]
        },
    )

    assert check.status == DiagnosticStatus.OK
    assert "4 raw articles" in check.summary


def test_build_artifact_check_preserves_invalid_raw_article_count_details(tmp_path: Path) -> None:
    summary_path = tmp_path / "latest.summary.json"
    check = source_health._build_artifact_check(
        source_name="ynet",
        latest_summary_path=summary_path,
        latest_summary_payload={
            "source_summaries": [
                {
                    "source_name": "ynet",
                    "raw_article_count": "unknown",
                    "had_error": False,
                    "returned_zero_results": False,
                }
            ]
        },
    )

    assert check.status == DiagnosticStatus.OK
    assert "0 raw articles" in check.summary
    assert (
        check.details["raw_article_count_warning"]
        == "Artifact raw_article_count was not an integer-compatible value"
    )
    assert check.details["raw_article_count_value"] == "unknown"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, (None, False)),
        (True, (None, True)),
        ("", (None, True)),
        (object(), (None, True)),
    ],
)
def test_coerce_optional_int_handles_edge_cases(
    value: object, expected: tuple[int | None, bool]
) -> None:
    assert source_health._coerce_optional_int(value) == expected


def test_render_source_diagnostic_report_includes_findings_and_empty_case() -> None:
    report = SourceDiagnosticReport(
        config_path="agents/news.yaml",
        days=21,
        sample_keywords=["זנות"],
        artifact_analysis_enabled=True,
        live_probe_enabled=True,
        results=[
            SourceDiagnosticResult(
                source_name="ynet",
                status=DiagnosticStatus.WARN,
                artifact_status=DiagnosticStatus.OK,
                live_status=DiagnosticStatus.WARN,
                failure_bucket=FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS,
                checks=[
                    ProbeCheck(
                        name="live_probe",
                        status=DiagnosticStatus.WARN,
                        summary="warning branch",
                    )
                ],
            ),
            SourceDiagnosticResult(
                source_name="ice",
                status=DiagnosticStatus.OK,
                artifact_status=DiagnosticStatus.OK,
                live_status=DiagnosticStatus.OK,
                checks=[],
            ),
        ],
    )

    rendered = source_health.render_source_diagnostic_report(report)

    assert "Source diagnostics" in rendered
    assert "ynet: warn artifact=ok live=warn bucket=keyword_filter_zeroed_results" in rendered
    assert "warning branch" in rendered

    empty = source_health.render_source_diagnostic_report(
        SourceDiagnosticReport(
            config_path="agents/news.yaml",
            days=21,
            sample_keywords=["זנות"],
            artifact_analysis_enabled=False,
            live_probe_enabled=False,
            results=[],
        )
    )
    assert "- none" in empty


def test_merge_status_and_derive_bucket_and_live_result() -> None:
    assert (
        source_health._merge_status(
            DiagnosticStatus.SKIP,
            DiagnosticStatus.OK,
            DiagnosticStatus.WARN,
        )
        == DiagnosticStatus.WARN
    )
    assert (
        source_health._derive_bucket_from_checks(
            [ProbeCheck(name="a", status=DiagnosticStatus.OK, summary="x", details={})]
        )
        is None
    )
    assert (
        source_health._derive_bucket_from_checks(
            [
                ProbeCheck(
                    name="a",
                    status=DiagnosticStatus.WARN,
                    summary="x",
                    details={"failure_bucket": FailureBucket.UNEXPECTED_REDIRECT.value},
                )
            ]
        )
        == FailureBucket.UNEXPECTED_REDIRECT
    )

    result = source_health._live_result(
        source_name="ynet",
        status=DiagnosticStatus.WARN,
        bucket=FailureBucket.UNEXPECTED_REDIRECT,
        checks=[ProbeCheck(name="probe", status=DiagnosticStatus.WARN, summary="x")],
    )
    assert result.probe_mode == "live_probe"
    assert result.checks[0].details["failure_bucket"] == FailureBucket.UNEXPECTED_REDIRECT.value


def test_is_unexpected_redirect() -> None:
    assert source_health._is_unexpected_redirect(
        "https://other.example.com/feed",
        "https://www.ynet.co.il/Integration/StoryRss190.xml",
    )
    assert source_health._is_unexpected_redirect(
        "https://www.ynet.co.il/other",
        "https://www.ynet.co.il/Integration/StoryRss190.xml",
    )
    assert not source_health._is_unexpected_redirect(
        "https://www.ynet.co.il/Integration/StoryRss190.xml",
        "https://www.ynet.co.il/Integration/StoryRss190.xml",
    )


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
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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
            entries=[
                {
                    "link": "https://example.com/1",
                    "title": "old",
                    "published": "Mon, 01 Jan 2024 00:00:00 GMT",
                }
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

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.FEED_EMPTY_OR_STALE


@pytest.mark.asyncio
async def test_probe_ynet_distinguishes_keyword_zeroing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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
                {
                    "link": "https://example.com/1",
                    "title": "חדשות כלליות",
                    "summary": "ללא התאמה",
                    "published": recent,
                }
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
async def test_probe_ynet_distinguishes_unexpected_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url="https://example.com/redirected",
            status_code=200,
            content_type="application/rss+xml",
            text="<rss />",
        )

    def fake_parse(text: str) -> object:
        del text
        return SimpleNamespace(
            entries=[
                {
                    "link": "https://example.com/1",
                    "title": "זנות",
                    "summary": "זנות",
                    "published": recent,
                }
            ],
            bozo=True,
            bozo_exception=ValueError("bad xml"),
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    monkeypatch.setattr(source_health.feedparser, "parse", fake_parse)

    result = await source_health._probe_ynet(
        source_cfg=SimpleNamespace(url="https://www.ynet.co.il/Integration/StoryRss190.xml"),
        days=21,
        sample_keywords=["זנות"],
    )

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.UNEXPECTED_REDIRECT
    assert result.checks[0].details["bozo_exception"] == "bad xml"


@pytest.mark.asyncio
async def test_probe_ynet_reports_success(monkeypatch: pytest.MonkeyPatch) -> None:
    recent = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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
            entries=[
                {
                    "link": "https://example.com/1",
                    "title": "זנות",
                    "summary": "זנות",
                    "published": recent,
                }
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

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


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
async def test_probe_maariv_uses_source_specific_relaxed_keywords(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <article class="category-article">
      <a class="category-article-link" href="/news/article-123">לינק</a>
      <h2>חשד לבית בושת בבני ברק</h2>
      <p>המשטרה עצרה חשודים.</p>
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

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


@pytest.mark.asyncio
async def test_probe_maariv_distinguishes_unexpected_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <article class="category-article">
      <a class="category-article-link" href="/news/article-123">לינק</a>
      <h2>זנות</h2>
      <p>זנות</p>
      <time datetime="2099-01-01T00:00:00+00:00"></time>
    </article>
    """

    async def fake_fetch(url: str, *, user_agent: str) -> _FetchResult:
        del user_agent
        return _FetchResult(
            requested_url=url,
            final_url="https://example.com/redirect",
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    result = await source_health._probe_maariv(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.UNEXPECTED_REDIRECT


@pytest.mark.asyncio
async def test_probe_maariv_reports_success(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <article class="category-article">
      <a class="category-article-link" href="/news/article-123">לינק</a>
      <h2>זנות</h2>
      <p>זנות</p>
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

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


@pytest.mark.asyncio
async def test_probe_ice_distinguishes_missing_results_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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
async def test_probe_ice_distinguishes_stale_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <html>
      <body>
        <h1>תוצאות חיפוש</h1>
        <article>
          <ul>
            <li>
              <a href="/article/123">בית בושת אותר</a>
              <span>01/01/2025 12:00</span>
            </li>
          </ul>
        </article>
      </body>
    </html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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
    assert result.failure_bucket == FailureBucket.STALE_RESULTS
    assert result.checks[0].details["stale_candidate_count"] == 1


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

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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


@pytest.mark.asyncio
async def test_probe_ice_handles_nested_heading_text(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <h1><span>נמצאו 388</span> <span>תוצאות חיפוש</span> <span>של זנות</span></h1>
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

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
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


@pytest.mark.asyncio
async def test_probe_ice_handles_all_fetch_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del url, user_agent, client
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    result = await source_health._probe_ice(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.HTTP_FETCH_FAILED
    assert "Search fetch failed" in result.checks[0].summary


@pytest.mark.asyncio
async def test_probe_ice_handles_unexpected_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html><body><h1>תוצאות חיפוש</h1><article><ul>
    <li><a href="/article/123">בית בושת אותר</a><span>01/01/2099 12:00</span></li>
    </ul></article></body></html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url="https://example.com/redirect",
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)
    result = await source_health._probe_ice(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.UNEXPECTED_REDIRECT


@pytest.mark.asyncio
async def test_probe_walla_distinguishes_selector_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text="<html><body><div>nothing here</div></body></html>",
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.SELECTOR_DRIFT_SUSPECTED


@pytest.mark.asyncio
async def test_probe_walla_distinguishes_keyword_zeroing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <html>
      <body>
        <li>
          <a href="https://news.walla.co.il/item/3823239">
            <article>
              <h3>חדשות כלליות</h3>
              <p>ללא התאמה</p>
              <span class="pub-date">12:00 01/01/2099</span>
            </article>
          </a>
        </li>
      </body>
    </html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS


@pytest.mark.asyncio
async def test_probe_walla_distinguishes_stale_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <html>
      <body>
        <li>
          <a href="https://news.walla.co.il/item/3823239">
            <article>
              <h3>בית בושת אותר</h3>
              <p>פרטים נוספים</p>
              <span class="pub-date">12:00 01/01/2025</span>
            </article>
          </a>
        </li>
      </body>
    </html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.STALE_RESULTS


@pytest.mark.asyncio
async def test_probe_walla_reports_success(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <li>
          <a href="https://news.walla.co.il/item/3823239">
            <article>
              <h3>בית בושת אותר</h3>
              <p>פרטים נוספים</p>
              <span class="pub-date">12:00 01/01/2099</span>
            </article>
          </a>
        </li>
      </body>
    </html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["בית בושת"])

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


@pytest.mark.asyncio
async def test_probe_walla_handles_all_fetch_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del url, user_agent, client
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.HTTP_FETCH_FAILED


@pytest.mark.asyncio
async def test_probe_walla_handles_unexpected_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <li>
          <a href="https://news.walla.co.il/item/3823239">
            <article>
              <h3>בית בושת אותר</h3>
              <p>פרטים נוספים</p>
              <span class="pub-date">12:00 01/01/2099</span>
            </article>
          </a>
        </li>
      </body>
    </html>
    """

    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url="https://example.com/redirect",
            status_code=200,
            content_type="text/html",
            text=html,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_walla(days=21, sample_keywords=["בית בושת"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.UNEXPECTED_REDIRECT


@pytest.mark.asyncio
async def test_probe_haaretz_distinguishes_keyword_zeroing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text="""
            <html><head><title>search</title></head><body>
              <h2>מציג תוצאות בנושא</h2>
              <article>
                <h3><a href="/news/politics/2026-04-06/ty-article/abc">מתנחלים הקימו מאחז חדש בצפון הגדה - בליווי חיילים</a></h3>
                <div>כתבה פוליטית כללית.</div>
                <time>6 באפריל 2026</time>
              </article>
            </body></html>
            """,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_haaretz(days=21, sample_keywords=["ליווי"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS
    assert result.checks[0].details["entry_count"] == 1


@pytest.mark.asyncio
async def test_probe_haaretz_reports_success(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(
        url: str, *, user_agent: str, client: httpx.AsyncClient | None = None
    ) -> _FetchResult:
        del user_agent, client
        return _FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text="""
            <html><head><title>search</title></head><body>
              <h2>מציג תוצאות בנושא</h2>
              <article>
                <h3><a href="/news/law/2026-04-06/ty-article/abc">המשטרה חשפה שירותי ליווי בדירה בתל אביב</a></h3>
                <div>חשד להפעלת זנות במקום.</div>
                <time>6 באפריל 2026</time>
              </article>
            </body></html>
            """,
        )

    monkeypatch.setattr(source_health, "_fetch_text", fake_fetch)

    result = await source_health._probe_haaretz(days=21, sample_keywords=["ליווי"])

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


class _ExplodingSource(Source):
    def __init__(self, name: str, exc: Exception) -> None:
        self._name = name
        self._exc = exc

    @property
    def name(self) -> str:
        return self._name

    async def fetch(self, days: int, keywords: list[str]) -> list[object]:
        del days, keywords
        raise self._exc


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
    url: https://www.ynet.co.il/Integration/StoryRss190.xml
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
        lambda config: [
            _FakeSource(source_cfg.name, 1) for source_cfg in config.sources if source_cfg.enabled
        ],
    )

    report = source_health.run_source_diagnostics(
        config_path=config_path,
        source_names=["ice"],
        include_artifacts=False,
        include_live=True,
    )

    assert [result.source_name for result in report.results] == ["ice"]


@pytest.mark.asyncio
async def test_probe_source_dispatch_and_missing_source() -> None:
    missing = await source_health._probe_source(
        source_name="unknown",
        source=None,
        source_cfg=SimpleNamespace(name="unknown"),
        days=21,
        sample_keywords=["זנות"],
    )
    assert missing.failure_bucket == FailureBucket.LIVE_PROBE_EXCEPTION
    assert missing.probe_mode is None


@pytest.mark.asyncio
async def test_probe_source_fallback_fetch_paths() -> None:
    warn_result = await source_health._probe_via_fallback_fetch(
        source=_FakeSource("walla", 0),
        days=21,
        sample_keywords=["זנות"],
    )
    assert warn_result.live_status == DiagnosticStatus.WARN
    assert warn_result.failure_bucket == FailureBucket.PARSE_ZEROED_RESULTS

    ok_result = await source_health._probe_via_fallback_fetch(
        source=_FakeSource("walla", 2),
        days=21,
        sample_keywords=["זנות"],
    )
    assert ok_result.live_status == DiagnosticStatus.OK
    assert ok_result.checks[0].details["article_count"] == 2

    skip_result = await source_health._probe_via_fallback_fetch(
        source=_ExplodingSource("mako", RuntimeError("Chromium not installed for Playwright")),
        days=21,
        sample_keywords=["זנות"],
    )
    assert skip_result.live_status == DiagnosticStatus.SKIP
    assert skip_result.failure_bucket is None

    fail_runtime = await source_health._probe_via_fallback_fetch(
        source=_ExplodingSource("mako", RuntimeError("other runtime failure")),
        days=21,
        sample_keywords=["זנות"],
    )
    assert fail_runtime.live_status == DiagnosticStatus.FAIL
    assert fail_runtime.failure_bucket == FailureBucket.LIVE_PROBE_EXCEPTION

    fail_exception = await source_health._probe_via_fallback_fetch(
        source=_ExplodingSource("mako", ValueError("bad value")),
        days=21,
        sample_keywords=["זנות"],
    )
    assert fail_exception.live_status == DiagnosticStatus.FAIL
    assert fail_exception.probe_mode == "fallback_fetch"


def test_run_source_diagnostics_handles_unknown_sources_and_report_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_root = tmp_path / "state"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
days: 21
sources:
  - name: ynet
    type: rss
    url: https://www.ynet.co.il/Integration/StoryRss190.xml
    enabled: true
store:
  state_root: {state_root}
        """.strip(),
        encoding="utf-8",
    )
    state_logs = state_root / "news_items" / "ingest" / "logs"
    _write_summary(
        state_logs,
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
        },
    )

    async def fake_probe_source(**kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(
            source_name=str(kwargs["source_name"]),
            status=DiagnosticStatus.OK,
            live_status=DiagnosticStatus.OK,
            checks=[],
        )

    monkeypatch.setattr(source_health, "_probe_source", fake_probe_source)

    def fake_create_sources(_config: Config) -> list[Source]:
        return [_FakeSource("ynet", 1)]

    monkeypatch.setattr(
        source_health,
        "create_sources",
        fake_create_sources,
    )

    report = source_health.run_source_diagnostics(config_path=config_path)
    assert report.latest_artifact_path is not None
    assert report.results[0].artifact_status == DiagnosticStatus.WARN

    with pytest.raises(ValueError, match="Unknown or disabled sources: missing"):
        source_health.run_source_diagnostics(
            config_path=config_path,
            source_names=["missing"],
        )


@pytest.mark.asyncio
async def test_run_source_diagnostics_async_uses_default_keywords_when_selection_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_root = tmp_path / "state"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
days: 21
keywords: []
sources:
  - name: ynet
    type: rss
    url: https://www.ynet.co.il/Integration/StoryRss190.xml
    enabled: true
store:
  state_root: {state_root}
        """.strip(),
        encoding="utf-8",
    )

    captured_keywords: list[str] = []

    async def fake_probe_source(**kwargs: object) -> SourceDiagnosticResult:
        captured_keywords.extend(kwargs["sample_keywords"])
        return SourceDiagnosticResult(
            source_name=str(kwargs["source_name"]),
            status=DiagnosticStatus.OK,
            live_status=DiagnosticStatus.OK,
            checks=[],
        )

    monkeypatch.setattr(source_health, "_probe_source", fake_probe_source)
    monkeypatch.setattr(source_health, "create_sources", lambda _config: [_FakeSource("ynet", 1)])

    report = await source_health.run_source_diagnostics_async(
        config_path=config_path,
        include_artifacts=False,
        include_live=True,
        sample_keywords=[],
    )

    assert report.sample_keywords == source_health.DEFAULT_KEYWORDS[:3]
    assert captured_keywords == source_health.DEFAULT_KEYWORDS[:3]


@pytest.mark.asyncio
async def test_run_source_diagnostics_rejects_running_loop() -> None:
    with pytest.raises(RuntimeError, match="use run_source_diagnostics_async\\(\\) instead"):
        source_health.run_source_diagnostics(
            config_path=Path("agents/news.yaml"),
            include_artifacts=False,
            include_live=False,
        )


@pytest.mark.asyncio
async def test_probe_source_dispatches_named_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_ynet(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(source_name="ynet", status=DiagnosticStatus.OK)

    async def fake_maariv(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(source_name="maariv", status=DiagnosticStatus.OK)

    async def fake_mako(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(source_name="mako", status=DiagnosticStatus.OK)

    async def fake_haaretz(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(source_name="haaretz", status=DiagnosticStatus.OK)

    async def fake_ice(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(source_name="ice", status=DiagnosticStatus.OK)

    monkeypatch.setattr(source_health, "_probe_ynet", fake_ynet)
    monkeypatch.setattr(source_health, "_probe_maariv", fake_maariv)
    monkeypatch.setattr(source_health, "_probe_mako", fake_mako)
    monkeypatch.setattr(source_health, "_probe_haaretz", fake_haaretz)
    monkeypatch.setattr(source_health, "_probe_ice", fake_ice)

    assert (
        await source_health._probe_source(
            source_name="ynet",
            source=_FakeSource("ynet", 1),
            source_cfg=SimpleNamespace(name="ynet"),
            days=21,
            sample_keywords=["זנות"],
        )
    ).source_name == "ynet"
    assert (
        await source_health._probe_source(
            source_name="maariv",
            source=_FakeSource("maariv", 1),
            source_cfg=SimpleNamespace(name="maariv"),
            days=21,
            sample_keywords=["זנות"],
        )
    ).source_name == "maariv"
    assert (
        await source_health._probe_source(
            source_name="mako",
            source=_FakeSource("mako", 1),
            source_cfg=SimpleNamespace(name="mako"),
            days=21,
            sample_keywords=["זנות"],
        )
    ).source_name == "mako"
    assert (
        await source_health._probe_source(
            source_name="haaretz",
            source=_FakeSource("haaretz", 1),
            source_cfg=SimpleNamespace(name="haaretz"),
            days=21,
            sample_keywords=["זנות"],
        )
    ).source_name == "haaretz"
    assert (
        await source_health._probe_source(
            source_name="ice",
            source=_FakeSource("ice", 1),
            source_cfg=SimpleNamespace(name="ice"),
            days=21,
            sample_keywords=["זנות"],
        )
    ).source_name == "ice"


@pytest.mark.asyncio
async def test_probe_source_uses_fallback_for_unknown_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fallback(**_kwargs: object) -> SourceDiagnosticResult:
        return SourceDiagnosticResult(
            source_name="walla",
            status=DiagnosticStatus.OK,
            live_status=DiagnosticStatus.OK,
            probe_mode="fallback_fetch",
        )

    monkeypatch.setattr(source_health, "_probe_via_fallback_fetch", fake_fallback)
    result = await source_health._probe_source(
        source_name="walla",
        source=_FakeSource("walla", 1),
        source_cfg=SimpleNamespace(name="walla"),
        days=21,
        sample_keywords=["זנות"],
    )

    assert result.probe_mode == "fallback_fetch"


@pytest.mark.asyncio
async def test_probe_mako_reports_parse_zeroed_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeScraper:
        def __init__(self, *, rate_limit_delay_seconds: float = 0.0) -> None:
            del rate_limit_delay_seconds

        async def _open_browser_session(self) -> SimpleNamespace:
            return SimpleNamespace(page=SimpleNamespace(url="https://www.mako.co.il/Search"))

        async def _close_browser_session(self, _session: object) -> None:
            return None

        def _build_search_url(self, keyword: str) -> str:
            return f"https://www.mako.co.il/Search?searchstring_input={keyword}"

        async def _fetch_search_html(self, session: SimpleNamespace, keyword: str) -> str:
            session.page.url = self._build_search_url(keyword)
            return "<html><body><div>לא נמצאו תוצאות</div></body></html>"

        async def _fetch_section_html(self, session: SimpleNamespace, _url: str) -> str:
            session.page.url = "https://www.mako.co.il/men-men_news"
            return "<html><body><div>empty</div></body></html>"

        def _parse_search_results(self, html: str, cutoff: datetime) -> list[object]:
            del html, cutoff
            return []

        def _parse_article_item(self, item: object, cutoff: datetime) -> None:
            del item, cutoff
            return None

        def _matches_keywords(self, article: object, keywords: list[str]) -> bool:
            del article, keywords
            return False

    monkeypatch.setattr(source_health.mako_source, "MakoScraper", FakeScraper)

    result = await source_health._probe_mako(days=21, sample_keywords=["בית בושת"])

    assert result.live_status == DiagnosticStatus.WARN
    assert result.failure_bucket == FailureBucket.PARSE_ZEROED_RESULTS
    assert {check.name for check in result.checks} == {
        "search:בית בושת",
        "section:men-men_news",
    }


@pytest.mark.asyncio
async def test_probe_mako_reports_section_keyword_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = SimpleNamespace(url="https://www.mako.co.il/men-men_news/Article-123.htm")

    class FakeScraper:
        def __init__(self, *, rate_limit_delay_seconds: float = 0.0) -> None:
            del rate_limit_delay_seconds

        async def _open_browser_session(self) -> SimpleNamespace:
            return SimpleNamespace(page=SimpleNamespace(url="https://www.mako.co.il/Search"))

        async def _close_browser_session(self, _session: object) -> None:
            return None

        def _build_search_url(self, keyword: str) -> str:
            return f"https://www.mako.co.il/Search?searchstring_input={keyword}"

        async def _fetch_search_html(self, session: SimpleNamespace, keyword: str) -> None:
            session.page.url = self._build_search_url(keyword)
            return None

        async def _fetch_section_html(self, session: SimpleNamespace, _url: str) -> str:
            session.page.url = "https://www.mako.co.il/men-men_news"
            return "<html><body><article>candidate</article></body></html>"

        def _parse_search_results(self, html: str | None, cutoff: datetime) -> list[object]:
            del html, cutoff
            return []

        def _parse_article_item(self, item: object, cutoff: datetime) -> object:
            del item, cutoff
            return article

        def _matches_keywords(self, candidate: object, keywords: list[str]) -> bool:
            del keywords
            return candidate is article

    monkeypatch.setattr(source_health.mako_source, "MakoScraper", FakeScraper)

    result = await source_health._probe_mako(days=21, sample_keywords=["בית בושת"])

    assert result.live_status == DiagnosticStatus.OK
    assert result.failure_bucket is None


@pytest.mark.asyncio
async def test_probe_maariv_distinguishes_parse_zeroed_results_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <article class="category-article">
      <a class="category-article-link" href="/news/article-123">לינק</a>
      <h2>זנות</h2>
      <p>זנות</p>
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

    def fake_parse_article_item(_self: object, _item: object, _cutoff: datetime) -> None:
        return None

    monkeypatch.setattr(
        source_health.maariv_source.MaarivScraper,
        "_parse_article_item",
        fake_parse_article_item,
    )

    result = await source_health._probe_maariv(days=21, sample_keywords=["זנות"])

    assert result.live_status == DiagnosticStatus.FAIL
    assert result.failure_bucket == FailureBucket.PARSE_ZEROED_RESULTS


def test_merge_status_returns_skip_for_empty_input() -> None:
    assert source_health._merge_status() == DiagnosticStatus.SKIP


def test_select_sample_keywords_falls_back_to_defaults_for_blank_inputs() -> None:
    assert (
        source_health._select_sample_keywords(["", "   "], None)
        == source_health.DEFAULT_KEYWORDS[:3]
    )
    assert (
        source_health._select_sample_keywords(["זנות"], ["", "   "])
        == source_health.DEFAULT_KEYWORDS[:3]
    )


def test_entry_value_supports_mappings_and_attributes() -> None:
    assert source_health._entry_value({"title": "dict-title"}, "title") == "dict-title"
    assert source_health._entry_value(SimpleNamespace(title="attr-title"), "title") == "attr-title"


def test_probe_rss_entry_date_handles_string_and_struct_time() -> None:
    assert source_health._probe_rss_entry_date(
        {"published": "Mon, 01 Jan 2024 00:00:00 GMT"}
    ) == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    parsed_entry = {
        "published_parsed": (2024, 1, 2, 3, 4, 5, 0, 0, 0),
    }
    assert source_health._probe_rss_entry_date(parsed_entry) == datetime(
        2024, 1, 2, 3, 4, 5, tzinfo=UTC
    )


def test_probe_rss_entry_date_returns_none_for_invalid_values() -> None:
    assert source_health._probe_rss_entry_date({"published": "not-a-date"}) is None
    assert source_health._probe_rss_entry_date({"updated_parsed": object()}) is None


def test_probe_rss_entry_matches_handles_missing_fields_and_description_fallback() -> None:
    cutoff = datetime(2024, 1, 1, tzinfo=UTC)
    assert not source_health._probe_rss_entry_matches({}, cutoff, ["זנות"], source_name="ynet")

    assert source_health._probe_rss_entry_matches(
        {
            "link": "https://example.com/article",
            "title": "בית בושת",
            "description": "<p>זנות</p>",
        },
        cutoff,
        ["זנות"],
        source_name="ynet",
    )


def test_probe_rss_entry_matches_uses_ynet_source_specific_relaxed_keywords() -> None:
    cutoff = datetime(2024, 1, 1, tzinfo=UTC)

    assert source_health._probe_rss_entry_matches(
        {
            "link": "https://www.ynet.co.il/news/article/r111111",
            "title": "חשד לבית בושת בבני ברק: המשטרה עצרה חשודים",
            "description": "<p>המשטרה עצרה חשודים בדירה ששימשה לפי החשד לבית בושת.</p>",
            "published": "Mon, 01 Jan 2099 00:00:00 GMT",
        },
        cutoff,
        ["זנות"],
        source_name="ynet",
    )
    assert not source_health._probe_rss_entry_matches(
        {
            "link": "https://example.com/article",
            "title": "בית בושת",
            "summary": object(),
            "description": object(),
        },
        cutoff,
        ["זנות"],
        source_name="ynet",
    )


@pytest.mark.asyncio
async def test_fetch_text_uses_response_metadata(respx_mock: object) -> None:
    route = respx_mock.get("https://example.com/feed").mock(
        return_value=httpx.Response(
            200,
            text="payload",
            headers={"content-type": "application/rss+xml"},
        )
    )

    result = await source_health._fetch_text("https://example.com/feed", user_agent="ua")

    assert route.called
    assert result.status_code == 200
    assert result.content_type == "application/rss+xml"
    assert result.text == "payload"


@pytest.mark.asyncio
async def test_fetch_text_reuses_supplied_client(respx_mock: object) -> None:
    route = respx_mock.get("https://example.com/shared").mock(
        return_value=httpx.Response(
            200,
            text="shared-payload",
            headers={"content-type": "text/html"},
        )
    )

    async with httpx.AsyncClient(
        headers={"User-Agent": "shared-ua"},
        follow_redirects=True,
    ) as client:
        result = await source_health._fetch_text(
            "https://example.com/shared",
            user_agent="ignored",
            client=client,
        )

    assert route.called
    assert result.text == "shared-payload"
    assert result.content_type == "text/html"
