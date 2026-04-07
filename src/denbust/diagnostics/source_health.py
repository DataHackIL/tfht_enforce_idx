"""Source-health diagnostics for zero-result investigations."""

from __future__ import annotations

import asyncio
import calendar
import json
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

import denbust.sources.ice as ice_source
import denbust.sources.maariv as maariv_source
import denbust.sources.rss as rss_source
from denbust.config import DEFAULT_KEYWORDS, Config, SourceConfig, load_config
from denbust.pipeline import create_sources
from denbust.sources.base import Source


class DiagnosticStatus(StrEnum):
    """Normalized status labels for diagnostics."""

    OK = "ok"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


class DiagnosticOutputFormat(StrEnum):
    """Supported CLI output formats."""

    TEXT = "text"
    JSON = "json"


class FailureBucket(StrEnum):
    """Stable source-failure buckets for diagnosis output."""

    FEED_FETCH_FAILED = "feed_fetch_failed"
    FEED_EMPTY_OR_STALE = "feed_empty_or_stale"
    KEYWORD_FILTER_ZEROED_RESULTS = "keyword_filter_zeroed_results"
    HTTP_FETCH_FAILED = "http_fetch_failed"
    UNEXPECTED_REDIRECT = "unexpected_redirect"
    SELECTOR_DRIFT_SUSPECTED = "selector_drift_suspected"
    PARSE_ZEROED_RESULTS = "parse_zeroed_results"
    LIVE_PROBE_EXCEPTION = "live_probe_exception"


class ProbeCheck(BaseModel):
    """A single diagnostic check and its structured details."""

    name: str
    status: DiagnosticStatus
    summary: str
    details: dict[str, Any] = Field(default_factory=dict)


class SourceDiagnosticResult(BaseModel):
    """Aggregated diagnostic result for one source."""

    source_name: str
    status: DiagnosticStatus
    artifact_status: DiagnosticStatus = DiagnosticStatus.SKIP
    live_status: DiagnosticStatus = DiagnosticStatus.SKIP
    failure_bucket: FailureBucket | None = None
    probe_mode: str | None = None
    checks: list[ProbeCheck] = Field(default_factory=list)


class SourceDiagnosticReport(BaseModel):
    """Full diagnostic report for a source-health run."""

    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    config_path: str
    days: int
    sample_keywords: list[str]
    artifact_analysis_enabled: bool
    live_probe_enabled: bool
    latest_artifact_path: str | None = None
    results: list[SourceDiagnosticResult] = Field(default_factory=list)


class _FetchResult(BaseModel):
    """Minimal HTTP fetch details for source probes."""

    requested_url: str
    final_url: str
    status_code: int
    content_type: str
    text: str


def run_source_diagnostics(
    *,
    config_path: Path,
    source_names: list[str] | None = None,
    days_override: int | None = None,
    include_artifacts: bool = True,
    include_live: bool = True,
    sample_keywords: list[str] | None = None,
) -> SourceDiagnosticReport:
    """Run source diagnostics synchronously for CLI use."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "run_source_diagnostics() cannot be used from a running event loop; "
            "use run_source_diagnostics_async() instead"
        )
    return asyncio.run(
        run_source_diagnostics_async(
            config_path=config_path,
            source_names=source_names,
            days_override=days_override,
            include_artifacts=include_artifacts,
            include_live=include_live,
            sample_keywords=sample_keywords,
        )
    )


async def run_source_diagnostics_async(
    *,
    config_path: Path,
    source_names: list[str] | None = None,
    days_override: int | None = None,
    include_artifacts: bool,
    include_live: bool,
    sample_keywords: list[str] | None,
) -> SourceDiagnosticReport:
    config = load_config(config_path)
    days = days_override if days_override is not None else config.days
    selected_keywords = _select_sample_keywords(config.keywords, sample_keywords)
    enabled_source_configs = [source_cfg for source_cfg in config.sources if source_cfg.enabled]
    enabled_source_names = [source_cfg.name for source_cfg in enabled_source_configs]

    if source_names:
        unknown_sources = [name for name in source_names if name not in enabled_source_names]
        if unknown_sources:
            unknown = ", ".join(sorted(unknown_sources))
            raise ValueError(f"Unknown or disabled sources: {unknown}")
        selected_source_names = source_names
    else:
        selected_source_names = enabled_source_names

    source_map = {source.name: source for source in create_sources(config)}
    config_map = {source_cfg.name: source_cfg for source_cfg in enabled_source_configs}
    report = SourceDiagnosticReport(
        config_path=str(config_path),
        days=days,
        sample_keywords=selected_keywords,
        artifact_analysis_enabled=include_artifacts,
        live_probe_enabled=include_live,
    )

    latest_summary_path, latest_summary_payload = _load_latest_debug_summary(config)
    if latest_summary_path is not None:
        report.latest_artifact_path = str(latest_summary_path)

    for source_name in selected_source_names:
        result = SourceDiagnosticResult(
            source_name=source_name,
            status=DiagnosticStatus.SKIP,
        )
        checks: list[ProbeCheck] = []

        if include_artifacts:
            artifact_check = _build_artifact_check(
                source_name=source_name,
                latest_summary_path=latest_summary_path,
                latest_summary_payload=latest_summary_payload,
            )
            checks.append(artifact_check)
            result.artifact_status = artifact_check.status

        if include_live:
            source = source_map.get(source_name)
            source_cfg = config_map[source_name]
            live_result = await _probe_source(
                source_name=source_name,
                source=source,
                source_cfg=source_cfg,
                days=days,
                sample_keywords=selected_keywords,
            )
            result.live_status = live_result.status
            result.failure_bucket = live_result.failure_bucket
            result.probe_mode = live_result.probe_mode
            checks.extend(live_result.checks)

        result.checks = checks
        result.status = _merge_status(result.artifact_status, result.live_status)
        if result.failure_bucket is None:
            result.failure_bucket = _derive_bucket_from_checks(checks)
        report.results.append(result)

    return report


def render_source_diagnostic_report(report: SourceDiagnosticReport) -> str:
    """Render a compact human-readable source diagnostic report."""
    lines = ["Source diagnostics"]
    for result in report.results:
        bucket = result.failure_bucket.value if result.failure_bucket is not None else "-"
        lines.append(
            f"- {result.source_name}: {result.status.value} "
            f"artifact={result.artifact_status.value} live={result.live_status.value} "
            f"bucket={bucket}"
        )

    findings: list[str] = []
    for result in report.results:
        for check in result.checks:
            if check.status in {DiagnosticStatus.WARN, DiagnosticStatus.FAIL}:
                findings.append(f"- {result.source_name} [{check.name}]: {check.summary}")

    lines.append("")
    lines.append("Key findings")
    if findings:
        lines.extend(findings)
    else:
        lines.append("- none")

    return "\n".join(lines)


def _load_latest_debug_summary(config: Config) -> tuple[Path | None, dict[str, Any] | None]:
    candidates = sorted(config.state_paths.logs_dir.glob("*.summary.json"), reverse=True)
    for path in candidates:
        try:
            with path.open(encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return path, payload
    return None, None


def _coerce_optional_int(value: Any) -> tuple[int | None, bool]:
    if value is None:
        return None, False
    if isinstance(value, bool):
        return None, True
    if isinstance(value, int):
        return value, False
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None, True
        try:
            return int(stripped), False
        except ValueError:
            return None, True
    return None, True


def _build_artifact_check(
    *,
    source_name: str,
    latest_summary_path: Path | None,
    latest_summary_payload: dict[str, Any] | None,
) -> ProbeCheck:
    if latest_summary_path is None or latest_summary_payload is None:
        return ProbeCheck(
            name="latest_artifact",
            status=DiagnosticStatus.SKIP,
            summary="No ingest debug summary found under the configured state root",
        )

    source_summaries = latest_summary_payload.get("source_summaries", [])
    source_summary = next(
        (
            item
            for item in source_summaries
            if isinstance(item, dict) and item.get("source_name") == source_name
        ),
        None,
    )
    if source_summary is None:
        return ProbeCheck(
            name="latest_artifact",
            status=DiagnosticStatus.SKIP,
            summary="Latest artifact does not include this source",
            details={"path": str(latest_summary_path)},
        )

    had_error = bool(source_summary.get("had_error"))
    returned_zero_results = bool(source_summary.get("returned_zero_results"))
    raw_article_count, raw_article_count_invalid = _coerce_optional_int(
        source_summary.get("raw_article_count")
    )
    if had_error:
        status = DiagnosticStatus.WARN
        summary = "Latest artifact recorded a source error"
    elif returned_zero_results:
        status = DiagnosticStatus.WARN
        summary = "Latest artifact recorded zero results for this source"
    else:
        status = DiagnosticStatus.OK
        summary = f"Latest artifact recorded {raw_article_count or 0} raw articles"

    details = {
        "path": str(latest_summary_path),
        "run_timestamp": latest_summary_payload.get("run_timestamp"),
        "source_summary": source_summary,
        "problems": latest_summary_payload.get("problems", {}),
        "suspicions": latest_summary_payload.get("suspicions", []),
        "warnings": latest_summary_payload.get("warnings", []),
        "errors": latest_summary_payload.get("errors", []),
    }
    if raw_article_count_invalid:
        details["raw_article_count_warning"] = (
            "Artifact raw_article_count was not an integer-compatible value"
        )
        details["raw_article_count_value"] = source_summary.get("raw_article_count")

    return ProbeCheck(
        name="latest_artifact",
        status=status,
        summary=summary,
        details=details,
    )


async def _probe_source(
    *,
    source_name: str,
    source: Source | None,
    source_cfg: SourceConfig,
    days: int,
    sample_keywords: list[str],
) -> SourceDiagnosticResult:
    if source_name == "ynet":
        return await _probe_ynet(source_cfg=source_cfg, days=days, sample_keywords=sample_keywords)
    if source_name == "maariv":
        return await _probe_maariv(days=days, sample_keywords=sample_keywords)
    if source_name == "ice":
        return await _probe_ice(days=days, sample_keywords=sample_keywords)
    if source is None:
        return SourceDiagnosticResult(
            source_name=source_name,
            status=DiagnosticStatus.FAIL,
            live_status=DiagnosticStatus.FAIL,
            failure_bucket=FailureBucket.LIVE_PROBE_EXCEPTION,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary="No source implementation is available for this configured source",
                )
            ],
        )
    return await _probe_via_fallback_fetch(
        source=source, days=days, sample_keywords=sample_keywords
    )


async def _probe_ynet(
    *,
    source_cfg: SourceConfig,
    days: int,
    sample_keywords: list[str],
) -> SourceDiagnosticResult:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    feed_url = source_cfg.url or ""

    try:
        fetch_result = await _fetch_text(feed_url, user_agent=rss_source.USER_AGENT)
    except Exception as exc:
        return _live_result(
            source_name="ynet",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.FEED_FETCH_FAILED,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary=f"RSS fetch failed: {exc}",
                    details={"url": feed_url},
                )
            ],
        )

    parsed = feedparser.parse(fetch_result.text)
    parse_warning = None
    if getattr(parsed, "bozo", 0) and getattr(parsed, "bozo_exception", None) is not None:
        parse_warning = str(parsed.bozo_exception)

    entries = list(getattr(parsed, "entries", []))
    parseable_date_count = 0
    recent_entry_count = 0
    keyword_match_count = 0
    for entry in entries:
        date = _probe_rss_entry_date(entry)
        if date is not None:
            parseable_date_count += 1
            if date >= cutoff:
                recent_entry_count += 1
        if _probe_rss_entry_matches(entry, cutoff, sample_keywords):
            keyword_match_count += 1

    unexpected_redirect = _is_unexpected_redirect(fetch_result.final_url, feed_url)
    details = {
        "requested_url": feed_url,
        "final_url": fetch_result.final_url,
        "status_code": fetch_result.status_code,
        "content_type": fetch_result.content_type,
        "payload_length": len(fetch_result.text),
        "bozo": bool(getattr(parsed, "bozo", 0)),
        "bozo_exception": parse_warning,
        "total_entry_count": len(entries),
        "parseable_date_count": parseable_date_count,
        "recent_entry_count": recent_entry_count,
        "keyword_match_count": keyword_match_count,
    }

    if unexpected_redirect:
        return _live_result(
            source_name="ynet",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.UNEXPECTED_REDIRECT,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.WARN,
                    summary="Feed request redirected to an unexpected URL",
                    details=details,
                )
            ],
        )
    if len(entries) == 0 or recent_entry_count == 0:
        return _live_result(
            source_name="ynet",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.FEED_EMPTY_OR_STALE,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary="Feed is empty or contains no recent entries inside the cutoff window",
                    details=details,
                )
            ],
        )
    if keyword_match_count == 0:
        return _live_result(
            source_name="ynet",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.WARN,
                    summary="Feed has recent items but none match the sampled keywords",
                    details=details,
                )
            ],
        )

    return _live_result(
        source_name="ynet",
        status=DiagnosticStatus.OK,
        checks=[
            ProbeCheck(
                name="live_probe",
                status=DiagnosticStatus.OK,
                summary="Feed returned recent keyword-matching entries",
                details=details,
            )
        ],
    )


async def _probe_maariv(
    *,
    days: int,
    sample_keywords: list[str],
) -> SourceDiagnosticResult:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    scraper = maariv_source.MaarivScraper()
    try:
        fetch_result = await _fetch_text(
            maariv_source.MAARIV_LAW_URL,
            user_agent=maariv_source.USER_AGENT,
        )
    except Exception as exc:
        return _live_result(
            source_name="maariv",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.HTTP_FETCH_FAILED,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary=f"Section fetch failed: {exc}",
                    details={"url": maariv_source.MAARIV_LAW_URL},
                )
            ],
        )

    soup = BeautifulSoup(fetch_result.text, "lxml")
    selector = "article.category-article, article, .article"
    containers = soup.select(selector)
    parsed_articles = [
        article
        for article in (scraper._parse_article_item(item, cutoff) for item in containers)
        if article is not None
    ]
    keyword_matches = [
        article
        for article in parsed_articles
        if scraper._matches_keywords(article, sample_keywords)
    ]
    details = {
        "requested_url": maariv_source.MAARIV_LAW_URL,
        "final_url": fetch_result.final_url,
        "status_code": fetch_result.status_code,
        "content_type": fetch_result.content_type,
        "payload_length": len(fetch_result.text),
        "container_selector": selector,
        "container_count": len(containers),
        "parsed_article_count": len(parsed_articles),
        "keyword_match_count": len(keyword_matches),
    }

    if _is_unexpected_redirect(fetch_result.final_url, maariv_source.MAARIV_LAW_URL):
        return _live_result(
            source_name="maariv",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.UNEXPECTED_REDIRECT,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.WARN,
                    summary="Section request redirected to an unexpected URL",
                    details=details,
                )
            ],
        )
    if len(containers) == 0:
        return _live_result(
            source_name="maariv",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.SELECTOR_DRIFT_SUSPECTED,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary="No article containers were found with the current Maariv selectors",
                    details=details,
                )
            ],
        )
    if len(parsed_articles) == 0:
        return _live_result(
            source_name="maariv",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.PARSE_ZEROED_RESULTS,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary="HTML contains candidate containers but none survive article parsing",
                    details=details,
                )
            ],
        )
    if len(keyword_matches) == 0:
        return _live_result(
            source_name="maariv",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.KEYWORD_FILTER_ZEROED_RESULTS,
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.WARN,
                    summary="Parsed articles were found but none match the sampled keywords",
                    details=details,
                )
            ],
        )

    return _live_result(
        source_name="maariv",
        status=DiagnosticStatus.OK,
        checks=[
            ProbeCheck(
                name="live_probe",
                status=DiagnosticStatus.OK,
                summary="Section probe returned parsed keyword-matching articles",
                details=details,
            )
        ],
    )


async def _probe_ice(
    *,
    days: int,
    sample_keywords: list[str],
) -> SourceDiagnosticResult:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    scraper = ice_source.IceScraper(rate_limit_delay_seconds=0.0)
    checks: list[ProbeCheck] = []
    saw_successful_page = False
    saw_results_container = False
    parsed_article_total = 0
    unexpected_redirect = False

    async with httpx.AsyncClient(
        timeout=30.0,
        headers={"User-Agent": ice_source.USER_AGENT},
        follow_redirects=True,
    ) as client:
        for keyword in sample_keywords:
            page_url = scraper._build_search_url(keyword, page_number=1)
            try:
                fetch_result = await _fetch_text(
                    page_url,
                    user_agent=ice_source.USER_AGENT,
                    client=client,
                )
            except Exception as exc:
                checks.append(
                    ProbeCheck(
                        name=f"search:{keyword}",
                        status=DiagnosticStatus.FAIL,
                        summary=f"Search fetch failed: {exc}",
                        details={"url": page_url},
                    )
                )
                continue

            saw_successful_page = True
            if _is_unexpected_redirect(fetch_result.final_url, page_url):
                unexpected_redirect = True

            soup = BeautifulSoup(fetch_result.text, "lxml")
            results_article = scraper._find_results_article(soup)
            candidates = results_article.select("ul > li") if results_article is not None else []
            if results_article is not None:
                saw_results_container = True

            unparseable_date_count = 0
            parsed_article_count = 0
            for candidate in candidates:
                if scraper._parse_date(candidate.get_text(" ", strip=True)) is None:
                    unparseable_date_count += 1
                if scraper._parse_article_item(candidate, cutoff) is not None:
                    parsed_article_count += 1

            parsed_article_total += parsed_article_count
            if results_article is None:
                status = DiagnosticStatus.FAIL
                summary = "Search page did not expose the expected ICE results container"
            elif parsed_article_count == 0:
                status = DiagnosticStatus.WARN
                summary = "Search page contains candidates but parsing returned zero articles"
            else:
                status = DiagnosticStatus.OK
                summary = "Search page returned parsed articles"

            checks.append(
                ProbeCheck(
                    name=f"search:{keyword}",
                    status=status,
                    summary=summary,
                    details={
                        "requested_url": page_url,
                        "final_url": fetch_result.final_url,
                        "status_code": fetch_result.status_code,
                        "content_type": fetch_result.content_type,
                        "payload_length": len(fetch_result.text),
                        "candidate_count": len(candidates),
                        "parsed_article_count": parsed_article_count,
                        "unparseable_date_count": unparseable_date_count,
                        "has_results_container": results_article is not None,
                    },
                )
            )

    if not saw_successful_page:
        return _live_result(
            source_name="ice",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.HTTP_FETCH_FAILED,
            checks=checks
            or [
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary="All ICE page fetches failed",
                )
            ],
        )
    if unexpected_redirect:
        return _live_result(
            source_name="ice",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.UNEXPECTED_REDIRECT,
            checks=checks,
        )
    if not saw_results_container:
        return _live_result(
            source_name="ice",
            status=DiagnosticStatus.FAIL,
            bucket=FailureBucket.SELECTOR_DRIFT_SUSPECTED,
            checks=checks,
        )
    if parsed_article_total == 0:
        return _live_result(
            source_name="ice",
            status=DiagnosticStatus.WARN,
            bucket=FailureBucket.PARSE_ZEROED_RESULTS,
            checks=checks,
        )

    return _live_result(
        source_name="ice",
        status=DiagnosticStatus.OK,
        checks=checks,
    )


async def _probe_via_fallback_fetch(
    *,
    source: Source,
    days: int,
    sample_keywords: list[str],
) -> SourceDiagnosticResult:
    source_name = source.name
    try:
        articles = await source.fetch(days=days, keywords=sample_keywords)
    except RuntimeError as exc:
        message = str(exc)
        if "playwright" in message.lower() or "chromium" in message.lower():
            status = DiagnosticStatus.SKIP
            bucket = None
        else:
            status = DiagnosticStatus.FAIL
            bucket = FailureBucket.LIVE_PROBE_EXCEPTION
        return SourceDiagnosticResult(
            source_name=source_name,
            status=status,
            live_status=status,
            failure_bucket=bucket,
            probe_mode="fallback_fetch",
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=status,
                    summary=message,
                )
            ],
        )
    except Exception as exc:
        return SourceDiagnosticResult(
            source_name=source_name,
            status=DiagnosticStatus.FAIL,
            live_status=DiagnosticStatus.FAIL,
            failure_bucket=FailureBucket.LIVE_PROBE_EXCEPTION,
            probe_mode="fallback_fetch",
            checks=[
                ProbeCheck(
                    name="live_probe",
                    status=DiagnosticStatus.FAIL,
                    summary=f"Fallback fetch raised an exception: {exc}",
                )
            ],
        )

    if len(articles) == 0:
        status = DiagnosticStatus.WARN
        bucket = FailureBucket.PARSE_ZEROED_RESULTS
        summary = "Fallback source fetch completed but returned zero articles"
    else:
        status = DiagnosticStatus.OK
        bucket = None
        summary = f"Fallback source fetch returned {len(articles)} articles"

    return SourceDiagnosticResult(
        source_name=source_name,
        status=status,
        live_status=status,
        failure_bucket=bucket,
        probe_mode="fallback_fetch",
        checks=[
            ProbeCheck(
                name="live_probe",
                status=status,
                summary=summary,
                details={"article_count": len(articles)},
            )
        ],
    )


async def _fetch_text(
    url: str,
    *,
    user_agent: str,
    client: httpx.AsyncClient | None = None,
) -> _FetchResult:
    if client is None:
        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": user_agent},
            follow_redirects=True,
        ) as temporary_client:
            response = await temporary_client.get(url)
    else:
        response = await client.get(url)
    response.raise_for_status()
    return _FetchResult(
        requested_url=url,
        final_url=str(response.url),
        status_code=response.status_code,
        content_type=response.headers.get("content-type", ""),
        text=response.text,
    )


def _merge_status(*statuses: DiagnosticStatus) -> DiagnosticStatus:
    ordered = [
        DiagnosticStatus.FAIL,
        DiagnosticStatus.WARN,
        DiagnosticStatus.OK,
        DiagnosticStatus.SKIP,
    ]
    for candidate in ordered:
        if candidate in statuses:
            return candidate
    return DiagnosticStatus.SKIP


def _derive_bucket_from_checks(checks: list[ProbeCheck]) -> FailureBucket | None:
    for check in checks:
        bucket_value = check.details.get("failure_bucket")
        if isinstance(bucket_value, str):
            return FailureBucket(bucket_value)
    return None


def _live_result(
    *,
    source_name: str,
    status: DiagnosticStatus,
    bucket: FailureBucket | None = None,
    checks: list[ProbeCheck],
) -> SourceDiagnosticResult:
    if bucket is not None:
        for check in checks:
            check.details.setdefault("failure_bucket", bucket.value)
    return SourceDiagnosticResult(
        source_name=source_name,
        status=status,
        live_status=status,
        failure_bucket=bucket,
        probe_mode="live_probe",
        checks=checks,
    )


def _is_unexpected_redirect(final_url: str, expected_url: str) -> bool:
    expected = _normalized_redirect_target(expected_url)
    final = _normalized_redirect_target(final_url)
    return bool(final[1]) and final != expected


def _normalized_redirect_target(url: str) -> tuple[str, str, str]:
    parts = urlsplit(url)
    return (
        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path or "/",
    )


def _select_sample_keywords(
    config_keywords: list[str],
    sample_keywords: list[str] | None,
) -> list[str]:
    selected_keywords = sample_keywords or config_keywords[:3] or DEFAULT_KEYWORDS[:3]
    filtered_keywords = [keyword for keyword in selected_keywords if keyword.strip()]
    if filtered_keywords:
        return filtered_keywords
    return [keyword for keyword in DEFAULT_KEYWORDS[:3] if keyword.strip()]


def _entry_value(entry: Any, field_name: str) -> Any:
    if isinstance(entry, dict):
        return entry.get(field_name)
    return getattr(entry, field_name, None)


def _probe_rss_entry_date(entry: Any) -> datetime | None:
    for field_name in ("published", "updated", "created"):
        value = _entry_value(entry, field_name)
        if isinstance(value, str) and value:
            try:
                return parsedate_to_datetime(value)
            except (ValueError, TypeError):
                pass

        parsed_value = _entry_value(entry, f"{field_name}_parsed")
        if parsed_value:
            try:
                return datetime.fromtimestamp(calendar.timegm(parsed_value), tz=UTC)
            except (ValueError, TypeError, OverflowError):
                pass

    return None


def _probe_rss_entry_matches(entry: Any, cutoff: datetime, sample_keywords: list[str]) -> bool:
    link = _entry_value(entry, "link")
    title = _entry_value(entry, "title")
    if not isinstance(link, str) or not link or not isinstance(title, str) or not title.strip():
        return False

    date = _probe_rss_entry_date(entry)
    if date is None:
        date = datetime.now(UTC)
    if date < cutoff:
        return False

    snippet = _entry_value(entry, "summary")
    if not isinstance(snippet, str):
        snippet = _entry_value(entry, "description")
    if not isinstance(snippet, str):
        snippet = ""

    haystack = f"{title.strip()} {BeautifulSoup(snippet, 'lxml').get_text(' ', strip=True)}".casefold()
    return any(keyword.casefold() in haystack for keyword in sample_keywords)
