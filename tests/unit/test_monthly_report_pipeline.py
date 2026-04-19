"""Unit tests for monthly-report pipeline orchestration."""

from __future__ import annotations

from collections.abc import Coroutine
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from denbust.config import Config
from denbust.models.runs import RunSnapshot
from denbust.news_items.monthly_report import (
    MONTHLY_REPORT_HQ_ACTIVITY_ENV,
    MONTHLY_REPORT_HQ_ACTIVITY_FILE_ENV,
    MONTHLY_REPORT_JSON_ENV,
    MONTHLY_REPORT_MARKDOWN_ENV,
    MONTHLY_REPORT_MONTH_ENV,
    CaseSummary,
    MonthlyReport,
    MonthlyReportArtifacts,
)
from denbust.pipeline import (
    _run_news_items_monthly_report_with_options,
    run_news_items_monthly_report,
    run_news_items_monthly_report_job,
)


def _config(tmp_path: Path) -> Config:
    return Config(job_name="monthly_report", store={"state_root": tmp_path})


def _snapshot(config: Config, config_path: Path | None = None) -> RunSnapshot:
    return RunSnapshot(
        config_name=config.name,
        dataset_name=config.dataset_name,
        job_name=config.job_name,
        config_path=str(config_path) if config_path is not None else None,
        days_searched=config.days,
    )


def _report(*, stats: dict[str, int]) -> MonthlyReport:
    month = date(2026, 3, 1)
    return MonthlyReport(
        month=month,
        month_key="2026-03",
        month_label_he="מרץ 2026",
        stats=stats,
        stats_labels_he={"administrative_closure": "צו סגירה"},
        selected_cases=[
            CaseSummary(
                headline="Headline row-1",
                narrative="Summary row-1",
                source_url="https://example.com/row-1",
                publication_datetime=datetime(2026, 3, 20),
                taxonomy_category_id="brothels",
                taxonomy_subcategory_id="administrative_closure",
                category="brothel",
                sub_category="closure",
            )
        ],
        hq_activity="HQ activity",
        rendered_markdown="# Report",
    )


@pytest.mark.asyncio
async def test_run_news_items_monthly_report_with_options_persists_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Monthly-report orchestration should persist artifacts and optional copies."""
    config = _config(tmp_path)
    snapshot = _snapshot(config, Path("agents/news/local.yaml"))
    report = _report(stats={"administrative_closure": 2})
    artifacts = MonthlyReportArtifacts(
        output_dir=tmp_path / "publication" / "2026-03",
        markdown_path=tmp_path / "publication" / "2026-03" / "monthly_report.md",
        json_path=tmp_path / "publication" / "2026-03" / "monthly_report.json",
        readme_path=tmp_path / "publication" / "2026-03" / "README.md",
    )
    write_markdown = MagicMock()
    write_json = MagicMock()

    monkeypatch.setattr("denbust.pipeline._build_run_snapshot", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(
        "denbust.pipeline._load_corrected_news_item_records",
        lambda *_args, **_kwargs: ["corrected-record"],
    )
    monkeypatch.setattr(
        "denbust.pipeline.resolve_report_month",
        lambda month_value: date(2026, 3, 1) if month_value == "2026-03" else date(2026, 4, 1),
    )
    monkeypatch.setattr(
        "denbust.pipeline.hq_activity_from_inputs",
        lambda **_kwargs: "HQ activity",
    )

    def fake_generate_monthly_report(
        records: list[object],
        *,
        month: date,
        hq_activity: str | None = None,
    ) -> MonthlyReport:
        assert records == ["corrected-record"]
        assert month == date(2026, 3, 1)
        assert hq_activity == "HQ activity"
        return report

    monkeypatch.setattr("denbust.pipeline.generate_monthly_report", fake_generate_monthly_report)
    monkeypatch.setattr(
        "denbust.pipeline.persist_monthly_report_artifacts",
        lambda *_args, **_kwargs: artifacts,
    )
    monkeypatch.setattr("denbust.pipeline.write_report_copy", write_markdown)
    monkeypatch.setattr("denbust.pipeline.write_report_json_copy", write_json)

    result, returned_report = await _run_news_items_monthly_report_with_options(
        config,
        config_path=Path("agents/news/local.yaml"),
        store=object(),  # type: ignore[arg-type]
        month_value="2026-03",
        markdown_output_path=tmp_path / "report.md",
        json_output_path=tmp_path / "report.json",
        hq_activity="ignored direct text",
        hq_activity_file=tmp_path / "hq.txt",
    )

    assert returned_report is report
    assert result.unified_item_count == 2
    assert result.warnings == []
    assert result.debug_payload == {
        "month": "2026-03",
        "markdown_path": str(artifacts.markdown_path),
        "json_path": str(artifacts.json_path),
    }
    assert result.result_summary == "monthly report built for 2026-03 (1 case(s))"
    write_markdown.assert_called_once_with(tmp_path / "report.md", report.rendered_markdown)
    write_json.assert_called_once_with(tmp_path / "report.json", report)


@pytest.mark.asyncio
async def test_run_news_items_monthly_report_with_options_warns_on_empty_stats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Monthly-report orchestration should warn when no public index rows remain."""
    config = _config(tmp_path)
    snapshot = _snapshot(config)
    report = _report(stats={})
    report.selected_cases = []
    artifacts = MonthlyReportArtifacts(
        output_dir=tmp_path / "publication" / "2026-03",
        markdown_path=tmp_path / "publication" / "2026-03" / "monthly_report.md",
        json_path=tmp_path / "publication" / "2026-03" / "monthly_report.json",
        readme_path=tmp_path / "publication" / "2026-03" / "README.md",
    )

    monkeypatch.setattr("denbust.pipeline._build_run_snapshot", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(
        "denbust.pipeline._load_corrected_news_item_records",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "denbust.pipeline.resolve_report_month",
        lambda _month_value: date(2026, 3, 1),
    )
    monkeypatch.setattr(
        "denbust.pipeline.hq_activity_from_inputs",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "denbust.pipeline.generate_monthly_report",
        lambda *_args, **_kwargs: report,
    )
    monkeypatch.setattr(
        "denbust.pipeline.persist_monthly_report_artifacts",
        lambda *_args, **_kwargs: artifacts,
    )

    result, returned_report = await _run_news_items_monthly_report_with_options(
        config,
        config_path=None,
        store=object(),  # type: ignore[arg-type]
        month_value=None,
        markdown_output_path=None,
        json_output_path=None,
        hq_activity=None,
        hq_activity_file=None,
    )

    assert returned_report is report
    assert result.unified_item_count == 0
    assert result.warnings == ["monthly_report_contains_zero_index_relevant_public_rows"]
    assert result.result_summary == "monthly report built for 2026-03 (0 case(s))"


@pytest.mark.asyncio
async def test_run_news_items_monthly_report_job_reads_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The job wrapper should translate monthly-report environment overrides."""
    config = _config(tmp_path)
    store = object()
    snapshot = _snapshot(config)
    report = _report(stats={"administrative_closure": 1})
    run_mock = AsyncMock(return_value=(snapshot, report))

    monkeypatch.setattr("denbust.pipeline.create_operational_store", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline._run_news_items_monthly_report_with_options", run_mock)
    monkeypatch.setenv(MONTHLY_REPORT_MONTH_ENV, "2026-03")
    monkeypatch.setenv(MONTHLY_REPORT_MARKDOWN_ENV, str(tmp_path / "report.md"))
    monkeypatch.setenv(MONTHLY_REPORT_JSON_ENV, str(tmp_path / "report.json"))
    monkeypatch.setenv(MONTHLY_REPORT_HQ_ACTIVITY_ENV, "HQ text")
    monkeypatch.setenv(MONTHLY_REPORT_HQ_ACTIVITY_FILE_ENV, str(tmp_path / "hq.txt"))

    result = await run_news_items_monthly_report_job(
        config,
        config_path=Path("agents/news/local.yaml"),
    )

    assert result is snapshot
    run_mock.assert_awaited_once_with(
        config,
        config_path=Path("agents/news/local.yaml"),
        store=store,
        month_value="2026-03",
        markdown_output_path=tmp_path / "report.md",
        json_output_path=tmp_path / "report.json",
        hq_activity="HQ text",
        hq_activity_file=tmp_path / "hq.txt",
    )


def test_run_news_items_monthly_report_persists_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """CLI monthly report should persist operational metadata and run snapshots."""
    loaded_config = Config(name="loaded", store={"state_root": tmp_path})
    snapshot = _snapshot(
        loaded_config.model_copy(update={"job_name": "monthly_report"}),
        Path("agents/news/local.yaml"),
    )
    report = _report(stats={"administrative_closure": 1})
    write_snapshot = MagicMock()
    setup_logging = MagicMock()

    class FakeStore:
        def __init__(self) -> None:
            self.metadata: list[RunSnapshot] = []
            self.closed = False

        def write_run_metadata(self, result: RunSnapshot) -> None:
            self.metadata.append(result)

        def close(self) -> None:
            self.closed = True

    store = FakeStore()

    def fake_asyncio_run(coro: object) -> tuple[RunSnapshot, MonthlyReport]:
        cast(Coroutine[Any, Any, object], coro).close()
        return snapshot, report

    monkeypatch.setattr("denbust.pipeline.setup_logging", setup_logging)
    monkeypatch.setattr("denbust.pipeline._load_config_or_exit", lambda _path: loaded_config)
    monkeypatch.setattr("denbust.pipeline.create_operational_store", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.asyncio.run", fake_asyncio_run)
    monkeypatch.setattr("denbust.pipeline.write_run_snapshot", write_snapshot)

    returned = run_news_items_monthly_report(
        config_path=Path("agents/news/local.yaml"),
        month="2026-03",
        output_path=tmp_path / "report.md",
        json_output_path=tmp_path / "report.json",
        hq_activity="HQ text",
        hq_activity_file=tmp_path / "hq.txt",
    )

    assert returned is report
    assert store.metadata == [snapshot]
    assert store.closed is True
    setup_logging.assert_called_once_with()
    write_snapshot.assert_called_once()
    assert write_snapshot.call_args.args[1] is snapshot


def test_run_news_items_monthly_report_warns_when_metadata_write_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """CLI monthly report should surface operational metadata write failures as warnings."""
    loaded_config = Config(name="loaded", store={"state_root": tmp_path})
    snapshot = _snapshot(
        loaded_config.model_copy(update={"job_name": "monthly_report"}),
        Path("agents/news/local.yaml"),
    )
    report = _report(stats={"administrative_closure": 1})
    write_snapshot = MagicMock()

    class FailingStore:
        def __init__(self) -> None:
            self.closed = False

        def write_run_metadata(self, result: RunSnapshot) -> None:
            del result
            raise RuntimeError("boom")

        def close(self) -> None:
            self.closed = True

    store = FailingStore()

    def fake_asyncio_run(coro: object) -> tuple[RunSnapshot, MonthlyReport]:
        cast(Coroutine[Any, Any, object], coro).close()
        return snapshot, report

    monkeypatch.setattr("denbust.pipeline.setup_logging", lambda: None)
    monkeypatch.setattr("denbust.pipeline._load_config_or_exit", lambda _path: loaded_config)
    monkeypatch.setattr("denbust.pipeline.create_operational_store", lambda _config: store)
    monkeypatch.setattr("denbust.pipeline.asyncio.run", fake_asyncio_run)
    monkeypatch.setattr("denbust.pipeline.write_run_snapshot", write_snapshot)

    returned = run_news_items_monthly_report(
        config_path=Path("agents/news/local.yaml"),
        month="2026-03",
    )

    assert returned is report
    assert store.closed is True
    assert snapshot.warnings == ["operational_run_metadata_write_failed=RuntimeError: boom"]
    write_snapshot.assert_called_once()
