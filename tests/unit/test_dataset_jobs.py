"""Unit tests for default dataset/job wrappers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from denbust.config import Config
from denbust.datasets.jobs import (
    _run_news_items_ingest,
    _run_scaffolded_backup,
    _run_scaffolded_release,
)
from denbust.models.runs import RunSnapshot


def build_snapshot() -> RunSnapshot:
    """Create a minimal run snapshot for wrapper tests."""
    return RunSnapshot(config_name="test-config")


@pytest.mark.asyncio
async def test_run_news_items_ingest_wrapper_calls_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ingest wrapper should delegate to the pipeline handler."""
    expected = build_snapshot()
    mock = AsyncMock(return_value=expected)
    monkeypatch.setattr("denbust.pipeline.run_news_ingest_job", mock)

    config = Config()
    result = await _run_news_items_ingest(config, Path("agents/news/local.yaml"), 7)

    assert result is expected
    mock.assert_awaited_once_with(
        config,
        config_path=Path("agents/news/local.yaml"),
        days_override=7,
    )


@pytest.mark.asyncio
async def test_run_scaffolded_release_wrapper_calls_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The release wrapper should delegate to the scaffolded pipeline handler."""
    expected = build_snapshot()
    mock = AsyncMock(return_value=expected)
    monkeypatch.setattr("denbust.pipeline.run_scaffolded_release_job", mock)

    config = Config()
    result = await _run_scaffolded_release(config, Path("agents/release/news_items.yaml"), None)

    assert result is expected
    mock.assert_awaited_once_with(config, config_path=Path("agents/release/news_items.yaml"))


@pytest.mark.asyncio
async def test_run_scaffolded_backup_wrapper_calls_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backup wrapper should delegate to the scaffolded pipeline handler."""
    expected = build_snapshot()
    mock = AsyncMock(return_value=expected)
    monkeypatch.setattr("denbust.pipeline.run_scaffolded_backup_job", mock)

    config = Config()
    result = await _run_scaffolded_backup(config, Path("agents/backup/news_items.yaml"), None)

    assert result is expected
    mock.assert_awaited_once_with(config, config_path=Path("agents/backup/news_items.yaml"))
