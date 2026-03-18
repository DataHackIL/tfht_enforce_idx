"""Default dataset/job registrations."""

from __future__ import annotations

from pathlib import Path

from denbust.config import Config
from denbust.datasets.registry import register_job
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot

_REGISTERED = False


async def _run_news_items_ingest(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
) -> RunSnapshot:
    from denbust.pipeline import run_news_ingest_job

    return await run_news_ingest_job(
        config,
        config_path=config_path,
        days_override=days_override,
    )


async def _run_scaffolded_release(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
) -> RunSnapshot:
    del days_override
    from denbust.pipeline import run_scaffolded_release_job

    return await run_scaffolded_release_job(config, config_path=config_path)


async def _run_scaffolded_backup(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
) -> RunSnapshot:
    del days_override
    from denbust.pipeline import run_scaffolded_backup_job

    return await run_scaffolded_backup_job(config, config_path=config_path)


def ensure_default_jobs_registered() -> None:
    """Register Phase A dataset jobs exactly once."""
    global _REGISTERED
    if _REGISTERED:
        return

    register_job(DatasetName.NEWS_ITEMS, JobName.INGEST, _run_news_items_ingest)
    register_job(DatasetName.NEWS_ITEMS, JobName.RELEASE, _run_scaffolded_release)
    register_job(DatasetName.NEWS_ITEMS, JobName.BACKUP, _run_scaffolded_backup)
    _REGISTERED = True
