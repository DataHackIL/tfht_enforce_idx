"""Default dataset/job registrations."""

from __future__ import annotations

from pathlib import Path

from denbust.config import Config
from denbust.datasets.registry import register_job
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot
from denbust.ops.storage import OperationalStore

_REGISTERED = False


async def _run_news_items_ingest(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    from denbust.pipeline import run_news_ingest_job

    if operational_store is None:
        return await run_news_ingest_job(
            config,
            config_path=config_path,
            days_override=days_override,
        )
    return await run_news_ingest_job(
        config,
        config_path=config_path,
        days_override=days_override,
        operational_store=operational_store,
    )


async def _run_scaffolded_release(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    del days_override
    from denbust.pipeline import run_scaffolded_release_job

    if operational_store is None:
        return await run_scaffolded_release_job(config, config_path=config_path)
    return await run_scaffolded_release_job(
        config,
        config_path=config_path,
        operational_store=operational_store,
    )


async def _run_scaffolded_backup(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    del days_override
    from denbust.pipeline import run_scaffolded_backup_job

    if operational_store is None:
        return await run_scaffolded_backup_job(config, config_path=config_path)
    return await run_scaffolded_backup_job(
        config,
        config_path=config_path,
        operational_store=operational_store,
    )


def ensure_default_jobs_registered() -> None:
    """Register Phase A dataset jobs exactly once."""
    global _REGISTERED
    if _REGISTERED:
        return

    register_job(DatasetName.NEWS_ITEMS, JobName.INGEST, _run_news_items_ingest)
    register_job(DatasetName.NEWS_ITEMS, JobName.RELEASE, _run_scaffolded_release)
    register_job(DatasetName.NEWS_ITEMS, JobName.BACKUP, _run_scaffolded_backup)
    _REGISTERED = True
