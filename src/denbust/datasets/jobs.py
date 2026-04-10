"""Default dataset/job registrations."""

from __future__ import annotations

from pathlib import Path

from denbust.config import Config
from denbust.datasets.registry import register_job
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot
from denbust.ops.storage import OperationalStore

_REGISTERED = False


def _scaffolded_job_error(config: Config) -> ValueError:
    """Build a clear placeholder error for scaffold-only jobs."""
    return ValueError(
        f"{config.dataset_name.value}/{config.job_name.value} is scaffolded but not implemented yet"
    )


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


async def _run_unimplemented_scaffold_job(
    config: Config,
    config_path: Path | None,
    days_override: int | None,
    operational_store: OperationalStore | None = None,
) -> RunSnapshot:
    """Fail scaffold-only jobs with a purpose-built message."""
    del config_path, days_override, operational_store
    raise _scaffolded_job_error(config)


def ensure_default_jobs_registered() -> None:
    """Register Phase A dataset jobs exactly once."""
    global _REGISTERED
    if _REGISTERED:
        return

    register_job(DatasetName.NEWS_ITEMS, JobName.INGEST, _run_news_items_ingest)
    register_job(DatasetName.NEWS_ITEMS, JobName.DISCOVER, _run_unimplemented_scaffold_job)
    register_job(
        DatasetName.NEWS_ITEMS,
        JobName.SCRAPE_CANDIDATES,
        _run_unimplemented_scaffold_job,
    )
    register_job(
        DatasetName.NEWS_ITEMS,
        JobName.BACKFILL_DISCOVER,
        _run_unimplemented_scaffold_job,
    )
    register_job(
        DatasetName.NEWS_ITEMS,
        JobName.BACKFILL_SCRAPE,
        _run_unimplemented_scaffold_job,
    )
    register_job(DatasetName.NEWS_ITEMS, JobName.RELEASE, _run_scaffolded_release)
    register_job(DatasetName.NEWS_ITEMS, JobName.BACKUP, _run_scaffolded_backup)
    _REGISTERED = True
