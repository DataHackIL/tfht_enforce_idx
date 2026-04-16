"""Minimal registry for dataset/job handlers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

from denbust.config import Config
from denbust.models.common import DatasetName, JobName
from denbust.models.runs import RunSnapshot
from denbust.ops.storage import OperationalStore

JobHandler = Callable[
    [Config, Path | None, int | None, OperationalStore | None],
    Awaitable[RunSnapshot],
]

_REGISTRY: dict[tuple[str, str], JobHandler] = {}


def register_job(
    dataset_name: DatasetName | str, job_name: JobName | str, handler: JobHandler
) -> None:
    """Register a dataset/job handler."""
    _REGISTRY[(str(dataset_name), str(job_name))] = handler


def get_job_handler(dataset_name: DatasetName | str, job_name: JobName | str) -> JobHandler | None:
    """Look up a registered dataset/job handler."""
    return _REGISTRY.get((str(dataset_name), str(job_name)))


def require_job_handler(dataset_name: DatasetName | str, job_name: JobName | str) -> JobHandler:
    """Return a handler or raise a clear error for unsupported jobs."""
    handler = get_job_handler(dataset_name, job_name)
    if handler is None:
        raise ValueError(f"Unsupported dataset/job combination: {dataset_name}/{job_name}")
    return handler
