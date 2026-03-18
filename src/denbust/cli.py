"""CLI entry point for denbust."""

from pathlib import Path
from typing import Annotated

import typer

from denbust.models.common import DatasetName, JobName

app = typer.Typer(
    name="denbust",
    help="Monitor enforcement of anti-brothel laws in Israel.",
    no_args_is_help=True,
)


@app.command()
def scan(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Days back to search (overrides config)"),
    ] = None,
) -> None:
    """Scan news sources for enforcement-related articles."""
    from denbust.pipeline import run_pipeline

    config_path = config or Path("agents/news/local.yaml")
    run_pipeline(config_path=config_path, days_override=days)


@app.command()
def run(
    dataset: Annotated[
        DatasetName,
        typer.Option("--dataset", help="Dataset to run"),
    ] = DatasetName.NEWS_ITEMS,
    job: Annotated[
        JobName,
        typer.Option("--job", help="Job to run"),
    ] = JobName.INGEST,
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Days back to search (ingest only)"),
    ] = None,
) -> None:
    """Run a dataset/job pair through the registry."""
    from denbust.pipeline import run_job

    config_path = config or Path("agents/news/local.yaml")
    run_job(config_path=config_path, dataset_name=dataset, job_name=job, days_override=days)


@app.command()
def release(
    dataset: Annotated[
        DatasetName,
        typer.Option("--dataset", help="Dataset to release"),
    ] = DatasetName.NEWS_ITEMS,
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
) -> None:
    """Run the scaffolded release job for a dataset."""
    from denbust.pipeline import run_release

    config_path = config or Path("agents/news/local.yaml")
    run_release(config_path=config_path, dataset_name=dataset)


@app.command()
def backup(
    dataset: Annotated[
        DatasetName,
        typer.Option("--dataset", help="Dataset to back up"),
    ] = DatasetName.NEWS_ITEMS,
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
) -> None:
    """Run the scaffolded backup job for a dataset."""
    from denbust.pipeline import run_backup

    config_path = config or Path("agents/news/local.yaml")
    run_backup(config_path=config_path, dataset_name=dataset)


@app.command()
def version() -> None:
    """Show version information."""
    from denbust import __version__

    typer.echo(f"denbust version {__version__}")


if __name__ == "__main__":
    app()
