"""CLI entry point for denbust."""

from pathlib import Path
from typing import Annotated

import typer

from denbust.models.common import DatasetName, JobName
from denbust.validation.common import DEFAULT_VALIDATION_SET_PATH, DEFAULT_VARIANT_MATRIX_PATH

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

    config_path = config or Path("agents/news.yaml")
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

    config_path = config or Path("agents/release/news_items.yaml")
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

    config_path = config or Path("agents/backup/news_items.yaml")
    run_backup(config_path=config_path, dataset_name=dataset)


@app.command("validation-collect")
def validation_collect(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Days back to search (defaults to 7)"),
    ] = None,
    per_source: Annotated[
        int,
        typer.Option("--per-source", help="Maximum candidate rows to collect per source"),
    ] = 10,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Explicit draft CSV output path"),
    ] = None,
) -> None:
    """Collect a local draft CSV for human review and annotation."""
    from denbust.validation import run_validation_collect

    config_path = config or Path("agents/news/local.yaml")
    result = run_validation_collect(
        config_path=config_path,
        days_override=days,
        per_source=per_source,
        output_path=output,
    )
    typer.echo(f"Wrote {result.total_rows} draft rows to {result.output_path}")
    for source_name, count in sorted(result.per_source_counts.items()):
        typer.echo(f"{source_name}: {count}")
    for error in result.errors:
        typer.echo(f"error: {error}", err=True)


@app.command("validation-finalize")
def validation_finalize(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Path to a reviewed draft CSV"),
    ],
    validation_set: Annotated[
        Path,
        typer.Option("--validation-set", help="Path to the tracked validation CSV"),
    ] = DEFAULT_VALIDATION_SET_PATH,
) -> None:
    """Merge reviewed draft rows into the tracked permanent validation set."""
    from denbust.validation import run_validation_finalize

    result = run_validation_finalize(
        input_path=input_path,
        validation_set_path=validation_set,
    )
    typer.echo(
        "Added "
        f"{result.added_rows} rows to {result.validation_set_path} "
        f"({result.skipped_duplicates} duplicate(s) skipped, {result.total_rows} total)."
    )


@app.command("validation-evaluate")
def validation_evaluate(
    validation_set: Annotated[
        Path,
        typer.Option("--validation-set", help="Path to the tracked validation CSV"),
    ] = DEFAULT_VALIDATION_SET_PATH,
    variants: Annotated[
        Path,
        typer.Option("--variants", help="Path to the classifier variant matrix YAML"),
    ] = DEFAULT_VARIANT_MATRIX_PATH,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Explicit JSON report output path"),
    ] = None,
) -> None:
    """Evaluate classifier variants against the permanent validation set."""
    from denbust.validation import run_validation_evaluate
    from denbust.validation.evaluate import render_rankings_table

    result = run_validation_evaluate(
        validation_set_path=validation_set,
        variants_path=variants,
        output_path=output,
    )
    typer.echo(render_rankings_table(result.rankings))
    typer.echo(f"Saved JSON report to {result.output_path}")


@app.command()
def version() -> None:
    """Show version information."""
    from denbust import __version__

    typer.echo(f"denbust version {__version__}")


if __name__ == "__main__":
    app()
