"""CLI entry point for denbust."""

from pathlib import Path
from typing import Annotated

import typer

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
def version() -> None:
    """Show version information."""
    from denbust import __version__

    typer.echo(f"denbust version {__version__}")


if __name__ == "__main__":
    app()
