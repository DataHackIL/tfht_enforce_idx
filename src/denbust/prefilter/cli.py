"""CLI subcommands for the local pre-classification filter cascade.

Registered under ``denbust prefilter ...`` via the main ``cli.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

prefilter_app = typer.Typer(
    name="prefilter",
    help="Manage and inspect the local pre-classification filter cascade.",
    no_args_is_help=True,
)


@prefilter_app.command("summary")
def summary(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
) -> None:
    """Print aggregate counts from pre-filter decision records.

    Reads all JSONL files under the prefilter decisions directory for the
    configured dataset and prints per-verdict and per-stage counts.
    """
    from denbust.config import load_config
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    config_path = config or Path("agents/news/local.yaml")
    loaded = load_config(config_path)
    paths = resolve_prefilter_state_paths(
        state_root=loaded.store.state_root,
        dataset_name=loaded.dataset_name,
    )

    decisions_dir = paths.decisions_dir
    if not decisions_dir.exists():
        typer.echo("no decisions yet")
        return

    jsonl_files = sorted(decisions_dir.glob("*.jsonl"))
    if not jsonl_files:
        typer.echo("no decisions yet")
        return

    import json

    total = 0
    by_verdict: dict[str, int] = {}
    by_stage: dict[str, int] = {}

    for jsonl_path in jsonl_files:
        with jsonl_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                verdict = record.get("verdict", "unknown")
                by_verdict[verdict] = by_verdict.get(verdict, 0) + 1
                stopped = record.get("stopped_at_stage", "")
                if stopped and stopped != "passed_all":
                    by_stage[stopped] = by_stage.get(stopped, 0) + 1

    typer.echo(f"Total decisions : {total}")
    for verdict, count in sorted(by_verdict.items()):
        typer.echo(f"  {verdict:10s}: {count}")
    if by_stage:
        typer.echo("Dropped at stage:")
        for stage, count in sorted(by_stage.items()):
            typer.echo(f"  Stage {stage}: {count}")
