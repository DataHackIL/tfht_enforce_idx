"""CLI subcommands for the local pre-classification filter cascade.

Registered under ``denbust prefilter ...`` via the main ``cli.py``.
"""

from __future__ import annotations

import json
from collections import Counter
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
    if config is None:
        typer.echo(
            "Error: --config is required.  Pass the path to your YAML config file.\n"
            "Example: denbust prefilter summary --config agents/news/local.yaml",
            err=True,
        )
        raise typer.Exit(1)

    from denbust.config import load_config
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    loaded = load_config(config)
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


@prefilter_app.command("assemble-labels")
def assemble_labels_cmd(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    out: Annotated[
        Path | None,
        typer.Option(
            "--out", "-o", help="Output path for labels.parquet (default: state_paths.labels_path)"
        ),
    ] = None,
) -> None:
    """Assemble the labeled-candidates dataset and write labels.parquet.

    Merges manual triage decisions, auto-triage decisions, and past Claude
    classifier outputs from the configured state repo.  Applies a deterministic
    stratified train/val/test split and logs per-split class counts to stdout.
    """
    if config is None:
        typer.echo(
            "Error: --config is required.  Pass the path to your YAML config file.\n"
            "Example: denbust prefilter assemble-labels --config agents/news/local.yaml",
            err=True,
        )
        raise typer.Exit(1)

    from denbust.config import load_config
    from denbust.prefilter.labels import assemble_labels, write_labels_parquet
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    loaded = load_config(config)
    discovery_paths = loaded.discovery_state_paths
    prefilter_paths = resolve_prefilter_state_paths(
        state_root=loaded.store.state_root,
        dataset_name=loaded.dataset_name,
    )
    out_path = out if out is not None else prefilter_paths.labels_path

    rows = assemble_labels(discovery_paths)
    if not rows:
        typer.echo("No labeled candidates found — labels.parquet not written.", err=True)
        raise typer.Exit(1)

    write_labels_parquet(rows, out_path)

    # Summary stats
    by_split_label: Counter[tuple[str, str]] = Counter((r.split, r.label) for r in rows)
    by_split_source: Counter[tuple[str, str]] = Counter((r.split, r.label_source) for r in rows)
    total = len(rows)
    typer.echo(f"Wrote {total} rows → {out_path}")
    typer.echo("")
    for split_name in ("train", "val", "test"):
        pos = by_split_label[(split_name, "positive")]
        neg = by_split_label[(split_name, "negative")]
        n = pos + neg
        if n == 0:
            continue
        typer.echo(f"  {split_name:5s}: {n:6d} rows  (pos={pos}, neg={neg})")
    typer.echo("")
    typer.echo("By source:")
    source_totals: Counter[str] = Counter()
    for (_, src), cnt in by_split_source.items():
        source_totals[src] += cnt
    for src, cnt in sorted(source_totals.items()):
        typer.echo(f"  {src}: {cnt}")
