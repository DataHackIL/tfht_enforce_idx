"""CLI subcommands for the local pre-classification filter cascade.

Registered under ``denbust prefilter ...`` via the main ``cli.py``.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Annotated, Any

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


@prefilter_app.command("retrain")
def retrain_cmd(
    stage: Annotated[
        str,
        typer.Option("--stage", "-s", help="Stage to retrain: a, b"),
    ],
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    model: Annotated[
        str,
        typer.Option(
            "--model",
            "-m",
            help="Stage B model kind: naive_bayes (default) or setfit",
        ),
    ] = "naive_bayes",
) -> None:
    """Rebuild stage artifacts from labels.parquet.

    Reads the labeled-candidates parquet from the prefilter state path and
    writes updated artifacts under ``models/<stage>/`` in the same state root.

    Supported stages
    ----------------
    a   Lexicon (chi-squared weighted terms) + domain reputation parquet.
    b   Calibrated text classifier.  Use ``--model`` to choose the variant:

        naive_bayes (default)
            Calibrated ComplementNB on char n-grams; no extra dependencies.
        setfit
            SetFit on ``intfloat/multilingual-e5-large``.  Requires the
            ``prefilter`` extras: ``pip install -e '.[dev,prefilter]'``.
    """
    if config is None:
        typer.echo(
            "Error: --config is required.  Pass the path to your YAML config file.\n"
            "Example: denbust prefilter retrain --stage b --config agents/news/local.yaml",
            err=True,
        )
        raise typer.Exit(1)

    stage = stage.lower().strip()
    if stage not in {"a", "b"}:
        typer.echo(
            f"Error: --stage {stage!r} is not supported.  Choose 'a' or 'b'.",
            err=True,
        )
        raise typer.Exit(1)

    model = model.lower().strip()
    if stage == "b" and model not in {"naive_bayes", "setfit"}:
        typer.echo(
            f"Error: --model {model!r} is not supported for stage b.  "
            "Choose 'naive_bayes' or 'setfit'.",
            err=True,
        )
        raise typer.Exit(1)

    from denbust.config import load_config
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    loaded = load_config(config)
    prefilter_paths = resolve_prefilter_state_paths(
        state_root=loaded.store.state_root,
        dataset_name=loaded.dataset_name,
    )

    if not prefilter_paths.labels_path.exists():
        typer.echo(
            f"Error: labels.parquet not found at {prefilter_paths.labels_path}.\n"
            "Run `denbust prefilter assemble-labels` first.",
            err=True,
        )
        raise typer.Exit(1)

    if stage == "a":
        from denbust.prefilter.stage_a import build_stage_a_artifacts

        typer.echo(f"Retraining Stage A from {prefilter_paths.labels_path} ...")
        lex_path, dom_path = build_stage_a_artifacts(
            labels_path=prefilter_paths.labels_path,
            out_dir=prefilter_paths.models_dir,
        )
        typer.echo(f"  lexicon           -> {lex_path}")
        typer.echo(f"  domain_reputation -> {dom_path}")
        typer.echo("Stage A retrain complete.")

    elif model == "naive_bayes":
        from denbust.prefilter.stage_b import train_naive_bayes

        typer.echo(f"Retraining Stage B (naive_bayes) from {prefilter_paths.labels_path} ...")
        meta, stage_dir = train_naive_bayes(
            labels_path=prefilter_paths.labels_path,
            out_dir=prefilter_paths.models_dir,
        )
        typer.echo(f"  thin_model        -> {stage_dir / 'thin_model.joblib'}")
        typer.echo(f"  thick_model       -> {stage_dir / 'thick_model.joblib'}")
        typer.echo(f"  meta.json         -> {stage_dir / 'meta.json'}")
        typer.echo(f"Stage B retrain complete.  model_version={meta.model_version}")

    else:  # setfit
        from denbust.prefilter.stage_b import train_setfit

        typer.echo(f"Retraining Stage B (setfit) from {prefilter_paths.labels_path} ...")
        typer.echo("  base model: intfloat/multilingual-e5-large  (download may take a while)")
        meta, stage_dir = train_setfit(
            labels_path=prefilter_paths.labels_path,
            out_dir=prefilter_paths.models_dir,
        )
        typer.echo(f"  thin_model/       -> {stage_dir / 'thin_model'}")
        typer.echo(f"  thick_model/      -> {stage_dir / 'thick_model'}")
        typer.echo(f"  meta.json         -> {stage_dir / 'meta.json'}")
        typer.echo(f"Stage B SetFit retrain complete.  model_version={meta.model_version}")


@prefilter_app.command("evaluate")
def evaluate_cmd(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    split: Annotated[
        str,
        typer.Option("--split", help="Labeled split to evaluate on: val or test"),
    ] = "val",
    compare_b: Annotated[
        str | None,
        typer.Option(
            "--compare-b",
            help=(
                "Comma-separated Stage B model kinds to compare, e.g. "
                "'naive_bayes,setfit'.  Both must be trained first."
            ),
        ),
    ] = None,
    recall_floor: Annotated[
        float,
        typer.Option(
            "--recall-floor",
            help="Minimum recall to maintain when choosing the operating threshold.",
        ),
    ] = 0.99,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", "-r", help="Write a markdown report to this path."),
    ] = None,
) -> None:
    """Compare Stage B model variants on a labeled evaluation split.

    Loads each model from the configured models directory, scores every
    candidate in the chosen split using the thin pass (title + snippet), and
    prints precision / recall / Brier score for each at the threshold that
    achieves ``--recall-floor`` on that same split.

    Both models must be trained before running this command:

    \\b
        denbust prefilter retrain --stage b --model naive_bayes --config CFG
        denbust prefilter retrain --stage b --model setfit --config CFG
        denbust prefilter evaluate --compare-b naive_bayes,setfit --config CFG
    """
    if config is None:
        typer.echo(
            "Error: --config is required.  Pass the path to your YAML config file.",
            err=True,
        )
        raise typer.Exit(1)

    if split not in {"val", "test"}:
        typer.echo(
            f"Error: --split must be 'val' or 'test', got {split!r}.",
            err=True,
        )
        raise typer.Exit(1)

    if compare_b is None:
        typer.echo(
            "Error: --compare-b is required (e.g. --compare-b naive_bayes,setfit).",
            err=True,
        )
        raise typer.Exit(1)

    model_kinds = [k.strip() for k in compare_b.split(",") if k.strip()]
    valid_kinds = {"naive_bayes", "setfit"}
    unknown = set(model_kinds) - valid_kinds
    if unknown:
        typer.echo(
            f"Error: unknown model kind(s): {', '.join(sorted(unknown))}.  "
            f"Choose from: {', '.join(sorted(valid_kinds))}.",
            err=True,
        )
        raise typer.Exit(1)

    from denbust.config import load_config
    from denbust.prefilter.labels import read_labels_parquet
    from denbust.prefilter.stage_b import StageBScorer, StageBSetFitScorer
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    loaded = load_config(config)
    prefilter_paths = resolve_prefilter_state_paths(
        state_root=loaded.store.state_root,
        dataset_name=loaded.dataset_name,
    )

    if not prefilter_paths.labels_path.exists():
        typer.echo(
            f"Error: labels.parquet not found at {prefilter_paths.labels_path}.\n"
            "Run `denbust prefilter assemble-labels` first.",
            err=True,
        )
        raise typer.Exit(1)

    all_rows = read_labels_parquet(prefilter_paths.labels_path)
    eval_rows = [r for r in all_rows if r.split == split]
    if not eval_rows:
        typer.echo(f"No rows found for split='{split}' in {prefilter_paths.labels_path}.", err=True)
        raise typer.Exit(1)

    y_true = [1 if r.label == "positive" else 0 for r in eval_rows]
    n_pos = sum(y_true)
    n_neg = len(y_true) - n_pos

    typer.echo(f"Evaluating on split='{split}': {len(eval_rows)} rows  (pos={n_pos}, neg={n_neg})")
    typer.echo(f"Recall floor: {recall_floor:.1%}")
    typer.echo("")

    results: list[dict[str, Any]] = []

    for kind in model_kinds:
        if kind == "naive_bayes":
            scorer: StageBScorer | StageBSetFitScorer = StageBScorer(
                models_dir=prefilter_paths.models_dir
            )
        else:
            scorer = StageBSetFitScorer(models_dir=prefilter_paths.models_dir)

        p_negatives: list[float] = []
        n_skipped = 0
        for row in eval_rows:
            score = scorer.evaluate(row, "thin")
            if score is None:
                n_skipped += 1
                p_negatives.append(0.5)  # no-info fallback: neutral probability
            else:
                p_negatives.append(score.p_negative)

        if n_skipped == len(eval_rows):
            typer.echo(
                f"  {kind}: scorer returned None for all candidates "
                "(artifacts missing or package not installed) — skipping.\n"
            )
            continue

        threshold = _threshold_at_recall_floor(p_negatives, y_true, recall_floor)

        dropped = [p >= threshold for p in p_negatives]
        true_drops = sum(1 for d, y in zip(dropped, y_true) if d and y == 0)
        false_drops = sum(1 for d, y in zip(dropped, y_true) if d and y == 1)
        total_dropped = sum(dropped)

        recall = (n_pos - false_drops) / n_pos if n_pos > 0 else 1.0
        drop_precision = true_drops / total_dropped if total_dropped > 0 else 0.0
        drop_rate = total_dropped / len(y_true)
        # Brier score: MSE between p_positive = (1 - p_negative) and y ∈ {0, 1}
        brier = sum((1.0 - p - y) ** 2 for p, y in zip(p_negatives, y_true)) / len(y_true)

        version = scorer.model_version or "(unknown)"
        typer.echo(f"  {kind}  [version: {version}]")
        typer.echo(f"    threshold      : {threshold:.4f}")
        typer.echo(f"    recall         : {recall:.4f}  (target ≥ {recall_floor:.4f})")
        typer.echo(f"    drop_precision : {drop_precision:.4f}  (true_neg / total_dropped)")
        typer.echo(f"    drop_rate      : {drop_rate:.4f}")
        typer.echo(f"    brier_score    : {brier:.4f}")
        if n_skipped:
            typer.echo(f"    no-score rows  : {n_skipped} (treated as p_negative=0.5)")
        typer.echo("")

        results.append(
            {
                "kind": kind,
                "version": version,
                "threshold": threshold,
                "recall": recall,
                "drop_precision": drop_precision,
                "drop_rate": drop_rate,
                "brier_score": brier,
            }
        )

    if len(results) >= 2:
        # Prefer higher drop_rate (more negatives filtered); break ties by lower Brier.
        best = max(results, key=lambda r: (r["drop_rate"], -r["brier_score"]))
        typer.echo(
            f"Recommendation: '{best['kind']}' achieves the highest drop rate "
            f"({best['drop_rate']:.1%}) at the given recall floor."
        )
        typer.echo("")

    if report_path is not None and results:
        _write_evaluate_report(report_path, split, recall_floor, n_pos, n_neg, results)
        typer.echo(f"Report written to {report_path}")


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
    from denbust.ops.factory import create_operational_store
    from denbust.prefilter.labels import assemble_labels, write_labels_parquet
    from denbust.prefilter.state_paths import resolve_prefilter_state_paths

    loaded = load_config(config)
    discovery_paths = loaded.discovery_state_paths
    prefilter_paths = resolve_prefilter_state_paths(
        state_root=loaded.store.state_root,
        dataset_name=loaded.dataset_name,
    )
    out_path = out if out is not None else prefilter_paths.labels_path

    try:
        operational_store = create_operational_store(loaded)
    except Exception:  # noqa: BLE001
        operational_store = None

    rows = assemble_labels(discovery_paths, operational_store=operational_store)
    if not rows:
        typer.echo("No labeled candidates found — labels.parquet not written.", err=True)
        raise typer.Exit(1)

    write_labels_parquet(rows, out_path)

    # Summary stats
    by_split_label: Counter[tuple[str, str]] = Counter((r.split, r.label) for r in rows)
    total = len(rows)
    typer.echo(f"Wrote {total} rows -> {out_path}")
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
    source_totals: Counter[str] = Counter(r.label_source for r in rows)
    for src, cnt in sorted(source_totals.items()):
        typer.echo(f"  {src}: {cnt}")


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _threshold_at_recall_floor(
    p_negatives: list[float],
    y_true: list[int],
    recall_floor: float,
) -> float:
    """Find the lowest drop-threshold that keeps recall ≥ *recall_floor*.

    A candidate is "dropped" when ``p_negative >= threshold``.  Lower
    thresholds drop more candidates and reduce recall.  This function finds
    the minimum threshold at which recall still meets the floor, maximising
    the drop rate for a given recall constraint.

    Parameters
    ----------
    p_negatives:
        Model output ``p_negative`` for each evaluation candidate.
    y_true:
        Ground-truth labels: ``1`` = positive, ``0`` = negative.
    recall_floor:
        Target minimum recall (e.g. ``0.99``).

    Returns
    -------
    float
        The operating threshold.  Returns ``1.0`` (no drops) when the
        positive set is empty.
    """
    n_pos = sum(y_true)
    if n_pos == 0:
        return 1.0

    # Maximum number of positives we can afford to drop.
    max_false_drops = int(n_pos * (1.0 - recall_floor))

    # Candidate thresholds: each unique p_negative value is a potential
    # boundary.  We try them in ascending order — lower threshold = more
    # drops.  We stop at the first threshold that drops too many positives.
    for threshold in sorted(set(p_negatives)):
        false_drops = sum(1 for p, y in zip(p_negatives, y_true) if p >= threshold and y == 1)
        if false_drops > max_false_drops:
            # This threshold drops too many positives; use a slightly higher
            # value so the previous set of candidates is still dropped.
            return threshold + 1e-9
    # Even the lowest threshold satisfies the recall floor.
    return min(p_negatives)


def _write_evaluate_report(
    path: Path,
    split: str,
    recall_floor: float,
    n_pos: int,
    n_neg: int,
    results: list[dict[str, Any]],
) -> None:
    """Write a markdown A/B evaluation report to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Stage B A/B Evaluation Report",
        "",
        f"**Split:** {split}  |  **Recall floor:** {recall_floor:.1%}  "
        f"|  **Rows:** {n_pos + n_neg} (pos={n_pos}, neg={n_neg})",
        "",
        "| Model | Version | Threshold | Recall | Drop Precision | Drop Rate | Brier |",
        "|-------|---------|-----------|--------|----------------|-----------|-------|",
    ]
    for r in results:
        lines.append(
            f"| {r['kind']} | {r['version']} "
            f"| {r['threshold']:.4f} | {r['recall']:.4f} "
            f"| {r['drop_precision']:.4f} | {r['drop_rate']:.4f} "
            f"| {r['brier_score']:.4f} |"
        )
    lines += ["", ""]
    path.write_text("\n".join(lines), encoding="utf-8")
