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

    c   Embedding similarity (centroid-cosine + FAISS kNN).  Requires the
        ``prefilter`` extras: ``pip install -e '.[dev,prefilter]'``.
        Thick-pass only — returns None on thin-pass candidates.
    """
    if config is None:
        typer.echo(
            "Error: --config is required.  Pass the path to your YAML config file.\n"
            "Example: denbust prefilter retrain --stage b --config agents/news/local.yaml",
            err=True,
        )
        raise typer.Exit(1)

    stage = stage.lower().strip()
    if stage not in {"a", "b", "c"}:
        typer.echo(
            f"Error: --stage {stage!r} is not supported.  Choose 'a', 'b', or 'c'.",
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

    elif stage == "c":
        from denbust.prefilter.stage_c import _DEFAULT_BASE_MODEL, train_stage_c

        typer.echo(f"Retraining Stage C from {prefilter_paths.labels_path} ...")
        typer.echo(f"  base model: {_DEFAULT_BASE_MODEL}  (download may take a while)")
        c_meta, stage_dir = train_stage_c(
            labels_path=prefilter_paths.labels_path,
            out_dir=prefilter_paths.models_dir,
        )
        typer.echo(f"  centroid.npy      -> {stage_dir / 'centroid.npy'}")
        typer.echo(f"  index.faiss       -> {stage_dir / 'index.faiss'}")
        typer.echo(f"  calibration.json  -> {stage_dir / 'calibration.json'}")
        typer.echo(f"  meta.json         -> {stage_dir / 'meta.json'}")
        typer.echo(f"Stage C retrain complete.  model_version={c_meta.model_version}")

    else:  # setfit
        from denbust.prefilter.stage_b import _DEFAULT_SETFIT_BASE_MODEL, train_setfit

        typer.echo(f"Retraining Stage B (setfit) from {prefilter_paths.labels_path} ...")
        typer.echo(f"  base model: {_DEFAULT_SETFIT_BASE_MODEL}  (download may take a while)")
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
        str,
        typer.Option(
            "--compare-b",
            help=(
                "Comma-separated Stage B model kinds to evaluate, e.g. "
                "'naive_bayes' or 'naive_bayes,setfit'.  All must be trained first."
            ),
        ),
    ] = "naive_bayes",
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
    """Evaluate Stage B model variants on a labeled split.

    Loads each model from the configured models directory, scores every
    candidate in the chosen split using the thin pass (title + snippet), and
    prints precision / recall / Brier score for each at the threshold that
    achieves ``--recall-floor`` on that same split.  Unscored candidates
    (scorer returned ``None``) are excluded from metric calculations.

    Models must be trained before running this command:

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
    from denbust.prefilter.stage_b import StageBScorer, StageBScorerProtocol, StageBSetFitScorer
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
        scorer: StageBScorerProtocol
        if kind == "naive_bayes":
            scorer = StageBScorer(models_dir=prefilter_paths.models_dir)
        else:
            scorer = StageBSetFitScorer(models_dir=prefilter_paths.models_dir)

        # Collect scores only for rows the scorer can actually handle.
        # Rows returning None pass through the filter — they must NOT be
        # imputed with a fake probability or they corrupt every metric.
        scored_p: list[float] = []
        scored_y: list[int] = []
        n_skipped = 0
        for row, y in zip(eval_rows, y_true):
            score = scorer.evaluate(row, "thin")
            if score is None:
                n_skipped += 1
            else:
                scored_p.append(score.p_negative)
                scored_y.append(y)

        if not scored_p:
            typer.echo(
                f"  {kind}: scorer returned None for all candidates "
                "(artifacts missing or package not installed) — skipping.\n"
            )
            continue

        metrics = _compute_stage_b_metrics(
            scored_p=scored_p,
            scored_y=scored_y,
            n_pos_total=n_pos,
            n_total=len(y_true),
            recall_floor=recall_floor,
        )

        version = scorer.model_version or "(unknown)"
        typer.echo(f"  {kind}  [version: {version}]")
        typer.echo(f"    threshold      : {metrics['threshold']:.4f}")
        typer.echo(f"    recall         : {metrics['recall']:.4f}  (target ≥ {recall_floor:.4f})")
        typer.echo(
            f"    drop_precision : {metrics['drop_precision']:.4f}  (true_neg / total_dropped)"
        )
        typer.echo(f"    drop_rate      : {metrics['drop_rate']:.4f}")
        typer.echo(f"    brier_score    : {metrics['brier_score']:.4f}")
        if n_skipped:
            typer.echo(
                f"    no-score rows  : {n_skipped} (excluded from metrics; "
                "treated as pass-through in production)"
            )
        typer.echo("")

        results.append({"kind": kind, "version": version, **metrics})

    if len(results) >= 2:
        # Prefer the model with the best calibration (lower Brier score).
        # Break ties by drop rate — a higher rate means more noise is filtered
        # at the same quality level.
        best = min(results, key=lambda r: (r["brier_score"], -r["drop_rate"]))
        typer.echo(
            f"Recommendation: '{best['kind']}' has the best calibration "
            f"(Brier={best['brier_score']:.4f}) at the given recall floor."
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

    A candidate is "dropped" when ``p_negative >= threshold``.  Higher
    thresholds drop fewer candidates and increase recall.  This function
    finds the minimum threshold at which recall still meets the floor,
    maximising the drop rate for a given recall constraint.

    Parameters
    ----------
    p_negatives:
        Model output ``p_negative`` for each evaluation candidate.
        Must contain only *scored* rows (no imputed values).
    y_true:
        Ground-truth labels aligned with *p_negatives*: ``1`` = positive,
        ``0`` = negative.
    recall_floor:
        Target minimum recall (e.g. ``0.99``).

    Returns
    -------
    float
        The tightest threshold that satisfies the recall floor.
        Returns ``1.0`` when the positive set is empty.
        Returns ``max(p_negatives) + 1e-9`` (no drops) when no candidate
        threshold can satisfy the floor.

    Notes
    -----
    The algorithm sweeps unique p_negative values in ascending order.
    Since false-drop count is monotonically non-increasing as the threshold
    rises, the first value where ``false_drops ≤ max_false_drops`` is the
    optimal (minimum) threshold.  If no value in the set satisfies the
    constraint, we return just above the maximum so that nothing is dropped
    and recall stays at 1.0.
    """
    n_pos = sum(y_true)
    if n_pos == 0:
        return 1.0

    # Maximum number of positives we can afford to drop.
    # Use int() (floor) rather than round() — dropping k positives achieves
    # recall = (n_pos − k) / n_pos; we need that ≥ recall_floor, so
    # k ≤ n_pos * (1 − recall_floor).  Float arithmetic can push the product
    # just below a true integer (e.g. 10 * 0.1 → 0.9999…), so we add a tiny
    # epsilon before truncating to recover the intended whole number.
    max_false_drops = int(n_pos * (1.0 - recall_floor) + 1e-9)

    # Sweep ascending: for each candidate threshold T, count how many
    # positives would be incorrectly dropped (p_negative >= T and y == 1).
    # False-drop count is non-increasing as T rises, so the first T that
    # satisfies the constraint IS the minimum (most aggressive) safe threshold.
    for threshold in sorted(set(p_negatives)):
        false_drops = sum(1 for p, y in zip(p_negatives, y_true) if p >= threshold and y == 1)
        if false_drops <= max_false_drops:
            return threshold

    # No value in the candidate set satisfies the floor (e.g. a positive
    # example has the single highest p_negative in the dataset).
    # Return just above the maximum so nothing is dropped and recall = 1.0.
    return max(p_negatives) + 1e-9


def _compute_stage_b_metrics(
    scored_p: list[float],
    scored_y: list[int],
    n_pos_total: int,
    n_total: int,
    recall_floor: float,
) -> dict[str, float]:
    """Compute Stage B evaluation metrics over *scored* candidates only.

    Parameters
    ----------
    scored_p:
        ``p_negative`` values for rows where the scorer returned a score.
        Must NOT include imputed values for rows that scored ``None``.
    scored_y:
        Ground-truth labels aligned with *scored_p* (``1`` = positive).
    n_pos_total:
        Total number of positives in the evaluation split, including any
        rows that were unscored (those pass through without being dropped).
    n_total:
        Total number of candidates in the evaluation split.
    recall_floor:
        Target minimum recall passed to :func:`_threshold_at_recall_floor`.

    Returns
    -------
    dict[str, float]
        Keys: ``threshold``, ``recall``, ``drop_precision``,
        ``drop_rate``, ``brier_score``.

    Notes
    -----
    *Recall* and *drop_rate* are expressed over the full split (``n_total``,
    ``n_pos_total``) so unscored rows are correctly treated as pass-through
    (not dropped) rather than being counted against recall.
    *Brier score* is computed only over scored rows — that is where we have
    actual model predictions.
    """
    if not scored_p:
        return {
            "threshold": 1.0,
            "recall": 1.0,
            "drop_precision": 0.0,
            "drop_rate": 0.0,
            "brier_score": 0.0,
        }

    threshold = _threshold_at_recall_floor(scored_p, scored_y, recall_floor)
    dropped = [p >= threshold for p in scored_p]
    true_drops = sum(1 for d, y in zip(dropped, scored_y) if d and y == 0)
    false_drops = sum(1 for d, y in zip(dropped, scored_y) if d and y == 1)
    total_dropped = sum(dropped)

    # Recall: unscored positives are NOT dropped, so they don't reduce recall.
    recall = (n_pos_total - false_drops) / n_pos_total if n_pos_total > 0 else 1.0
    drop_precision = true_drops / total_dropped if total_dropped > 0 else 0.0
    # Drop rate: over the full split so unscored rows are counted as retained.
    drop_rate = total_dropped / n_total if n_total > 0 else 0.0
    # Brier score: MSE between p_positive = (1 − p_negative) and y ∈ {0, 1},
    # computed only over rows that actually scored.
    brier = (
        sum((1.0 - p - y) ** 2 for p, y in zip(scored_p, scored_y)) / len(scored_p)
        if scored_p
        else 0.0
    )

    return {
        "threshold": threshold,
        "recall": recall,
        "drop_precision": drop_precision,
        "drop_rate": drop_rate,
        "brier_score": brier,
    }


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
