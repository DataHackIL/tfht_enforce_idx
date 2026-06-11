"""CLI entry point for denbust."""

import os
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from denbust.models.common import DatasetName, JobName
from denbust.prefilter.cli import prefilter_app
from denbust.validation.common import DEFAULT_VALIDATION_SET_PATH, DEFAULT_VARIANT_MATRIX_PATH


class DiagnosticOutputFormat(StrEnum):
    """Supported CLI output formats for diagnostics commands."""

    TEXT = "text"
    JSON = "json"


app = typer.Typer(
    name="denbust",
    help="Monitor enforcement of anti-brothel laws in Israel.",
    no_args_is_help=True,
)
report_app = typer.Typer(help="Generate report artifacts.")
app.add_typer(report_app, name="report")
app.add_typer(prefilter_app, name="prefilter")


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
    pub_date_from: Annotated[
        str | None,
        typer.Option(
            "--pub-date-from",
            help="ISO date (YYYY-MM-DD): only scrape candidates published on or after this date"
            " (scrape_candidates job only).",
        ),
    ] = None,
    balanced_batch: Annotated[
        int | None,
        typer.Option(
            "--balanced-batch",
            help="Scrape a month-frequency-weighted, source-balanced batch of this size"
            " from the full prefilter-passing pool (scrape_candidates job only).",
        ),
    ] = None,
    min_domain_frequency: Annotated[
        int | None,
        typer.Option(
            "--min-domain-frequency",
            help="Domain-frequency gate: hold back candidates on a domain seen fewer than N"
            " times across the store (curated known outlets exempt). Kills the single-shot"
            " spam tail. Balanced-batch mode only.",
        ),
    ] = None,
    use_domain_verdicts: Annotated[
        bool,
        typer.Option(
            "--use-domain-verdicts",
            help="Apply the cached per-domain LLM verdict gate (drop 'block' domains)."
            " Populate the cache first with `denbust classify-domains`. Balanced-batch mode only.",
        ),
    ] = False,
    query_budget: Annotated[
        int | None,
        typer.Option(
            "--query-budget",
            help="Cap discovery to this many queries per engine per run (highest-priority"
            " open-web kinds kept first). Saves Brave/Exa search budget. Discover job only.",
        ),
    ] = None,
) -> None:
    """Run a dataset/job pair through the registry."""
    from denbust.pipeline import run_job

    config_path = config or Path("agents/news/local.yaml")
    run_job(
        config_path=config_path,
        dataset_name=dataset,
        job_name=job,
        days_override=days,
        scrape_pub_date_from=pub_date_from,
        scrape_balanced_batch_size=balanced_batch,
        scrape_min_domain_frequency=min_domain_frequency,
        scrape_use_domain_verdicts=use_domain_verdicts,
        query_budget=query_budget,
    )


@app.command("diagnose-sources")
def diagnose_sources(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    source: Annotated[
        list[str] | None,
        typer.Option("--source", help="Source name to diagnose; repeat to limit scope"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Days back to analyze (overrides config)"),
    ] = None,
    artifacts_only: Annotated[
        bool,
        typer.Option("--artifacts-only", help="Disable live probing and inspect artifacts only"),
    ] = False,
    live_only: Annotated[
        bool,
        typer.Option("--live-only", help="Skip artifact analysis and run live probes only"),
    ] = False,
    format: Annotated[
        DiagnosticOutputFormat,
        typer.Option("--format", help="Output format"),
    ] = DiagnosticOutputFormat.TEXT,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Optional JSON output path"),
    ] = None,
    sample_keyword: Annotated[
        list[str] | None,
        typer.Option(
            "--sample-keyword",
            help="Keyword to use for live search probes; repeat to override defaults",
        ),
    ] = None,
) -> None:
    """Run source-health diagnostics for zero-result investigations."""
    from denbust.diagnostics import (
        render_source_diagnostic_report,
        run_source_diagnostics,
    )

    if artifacts_only and live_only:
        raise typer.BadParameter("Choose at most one of --artifacts-only and --live-only")

    config_path = config or Path("agents/news.yaml")
    try:
        report = run_source_diagnostics(
            config_path=config_path,
            source_names=source,
            days_override=days,
            include_artifacts=not live_only,
            include_live=not artifacts_only,
            sample_keywords=sample_keyword,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    if output is not None:
        output.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    if format == DiagnosticOutputFormat.JSON:
        typer.echo(report.model_dump_json(indent=2))
        return

    typer.echo(render_source_diagnostic_report(report))


@app.command("diagnose-discovery")
def diagnose_discovery(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    stale_days: Annotated[
        int,
        typer.Option("--stale-days", help="Age threshold for stale queued candidates"),
    ] = 7,
    format: Annotated[
        DiagnosticOutputFormat,
        typer.Option("--format", help="Output format"),
    ] = DiagnosticOutputFormat.TEXT,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Optional JSON output path"),
    ] = None,
) -> None:
    """Summarize discovery-layer overlap, queue health, and conversion diagnostics."""
    from denbust.diagnostics import (
        render_discovery_diagnostic_report,
        run_discovery_diagnostics,
    )

    config_path = config or Path("agents/news/local.yaml")
    report = run_discovery_diagnostics(
        config_path=config_path,
        stale_after_days=stale_days,
    )

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    if format == DiagnosticOutputFormat.JSON:
        typer.echo(report.model_dump_json(indent=2))
        return

    typer.echo(render_discovery_diagnostic_report(report))


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
    publish: Annotated[
        bool | None,
        typer.Option(
            "--publish/--no-publish",
            help="Enable or disable publication to configured public targets.",
        ),
    ] = None,
) -> None:
    """Run the scaffolded release job for a dataset."""
    from denbust.pipeline import run_release

    config_path = config or Path("agents/release/news_items.yaml")
    previous_publish = os.environ.get("DENBUST_RELEASE_PUBLISH")
    try:
        if publish is not None:
            os.environ["DENBUST_RELEASE_PUBLISH"] = "true" if publish else "false"
        run_release(config_path=config_path, dataset_name=dataset)
    finally:
        if publish is not None:
            if previous_publish is None:
                os.environ.pop("DENBUST_RELEASE_PUBLISH", None)
            else:
                os.environ["DENBUST_RELEASE_PUBLISH"] = previous_publish


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


@report_app.command("monthly")
def report_monthly(
    month: Annotated[
        str,
        typer.Option("--month", help="Calendar month to report in YYYY-MM format"),
    ],
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Optional Markdown output path"),
    ] = None,
    json_output: Annotated[
        Path | None,
        typer.Option("--json-output", help="Optional JSON output path"),
    ] = None,
    hq_activity: Annotated[
        str | None,
        typer.Option("--hq-activity", help="Optional manual TFHT activity text"),
    ] = None,
    hq_activity_file: Annotated[
        Path | None,
        typer.Option("--hq-activity-file", help="Optional UTF-8 file with HQ activity text"),
    ] = None,
) -> None:
    """Generate the monthly public report bundle for news_items."""
    from denbust.news_items.monthly_report import parse_month_key
    from denbust.pipeline import run_news_items_monthly_report

    try:
        parse_month_key(month)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    config_path = config or Path("agents/news/local.yaml")
    report = run_news_items_monthly_report(
        config_path=config_path,
        month=month,
        output_path=output,
        json_output_path=json_output,
        hq_activity=hq_activity,
        hq_activity_file=hq_activity_file,
    )
    if output is None:
        typer.echo(report.rendered_markdown)


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


@app.command("validation-import-reviewed-table")
def validation_import_reviewed_table(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Path to a reviewed table workbook"),
    ],
    format_name: Annotated[
        str,
        typer.Option("--format", help="Reviewed table format adapter to use"),
    ] = "tfht_manual_tracking_v1",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Path to the generated validation draft CSV"),
    ] = None,
) -> None:
    """Normalize a reviewed TFHT example table into the validation draft CSV shape."""
    from denbust.validation import import_reviewed_table

    result = import_reviewed_table(
        input_path=input_path,
        format_name=format_name,
        output_path=output,
    )
    typer.echo(f"Wrote {result.imported_rows} reviewed rows to {result.output_path}")
    for warning in result.warnings:
        typer.echo(f"warning: {warning}", err=True)


@app.command("news-items-import-corrections")
def news_items_import_corrections(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Path to a manual corrections CSV"),
    ],
    format_name: Annotated[
        str,
        typer.Option("--format", help="Correction import format adapter to use"),
    ] = "news_items_corrections_csv_v1",
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
) -> None:
    """Import manual news_items corrections into the configured operational store."""
    from denbust.config import load_config
    from denbust.news_items.annotations import (
        NEWS_ITEMS_CORRECTIONS_CSV_V1,
        import_news_item_corrections_csv,
    )
    from denbust.ops.factory import create_operational_store

    if format_name != NEWS_ITEMS_CORRECTIONS_CSV_V1:
        raise typer.BadParameter(f"Unsupported corrections format: {format_name}")

    config_path = config or Path("agents/news/local.yaml")
    loaded_config = load_config(config_path)
    corrections, warnings = import_news_item_corrections_csv(input_path)
    store = create_operational_store(loaded_config)
    try:
        store.upsert_news_item_corrections(
            loaded_config.dataset_name.value,
            [row.model_dump(mode="json") for row in corrections],
        )
    finally:
        store.close()
    typer.echo(f"Imported {len(corrections)} correction row(s) from {input_path}")
    for warning in warnings:
        typer.echo(f"warning: {warning}", err=True)


@app.command("news-items-import-missing-items")
def news_items_import_missing_items(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Path to a missing-items CSV"),
    ],
    format_name: Annotated[
        str,
        typer.Option("--format", help="Missing-items import format adapter to use"),
    ] = "news_items_missing_items_csv_v1",
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
) -> None:
    """Import manually curated missing news_items rows into the operational store."""
    from denbust.config import load_config
    from denbust.news_items.annotations import (
        NEWS_ITEMS_MISSING_ITEMS_CSV_V1,
        import_missing_news_items_csv,
    )
    from denbust.ops.factory import create_operational_store

    if format_name != NEWS_ITEMS_MISSING_ITEMS_CSV_V1:
        raise typer.BadParameter(f"Unsupported missing-items format: {format_name}")

    config_path = config or Path("agents/news/local.yaml")
    loaded_config = load_config(config_path)
    missing_items, warnings = import_missing_news_items_csv(input_path)
    store = create_operational_store(loaded_config)
    try:
        store.upsert_missing_news_items(
            loaded_config.dataset_name.value,
            [row.model_dump(mode="json") for row in missing_items],
        )
    finally:
        store.close()
    typer.echo(f"Imported {len(missing_items)} missing-item row(s) from {input_path}")
    for warning in warnings:
        typer.echo(f"warning: {warning}", err=True)


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
    typer.echo(f"Saved Markdown report to {result.markdown_path}")


@app.command("validation-lint")
def validation_lint(
    validation_set: Annotated[
        Path,
        typer.Option("--validation-set", help="Path to the tracked validation CSV"),
    ] = DEFAULT_VALIDATION_SET_PATH,
) -> None:
    """Lint the permanent validation set without model credentials."""
    from denbust.validation import run_validation_lint

    result = run_validation_lint(validation_set_path=validation_set)
    if result.passed:
        typer.echo(f"Validation set OK: {result.row_count} row(s) in {result.validation_set_path}")
        return
    for issue in result.issues:
        typer.echo(issue.render(), err=True)
    raise typer.Exit(code=1)


@app.command("live-check")
def live_check(
    config: Annotated[
        Path,
        typer.Option("--config", "-c", help="Path to the live-check scenario YAML"),
    ],
    output_root: Annotated[
        Path | None,
        typer.Option("--output-root", help="Directory for live-check output bundles"),
    ] = None,
) -> None:
    """Run a tracked live-check scenario."""
    from denbust.live_checks.runner import run_live_check_scenario_sync

    report = run_live_check_scenario_sync(config, output_root=output_root)
    typer.echo(f"Live check {report.overall_status}: {report.output_dir}")
    for case in report.case_results:
        status = "passed" if case.passed else "failed"
        typer.echo(f"{case.case_id}: {status}")
        if case.error:
            typer.echo(f"{case.case_id} error: {case.error}", err=True)
    if report.overall_status != "passed":
        raise typer.Exit(code=1)


@app.command("candidates-b2-suppress")
def candidates_b2_suppress(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    ids: Annotated[
        str | None,
        typer.Option("--ids", help="Comma-separated candidate ids to suppress (Stage B2)."),
    ] = None,
    domains: Annotated[
        str | None,
        typer.Option(
            "--domains",
            help="Comma-separated spam domains; suppress all still-scrapeable candidates on them.",
        ),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional reason recorded on suppressed candidates."),
    ] = None,
) -> None:
    """Stage B2 — manually suppress clearly-junk candidates before they consume scrape budget.

    Use ``--ids`` for one-off junk on legitimate domains, ``--domains`` for whole
    spam domains (also add the domain to ``_IRRELEVANT_CONTENT_DOMAINS`` to block
    future discovery). See ``docs/batch_scraping_protocol.md``.
    """
    from denbust.config import load_config
    from denbust.discovery.manual_filter import (
        suppress_candidates_b2,
        suppress_candidates_b2_by_domain,
    )
    from denbust.discovery.storage import create_discovery_persistence

    if not ids and not domains:
        typer.echo("Provide --ids and/or --domains.")
        raise typer.Exit(code=1)

    config_path = config or Path("agents/news/local.yaml")
    cfg = load_config(config_path)
    persistence = create_discovery_persistence(cfg)
    total = 0
    try:
        if ids:
            id_list = [value.strip() for value in ids.split(",") if value.strip()]
            suppressed = suppress_candidates_b2(persistence, id_list, note=note)
            total += len(suppressed)
            typer.echo(f"Stage B2: suppressed {len(suppressed)} candidate(s) by id.")
        if domains:
            domain_list = [value.strip() for value in domains.split(",") if value.strip()]
            suppressed = suppress_candidates_b2_by_domain(persistence, domain_list, note=note)
            total += len(suppressed)
            typer.echo(f"Stage B2: suppressed {len(suppressed)} candidate(s) by domain.")
    finally:
        persistence.close()
    typer.echo(f"Stage B2 total suppressed: {total}")


@app.command("classify-domains")
def classify_domains(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Max number of new domains to classify this run."),
    ] = None,
    suppress: Annotated[
        bool,
        typer.Option("--suppress", help="Stage-B2-suppress candidates on 'block'-verdict domains."),
    ] = False,
) -> None:
    """Classify not-yet-judged candidate domains with the per-domain LLM verdict gate.

    Populates the durable verdict cache (``domain_verdicts.jsonl``). The automated
    successor to manual blocklist rounds: each new domain is judged once and
    cached. With ``--suppress``, candidates on ``block`` domains are removed from
    the scrapeable pool immediately. See ``docs/batch_scraping_protocol.md``.
    """
    from denbust.config import load_config
    from denbust.discovery.candidate_filters import globally_excluded_search_domains
    from denbust.discovery.domain_verdicts import (
        DomainClassifier,
        DomainVerdictStore,
        blocked_domains,
        classify_pool_domains,
    )
    from denbust.discovery.manual_filter import suppress_candidates_b2_by_domain
    from denbust.discovery.scrape_queue import SCRAPEABLE_CANDIDATE_STATUSES
    from denbust.discovery.storage import create_discovery_persistence

    cfg = load_config(config or Path("agents/news/local.yaml"))
    if not cfg.anthropic_api_key:
        typer.echo("ANTHROPIC_API_KEY not set.")
        raise typer.Exit(code=1)

    store = DomainVerdictStore(cfg.discovery_state_paths.domain_verdicts_path)
    classifier = DomainClassifier(api_key=cfg.anthropic_api_key, model=cfg.classifier.model)
    persistence = create_discovery_persistence(cfg)
    try:
        pool = persistence.list_candidates(statuses=SCRAPEABLE_CANDIDATE_STATUSES)
        new_verdicts = classify_pool_domains(
            pool,
            store=store,
            classifier=classifier,
            static_blocklist=frozenset(globally_excluded_search_domains()),
            limit=limit,
        )
        allow = sum(1 for v in new_verdicts if v.decision == "allow")
        block = sum(1 for v in new_verdicts if v.decision == "block")
        typer.echo(f"Classified {len(new_verdicts)} new domain(s): {allow} allow, {block} block.")
        if suppress:
            suppressed = suppress_candidates_b2_by_domain(
                persistence, blocked_domains(store), note="domain-verdict gate: block"
            )
            typer.echo(f"Suppressed {len(suppressed)} candidate(s) on block-verdict domains.")
    finally:
        persistence.close()


@app.command("search-budget")
def search_budget(
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to YAML config file"),
    ] = None,
    month: Annotated[
        str | None,
        typer.Option("--month", help="Month to report as YYYY-MM (defaults to current UTC month)."),
    ] = None,
) -> None:
    """Show month-to-date Brave/Exa/Google-CSE search spend vs configured budgets."""
    from datetime import UTC, datetime

    from denbust.config import load_config
    from denbust.discovery.search_budget import SearchBudgetLedger, month_to_date_summary

    cfg = load_config(config or Path("agents/news/local.yaml"))
    year_month = month or datetime.now(UTC).strftime("%Y-%m")
    ledger = SearchBudgetLedger(cfg.discovery_state_paths.search_budget_path)
    engines = ("brave", "exa", "google_cse")
    summary = month_to_date_summary(ledger, year_month=year_month, engines=engines)

    typer.echo(f"Search budget — {year_month}")
    for engine in engines:
        queries, usd = summary[engine]
        engine_cfg = getattr(cfg.discovery.engines, engine, None)
        budget = getattr(engine_cfg, "monthly_budget_usd", None)
        cap = f" / ${budget:.2f}" if budget is not None else " (no cap)"
        typer.echo(f"  {engine:<11} {queries:>6} queries  ${usd:>7.3f}{cap}")


@app.command()
def version() -> None:
    """Show version information."""
    from denbust import __version__

    typer.echo(f"denbust version {__version__}")


if __name__ == "__main__":
    app()
