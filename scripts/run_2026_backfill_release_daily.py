"""Run the 2026 news_items backfill with cost guards and release dry-run output."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

import yaml

from denbust.config import Config, load_config
from denbust.discovery.backfill import BackfillWindow, build_backfill_queries, plan_backfill_windows

BRAVE_SEARCH_REQUEST_USD = 0.005
EXA_SEARCH_REQUEST_USD = 0.007


@dataclass(frozen=True)
class LedgerEntry:
    """One command or guardrail event emitted by the backfill harness."""

    step: str
    status: str
    batch_id: str | None
    window_start: str | None
    window_end: str | None
    command: list[str]
    log_path: str | None
    estimated_cost_usd: float
    cumulative_estimated_cost_usd: float
    returncode: int | None = None
    note: str | None = None


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _day_start(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=UTC)


def _day_end(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=UTC)


def _batch_id(window: BackfillWindow) -> str:
    start = window.date_from.date().isoformat()
    end = window.date_to.date().isoformat()
    return f"backfill-2026-{start}_{end}"


def _enabled_search_engines(config: Config) -> list[str]:
    engines: list[str] = []
    if config.discovery.engines.brave.enabled:
        engines.append("brave")
    if config.discovery.engines.exa.enabled:
        engines.append("exa")
    if config.discovery.engines.google_cse.enabled:
        engines.append("google_cse")
    return engines


def _estimate_discover_cost(config: Config, window: BackfillWindow) -> tuple[float, str]:
    query_count = len(build_backfill_queries(config, window=window))
    engines = _enabled_search_engines(config)
    brave_requests = query_count if "brave" in engines else 0
    exa_requests = query_count if "exa" in engines else 0
    unknown_requests = query_count if "google_cse" in engines else 0
    cost = (brave_requests * BRAVE_SEARCH_REQUEST_USD) + (exa_requests * EXA_SEARCH_REQUEST_USD)
    note = (
        f"estimated query_count={query_count}, brave_requests={brave_requests}, "
        f"exa_requests={exa_requests}, google_cse_requests={unknown_requests}"
    )
    return cost, note


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_markdown(path: Path, entries: list[LedgerEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 2026 Backfill Run Ledger",
        "",
        "| step | status | batch | estimated cost | cumulative | note |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for entry in entries:
        lines.append(
            "| {step} | {status} | {batch} | ${cost:.2f} | ${cumulative:.2f} | {note} |".format(
                step=entry.step,
                status=entry.status,
                batch=entry.batch_id or "",
                cost=entry.estimated_cost_usd,
                cumulative=entry.cumulative_estimated_cost_usd,
                note=(entry.note or "").replace("|", "\\|"),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_command(
    command: list[str],
    *,
    env: dict[str, str],
    log_path: Path,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        completed = subprocess.run(
            command,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            check=False,
        )
    return completed.returncode


def _add_entry(
    entries: list[LedgerEntry],
    *,
    step: str,
    status: str,
    batch_id: str | None,
    window: BackfillWindow | None,
    command: list[str],
    log_path: Path | None,
    estimated_cost_usd: float,
    returncode: int | None = None,
    note: str | None = None,
) -> float:
    cumulative = sum(entry.estimated_cost_usd for entry in entries) + estimated_cost_usd
    entries.append(
        LedgerEntry(
            step=step,
            status=status,
            batch_id=batch_id,
            window_start=window.date_from.isoformat() if window else None,
            window_end=window.date_to.isoformat() if window else None,
            command=command,
            log_path=str(log_path) if log_path else None,
            estimated_cost_usd=round(estimated_cost_usd, 4),
            cumulative_estimated_cost_usd=round(cumulative, 4),
            returncode=returncode,
            note=note,
        )
    )
    return cumulative


def _guard_budget(
    entries: list[LedgerEntry],
    *,
    projected_cost: float,
    max_budget_usd: float,
) -> None:
    current = sum(entry.estimated_cost_usd for entry in entries)
    if current + projected_cost > max_budget_usd:
        raise RuntimeError(
            f"estimated budget would exceed ${max_budget_usd:.2f}: "
            f"current=${current:.2f}, projected_step=${projected_cost:.2f}"
        )


def _base_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    env["DENBUST_STATE_ROOT"] = str(args.state_root)
    env["DENBUST_RELEASE_PUBLISH"] = "false"
    return env


def _command(args: argparse.Namespace, *parts: str) -> list[str]:
    return [args.denbust_bin, *parts]


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    raw_config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_config, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return raw_config


def _apply_operational_overrides(raw_config: dict[str, object], args: argparse.Namespace) -> None:
    if args.operational_provider == "config":
        return
    operational = raw_config.setdefault("operational", {})
    if not isinstance(operational, dict):
        raise ValueError("operational config must be a mapping")
    operational["provider"] = args.operational_provider
    if args.operational_provider == "local_json":
        output = raw_config.setdefault("output", {})
        if not isinstance(output, dict):
            raise ValueError("output config must be a mapping")
        output["formats"] = ["cli"]


def _effective_config_path(args: argparse.Namespace) -> Path:
    raw_config = _load_yaml_mapping(args.config)
    _apply_operational_overrides(raw_config, args)
    if args.search_mode == "config":
        path = args.artifacts_root / "effective_config.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return path

    discovery = raw_config.setdefault("discovery", {})
    if not isinstance(discovery, dict):
        raise ValueError("discovery config must be a mapping")
    engines = discovery.setdefault("engines", {})
    if not isinstance(engines, dict):
        raise ValueError("discovery.engines config must be a mapping")

    brave = engines.setdefault("brave", {})
    exa = engines.setdefault("exa", {})
    google_cse = engines.setdefault("google_cse", {})
    if not all(isinstance(engine, dict) for engine in (brave, exa, google_cse)):
        raise ValueError("discovery engine configs must be mappings")

    brave["enabled"] = True
    exa["enabled"] = args.search_mode == "brave-exa"
    google_cse["enabled"] = False
    path = args.artifacts_root / "effective_config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _effective_release_config_path(args: argparse.Namespace) -> Path:
    raw_config = _load_yaml_mapping(args.release_config)
    _apply_operational_overrides(raw_config, args)
    release = raw_config.setdefault("release", {})
    if not isinstance(release, dict):
        raise ValueError("release config must be a mapping")
    release["publish_public_targets"] = False
    path = args.artifacts_root / "effective_release_config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(raw_config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-from", type=_parse_date, default=date(2026, 1, 1))
    parser.add_argument("--date-to", type=_parse_date, default=date(2026, 5, 15))
    parser.add_argument("--config", type=Path, default=Path("agents/news/github.yaml"))
    parser.add_argument(
        "--release-config",
        type=Path,
        default=Path("agents/release/news_items_no_publish.yaml"),
    )
    parser.add_argument("--state-root", type=Path, default=Path("data/2026_backfill_state"))
    parser.add_argument("--artifacts-root", type=Path, default=Path("data/2026_backfill_run"))
    parser.add_argument("--denbust-bin", default="denbust")
    parser.add_argument(
        "--search-mode",
        choices=["brave-only", "brave-exa", "config"],
        default="brave-only",
        help="Provider mix for generated effective config; default keeps full run under budget.",
    )
    parser.add_argument("--max-budget-usd", type=float, default=50.0)
    parser.add_argument("--soft-budget-usd", type=float, default=40.0)
    parser.add_argument(
        "--operational-provider",
        choices=["local_json", "supabase", "config"],
        default="local_json",
        help="Operational store for this harness run; local_json avoids placeholder Supabase env.",
    )
    parser.add_argument("--max-scrape-drains-per-window", type=int, default=2)
    parser.add_argument("--estimated-classifier-cost-per-scrape-run-usd", type=float, default=0.5)
    parser.add_argument("--skip-discover", action="store_true")
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-release", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.date_from > args.date_to:
        raise SystemExit("--date-from must be on or before --date-to")
    if args.max_scrape_drains_per_window < 0:
        raise SystemExit("--max-scrape-drains-per-window must be non-negative")

    effective_config_path = _effective_config_path(args)
    effective_release_config_path = _effective_release_config_path(args)
    config = load_config(effective_config_path)
    windows = plan_backfill_windows(
        date_from=_day_start(args.date_from),
        date_to=_day_end(args.date_to),
        batch_window_days=config.backfill.batch_window_days,
    )
    ledger_entries: list[LedgerEntry] = []
    ledger_json = args.artifacts_root / "ledger.json"
    ledger_md = args.artifacts_root / "ledger.md"
    logs_dir = args.artifacts_root / "logs"
    diagnostics_dir = args.artifacts_root / "diagnostics"
    env = _base_env(args)
    abort_run = False

    try:
        for window in windows:
            if abort_run:
                break
            batch_id = _batch_id(window)
            window_env = {
                **env,
                "DENBUST_BACKFILL_DATE_FROM": window.date_from.isoformat(),
                "DENBUST_BACKFILL_DATE_TO": window.date_to.isoformat(),
                "DENBUST_BACKFILL_BATCH_ID": batch_id,
            }
            if not args.skip_discover:
                estimate, note = _estimate_discover_cost(config, window)
                _guard_budget(
                    ledger_entries,
                    projected_cost=estimate,
                    max_budget_usd=args.max_budget_usd,
                )
                command = _command(
                    args,
                    "run",
                    "--dataset",
                    "news_items",
                    "--job",
                    "backfill_discover",
                    "--config",
                    str(effective_config_path),
                )
                log_path = logs_dir / f"{batch_id}_discover.log"
                returncode = (
                    0 if args.dry_run else _run_command(command, env=window_env, log_path=log_path)
                )
                status = "dry_run" if args.dry_run else ("ok" if returncode == 0 else "failed")
                _add_entry(
                    ledger_entries,
                    step="backfill_discover",
                    status=status,
                    batch_id=batch_id,
                    window=window,
                    command=command,
                    log_path=log_path,
                    estimated_cost_usd=estimate,
                    returncode=returncode,
                    note=note,
                )
                if returncode != 0:
                    abort_run = True
                    break

            if not args.skip_scrape:
                for drain_index in range(args.max_scrape_drains_per_window):
                    estimate = args.estimated_classifier_cost_per_scrape_run_usd
                    _guard_budget(
                        ledger_entries,
                        projected_cost=estimate,
                        max_budget_usd=args.max_budget_usd,
                    )
                    command = _command(
                        args,
                        "run",
                        "--dataset",
                        "news_items",
                        "--job",
                        "backfill_scrape",
                        "--config",
                        str(effective_config_path),
                    )
                    log_path = logs_dir / f"{batch_id}_scrape_{drain_index + 1:02d}.log"
                    returncode = (
                        0
                        if args.dry_run
                        else _run_command(command, env=window_env, log_path=log_path)
                    )
                    status = "dry_run" if args.dry_run else ("ok" if returncode == 0 else "failed")
                    _add_entry(
                        ledger_entries,
                        step=f"backfill_scrape_{drain_index + 1}",
                        status=status,
                        batch_id=batch_id,
                        window=window,
                        command=command,
                        log_path=log_path,
                        estimated_cost_usd=estimate,
                        returncode=returncode,
                        note="rough classifier cost estimate for one scrape drain",
                    )
                    if returncode != 0:
                        abort_run = True
                        break

            if not abort_run:
                diagnose_command = _command(
                    args,
                    "diagnose-discovery",
                    "--config",
                    str(effective_config_path),
                    "--format",
                    "json",
                    "--output",
                    str(diagnostics_dir / f"{batch_id}.json"),
                )
                diagnose_log = logs_dir / f"{batch_id}_diagnose_discovery.log"
                diagnose_returncode = (
                    0
                    if args.dry_run
                    else _run_command(diagnose_command, env=window_env, log_path=diagnose_log)
                )
                diagnose_status = (
                    "dry_run" if args.dry_run else ("ok" if diagnose_returncode == 0 else "failed")
                )
                _add_entry(
                    ledger_entries,
                    step="diagnose_discovery",
                    status=diagnose_status,
                    batch_id=batch_id,
                    window=window,
                    command=diagnose_command,
                    log_path=diagnose_log,
                    estimated_cost_usd=0.0,
                    returncode=diagnose_returncode,
                    note="local diagnostics only",
                )
                if diagnose_returncode != 0:
                    abort_run = True

            _write_json(ledger_json, [asdict(entry) for entry in ledger_entries])
            _write_markdown(ledger_md, ledger_entries)
            if abort_run:
                _add_entry(
                    ledger_entries,
                    step="abort",
                    status="failed",
                    batch_id=batch_id,
                    window=window,
                    command=[],
                    log_path=None,
                    estimated_cost_usd=0.0,
                    note="stopped after failed step; release skipped",
                )
                break
            if sum(entry.estimated_cost_usd for entry in ledger_entries) >= args.soft_budget_usd:
                _add_entry(
                    ledger_entries,
                    step="soft_budget_notice",
                    status="notice",
                    batch_id=None,
                    window=None,
                    command=[],
                    log_path=None,
                    estimated_cost_usd=0.0,
                    note=f"estimated cost reached soft budget ${args.soft_budget_usd:.2f}",
                )
                break

        if not args.skip_release and not abort_run:
            release_command = _command(
                args,
                "release",
                "--dataset",
                "news_items",
                "--config",
                str(effective_release_config_path),
                "--no-publish",
            )
            release_log = logs_dir / "release_no_publish.log"
            release_returncode = (
                0 if args.dry_run else _run_command(release_command, env=env, log_path=release_log)
            )
            release_status = (
                "dry_run" if args.dry_run else ("ok" if release_returncode == 0 else "failed")
            )
            _add_entry(
                ledger_entries,
                step="release_no_publish",
                status=release_status,
                batch_id=None,
                window=None,
                command=release_command,
                log_path=release_log,
                estimated_cost_usd=0.0,
                returncode=release_returncode,
                note="public publication disabled",
            )
    finally:
        _write_json(ledger_json, [asdict(entry) for entry in ledger_entries])
        _write_markdown(ledger_md, ledger_entries)

    failed = [entry for entry in ledger_entries if entry.status == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
