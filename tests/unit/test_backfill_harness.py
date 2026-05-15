"""Focused tests for the 2026 backfill harness."""

from __future__ import annotations

import json
from pathlib import Path

import scripts.run_2026_backfill_release_daily as harness

from denbust.config import Config


def test_backfill_harness_aborts_after_failed_scrape_and_skips_release(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A failed scrape must not continue into later windows or release output."""
    effective_config = tmp_path / "effective_config.yaml"
    effective_release_config = tmp_path / "effective_release_config.yaml"
    effective_config.write_text("dataset_name: news_items\n", encoding="utf-8")
    effective_release_config.write_text("dataset_name: news_items\n", encoding="utf-8")
    monkeypatch.setattr(harness, "_effective_config_path", lambda _args: effective_config)
    monkeypatch.setattr(
        harness,
        "_effective_release_config_path",
        lambda _args: effective_release_config,
    )
    monkeypatch.setattr(harness, "load_config", lambda _path: Config())
    monkeypatch.setattr(harness, "_estimate_discover_cost", lambda _config, _window: (0.0, "test"))

    calls: list[list[str]] = []

    def fake_run_command(command: list[str], *, env: dict[str, str], log_path: Path) -> int:
        del env
        calls.append(command)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("log\n", encoding="utf-8")
        if "--job" in command and command[command.index("--job") + 1] == "backfill_scrape":
            return 1
        return 0

    monkeypatch.setattr(harness, "_run_command", fake_run_command)

    result = harness.main(
        [
            "--date-from",
            "2026-01-01",
            "--date-to",
            "2026-01-14",
            "--state-root",
            str(tmp_path / "state"),
            "--artifacts-root",
            str(tmp_path / "artifacts"),
            "--denbust-bin",
            "denbust",
        ]
    )

    assert result == 1
    command_text = [" ".join(command) for command in calls]
    assert sum("backfill_discover" in command for command in command_text) == 1
    assert sum("backfill_scrape" in command for command in command_text) == 1
    assert all("diagnose-discovery" not in command for command in command_text)
    assert all(" release " not in f" {command} " for command in command_text)

    ledger = json.loads((tmp_path / "artifacts" / "ledger.json").read_text(encoding="utf-8"))
    assert [entry["step"] for entry in ledger] == [
        "backfill_discover",
        "backfill_scrape_1",
        "abort",
    ]
