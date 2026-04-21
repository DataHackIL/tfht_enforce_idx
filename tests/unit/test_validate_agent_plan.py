"""Unit tests for the .agent-plan validator script."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "validate_agent_plan.py"


def _run_validator(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(path)],
        capture_output=True,
        text=True,
        check=False,
    )


def _write_plan(path: Path, ledger_lines: list[str], *, blockers: str = "none") -> None:
    path.write_text(
        "\n".join(
            [
                "# .agent-plan.md",
                "",
                "> Semantics: This file is authored from the perspective of the repository state",
                "> immediately after the current branch is merged into `main`. On a feature",
                "> branch it is a forward-looking merge contract; on `main` the same text is",
                "> present-tense fact.",
                "",
                "## Mainline Status",
                "",
                "- Last merged PR on main: `DL-PR-10` via PR `#91`.",
                "- Next planned PR: `C-8`.",
                f"- Current blockers on main: {blockers}.",
                "",
                "## Task Ledger",
                "",
                *ledger_lines,
                "",
                "## Planning Workflow",
                "",
                "- Keep the file aligned to post-merge mainline truth.",
                "",
                "## Context Pointers",
                "",
                "- README",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_validator_accepts_valid_agent_plan(tmp_path: Path) -> None:
    plan_path = tmp_path / ".agent-plan.md"
    _write_plan(
        plan_path,
        [
            "- [done] Finish `DL-PR-10` source suggestions + Facebook-targeted discovery support.",
            "- [next] Update keywords / re-scan with new taxonomy from `C-8`.",
            "- [later] Optional `DL-PR-12` self-healing scaffolding hooks.",
        ],
    )

    result = _run_validator(plan_path)

    assert result.returncode == 0
    assert "OK" in result.stdout


def test_validator_rejects_missing_required_heading(tmp_path: Path) -> None:
    plan_path = tmp_path / ".agent-plan.md"
    plan_path.write_text(
        "\n".join(
            [
                "# .agent-plan.md",
                "",
                "## Mainline Status",
                "",
                "- Last merged PR on main: `DL-PR-10`.",
                "- Next planned PR: `C-8`.",
                "- Current blockers on main: none.",
                "",
                "## Task Ledger",
                "",
                "- [next] Update keywords / re-scan with new taxonomy from `C-8`.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run_validator(plan_path)

    assert result.returncode == 1
    assert "missing required heading: ## Planning Workflow" in result.stderr
    assert "missing required heading: ## Context Pointers" in result.stderr


def test_validator_rejects_unknown_status(tmp_path: Path) -> None:
    plan_path = tmp_path / ".agent-plan.md"
    _write_plan(
        plan_path,
        [
            "- [done] Finish `DL-PR-10` source suggestions + Facebook-targeted discovery support.",
            "- [todo] Update keywords / re-scan with new taxonomy from `C-8`.",
        ],
    )

    result = _run_validator(plan_path)

    assert result.returncode == 1
    assert "invalid task ledger status [todo]" in result.stderr


def test_validator_requires_exactly_one_next_entry(tmp_path: Path) -> None:
    no_next_path = tmp_path / "no-next.md"
    _write_plan(
        no_next_path,
        [
            "- [done] Finish `DL-PR-10` source suggestions + Facebook-targeted discovery support.",
            "- [later] Update keywords / re-scan with new taxonomy from `C-8`.",
        ],
    )
    no_next = _run_validator(no_next_path)
    assert no_next.returncode == 1
    assert "expected exactly one [next] task ledger entry, found 0" in no_next.stderr

    multi_next_path = tmp_path / "multi-next.md"
    _write_plan(
        multi_next_path,
        [
            "- [next] Update keywords / re-scan with new taxonomy from `C-8`.",
            "- [next] Optional `DL-PR-12` self-healing scaffolding hooks.",
        ],
    )
    multi_next = _run_validator(multi_next_path)
    assert multi_next.returncode == 1
    assert "expected exactly one [next] task ledger entry, found 2" in multi_next.stderr


def test_validator_rejects_ambiguous_in_progress_phrase(tmp_path: Path) -> None:
    plan_path = tmp_path / ".agent-plan.md"
    _write_plan(
        plan_path,
        [
            "- [done] Finish `DL-PR-10` source suggestions + Facebook-targeted discovery support.",
            "- [next] Update keywords / re-scan with new taxonomy from `C-8`.",
        ],
        blockers="no explicit blocker; `DL-PR-11` is in progress",
    )

    result = _run_validator(plan_path)

    assert result.returncode == 1
    assert "forbidden ambiguous status phrase found: 'in progress'" in result.stderr
