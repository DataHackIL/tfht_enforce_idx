#!/usr/bin/env python3
"""Validate the repository's mainline-semantic .agent-plan.md format."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REQUIRED_HEADINGS = (
    "## Mainline Status",
    "## Task Ledger",
    "## Planning Workflow",
    "## Context Pointers",
)
MAINLINE_PREFIXES = (
    "- Last merged PR on main:",
    "- Next planned PR:",
    "- Current blockers on main:",
)
ALLOWED_STATUSES = {"done", "next", "later", "blocked"}
FORBIDDEN_PHRASES = ("in progress",)
TASK_LEDGER_HEADING = "## Task Ledger"
SECTION_HEADING_RE = re.compile(r"^##\s+")
TASK_STATUS_RE = re.compile(r"^- \[([a-z]+)\] ")


def _extract_section(lines: list[str], heading: str) -> list[str]:
    """Return the lines that belong to one second-level heading."""
    try:
        start = lines.index(heading)
    except ValueError:
        return []

    section: list[str] = []
    for line in lines[start + 1 :]:
        if SECTION_HEADING_RE.match(line):
            break
        section.append(line)
    return section


def validate_agent_plan(path: Path) -> list[str]:
    """Validate one .agent-plan.md file and return all detected problems."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    errors: list[str] = []

    for heading in REQUIRED_HEADINGS:
        if heading not in lines:
            errors.append(f"missing required heading: {heading}")

    lowered = text.casefold()
    for phrase in FORBIDDEN_PHRASES:
        if phrase in lowered:
            errors.append(f"forbidden ambiguous status phrase found: {phrase!r}")

    mainline_section = _extract_section(lines, "## Mainline Status")
    if mainline_section:
        for prefix in MAINLINE_PREFIXES:
            if not any(line.startswith(prefix) for line in mainline_section):
                errors.append(f"missing required mainline status field: {prefix}")

    task_ledger = _extract_section(lines, TASK_LEDGER_HEADING)
    next_count = 0
    for line in task_ledger:
        if not line.startswith("- ["):
            continue
        match = TASK_STATUS_RE.match(line)
        if match is None:
            errors.append(f"invalid task ledger entry format: {line}")
            continue
        status = match.group(1)
        if status not in ALLOWED_STATUSES:
            errors.append(f"invalid task ledger status [{status}] in line: {line}")
            continue
        if status == "next":
            next_count += 1

    if next_count != 1:
        errors.append(f"expected exactly one [next] task ledger entry, found {next_count}")

    return errors


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Validate the repository's mainline-semantic .agent-plan.md file."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".agent-plan.md",
        help="Path to the plan file to validate. Defaults to .agent-plan.md.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)
    path = Path(args.path)
    if not path.is_file():
        print(f"missing file: {path}", file=sys.stderr)
        return 1

    errors = validate_agent_plan(path)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print(f"{path}: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
