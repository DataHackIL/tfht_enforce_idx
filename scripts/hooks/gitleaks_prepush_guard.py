#!/usr/bin/env python3
"""Claude Code PreToolUse hook: secret-scan before a `git push`.

Reads the PreToolUse event JSON on stdin. When the Bash command is a ``git
push``, it runs gitleaks over the target repo's tracked content (using the
repo-root ``.gitleaks.toml``) and **blocks** the push (exit code 2) when secrets
are found, printing the redacted findings so Claude can fix them instead of
publishing.

This is the best-effort "Claude ability" pre-push guard. The *enforced* secret
scanning lives in the git pre-push hook (pre-commit) and the
``scripts/state-run.sh`` push guard; this one is a convenience net that stops an
agent from pushing a secret in the first place.

Design notes:
  * Push detection is shlex-tokenized across ``&&``/``;``/``|`` and tolerates git
    global options (``--no-pager``, ``-c k=v``, ``-C <dir>``, env-var prefixes),
    so it does not fail *open* on a push form an over-specific regex would miss.
  * Each detected push is scanned in the directory named by its ``-C <dir>``
    (default the current dir), not a hardcoded ``.``.
  * The config is resolved relative to this script and passed explicitly, so the
    strict ruleset is always applied rather than relying on cwd auto-discovery.

Exit codes (Claude Code hook protocol): 0 = allow, 2 = block.
"""

from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# .gitleaks.toml lives at the repo root: scripts/hooks/<this file> -> parents[2].
_CONFIG = Path(__file__).resolve().parents[2] / ".gitleaks.toml"

# git global options that consume the following token as their argument.
_OPTS_WITH_ARG = {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path"}


def _push_targets(command: str) -> list[str]:
    """Return the scan directory for each `git push` in a shell command.

    Splits on shell separators, then for each segment walks tokens: skip a
    leading run of ``VAR=value`` env assignments, require ``git``, consume git
    global options (tracking ``-C <dir>``), and if the resulting subcommand is
    ``push`` record the target dir. Returns ``["."]`` as a conservative fallback
    when the command cannot be tokenized but clearly contains a git push.
    """
    try:
        tokens = shlex.split(command, comments=False)
    except ValueError:
        # Unbalanced quotes etc. — be conservative: if it smells like a push,
        # scan the current dir rather than waving it through.
        return ["."] if ("git" in command and "push" in command) else []

    targets: list[str] = []
    segment: list[str] = []
    for tok in (*tokens, "&&"):  # sentinel flushes the final segment
        if tok in ("&&", "||", "|", ";", "&", "\n"):
            target = _push_target_for_segment(segment)
            if target is not None:
                targets.append(target)
            segment = []
        else:
            segment.append(tok)
    return targets


def _push_target_for_segment(tokens: list[str]) -> str | None:
    i = 0
    # Skip leading env-var assignments (e.g. `GIT_DIR=… git push`).
    while i < len(tokens) and "=" in tokens[i] and not tokens[i].startswith("-"):
        i += 1
    if i >= len(tokens) or tokens[i] != "git":
        return None
    i += 1
    cwd = "."
    while i < len(tokens):
        tok = tokens[i]
        if tok == "-C" and i + 1 < len(tokens):
            cwd = tokens[i + 1]
            i += 2
            continue
        if tok in _OPTS_WITH_ARG and i + 1 < len(tokens):
            i += 2
            continue
        if tok.startswith("-"):
            i += 1
            continue
        return cwd if tok == "push" else None
    return None


def _scan(target: str) -> tuple[bool, str]:
    """Scan ``target`` with gitleaks. Returns (has_leaks, redacted_details)."""
    config_arg = ["--config", str(_CONFIG)] if _CONFIG.is_file() else []
    if not config_arg:
        print(
            f"gitleaks-prepush: WARNING — {_CONFIG} not found; scanning with gitleaks "
            "defaults, which miss the low-entropy Google key class.",
            file=sys.stderr,
        )
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        report = tf.name
    try:
        scan = subprocess.run(
            [
                "gitleaks",
                "git",
                target,
                *config_arg,
                "--no-banner",
                "--redact",
                "--report-format",
                "json",
                "--report-path",
                report,
            ],
            capture_output=True,
            text=True,
        )
        if scan.returncode == 0:
            return False, ""
        body = Path(report).read_text() if Path(report).exists() else ""
        if '"RuleID"' in body:
            return True, (scan.stderr or body).strip()[-2000:]
        # Non-zero without findings = gitleaks operational error (e.g. not a git
        # repo). Do not block the agent on a tool error; warn instead.
        print(
            f"gitleaks-prepush: gitleaks errored on '{target}' (exit "
            f"{scan.returncode}); not blocking. {scan.stderr.strip()[-300:]}",
            file=sys.stderr,
        )
        return False, ""
    finally:
        Path(report).unlink(missing_ok=True)


def main() -> int:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0  # not parseable -> do not interfere

    if event.get("tool_name") != "Bash":
        return 0
    command = (event.get("tool_input") or {}).get("command", "")
    targets = _push_targets(command)
    if not targets:
        return 0

    if shutil.which("gitleaks") is None:
        print(
            "gitleaks-prepush: gitleaks not installed; skipping pre-push secret scan "
            "(install: 'brew install gitleaks').",
            file=sys.stderr,
        )
        return 0

    for target in targets:
        has_leaks, details = _scan(target)
        if has_leaks:
            print(
                "BLOCKED by gitleaks-prepush: potential secrets detected in tracked "
                f"content of '{target}'; refusing `git push`. Remove/rotate the secret "
                "(and purge it from history) before pushing. Findings (redacted):\n" + details,
                file=sys.stderr,
            )
            return 2  # block the tool call

    return 0


if __name__ == "__main__":
    sys.exit(main())
