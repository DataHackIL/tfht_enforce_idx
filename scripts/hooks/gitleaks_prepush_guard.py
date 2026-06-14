#!/usr/bin/env python3
"""Claude Code PreToolUse hook: secret-scan before a `git push`.

Reads the PreToolUse event JSON on stdin. If the Bash command is a ``git push``,
it runs gitleaks over the repo's tracked content (using the repo-root
``.gitleaks.toml``) and **blocks** the push (exit code 2) when secrets are found,
printing the redacted findings so Claude can fix them instead of publishing.

This is the "Claude ability" pre-push guard, complementing the git pre-push hook
(pre-commit) and the `scripts/state-run.sh` push guard. gitleaks is run in *git*
mode, so untracked working-tree files (e.g. local `data/`) are not scanned.

Exit codes (Claude Code hook protocol): 0 = allow, 2 = block.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys

_GIT_PUSH = re.compile(r"\bgit\b(?:\s+-C\s+\S+)?(?:\s+-c\s+\S+)*\s+push\b")


def main() -> int:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0  # not parseable -> do not interfere

    if event.get("tool_name") != "Bash":
        return 0
    command = (event.get("tool_input") or {}).get("command", "")
    if not _GIT_PUSH.search(command):
        return 0

    if shutil.which("gitleaks") is None:
        print(
            "gitleaks-prepush: gitleaks not installed; skipping pre-push secret scan "
            "(install: 'brew install gitleaks').",
            file=sys.stderr,
        )
        return 0

    scan = subprocess.run(
        ["gitleaks", "git", ".", "--no-banner", "--redact"],
        capture_output=True,
        text=True,
    )
    if scan.returncode != 0:
        details = (scan.stdout + scan.stderr).strip()[-2000:]
        print(
            "BLOCKED by gitleaks-prepush: potential secrets detected in tracked content; "
            "refusing `git push`. Remove/rotate the secret (and purge it from history) "
            "before pushing. Findings (redacted):\n" + details,
            file=sys.stderr,
        )
        return 2  # block the tool call

    return 0


if __name__ == "__main__":
    sys.exit(main())
