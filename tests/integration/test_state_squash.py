"""Integration tests for scripts/state-squash.sh (orphan-squash maintenance).

Shell out to real ``git`` against local ``file://`` remotes — no network — to
verify history flattening, idempotence, dry-run, and that a normal state-run
still pushes cleanly after a squash rewrites history.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
SQUASH = SCRIPTS / "state-squash.sh"
RUN = SCRIPTS / "state-run.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("bash") is None,
    reason="git and bash are required for the state-repo wrapper tests",
)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _make_remote(tmp_path: Path, *, commits: int) -> Path:
    """Create a bare remote on ``main`` seeded with ``commits`` commits."""
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-q", "--initial-branch=main")
    _git(seed, "config", "user.email", "seed@example.com")
    _git(seed, "config", "user.name", "seed")
    (seed / "news_items" / "discover").mkdir(parents=True)
    for i in range(1, commits + 1):
        (seed / "news_items" / "discover" / "latest.jsonl").write_text(f"v{i}\n")
        _git(seed, "add", "-A")
        _git(seed, "commit", "-qm", f"run {i}")
    _git(seed, "remote", "add", "origin", bare.as_uri())
    _git(seed, "push", "-q", "-u", "origin", "main")
    subprocess.run(["git", "-C", str(bare), "symbolic-ref", "HEAD", "refs/heads/main"], check=True)
    return bare


def _env(work: Path, remote: Path) -> dict[str, str]:
    return {
        **os.environ,
        "STATE_REPO_URL": remote.as_uri(),
        "STATE_REPO_DIR": str(work),
        "STATE_REPO_BRANCH": "main",
        "GIT_AUTHOR_NAME": "tester",
        "GIT_AUTHOR_EMAIL": "tester@example.com",
        # These tests exercise squash/coexistence mechanics, not the secret-scan
        # guard (which fails closed without gitleaks); opt out so they run anywhere.
        "STATE_RUN_SKIP_SECRET_SCAN": "1",
    }


def _run(script: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True)


def _remote_commits(remote: Path) -> int:
    return int(_git(remote, "rev-list", "--count", "main"))


def _remote_file(remote: Path, path: str) -> str:
    return _git(remote, "show", f"main:{path}")


def test_squash_flattens_history_preserving_current_tree(tmp_path: Path) -> None:
    """A multi-commit branch collapses to one commit with the latest content."""
    remote = _make_remote(tmp_path, commits=5)
    assert _remote_commits(remote) == 5
    result = _run(SQUASH, _env(tmp_path / "work", remote), "--message", "squash")
    assert result.returncode == 0, result.stderr
    assert _remote_commits(remote) == 1
    assert _remote_file(remote, "news_items/discover/latest.jsonl") == "v5"


def test_squash_captures_only_tracked_tree_not_untracked_cruft(tmp_path: Path) -> None:
    """The squash snapshots the tracked tree, never untracked working-dir files."""
    remote = _make_remote(tmp_path, commits=3)
    work = tmp_path / "work"
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(work)], check=True)
    # A stray untracked file that must NOT be baked into the canonical state.
    (work / "news_items" / "discover" / "stray.tmp").write_text("junk\n")
    result = _run(SQUASH, _env(work, remote))
    assert result.returncode == 0, result.stderr
    assert _remote_commits(remote) == 1
    tracked = _git(remote, "ls-tree", "-r", "--name-only", "main").splitlines()
    assert "news_items/discover/stray.tmp" not in tracked
    assert "news_items/discover/latest.jsonl" in tracked


def test_squash_is_noop_when_already_flat(tmp_path: Path) -> None:
    """Squashing a single-commit branch does nothing (no new root commit)."""
    remote = _make_remote(tmp_path, commits=1)
    before = _git(remote, "rev-parse", "main")
    result = _run(SQUASH, _env(tmp_path / "work", remote))
    assert result.returncode == 0, result.stderr
    assert "already flat" in result.stdout
    assert _git(remote, "rev-parse", "main") == before  # untouched


def test_dry_run_does_not_push(tmp_path: Path) -> None:
    """--dry-run builds the squashed commit locally but leaves the remote alone."""
    remote = _make_remote(tmp_path, commits=3)
    before = _git(remote, "rev-parse", "main")
    result = _run(SQUASH, _env(tmp_path / "work", remote), "--dry-run")
    assert result.returncode == 0, result.stderr
    assert "dry run" in result.stdout
    assert _remote_commits(remote) == 3
    assert _git(remote, "rev-parse", "main") == before


def test_state_run_pushes_cleanly_after_a_squash(tmp_path: Path) -> None:
    """After a squash rewrites history, a normal state-run resets to the squashed
    root and pushes a single clean commit on top — squash and runs coexist."""
    remote = _make_remote(tmp_path, commits=4)
    _run(SQUASH, _env(tmp_path / "work_sq", remote), "--message", "squash")
    assert _remote_commits(remote) == 1

    # A fresh state-run against the squashed remote.
    run_env = _env(tmp_path / "work_run", remote)
    result = subprocess.run(
        [
            "bash",
            str(RUN),
            "--subtree",
            "news_items/discover",
            "--message",
            "run after squash",
            "--",
            "bash",
            "-c",
            'echo next > "$DENBUST_STATE_ROOT/news_items/discover/next.jsonl"',
        ],
        env=run_env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert _remote_commits(remote) == 2  # squashed root + the new run
    assert _remote_file(remote, "news_items/discover/latest.jsonl") == "v4"  # preserved
    assert _remote_file(remote, "news_items/discover/next.jsonl") == "next"
