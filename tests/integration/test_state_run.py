"""Integration tests for the shared ``scripts/state-run.sh`` wrapper.

These shell out to real ``git`` against local ``file://`` remotes — no network,
no browser — to verify the pull -> run -> commit-only-on-change -> push cycle.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

WRAPPER = Path(__file__).resolve().parents[2] / "scripts" / "state-run.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("bash") is None,
    reason="git and bash are required for the state-run wrapper tests",
)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _make_remote(tmp_path: Path) -> Path:
    """Create a bare remote seeded with one commit on ``main`` and return its path."""
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-q", "--initial-branch=main")
    _git(seed, "config", "user.email", "seed@example.com")
    _git(seed, "config", "user.name", "seed")
    (seed / "news_items" / "discover").mkdir(parents=True)
    (seed / "news_items" / "discover" / ".keep").write_text("seed\n")
    _git(seed, "add", "-A")
    _git(seed, "commit", "-qm", "init")
    _git(seed, "remote", "add", "origin", bare.as_uri())
    _git(seed, "push", "-q", "-u", "origin", "main")
    # Point the bare repo's HEAD at main so plain clones check it out by default.
    subprocess.run(["git", "-C", str(bare), "symbolic-ref", "HEAD", "refs/heads/main"], check=True)
    return bare


def _run_wrapper(
    *,
    work: Path,
    remote: Path,
    command: Sequence[str],
    subtrees: Sequence[str] = (),
    message: str | None = None,
    offline: bool = False,
    no_fetch: bool = False,
) -> subprocess.CompletedProcess[str]:
    args: list[str] = ["bash", str(WRAPPER)]
    for subtree in subtrees:
        args += ["--subtree", subtree]
    if message is not None:
        args += ["--message", message]
    if offline:
        args.append("--offline")
    if no_fetch:
        args.append("--no-fetch")
    args += ["--", *command]
    env = {
        **os.environ,
        "STATE_REPO_URL": remote.as_uri(),
        "STATE_REPO_DIR": str(work),
        "STATE_REPO_BRANCH": "main",
        "GIT_AUTHOR_NAME": "tester",
        "GIT_AUTHOR_EMAIL": "tester@example.com",
    }
    return subprocess.run(args, env=env, capture_output=True, text=True)


def _remote_commit_count(remote: Path) -> int:
    return int(_git(remote, "rev-list", "--count", "main"))


def _remote_file(remote: Path, path: str) -> str:
    return _git(remote, "show", f"main:{path}")


def test_clones_runs_commits_and_pushes(tmp_path: Path) -> None:
    """A missing work dir is cloned; a state change is committed and pushed."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    result = _run_wrapper(
        work=work,
        remote=remote,
        subtrees=["news_items/discover"],
        message="test: add candidate",
        command=[
            "bash",
            "-c",
            'echo "{\\"id\\": 1}" > "$DENBUST_STATE_ROOT/news_items/discover/latest_candidates.jsonl"',
        ],
    )
    assert result.returncode == 0, result.stderr
    assert _remote_commit_count(remote) == 2
    assert _git(remote, "log", "-1", "--pretty=%s", "main") == "test: add candidate"
    assert _remote_file(remote, "news_items/discover/latest_candidates.jsonl") == '{"id": 1}'


def test_no_commit_when_state_unchanged(tmp_path: Path) -> None:
    """A run that produces no state change must not create a commit."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    result = _run_wrapper(
        work=work, remote=remote, subtrees=["news_items/discover"], command=["true"]
    )
    assert result.returncode == 0, result.stderr
    assert "no state changes to commit" in result.stdout
    assert _remote_commit_count(remote) == 1


def test_offline_commits_locally_without_pushing(tmp_path: Path) -> None:
    """--offline commits in the work tree but never touches the remote."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    # Seed the work dir by cloning once (offline mode does not clone).
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(work)], check=True)
    result = _run_wrapper(
        work=work,
        remote=remote,
        offline=True,
        subtrees=["news_items/discover"],
        command=[
            "bash",
            "-c",
            'echo local > "$DENBUST_STATE_ROOT/news_items/discover/x.jsonl"',
        ],
    )
    assert result.returncode == 0, result.stderr
    assert _remote_commit_count(remote) == 1  # remote untouched
    assert _git(work, "rev-list", "--count", "HEAD") == "2"  # local commit made


def test_only_named_subtree_is_committed(tmp_path: Path) -> None:
    """Files outside the named --subtree are not staged or pushed."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    result = _run_wrapper(
        work=work,
        remote=remote,
        subtrees=["news_items/discover"],
        command=[
            "bash",
            "-c",
            'mkdir -p "$DENBUST_STATE_ROOT/news_items/ingest"; '
            'echo keep > "$DENBUST_STATE_ROOT/news_items/discover/keep.jsonl"; '
            'echo stray > "$DENBUST_STATE_ROOT/news_items/ingest/stray.jsonl"',
        ],
    )
    assert result.returncode == 0, result.stderr
    tracked = _git(remote, "ls-tree", "-r", "--name-only", "main").splitlines()
    assert "news_items/discover/keep.jsonl" in tracked
    assert "news_items/ingest/stray.jsonl" not in tracked


def test_command_failure_still_persists_state_then_propagates_exit_code(
    tmp_path: Path,
) -> None:
    """A failing command still has its partial state committed; its code is returned."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    result = _run_wrapper(
        work=work,
        remote=remote,
        subtrees=["news_items/discover"],
        command=[
            "bash",
            "-c",
            'echo partial > "$DENBUST_STATE_ROOT/news_items/discover/partial.jsonl"; exit 7',
        ],
    )
    assert result.returncode == 7
    assert _remote_commit_count(remote) == 2  # partial progress persisted
    assert _remote_file(remote, "news_items/discover/partial.jsonl") == "partial"


def test_fetches_canonical_state_before_running(tmp_path: Path) -> None:
    """A stale work dir is reset to the remote tip first, so a concurrent push is
    integrated rather than clobbered, and the resulting push fast-forwards."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    # Clone the work dir at the current tip (commit 1).
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(work)], check=True)
    # A concurrent writer advances the remote (commit 2) while our work dir is stale.
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(other)], check=True)
    _git(other, "config", "user.email", "other@example.com")
    _git(other, "config", "user.name", "other")
    (other / "news_items" / "discover" / "other.jsonl").write_text("other\n")
    _git(other, "add", "-A")
    _git(other, "commit", "-qm", "concurrent")
    _git(other, "push", "-q", "origin", "main")
    # Our online run must fetch+reset to the remote tip before committing its own
    # change, so it neither loses the concurrent commit nor fails to push.
    result = _run_wrapper(
        work=work,
        remote=remote,
        subtrees=["news_items/discover"],
        command=[
            "bash",
            "-c",
            'echo mine > "$DENBUST_STATE_ROOT/news_items/discover/mine.jsonl"',
        ],
    )
    assert result.returncode == 0, result.stderr
    assert _remote_commit_count(remote) == 3
    assert _remote_file(remote, "news_items/discover/other.jsonl") == "other"  # not clobbered
    assert _remote_file(remote, "news_items/discover/mine.jsonl") == "mine"


def test_rejected_push_recovers_via_refetch_rebase(tmp_path: Path) -> None:
    """When a push is rejected (a writer raced in after our fetch), the wrapper
    refetches, rebases its commit onto the new tip, and retries — never clobbering."""
    remote = _make_remote(tmp_path)
    work = tmp_path / "work"
    # Clone the work dir at the current tip (commit 1).
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(work)], check=True)
    # A concurrent writer advances the remote (commit 2) AFTER we are positioned.
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", remote.as_uri(), str(other)], check=True)
    _git(other, "config", "user.email", "other@example.com")
    _git(other, "config", "user.name", "other")
    (other / "news_items" / "discover" / "other.jsonl").write_text("other\n")
    _git(other, "add", "-A")
    _git(other, "commit", "-qm", "concurrent")
    _git(other, "push", "-q", "origin", "main")
    # --no-fetch skips the pre-run realign, so our commit lands on the now-stale
    # base and the FIRST push is rejected — exercising the refetch+rebase retry.
    result = _run_wrapper(
        work=work,
        remote=remote,
        no_fetch=True,
        subtrees=["news_items/discover"],
        command=[
            "bash",
            "-c",
            'echo mine > "$DENBUST_STATE_ROOT/news_items/discover/mine.jsonl"',
        ],
    )
    assert result.returncode == 0, result.stderr
    assert "push rejected" in result.stderr  # the retry path actually fired
    assert _remote_commit_count(remote) == 3
    assert _remote_file(remote, "news_items/discover/other.jsonl") == "other"  # not clobbered
    assert _remote_file(remote, "news_items/discover/mine.jsonl") == "mine"
