#!/usr/bin/env bash
#
# Orphan-squash maintenance for the denbust git state repo.
#
# The state repo accumulates one commit per run. The state files are plain
# JSONL (git already delta+zlib-compresses them; see docs/operational_reference.md),
# so per-run blobs stay small, but the *number* of commits grows without bound.
# This job periodically flattens history: it replaces the branch with a single
# fresh root commit containing the current tree, so a clone never pays for the
# full run-by-run history. Run it on a slow cadence (e.g. weekly).
#
# It is force-push by nature (a history rewrite), but it is safe against
# concurrent state-run writers: it holds the same same-machine lock, and a
# state-run that races simply has its push rejected and recovers by rebasing its
# one commit onto the new squashed root (see scripts/state-run.sh).
#
# Usage:
#   scripts/state-squash.sh [--message MSG] [--dry-run]
#
# Options:
#   --message MSG   Commit message for the squashed snapshot.
#   --dry-run       Build the squashed commit locally but do not force-push.
#   -h, --help      Show this help.
#
# Environment: same as scripts/state-run.sh (STATE_REPO_DIR / _URL / _SLUG /
#   _TOKEN / _BRANCH, GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL).
set -euo pipefail

# shellcheck source=scripts/state-repo-common.sh
source "$(dirname "$0")/state-repo-common.sh"

usage() {
  cat >&2 <<'EOF'
Orphan-squash the denbust git state repo: replace branch history with a single
fresh root commit of the current tree, to bound clone/history size.

Usage:
  scripts/state-squash.sh [--message MSG] [--dry-run]

Options:
  --message MSG   Commit message for the squashed snapshot.
  --dry-run       Build the squashed commit locally but do not force-push.
  -h, --help      Show this help.
EOF
}

message=""
dry_run=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --message) message="$2"; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) echo "state-squash: unknown option: $1" >&2; exit 2 ;;
  esac
done

acquire_state_repo_lock
ensure_canonical_state_checkout

git_state config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
git_state config user.email "${GIT_AUTHOR_EMAIL:-41898282+github-actions[bot]@users.noreply.github.com}"

# ensure_canonical_state_checkout fetches shallow, so the local history is
# truncated and `rev-list --count` would always read 1. Deepen to count the real
# history before deciding whether a squash is worthwhile (cheap once flat; the
# job is infrequent, so paying for full history on a bloated repo is fine).
if [[ "$(git_state rev-parse --is-shallow-repository 2>/dev/null || echo false)" == "true" ]]; then
  if ! git_state fetch --unshallow origin "$STATE_REPO_BRANCH" 2>/dev/null; then
    echo "state-squash: warning: could not deepen history; skipping (cannot tell if flat)." >&2
    exit 0
  fi
fi

# Already a single commit? Then history is as flat as it gets; nothing to do.
commits="$(git_state rev-list --count HEAD)"
if [[ "$commits" -le 1 ]]; then
  echo "state-squash: history already flat (${commits} commit); nothing to do."
  exit 0
fi

# Build a fresh root commit holding EXACTLY the tracked tree, via plumbing. Using
# `commit-tree HEAD^{tree}` (rather than `checkout --orphan` + `git add -A`) means
# the snapshot never picks up untracked working-tree cruft and creates no temp
# branch (so repeated runs in a persistent checkout don't collide).
tree="$(git_state rev-parse 'HEAD^{tree}')"
squashed="$(git_state commit-tree "$tree" -m "${message:-chore(state): squash history}")"

if [[ $dry_run -eq 1 ]]; then
  echo "state-squash: dry run — built squashed root ${squashed} from ${commits} commits (not pushed)."
  exit 0
fi

# Replace the branch with the squashed root. --force is intentional (history
# rewrite); a racing state-run recovers via its push retry.
git_state push --force origin "${squashed}:refs/heads/$STATE_REPO_BRANCH"
echo "state-squash: squashed ${commits} commits into 1 and force-pushed to ${STATE_REPO_BRANCH}."
