#!/usr/bin/env bash
#
# Shared state-run wrapper for the denbust git state repo.
#
# One place — used by both local runs and GitHub Actions — for the
# pull -> run -> commit-only-on-change -> push cycle around a state-producing
# command, so that logic is not duplicated across every workflow.
#
#   - Brings the state repo to canonical HEAD (clone if missing, else fetch +
#     reset to origin/<branch>) so a run always starts from the single source
#     of truth.
#   - Runs the given command with DENBUST_STATE_ROOT pointed at the state repo.
#   - Commits only the requested subtrees, and only when they actually changed.
#   - Pushes with --force-with-lease (fails safely if another writer pushed),
#     under a portable single-writer lock (no flock dependency; macOS + Linux).
#
# The state files are plain JSONL on purpose; do NOT pre-compress them (git
# already delta+zlib-compresses blobs, and gzip would bloat history and break
# the unchanged-run check below). See docs/operational_reference.md.
#
# Usage:
#   scripts/state-run.sh [options] -- <command> [args...]
#
# Options:
#   --subtree PATH   Path under the state repo to stage/commit (repeatable).
#                    If omitted, all changes are staged (git add -A).
#   --message MSG    Commit message (default: "chore(state): update <subtrees>").
#   --offline        Skip clone/fetch and push; run + commit locally only.
#   -h, --help       Show this help.
#
# Environment:
#   STATE_REPO_DIR     Working dir for the state repo (default: state_repo).
#   STATE_REPO_URL     Full clone URL. If unset, derived from STATE_REPO_SLUG
#                      (+ STATE_REPO_TOKEN for HTTPS auth). In CI the repo is
#                      already checked out by actions/checkout, so neither is
#                      needed — the wrapper reuses that configured remote.
#   STATE_REPO_SLUG    owner/repo (default: DataHackIL/tfht_enforce_idx_state).
#   STATE_REPO_TOKEN   Token for HTTPS clone/push auth (optional).
#   STATE_REPO_BRANCH  Branch to track (default: main).
#   GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL
#                      Commit identity (default: the github-actions bot).
set -euo pipefail

usage() { sed -n '2,46p' "$0" | sed 's/^# \{0,1\}//'; }

subtrees=()
message=""
offline=0
cmd=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subtree) subtrees+=("$2"); shift 2 ;;
    --message) message="$2"; shift 2 ;;
    --offline) offline=1; shift ;;
    -h | --help) usage; exit 0 ;;
    --) shift; cmd=("$@"); break ;;
    *) echo "state-run: unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ ${#cmd[@]} -eq 0 ]]; then
  echo "state-run: missing command (expected '-- <command> ...')" >&2
  exit 2
fi

STATE_REPO_DIR="${STATE_REPO_DIR:-state_repo}"
STATE_REPO_SLUG="${STATE_REPO_SLUG:-DataHackIL/tfht_enforce_idx_state}"
STATE_REPO_BRANCH="${STATE_REPO_BRANCH:-main}"

git_state() { git -C "$STATE_REPO_DIR" "$@"; }

resolve_url() {
  if [[ -n "${STATE_REPO_URL:-}" ]]; then
    printf '%s' "$STATE_REPO_URL"
  elif [[ -n "${STATE_REPO_TOKEN:-}" ]]; then
    printf 'https://x-access-token:%s@github.com/%s.git' "$STATE_REPO_TOKEN" "$STATE_REPO_SLUG"
  else
    printf 'https://github.com/%s.git' "$STATE_REPO_SLUG"
  fi
}

# Portable single-writer lock via atomic mkdir (flock is absent on macOS).
lock_dir="${STATE_REPO_DIR%/}.lock"
acquire_lock() {
  local waited=0
  until mkdir "$lock_dir" 2>/dev/null; do
    if [[ $waited -ge 300 ]]; then
      echo "state-run: timed out acquiring lock $lock_dir after ${waited}s" >&2
      exit 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT
}

acquire_lock

# 1. Bring the state repo to canonical HEAD.
if [[ $offline -eq 0 ]]; then
  if [[ -d "$STATE_REPO_DIR/.git" ]]; then
    git_state fetch --depth=1 origin "$STATE_REPO_BRANCH"
    git_state reset --hard FETCH_HEAD
  else
    git clone --depth=1 --branch "$STATE_REPO_BRANCH" "$(resolve_url)" "$STATE_REPO_DIR"
  fi
fi
mkdir -p "$STATE_REPO_DIR"

# 2. Run the state-producing command against the state repo.
export DENBUST_STATE_ROOT="$STATE_REPO_DIR"
rc=0
"${cmd[@]}" || rc=$?

# 3. Commit only when the requested state actually changed.
if [[ ${#subtrees[@]} -gt 0 ]]; then
  for subtree in "${subtrees[@]}"; do
    [[ -e "$STATE_REPO_DIR/$subtree" ]] && git_state add -- "$subtree"
  done
else
  git_state add -A
fi

if [[ -z "$(git_state diff --cached --name-only)" ]]; then
  echo "state-run: no state changes to commit."
else
  git_state config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
  git_state config user.email "${GIT_AUTHOR_EMAIL:-41898282+github-actions[bot]@users.noreply.github.com}"
  git_state commit -m "${message:-chore(state): update ${subtrees[*]:-state}}"
  if [[ $offline -eq 0 ]]; then
    git_state push --force-with-lease origin "HEAD:refs/heads/$STATE_REPO_BRANCH"
  fi
fi

exit "$rc"
