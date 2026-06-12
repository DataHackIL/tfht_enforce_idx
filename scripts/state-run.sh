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
#   - Pushes with a plain (non-forced) push, retrying with refetch+rebase if a
#     concurrent writer advanced the branch — so a race recovers instead of
#     failing, and a concurrent commit is never clobbered.
#   - Serializes writers on one machine with a portable, self-recovering lock
#     (atomic mkdir; no flock dependency, so it works on macOS + Linux). The
#     lock is same-machine only; cross-machine safety (local vs CI) comes from
#     the fetch-before-run and the push retry, not the lock.
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
#   --no-fetch       Skip the pre-run fetch/reset (use the checkout as-is) but
#                    still push. Mainly a test seam for the push-retry path.
#   -h, --help       Show this help.
#
# Environment:
#   STATE_REPO_DIR     Working dir for the state repo (default: state_repo).
#   STATE_REPO_URL     Full clone URL. If unset, derived from STATE_REPO_SLUG
#                      (+ STATE_REPO_TOKEN for HTTPS auth). In CI the repo is
#                      already checked out by actions/checkout, so neither is
#                      needed — the wrapper reuses that configured remote.
#   STATE_REPO_SLUG    owner/repo (default: DataHackIL/tfht_enforce_idx_state).
#   STATE_REPO_TOKEN   Token for HTTPS clone/push auth (optional; passed via an
#                      in-memory http.extraheader, not baked into the repo URL).
#   STATE_REPO_BRANCH  Branch to track (default: main).
#   GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL
#                      Commit identity (default: the github-actions bot).
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Shared state-run wrapper: bring the git state repo to canonical HEAD, run a
state-producing command against it, commit only changed subtrees, and push.

Usage:
  scripts/state-run.sh [options] -- <command> [args...]

Options:
  --subtree PATH   Path under the state repo to stage/commit (repeatable).
                   If omitted, all changes are staged (git add -A).
  --message MSG    Commit message (default: "chore(state): update <subtrees>").
  --offline        Skip clone/fetch and push; run + commit locally only.
  --no-fetch       Skip the pre-run fetch/reset but still push (test seam).
  -h, --help       Show this help.

Environment:
  STATE_REPO_DIR     Working dir for the state repo (default: state_repo).
  STATE_REPO_URL     Full clone URL (overrides STATE_REPO_SLUG/_TOKEN).
  STATE_REPO_SLUG    owner/repo (default: DataHackIL/tfht_enforce_idx_state).
  STATE_REPO_TOKEN   HTTPS auth token (passed via http.extraheader, not the URL).
  STATE_REPO_BRANCH  Branch to track (default: main).
  GIT_AUTHOR_NAME / GIT_AUTHOR_EMAIL   Commit identity (default: github-actions bot).
EOF
}

subtrees=()
message=""
offline=0
no_fetch=0
cmd=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --subtree) subtrees+=("$2"); shift 2 ;;
    --message) message="$2"; shift 2 ;;
    --offline) offline=1; shift ;;
    --no-fetch) no_fetch=1; shift ;;
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

# Auth: prefer an in-memory extraheader so a token is never persisted into the
# clone's .git/config URL (where it would leak in error output and on disk).
git_auth=()
if [[ -z "${STATE_REPO_URL:-}" && -n "${STATE_REPO_TOKEN:-}" ]]; then
  _basic="$(printf 'x-access-token:%s' "$STATE_REPO_TOKEN" | base64 | tr -d '\n')"
  git_auth=(-c "http.extraheader=Authorization: Basic ${_basic}")
fi

git_state() { git "${git_auth[@]}" -C "$STATE_REPO_DIR" "$@"; }

resolve_url() {
  if [[ -n "${STATE_REPO_URL:-}" ]]; then
    printf '%s' "$STATE_REPO_URL"
  else
    printf 'https://github.com/%s.git' "$STATE_REPO_SLUG"
  fi
}

# Portable single-writer lock via atomic mkdir (flock is absent on macOS), with
# stale-lock recovery: the holder records its PID, and a lock left by a dead
# process (kill -9, reboot) is broken instead of wedging every future run.
lock_dir="${STATE_REPO_DIR%/}.lock"
release_lock() { rm -rf "$lock_dir" 2>/dev/null || true; }
acquire_lock() {
  local waited=0
  while ! mkdir "$lock_dir" 2>/dev/null; do
    local holder
    holder="$(cat "$lock_dir/pid" 2>/dev/null || true)"
    if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then
      echo "state-run: breaking stale lock $lock_dir (dead PID $holder)" >&2
      rm -rf "$lock_dir"
      continue
    fi
    if [[ $waited -ge 300 ]]; then
      echo "state-run: timed out acquiring lock $lock_dir after ${waited}s (held by PID ${holder:-unknown})" >&2
      exit 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  # Clean the lock on exit. Trapping INT/TERM to `exit` (rather than to the
  # cleanup directly) makes Ctrl-C / SIGTERM actually terminate the script and
  # fire the EXIT trap — a bare `trap ... INT` would run cleanup and then resume.
  trap 'release_lock' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  printf '%s' "$$" >"$lock_dir/pid"
}

# Plain push, with refetch+rebase retry on rejection. A plain push already
# refuses to clobber a concurrent commit (non-fast-forward is rejected); the
# retry rebases our single commit onto the new tip so a race recovers rather
# than failing the run. --force-with-lease is intentionally NOT used: we never
# want to overwrite the remote, and a stale lease over a shallow fetch could.
push_state() {
  local tries=0
  until git_state push origin "HEAD:refs/heads/$STATE_REPO_BRANCH"; do
    tries=$((tries + 1))
    if [[ $tries -ge 3 ]]; then
      echo "state-run: push still rejected after ${tries} attempts" >&2
      return 1
    fi
    echo "state-run: push rejected (attempt ${tries}); refetching and rebasing." >&2
    git_state fetch --depth=1 origin "$STATE_REPO_BRANCH"
    if ! git_state rebase FETCH_HEAD; then
      git_state rebase --abort 2>/dev/null || true
      return 1
    fi
  done
}

acquire_lock

# 1. Bring the state repo to canonical HEAD.
if [[ $offline -eq 0 && $no_fetch -eq 0 ]]; then
  if [[ -d "$STATE_REPO_DIR/.git" ]]; then
    git_state fetch --depth=1 origin "$STATE_REPO_BRANCH"
    git_state reset --hard FETCH_HEAD
  else
    git "${git_auth[@]}" clone --depth=1 --branch "$STATE_REPO_BRANCH" \
      "$(resolve_url)" "$STATE_REPO_DIR"
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
    push_state
  fi
fi

exit "$rc"
