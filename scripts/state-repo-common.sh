# shellcheck shell=bash
#
# Shared helpers for the denbust git state repo, sourced by scripts/state-run.sh
# and scripts/state-squash.sh so the auth/lock/checkout logic lives in one place.
#
# Provides:
#   - config resolution (STATE_REPO_DIR / _SLUG / _BRANCH / _URL / _TOKEN);
#   - git_state(): run git against the state repo with auth applied via an
#     in-memory http.extraheader, so a token is never persisted into .git/config;
#   - acquire_state_repo_lock() / release_state_repo_lock(): a portable
#     (mkdir-based, no flock) same-machine single-writer lock with stale-PID
#     recovery and signal-safe cleanup;
#   - ensure_canonical_state_checkout(): bring the repo to origin/<branch>.
#
# Callers must `set -euo pipefail` before sourcing.

STATE_REPO_DIR="${STATE_REPO_DIR:-state_repo}"
STATE_REPO_SLUG="${STATE_REPO_SLUG:-DataHackIL/tfht_enforce_idx_state}"
STATE_REPO_BRANCH="${STATE_REPO_BRANCH:-main}"

# Auth: an in-memory extraheader keeps a token out of the persisted clone URL
# (where it would leak in error output and on disk).
state_repo_git_auth=()
if [[ -z "${STATE_REPO_URL:-}" && -n "${STATE_REPO_TOKEN:-}" ]]; then
  _state_repo_basic="$(printf 'x-access-token:%s' "$STATE_REPO_TOKEN" | base64 | tr -d '\n')"
  state_repo_git_auth=(-c "http.extraheader=Authorization: Basic ${_state_repo_basic}")
fi

git_state() { git "${state_repo_git_auth[@]}" -C "$STATE_REPO_DIR" "$@"; }

state_repo_url() {
  if [[ -n "${STATE_REPO_URL:-}" ]]; then
    printf '%s' "$STATE_REPO_URL"
  else
    printf 'https://github.com/%s.git' "$STATE_REPO_SLUG"
  fi
}

# Portable single-writer lock via atomic mkdir (flock is absent on macOS), with
# stale-lock recovery: the holder records its PID, and a lock left by a dead
# process (kill -9, reboot) is broken instead of wedging every future run. The
# lock is same-machine only; cross-machine safety comes from fetch-before-write
# plus the callers' push handling, not the lock.
state_repo_lock_dir="${STATE_REPO_DIR%/}.lock"
release_state_repo_lock() { rm -rf "$state_repo_lock_dir" 2>/dev/null || true; }
acquire_state_repo_lock() {
  local waited=0 holder
  while ! mkdir "$state_repo_lock_dir" 2>/dev/null; do
    holder="$(cat "$state_repo_lock_dir/pid" 2>/dev/null || true)"
    if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then
      echo "state-repo: breaking stale lock $state_repo_lock_dir (dead PID $holder)" >&2
      rm -rf "$state_repo_lock_dir"
      continue
    fi
    if [[ $waited -ge 300 ]]; then
      echo "state-repo: timed out acquiring lock $state_repo_lock_dir after ${waited}s (held by PID ${holder:-unknown})" >&2
      exit 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  # Clean the lock on exit. Trapping INT/TERM to `exit` (rather than to the
  # cleanup directly) makes Ctrl-C / SIGTERM actually terminate the script and
  # fire the EXIT trap — a bare `trap ... INT` would run cleanup and then resume.
  trap 'release_state_repo_lock' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  printf '%s' "$$" >"$state_repo_lock_dir/pid"
}

# Bring the state repo to canonical HEAD: clone if missing, else shallow fetch
# + reset to origin/<branch>, so an operation always starts from the single
# source of truth.
ensure_canonical_state_checkout() {
  if [[ -d "$STATE_REPO_DIR/.git" ]]; then
    git_state fetch --depth=1 origin "$STATE_REPO_BRANCH"
    git_state reset --hard FETCH_HEAD
  else
    git "${state_repo_git_auth[@]}" clone --depth=1 --branch "$STATE_REPO_BRANCH" \
      "$(state_repo_url)" "$STATE_REPO_DIR"
  fi
  mkdir -p "$STATE_REPO_DIR"
}
