#!/usr/bin/env python3
"""Sync triage decisions to Supabase (lightweight PATCH per changed candidate).

Reads triage_decisions.jsonl, applies latest decision per candidate, and
PATCHes only the two changed fields (candidate_status, retry_priority) for
each decided candidate.

Usage:
    python triage_app/sync.py [--dry-run]

Env vars required (same as .env.local):
    DENBUST_SUPABASE_URL
    DENBUST_SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.models.common import DatasetName, JobName

REPO_ROOT = Path(__file__).resolve().parent.parent
_STATE = resolve_discovery_state_paths(
    state_root=REPO_ROOT / "data",
    dataset_name=DatasetName.NEWS_ITEMS,
    job_name=JobName.DISCOVER,
)
CANDIDATES_FILE = _STATE.latest_candidates_path
DECISIONS_FILE = _STATE.candidates_dir / "triage_decisions.jsonl"

TABLE = "persistent_candidates"


def load_decisions() -> dict[str, dict[str, str]]:
    """Return latest decision per candidate_id."""
    if not DECISIONS_FILE.exists():
        print("No triage_decisions.jsonl found — nothing to sync.")
        sys.exit(0)
    latest: dict[str, dict[str, str]] = {}
    with DECISIONS_FILE.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                d = json.loads(line)
                latest[d["candidate_id"]] = d
    return latest


def load_candidate_index() -> dict[str, dict[str, object]]:
    index: dict[str, dict[str, object]] = {}
    if not CANDIDATES_FILE.exists():
        return index
    with CANDIDATES_FILE.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                c = json.loads(line)
                index[c["candidate_id"]] = c
    return index


_ACTION_PATCHES: dict[str, dict[str, object]] = {
    "exclude": {"candidate_status": "suppressed"},
    "prioritize": {"candidate_status": "new", "retry_priority": 100},
}


def decision_to_patch(action: str, original_priority: int) -> dict[str, object]:
    if action in _ACTION_PATCHES:
        return dict(_ACTION_PATCHES[action])
    if action == "reset":
        return {"candidate_status": "new", "retry_priority": original_priority}
    raise ValueError(f"Unknown action: {action}")


def patch_candidate(
    client: httpx.Client,
    supabase_url: str,
    service_key: str,
    candidate_id: str,
    patch: dict[str, object],
    dry_run: bool,
) -> bool:
    if dry_run:
        print(f"  [dry-run] PATCH {candidate_id}: {patch}")
        return True
    url = f"{supabase_url}/rest/v1/{TABLE}?candidate_id=eq.{candidate_id}"
    resp = client.patch(
        url,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=patch,
    )
    if resp.status_code not in (200, 204):
        print(
            f"  ERROR {candidate_id}: HTTP {resp.status_code} — {resp.text[:200]}",
            file=sys.stderr,
        )
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync triage decisions to Supabase")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    args = parser.parse_args()

    supabase_url = os.environ.get("DENBUST_SUPABASE_URL", "").rstrip("/")
    service_key = os.environ.get("DENBUST_SUPABASE_SERVICE_ROLE_KEY", "")
    if not args.dry_run and (not supabase_url or not service_key):
        print(
            "ERROR: DENBUST_SUPABASE_URL and DENBUST_SUPABASE_SERVICE_ROLE_KEY must be set.",
            file=sys.stderr,
        )
        sys.exit(1)

    decisions = load_decisions()
    candidates = load_candidate_index()

    if not decisions:
        print("No decisions found — nothing to sync.")
        return

    # Collapse to final state per candidate
    final: list[tuple[str, dict[str, object]]] = []
    for cid, d in decisions.items():
        raw_priority = (candidates.get(cid) or {}).get("retry_priority", 0)
        orig_priority = int(raw_priority) if isinstance(raw_priority, (int, float, str)) else 0
        patch = decision_to_patch(d["action"], orig_priority)
        final.append((cid, patch))

    print(
        f"Syncing {len(final)} candidate decisions to Supabase "
        f"({'dry-run' if args.dry_run else 'live'}) …"
    )

    ok = 0
    err = 0
    with httpx.Client(timeout=30) as client:
        for cid, patch in final:
            success = patch_candidate(client, supabase_url, service_key, cid, patch, args.dry_run)
            if success:
                ok += 1
                if not args.dry_run:
                    print(f"  ✓ {cid[:8]}… → {patch}")
            else:
                err += 1

    print(f"\nDone: {ok} synced, {err} errors.")
    if err:
        sys.exit(1)


if __name__ == "__main__":
    main()
