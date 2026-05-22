#!/usr/bin/env python3
"""Local pre-scrape triage server.

Serves candidates from the local state JSONL for fast batch exclude/prioritize
decisions before the scrape phase.  No Supabase reads during a review session —
use sync.py when you're done to push decisions upstream.

Usage:
    python triage_app/serve.py [--port 7070]
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

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
PUBLIC_DIR = Path(__file__).resolve().parent / "public"
DEFAULT_PORT = 7070

_candidates: dict[str, dict[str, Any]] = {}
_batch_ids: list[str] = []
_origin: str = f"http://localhost:{DEFAULT_PORT}"
# Stage B p_negative scores keyed by candidate_id (lower = more likely positive)
_stage_b_scores: dict[str, float] = {}


def _load_candidates() -> None:
    if not CANDIDATES_FILE.exists():
        print(f"ERROR: candidates file not found: {CANDIDATES_FILE}", file=sys.stderr)
        sys.exit(1)
    with CANDIDATES_FILE.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            c = json.loads(line)
            _candidates[c["candidate_id"]] = c

    seen: set[str] = set()
    for c in _candidates.values():
        bid = c.get("backfill_batch_id")
        if bid and bid not in seen:
            seen.add(bid)
            _batch_ids.append(bid)


def _load_stage_b_scores() -> None:
    """Score every candidate with Stage B NaiveBayes and cache p_negative.

    Lower score = more likely to be a relevant (positive) candidate.
    Falls back silently if the model artifacts or config are missing.
    """
    _cfg_candidates = [
        REPO_ROOT / "agents/news/local_search_brave_exa.yaml",
        REPO_ROOT / "agents/news/local.yaml",
        REPO_ROOT / "agents/news/local_search.yaml",
    ]
    cfg_path = next((p for p in _cfg_candidates if p.exists()), None)
    if cfg_path is None:
        print("  Stage B ranking skipped: no YAML config found under agents/news/")
        return

    try:
        from denbust.config import load_config as _load_config
        from denbust.prefilter.adapters import _etld1_from_host as _etld1
        from denbust.prefilter.stage_b import StageBScorer as _StageBScorer
        from denbust.prefilter.state_paths import resolve_prefilter_state_paths as _resolve

        loaded = _load_config(cfg_path)
        pp = _resolve(state_root=loaded.store.state_root, dataset_name=loaded.dataset_name)
        model_dir = pp.models_dir / "stage_b"
        if not model_dir.exists():
            print(
                "  Stage B ranking skipped: no trained model (run: denbust prefilter retrain --stage b)"
            )
            return

        scorer = _StageBScorer(models_dir=pp.models_dir, threshold=0.9962)

        class _View:
            __slots__ = ("_c",)

            def __init__(self, c: dict[str, Any]) -> None:
                self._c = c

            @property
            def candidate_id(self) -> str:
                return self._c["candidate_id"]

            @property
            def domain(self) -> str | None:
                return _etld1(self._c.get("domain") or "")

            @property
            def title(self) -> str | None:
                t = self._c.get("titles") or []
                return t[0] if t else None

            @property
            def snippet(self) -> str | None:
                s = self._c.get("snippets") or []
                return s[0] if s else None

            @property
            def url(self) -> str | None:
                return str(self._c.get("canonical_url") or self._c.get("current_url") or "")

        for cid, c in _candidates.items():
            score = scorer.evaluate(_View(c), "thin")
            _stage_b_scores[cid] = score.p_negative if score is not None else 1.0

        print(f"  Stage B scored {len(_stage_b_scores)} candidates")
    except Exception as exc:  # noqa: BLE001
        print(f"  Stage B ranking failed (non-fatal): {exc}", file=sys.stderr)


def _load_decisions() -> None:
    if not DECISIONS_FILE.exists():
        return
    with DECISIONS_FILE.open(encoding="utf-8") as fh:
        latest: dict[str, dict[str, Any]] = {}
        for line in fh:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            latest[d["candidate_id"]] = d
    for d in latest.values():
        cid = d["candidate_id"]
        if cid in _candidates:
            c = _candidates[cid]
            # Snapshot original priority before any decision so reset is idempotent
            if "_orig_priority" not in c:
                c["_orig_priority"] = c.get("retry_priority", 0)
            _apply(c, d["action"])


def _apply(candidate: dict[str, Any], action: str) -> None:
    if action == "exclude":
        candidate["candidate_status"] = "suppressed"
        candidate["_triage"] = "excluded"
    elif action == "prioritize":
        candidate["retry_priority"] = 100
        candidate["_triage"] = "prioritized"
    elif action == "reset":
        candidate["candidate_status"] = "new"
        candidate["retry_priority"] = candidate.get("_orig_priority", 0)
        candidate.pop("_triage", None)


def _record_decision(candidate_id: str, action: str) -> bool:
    if candidate_id not in _candidates:
        return False
    # Preserve original priority before first triage so reset can restore it
    c = _candidates[candidate_id]
    if "_orig_priority" not in c:
        c["_orig_priority"] = c.get("retry_priority", 0)
    _apply(c, action)
    d = {
        "candidate_id": candidate_id,
        "action": action,
        "decided_at": datetime.now(UTC).isoformat(),
        "batch_id": c.get("backfill_batch_id"),
    }
    with DECISIONS_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(d, ensure_ascii=False) + "\n")
    return True


def _stats() -> dict[str, Any]:
    vals = list(_candidates.values())
    return {
        "total": len(vals),
        "unreviewed": sum(1 for c in vals if "_triage" not in c),
        "excluded": sum(1 for c in vals if c.get("_triage") == "excluded"),
        "prioritized": sum(1 for c in vals if c.get("_triage") == "prioritized"),
        "batch_ids": _batch_ids,
    }


def _serialize(c: dict[str, Any]) -> dict[str, Any]:
    titles = c.get("titles") or []
    snippets = c.get("snippets") or []
    return {
        "candidate_id": c["candidate_id"],
        "url": str(c.get("current_url") or c.get("canonical_url") or ""),
        "domain": c.get("domain", ""),
        "title": titles[0] if titles else "",
        "snippet": snippets[0] if snippets else "",
        "first_seen_at": str(c.get("first_seen_at", "")),
        "backfill_batch_id": c.get("backfill_batch_id"),
        "candidate_status": c.get("candidate_status", "new"),
        "retry_priority": c.get("retry_priority", 0),
        "triage": c.get("_triage", ""),
        "stage_b_score": _stage_b_scores.get(c["candidate_id"]),
    }


def _query_candidates(
    batch_id: str | None,
    status: str,
    q: str,
    page: int,
    limit: int,
    sort: str = "default",
) -> dict[str, Any]:
    items = list(_candidates.values())

    if batch_id:
        items = [c for c in items if c.get("backfill_batch_id") == batch_id]

    if status == "unreviewed":
        items = [c for c in items if "_triage" not in c]
    elif status == "excluded":
        items = [c for c in items if c.get("_triage") == "excluded"]
    elif status == "prioritized":
        items = [c for c in items if c.get("_triage") == "prioritized"]

    if q:
        ql = q.lower()
        items = [
            c
            for c in items
            if ql in (c.get("domain") or "").lower()
            or any(ql in t.lower() for t in (c.get("titles") or []))
            or any(ql in s.lower() for s in (c.get("snippets") or []))
        ]

    def _sort_key(c: dict[str, Any]) -> tuple[int, float, str]:
        t = c.get("_triage", "")
        triage_order = 0 if t == "prioritized" else 1 if t == "" else 2
        if sort == "stage_b_asc":
            sb = _stage_b_scores.get(c["candidate_id"], 1.0)
            return (triage_order, sb, str(c.get("first_seen_at", "")))
        # default: triage group, then chronological (oldest first)
        return (triage_order, 0.0, str(c.get("first_seen_at", "")))

    items.sort(key=_sort_key)

    total = len(items)
    start = (page - 1) * limit
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": max(1, (total + limit - 1) // limit),
        "items": [_serialize(c) for c in items[start : start + limit]],
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:  # silence access log
        pass

    def _json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", _origin)
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", _origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if parsed.path == "/api/candidates":
            try:
                page = max(1, int(qs.get("page", ["1"])[0]))
                limit = max(1, min(200, int(qs.get("limit", ["50"])[0])))
            except ValueError:
                self._json({"error": "invalid page or limit"}, 400)
                return
            self._json(
                _query_candidates(
                    batch_id=qs.get("batch_id", [None])[0],
                    status=qs.get("status", ["all"])[0],
                    q=qs.get("q", [""])[0],
                    page=page,
                    limit=limit,
                    sort=qs.get("sort", ["default"])[0],
                )
            )
        elif parsed.path == "/api/stats":
            self._json(_stats())
        else:
            # Static files — resolve and confine to PUBLIC_DIR
            rel = parsed.path.lstrip("/") or "index.html"
            filepath = (PUBLIC_DIR / rel).resolve()
            if not filepath.is_relative_to(PUBLIC_DIR.resolve()):
                self.send_response(403)
                self.end_headers()
                return
            if not filepath.exists() or not filepath.is_file():
                self.send_response(404)
                self.end_headers()
                return
            mime, _ = mimetypes.guess_type(str(filepath))
            body = filepath.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mime or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/api/triage":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length))
        except Exception:
            self._json({"error": "bad json"}, 400)
            return
        cid = body.get("candidate_id", "")
        action = body.get("action", "")
        if not cid or action not in ("exclude", "prioritize", "reset"):
            self._json({"error": "invalid request"}, 400)
            return
        ok = _record_decision(cid, action)
        if not ok:
            self._json({"error": "unknown candidate"}, 404)
            return
        self._json({"ok": True, "stats": _stats()})


def main() -> None:
    parser = argparse.ArgumentParser(description="Local pre-scrape triage server")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    global _origin
    _origin = f"http://localhost:{args.port}"

    print(f"Loading candidates from {CANDIDATES_FILE} …")
    _load_candidates()
    _load_stage_b_scores()
    _load_decisions()
    s = _stats()
    print(
        f"  {s['total']} candidates  |  "
        f"{s['unreviewed']} unreviewed  |  "
        f"{s['excluded']} excluded  |  "
        f"{s['prioritized']} prioritized"
    )
    if _batch_ids:
        print(f"  Batch IDs: {', '.join(_batch_ids)}")
    print(f"\nOpen http://localhost:{args.port}  (Ctrl-C to stop)\n")
    HTTPServer(("localhost", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
