"""Unit tests for label conflict-resolution priority in prefilter.labels."""

from __future__ import annotations

import json
from pathlib import Path

from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.models.common import DatasetName
from denbust.ops.storage import NullOperationalStore
from denbust.prefilter.labels import assemble_labels

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _candidate(cid: str, url: str = "") -> dict[str, object]:
    return {
        "candidate_id": cid,
        "canonical_url": url or f"https://example.com/{cid}",
        "current_url": url or f"https://example.com/{cid}",
        "domain": "example.com",
        "titles": [f"Title {cid}"],
        "snippets": [f"Snippet {cid}"],
    }


def _paths(tmp_path: Path) -> object:
    """Return a DiscoveryStatePaths for *tmp_path* backed by the NEWS_ITEMS dataset."""
    return resolve_discovery_state_paths(
        state_root=tmp_path,
        dataset_name=DatasetName.NEWS_ITEMS,
    )


def _candidates_dir(tmp_path: Path) -> Path:
    return tmp_path / "news_items" / "discover" / "candidates"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTriageManualWins:
    """triage_manual overrides both claude_classifier and triage_auto."""

    def test_manual_exclude_beats_triage_auto(self, tmp_path: Path) -> None:
        """A manual exclude decision (no auto flag) supersedes any auto label."""
        cd = _candidates_dir(tmp_path)
        _write_jsonl(cd / "latest_candidates.jsonl", [_candidate("cand_A")])
        # First an auto exclude, then a manual exclude
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_A",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                },
                {
                    "candidate_id": "cand_A",
                    "action": "exclude",
                    "decided_at": "2026-01-02T00:00:00Z",
                },
            ],
        )

        rows = assemble_labels(_paths(tmp_path))
        assert len(rows) == 1
        assert rows[0].label == "negative"
        assert rows[0].label_source == "triage_manual"

    def test_prioritize_beats_auto_exclude(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        _write_jsonl(cd / "latest_candidates.jsonl", [_candidate("cand_B")])
        # Latest decision is prioritize (manual positive)
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_B",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                },
                {
                    "candidate_id": "cand_B",
                    "action": "prioritize",
                    "decided_at": "2026-01-02T00:00:00Z",
                },
            ],
        )

        rows = assemble_labels(_paths(tmp_path))
        assert len(rows) == 1
        assert rows[0].label == "positive"
        assert rows[0].label_source == "triage_manual"


class TestClaudeClassifierPriority:
    """claude_classifier beats triage_auto but loses to triage_manual."""

    def test_claude_beats_triage_auto(self, tmp_path: Path) -> None:
        """When a candidate has both auto-exclude and a claude positive label,
        claude_classifier wins because it has higher priority."""
        cd = _candidates_dir(tmp_path)
        cand = _candidate("cand_C", url="https://example.com/cand_C")
        _write_jsonl(cd / "latest_candidates.jsonl", [cand])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_C",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        class _FakeStore(NullOperationalStore):
            def fetch_records(self, _dataset_name: str, *, _limit: int | None = None) -> list[dict]:  # type: ignore[override]
                return [{"canonical_url": "https://example.com/cand_C", "index_relevant": True}]

        rows = assemble_labels(_paths(tmp_path), operational_store=_FakeStore())
        assert len(rows) == 1
        assert rows[0].label == "positive"
        assert rows[0].label_source == "claude_classifier"

    def test_triage_manual_beats_claude(self, tmp_path: Path) -> None:
        """A manual triage decision overrides the claude_classifier label."""
        cd = _candidates_dir(tmp_path)
        cand = _candidate("cand_D", url="https://example.com/cand_D")
        _write_jsonl(cd / "latest_candidates.jsonl", [cand])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_D",
                    "action": "exclude",
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        class _FakeStore(NullOperationalStore):
            def fetch_records(self, _dataset_name: str, *, _limit: int | None = None) -> list[dict]:  # type: ignore[override]
                return [{"canonical_url": "https://example.com/cand_D", "index_relevant": True}]

        rows = assemble_labels(_paths(tmp_path), operational_store=_FakeStore())
        assert len(rows) == 1
        assert rows[0].label == "negative"
        assert rows[0].label_source == "triage_manual"


class TestResetDropsBothSources:
    """reset drops a candidate even if the operational store has a label."""

    def test_reset_removes_claude_label(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        cand = _candidate("cand_E", url="https://example.com/cand_E")
        _write_jsonl(cd / "latest_candidates.jsonl", [cand])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_E",
                    "action": "reset",
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        class _FakeStore(NullOperationalStore):
            def fetch_records(self, _dataset_name: str, *, _limit: int | None = None) -> list[dict]:  # type: ignore[override]
                return [{"canonical_url": "https://example.com/cand_E", "index_relevant": True}]

        rows = assemble_labels(_paths(tmp_path), operational_store=_FakeStore())
        assert rows == []

    def test_reset_removes_triage_auto_label(self, tmp_path: Path) -> None:
        """Even if a prior decision was auto-exclude, a later reset drops the candidate."""
        cd = _candidates_dir(tmp_path)
        _write_jsonl(cd / "latest_candidates.jsonl", [_candidate("cand_F")])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_F",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                },
                {
                    "candidate_id": "cand_F",
                    "action": "reset",
                    "decided_at": "2026-01-02T00:00:00Z",
                },
            ],
        )

        rows = assemble_labels(_paths(tmp_path))
        assert rows == []


class TestTriageAutoSource:
    """Candidates with only auto-exclude get triage_auto label source."""

    def test_auto_exclude_label_source(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        _write_jsonl(cd / "latest_candidates.jsonl", [_candidate("cand_G")])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_G",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        rows = assemble_labels(_paths(tmp_path))
        assert len(rows) == 1
        assert rows[0].label == "negative"
        assert rows[0].label_source == "triage_auto"


class TestNoSnapshotSkipped:
    """Candidates with a triage decision but no candidate snapshot are skipped."""

    def test_missing_candidate_snapshot_skipped(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        # No candidate in latest_candidates.jsonl for "ghost_cand"
        _write_jsonl(cd / "latest_candidates.jsonl", [])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "ghost_cand",
                    "action": "exclude",
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        rows = assemble_labels(_paths(tmp_path))
        assert rows == []


class TestPriorityAcrossAllThreeSources:
    """Full priority ladder: triage_manual > claude_classifier > triage_auto."""

    def test_three_source_priority_order(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        # cand_H: auto-exclude AND claude positive → claude wins
        # cand_I: manual-exclude AND claude positive → manual wins (negative)
        # cand_J: auto-exclude only → triage_auto
        cands = [
            _candidate("cand_H", url="https://example.com/cand_H"),
            _candidate("cand_I", url="https://example.com/cand_I"),
            _candidate("cand_J"),
        ]
        _write_jsonl(cd / "latest_candidates.jsonl", cands)
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_H",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                },
                {
                    "candidate_id": "cand_I",
                    "action": "exclude",
                    "decided_at": "2026-01-01T00:00:00Z",
                },
                {
                    "candidate_id": "cand_J",
                    "action": "exclude",
                    "auto": True,
                    "decided_at": "2026-01-01T00:00:00Z",
                },
            ],
        )

        class _FakeStore(NullOperationalStore):
            def fetch_records(self, _dataset_name: str, *, _limit: int | None = None) -> list[dict]:  # type: ignore[override]
                return [
                    {"canonical_url": "https://example.com/cand_H", "index_relevant": True},
                    {"canonical_url": "https://example.com/cand_I", "index_relevant": True},
                ]

        rows = assemble_labels(_paths(tmp_path), operational_store=_FakeStore())
        by_id = {r.candidate_id: r for r in rows}
        assert by_id["cand_H"].label == "positive"
        assert by_id["cand_H"].label_source == "claude_classifier"
        assert by_id["cand_I"].label == "negative"
        assert by_id["cand_I"].label_source == "triage_manual"
        assert by_id["cand_J"].label == "negative"
        assert by_id["cand_J"].label_source == "triage_auto"


class TestOperationalStoreFallback:
    """Errors from the operational store are silenced and don't abort assembly."""

    def test_failing_operational_store_falls_back_gracefully(self, tmp_path: Path) -> None:
        cd = _candidates_dir(tmp_path)
        _write_jsonl(cd / "latest_candidates.jsonl", [_candidate("cand_X")])
        _write_jsonl(
            cd / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "cand_X",
                    "action": "exclude",
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        class _BrokenStore(NullOperationalStore):
            def fetch_records(self, _dataset_name: str, *, _limit: int | None = None) -> list[dict]:  # type: ignore[override]
                raise RuntimeError("connection refused")

        rows = assemble_labels(_paths(tmp_path), operational_store=_BrokenStore())
        assert len(rows) == 1
        assert rows[0].label_source == "triage_manual"
