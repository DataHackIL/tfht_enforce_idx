"""Unit tests for the stratified train/val/test split in prefilter.labels."""

from __future__ import annotations

import json
from pathlib import Path

from denbust.prefilter.labels import assemble_labels

# ---------------------------------------------------------------------------
# Shared fixture factory
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _make_state(
    tmp_path: Path,
    n_manual_neg: int = 20,
    n_auto_neg: int = 20,
    n_manual_pos: int = 10,
) -> object:
    """Write a synthetic state repo and return resolved DiscoveryStatePaths."""
    from denbust.discovery.state_paths import resolve_discovery_state_paths
    from denbust.models.common import DatasetName

    candidates_dir = tmp_path / "news_items" / "discover" / "candidates"
    candidates: list[dict[str, object]] = []
    decisions: list[dict[str, object]] = []

    for i in range(n_manual_neg):
        cid = f"manual_neg_{i:04d}"
        candidates.append(
            {
                "candidate_id": cid,
                "canonical_url": f"https://example.com/{cid}",
                "domain": "example.com",
                "titles": [f"Title {cid}"],
                "snippets": [f"Snippet {cid}"],
            }
        )
        decisions.append(
            {"candidate_id": cid, "action": "exclude", "decided_at": "2026-01-01T00:00:00Z"}
        )

    for i in range(n_auto_neg):
        cid = f"auto_neg_{i:04d}"
        candidates.append(
            {
                "candidate_id": cid,
                "canonical_url": f"https://example.com/{cid}",
                "domain": "example.com",
                "titles": [f"Title {cid}"],
                "snippets": [f"Snippet {cid}"],
            }
        )
        decisions.append(
            {
                "candidate_id": cid,
                "action": "exclude",
                "auto": True,
                "decided_at": "2026-01-01T00:00:00Z",
            }
        )

    for i in range(n_manual_pos):
        cid = f"manual_pos_{i:04d}"
        candidates.append(
            {
                "candidate_id": cid,
                "canonical_url": f"https://example.com/{cid}",
                "domain": "example.com",
                "titles": [f"Title {cid}"],
                "snippets": [f"Snippet {cid}"],
            }
        )
        decisions.append(
            {"candidate_id": cid, "action": "prioritize", "decided_at": "2026-01-01T00:00:00Z"}
        )

    _write_jsonl(candidates_dir / "latest_candidates.jsonl", candidates)
    _write_jsonl(candidates_dir / "triage_decisions.jsonl", decisions)

    return resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_same_split(self, tmp_path: Path) -> None:
        paths = _make_state(tmp_path)
        rows_a = assemble_labels(paths, seed=42)
        rows_b = assemble_labels(paths, seed=42)
        assert [r.split for r in rows_a] == [r.split for r in rows_b]

    def test_different_seeds_differ(self, tmp_path: Path) -> None:
        paths = _make_state(tmp_path, n_manual_neg=30, n_auto_neg=30, n_manual_pos=15)
        rows_a = assemble_labels(paths, seed=1)
        rows_b = assemble_labels(paths, seed=99999)
        splits_a = [r.split for r in rows_a]
        splits_b = [r.split for r in rows_b]
        assert splits_a != splits_b, "Different seeds should produce different splits"


class TestFractionCoverage:
    def test_all_rows_assigned_a_split(self, tmp_path: Path) -> None:
        paths = _make_state(tmp_path)
        rows = assemble_labels(paths, val_fraction=0.15, test_fraction=0.15)
        valid_splits = {"train", "val", "test"}
        for row in rows:
            assert row.split in valid_splits

    def test_val_and_test_fractions_are_approximate(self, tmp_path: Path) -> None:
        """Val and test each get ≈15 % of total rows (within 5 pp margin)."""
        paths = _make_state(tmp_path, n_manual_neg=40, n_auto_neg=40, n_manual_pos=20)
        rows = assemble_labels(paths, seed=20260521, val_fraction=0.15, test_fraction=0.15)
        total = len(rows)
        val_frac = sum(1 for r in rows if r.split == "val") / total
        test_frac = sum(1 for r in rows if r.split == "test") / total
        assert abs(val_frac - 0.15) < 0.08, f"val fraction {val_frac:.3f} too far from 0.15"
        assert abs(test_frac - 0.15) < 0.08, f"test fraction {test_frac:.3f} too far from 0.15"

    def test_train_has_most_rows(self, tmp_path: Path) -> None:
        paths = _make_state(tmp_path, n_manual_neg=40, n_auto_neg=40, n_manual_pos=20)
        rows = assemble_labels(paths, val_fraction=0.15, test_fraction=0.15)
        by_split: dict[str, int] = {}
        for r in rows:
            by_split[r.split] = by_split.get(r.split, 0) + 1
        assert by_split["train"] > by_split.get("val", 0)
        assert by_split["train"] > by_split.get("test", 0)


class TestStratification:
    def test_each_stratum_represented_in_train(self, tmp_path: Path) -> None:
        """Every (label, label_source) stratum contributes rows to the train split."""
        paths = _make_state(tmp_path, n_manual_neg=20, n_auto_neg=20, n_manual_pos=10)
        rows = assemble_labels(paths, seed=20260521, val_fraction=0.15, test_fraction=0.15)
        train_strata = {(r.label, r.label_source) for r in rows if r.split == "train"}
        all_strata = {(r.label, r.label_source) for r in rows}
        assert train_strata == all_strata

    def test_label_balance_consistent_across_splits(self, tmp_path: Path) -> None:
        """Positive rate in each non-empty split stays within 10 pp of global rate."""
        paths = _make_state(tmp_path, n_manual_neg=60, n_auto_neg=60, n_manual_pos=30)
        rows = assemble_labels(paths, seed=20260521, val_fraction=0.15, test_fraction=0.15)

        global_pos_rate = sum(1 for r in rows if r.label == "positive") / len(rows)

        for split_name in ("train", "val", "test"):
            split_rows = [r for r in rows if r.split == split_name]
            if not split_rows:
                continue
            pos_rate = sum(1 for r in split_rows if r.label == "positive") / len(split_rows)
            assert abs(pos_rate - global_pos_rate) < 0.12, (
                f"{split_name} positive rate {pos_rate:.3f} too far from global {global_pos_rate:.3f}"
            )


class TestEdgeCases:
    def test_single_candidate_goes_to_train(self, tmp_path: Path) -> None:
        """A stratum with a single candidate should not crash and goes to train."""
        candidates_dir = tmp_path / "news_items" / "discover" / "candidates"
        _write_jsonl(
            candidates_dir / "latest_candidates.jsonl",
            [
                {
                    "candidate_id": "sole",
                    "canonical_url": "https://example.com/sole",
                    "domain": "example.com",
                    "titles": ["Only title"],
                    "snippets": ["Only snippet"],
                }
            ],
        )
        _write_jsonl(
            candidates_dir / "triage_decisions.jsonl",
            [
                {
                    "candidate_id": "sole",
                    "action": "prioritize",
                    "decided_at": "2026-01-01T00:00:00Z",
                }
            ],
        )

        from denbust.discovery.state_paths import resolve_discovery_state_paths
        from denbust.models.common import DatasetName

        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        rows = assemble_labels(paths, val_fraction=0.15, test_fraction=0.15)
        assert len(rows) == 1
        assert rows[0].split == "train"

    def test_empty_decisions_returns_empty(self, tmp_path: Path) -> None:
        candidates_dir = tmp_path / "news_items" / "discover" / "candidates"
        _write_jsonl(candidates_dir / "latest_candidates.jsonl", [])
        _write_jsonl(candidates_dir / "triage_decisions.jsonl", [])

        from denbust.discovery.state_paths import resolve_discovery_state_paths
        from denbust.models.common import DatasetName

        paths = resolve_discovery_state_paths(
            state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS
        )
        rows = assemble_labels(paths)
        assert rows == []
