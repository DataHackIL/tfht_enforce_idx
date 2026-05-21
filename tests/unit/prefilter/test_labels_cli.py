"""Unit tests for `denbust prefilter assemble-labels` CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from denbust.prefilter.cli import prefilter_app
from denbust.prefilter.labels import read_labels_parquet

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _write_config(config_path: Path, state_root: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "test",
        "dataset_name": "news_items",
        "store": {"state_root": str(state_root)},
    }
    with config_path.open("w", encoding="utf-8") as fh:
        yaml.dump(payload, fh)


def _make_fixture_state(tmp_path: Path) -> tuple[Path, Path]:
    """Build a minimal fixture state repo.  Returns (config_path, state_root)."""
    state_root = tmp_path / "state"
    candidates_dir = state_root / "news_items" / "discover" / "candidates"

    candidates = [
        {
            "candidate_id": f"cand_{i:04d}",
            "canonical_url": f"https://example.com/cand_{i:04d}",
            "domain": "example.com",
            "titles": [f"Title {i}"],
            "snippets": [f"Snippet {i}"],
        }
        for i in range(15)
    ]
    decisions = [
        {
            "candidate_id": f"cand_{i:04d}",
            "action": "exclude" if i % 3 != 0 else "prioritize",
            "decided_at": "2026-01-01T00:00:00Z",
            **({"auto": True} if i % 5 == 0 and i % 3 != 0 else {}),
        }
        for i in range(15)
    ]

    _write_jsonl(candidates_dir / "latest_candidates.jsonl", candidates)
    _write_jsonl(candidates_dir / "triage_decisions.jsonl", decisions)

    config_path = tmp_path / "test_config.yaml"
    _write_config(config_path, state_root)
    return config_path, state_root


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAssembleLabelsCommand:
    def test_writes_parquet_to_default_path(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        result = runner.invoke(prefilter_app, ["assemble-labels", "--config", str(config_path)])
        assert result.exit_code == 0, result.output
        expected_path = state_root / "news_items" / "discover" / "prefilter" / "labels.parquet"
        assert expected_path.exists(), f"Expected {expected_path} to exist"

    def test_parquet_has_expected_rows(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["assemble-labels", "--config", str(config_path)])
        path = state_root / "news_items" / "discover" / "prefilter" / "labels.parquet"
        rows = read_labels_parquet(path)
        # Fixture: 15 candidates, all with a non-reset decision, no operational store.
        # i%3==0 → prioritize (i=0,3,6,9,12 → 5 manual-positive)
        # i%5==0 and i%3!=0 → auto exclude (i=5,10 → 2 triage_auto)
        # remaining → manual exclude (i=1,2,4,7,8,11,13,14 → 8 triage_manual negative)
        # Total: 5 + 2 + 8 = 15
        assert len(rows) == 15

    def test_custom_out_path(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        out_path = tmp_path / "custom_labels.parquet"
        result = runner.invoke(
            prefilter_app,
            ["assemble-labels", "--config", str(config_path), "--out", str(out_path)],
        )
        assert result.exit_code == 0, result.output
        assert out_path.exists()
        rows = read_labels_parquet(out_path)
        assert len(rows) > 0

    def test_output_includes_split_summary(self, tmp_path: Path) -> None:
        config_path, _ = _make_fixture_state(tmp_path)
        result = runner.invoke(prefilter_app, ["assemble-labels", "--config", str(config_path)])
        assert result.exit_code == 0, result.output
        assert "train" in result.output
        assert "Wrote" in result.output

    def test_missing_config_exits_nonzero(self) -> None:
        result = runner.invoke(prefilter_app, ["assemble-labels"])
        assert result.exit_code != 0

    def test_splits_assigned_to_all_rows(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["assemble-labels", "--config", str(config_path)])
        path = state_root / "news_items" / "discover" / "prefilter" / "labels.parquet"
        rows = read_labels_parquet(path)
        valid_splits = {"train", "val", "test"}
        for row in rows:
            assert row.split in valid_splits

    def test_parquet_rows_are_frozen_dataclasses(self, tmp_path: Path) -> None:
        import dataclasses

        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["assemble-labels", "--config", str(config_path)])
        path = state_root / "news_items" / "discover" / "prefilter" / "labels.parquet"
        rows = read_labels_parquet(path)
        assert len(rows) > 0
        with pytest.raises(dataclasses.FrozenInstanceError):
            rows[0].label = "positive"  # type: ignore[misc]
