"""Unit tests for `denbust prefilter retrain --stage a` CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

from denbust.prefilter.cli import prefilter_app
from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_config(config_path: Path, state_root: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "test",
        "dataset_name": "news_items",
        "store": {"state_root": str(state_root)},
    }
    with config_path.open("w", encoding="utf-8") as fh:
        yaml.dump(payload, fh)


def _make_labeled_candidate(
    cid: str,
    label: str,
    split: str,
    domain: str = "example.co.il",
    url: str = "",
) -> LabeledCandidate:
    return LabeledCandidate(
        candidate_id=cid,
        domain=domain,
        url=url or f"https://{domain}/{cid}",
        title=f"Title {cid}",
        snippet=f"Snippet {cid}",
        article_body=None,
        label=label,  # type: ignore[arg-type]
        label_source="triage_manual",  # type: ignore[arg-type]
        split=split,  # type: ignore[arg-type]
        labeled_at="2026-01-01T00:00:00Z",
        decision_hash=f"hash_{cid}",
    )


def _make_fixture_state(tmp_path: Path) -> tuple[Path, Path]:
    """Create a fixture state repo with 40 training rows, return (config_path, state_root)."""
    state_root = tmp_path / "state"
    prefilter_dir = state_root / "news_items" / "discover" / "prefilter"
    prefilter_dir.mkdir(parents=True, exist_ok=True)

    rows: list[LabeledCandidate] = []
    # 20 negatives (train)
    for i in range(20):
        rows.append(_make_labeled_candidate(f"neg_{i:04d}", "negative", "train"))
    # 10 positives (train)
    for i in range(10):
        rows.append(_make_labeled_candidate(f"pos_{i:04d}", "positive", "train"))
    # 5 val, 5 test — should be ignored by retrain
    for i in range(5):
        rows.append(_make_labeled_candidate(f"val_{i}", "negative", "val"))
    for i in range(5):
        rows.append(_make_labeled_candidate(f"test_{i}", "positive", "test"))

    write_labels_parquet(rows, prefilter_dir / "labels.parquet")
    config_path = tmp_path / "config.yaml"
    _write_config(config_path, state_root)
    return config_path, state_root


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRetrainStageA:
    def test_retrain_exits_zero(self, tmp_path: Path) -> None:
        config_path, _ = _make_fixture_state(tmp_path)
        result = runner.invoke(
            prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)]
        )
        assert result.exit_code == 0, result.output

    def test_retrain_creates_lexicon_json(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)])
        lex_path = (
            state_root
            / "news_items"
            / "discover"
            / "prefilter"
            / "models"
            / "stage_a"
            / "lexicon.json"
        )
        assert lex_path.exists(), f"Expected {lex_path} to exist"

    def test_retrain_creates_domain_reputation_parquet(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)])
        dom_path = (
            state_root
            / "news_items"
            / "discover"
            / "prefilter"
            / "models"
            / "stage_a"
            / "domain_reputation.parquet"
        )
        assert dom_path.exists(), f"Expected {dom_path} to exist"

    def test_lexicon_json_is_valid(self, tmp_path: Path) -> None:
        config_path, state_root = _make_fixture_state(tmp_path)
        runner.invoke(prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)])
        lex_path = (
            state_root
            / "news_items"
            / "discover"
            / "prefilter"
            / "models"
            / "stage_a"
            / "lexicon.json"
        )
        data = json.loads(lex_path.read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert len(data) > 0
        first = data[0]
        assert "term" in first
        assert "log_weight_negative" in first
        assert "k_neg" in first
        assert "k_pos" in first

    def test_retrain_output_mentions_artifacts(self, tmp_path: Path) -> None:
        config_path, _ = _make_fixture_state(tmp_path)
        result = runner.invoke(
            prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)]
        )
        assert "lexicon" in result.output.lower()

    def test_retrain_missing_config_exits_nonzero(self) -> None:
        result = runner.invoke(prefilter_app, ["retrain", "--stage", "a"])
        assert result.exit_code != 0

    def test_retrain_missing_labels_exits_nonzero(self, tmp_path: Path) -> None:
        """If labels.parquet doesn't exist, retrain should exit non-zero."""
        state_root = tmp_path / "state"
        state_root.mkdir()
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, state_root)
        result = runner.invoke(
            prefilter_app, ["retrain", "--stage", "a", "--config", str(config_path)]
        )
        assert result.exit_code != 0

    def test_retrain_unsupported_stage_exits_nonzero(self, tmp_path: Path) -> None:
        config_path, _ = _make_fixture_state(tmp_path)
        result = runner.invoke(
            prefilter_app, ["retrain", "--stage", "z", "--config", str(config_path)]
        )
        assert result.exit_code != 0
