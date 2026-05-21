"""Unit tests for train_naive_bayes() in prefilter.stage_b."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet
from denbust.prefilter.stage_b import StageBModelMeta, train_naive_bayes

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_LABELED_AT = "2026-01-01T00:00:00+00:00"


def _make_row(
    idx: int,
    label: str,
    split: str,
    title: str,
    snippet: str,
    body: str | None = None,
) -> LabeledCandidate:
    return LabeledCandidate(
        candidate_id=f"cand-{idx:04d}",
        domain="example.co.il",
        url=f"https://example.co.il/article/{idx}",
        title=title,
        snippet=snippet,
        article_body=body,
        label=label,  # type: ignore[arg-type]
        label_source="triage_manual",
        split=split,  # type: ignore[arg-type]
        labeled_at=_LABELED_AT,
        decision_hash=f"hash{idx:04d}",
    )


def _write_fixture(tmp_path: Path, n_per_class: int = 15) -> Path:
    """Write a minimal labels.parquet with *n_per_class* examples per label."""
    rows: list[LabeledCandidate] = []
    idx = 0
    for split, count in [("train", n_per_class), ("val", 4), ("test", 4)]:
        for _ in range(count):
            rows.append(
                _make_row(
                    idx,
                    "negative",
                    split,
                    title="ספורט ופנאי",
                    snippet="כדורגל ושחמט",
                    body="משחקי כדורגל וטניס וכדורסל הם ספורט פנאי" if split == "train" else None,
                )
            )
            idx += 1
            rows.append(
                _make_row(
                    idx,
                    "positive",
                    split,
                    title="עצור חשוד ברצח",
                    snippet="המשטרה עצרה חשוד",
                    body="החשוד נעצר לאחר חקירה ממושכת של המשטרה" if split == "train" else None,
                )
            )
            idx += 1

    labels_path = tmp_path / "labels.parquet"
    write_labels_parquet(rows, labels_path)
    return labels_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTrainNaiveBayes:
    def test_returns_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta = train_naive_bayes(labels_path, tmp_path / "models")
        assert isinstance(meta, StageBModelMeta)

    def test_meta_model_kind(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.model_kind == "naive_bayes"

    def test_meta_n_train_matches_split(self, tmp_path: Path) -> None:
        n = 15
        labels_path = _write_fixture(tmp_path, n_per_class=n)
        meta = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.n_train == n * 2  # n positive + n negative

    def test_meta_n_val_matches_split(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.n_val == 8  # 4 positive + 4 negative

    def test_thin_model_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_naive_bayes(labels_path, models_dir)
        assert (models_dir / "stage_b" / "thin_model.joblib").exists()

    def test_thick_model_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_naive_bayes(labels_path, models_dir)
        assert (models_dir / "stage_b" / "thick_model.joblib").exists()

    def test_meta_json_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_naive_bayes(labels_path, models_dir)
        assert (models_dir / "stage_b" / "meta.json").exists()

    def test_meta_json_is_valid(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_naive_bayes(labels_path, models_dir)
        raw = json.loads((models_dir / "stage_b" / "meta.json").read_text(encoding="utf-8"))
        assert raw["model_kind"] == "naive_bayes"
        assert len(raw["model_version"]) == 12
        assert isinstance(raw["n_train"], int)
        assert isinstance(raw["n_val"], int)

    def test_model_version_is_12_char_sha1(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta = train_naive_bayes(labels_path, tmp_path / "models")
        assert len(meta.model_version) == 12
        assert all(c in "0123456789abcdef" for c in meta.model_version)

    def test_creates_stage_b_subdir(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "deep" / "nested" / "models"
        train_naive_bayes(labels_path, models_dir)
        assert (models_dir / "stage_b").is_dir()

    def test_raises_on_empty_training_split(self, tmp_path: Path) -> None:
        """ValueError when no 'train' rows exist."""
        rows = [
            _make_row(0, "negative", "val", "ספורט", "כדורגל"),
            _make_row(1, "positive", "val", "עצור", "חשוד"),
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="No training rows"):
            train_naive_bayes(labels_path, tmp_path / "models")

    def test_raises_on_single_class_training(self, tmp_path: Path) -> None:
        """ValueError when only one label class is present in the train split."""
        rows = [_make_row(i, "negative", "train", "ספורט", "כדורגל") for i in range(10)]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="only one label class"):
            train_naive_bayes(labels_path, tmp_path / "models")
