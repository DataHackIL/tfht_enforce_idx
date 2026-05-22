"""Unit tests for train_naive_bayes() in prefilter.stage_b."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest

from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet
from denbust.prefilter.stage_b import StageBModelMeta, train_naive_bayes
from tests.unit.prefilter._helpers import make_labeled_row

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_fixture(tmp_path: Path, n_per_class: int = 15) -> Path:
    """Write a minimal labels.parquet with *n_per_class* examples per label."""
    rows: list[LabeledCandidate] = []
    idx = 0
    for split, count in [("train", n_per_class), ("val", 4), ("test", 4)]:
        for _ in range(count):
            rows.append(
                make_labeled_row(
                    idx,
                    "negative",
                    split,  # type: ignore[arg-type]
                    title="ספורט ופנאי",
                    snippet="כדורגל ושחמט",
                    body="משחקי כדורגל וטניס וכדורסל הם ספורט פנאי" if split == "train" else None,
                )
            )
            idx += 1
            rows.append(
                make_labeled_row(
                    idx,
                    "positive",
                    split,  # type: ignore[arg-type]
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
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert isinstance(meta, StageBModelMeta)

    def test_returns_stage_dir(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_naive_bayes(labels_path, tmp_path / "models")
        assert stage_dir.is_dir()

    def test_meta_model_kind(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.model_kind == "naive_bayes"

    def test_meta_n_train_matches_split(self, tmp_path: Path) -> None:
        n = 15
        labels_path = _write_fixture(tmp_path, n_per_class=n)
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.n_train == n * 2  # n positive + n negative

    def test_meta_n_val_matches_split(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.n_val == 8  # 4 positive + 4 negative

    def test_meta_n_thick_with_body_counts_non_none_bodies(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path, n_per_class=15)
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        # _write_fixture gives body to all 15*2=30 train rows
        assert meta.n_thick_with_body == 30

    def test_meta_n_thick_with_body_zero_when_no_bodies(self, tmp_path: Path) -> None:
        rows: list[LabeledCandidate] = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ]
        rows += [
            make_labeled_row(i + 10, "positive", "train", "עצור", "חשוד")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert meta.n_thick_with_body == 0

    def test_thin_model_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        assert (stage_dir / "thin_model.joblib").exists()

    def test_thick_model_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        assert (stage_dir / "thick_model.joblib").exists()

    def test_meta_json_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        assert (stage_dir / "meta.json").exists()

    def test_stage_dir_matches_returned_path(self, tmp_path: Path) -> None:
        """The returned stage_dir is the actual artifact directory."""
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        assert stage_dir == models_dir / "stage_b"

    def test_meta_json_is_valid(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["model_kind"] == "naive_bayes"
        assert len(raw["model_version"]) == 12
        assert isinstance(raw["n_train"], int)
        assert isinstance(raw["n_val"], int)
        assert isinstance(raw["n_thick_with_body"], int)

    def test_model_version_is_12_char_sha1(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_naive_bayes(labels_path, tmp_path / "models")
        assert len(meta.model_version) == 12
        assert all(c in "0123456789abcdef" for c in meta.model_version)

    def test_creates_stage_b_subdir(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "deep" / "nested" / "models"
        _, stage_dir = train_naive_bayes(labels_path, models_dir)
        assert stage_dir.is_dir()

    def test_atomic_write_no_tmp_dir_left_on_success(self, tmp_path: Path) -> None:
        """After a successful retrain, no temp directory should remain."""
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_naive_bayes(labels_path, models_dir)
        tmp_dirs = list(models_dir.glob("stage_b.tmp.*"))
        assert tmp_dirs == [], f"Stale tmp dirs: {tmp_dirs}"

    def test_second_retrain_replaces_artifacts(self, tmp_path: Path) -> None:
        """Calling train_naive_bayes twice should overwrite the artifacts cleanly."""
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        meta1, stage_dir1 = train_naive_bayes(labels_path, models_dir)
        meta2, stage_dir2 = train_naive_bayes(labels_path, models_dir)
        assert stage_dir1 == stage_dir2
        assert meta1.model_kind == meta2.model_kind

    def test_raises_on_empty_training_split(self, tmp_path: Path) -> None:
        """ValueError when no 'train' rows exist."""
        rows = [
            make_labeled_row(0, "negative", "val", "ספורט", "כדורגל"),  # type: ignore[arg-type]
            make_labeled_row(1, "positive", "val", "עצור", "חשוד"),  # type: ignore[arg-type]
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="No training rows"):
            train_naive_bayes(labels_path, tmp_path / "models")

    def test_raises_on_single_class_training(self, tmp_path: Path) -> None:
        """ValueError when only one label class is present in the train split."""
        rows = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="only one label class"):
            train_naive_bayes(labels_path, tmp_path / "models")

    def test_warns_when_all_bodies_none(self, tmp_path: Path) -> None:
        """UserWarning emitted when thick model falls back to thin at training."""
        rows: list[LabeledCandidate] = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ]
        rows += [
            make_labeled_row(i + 10, "positive", "train", "עצור", "חשוד")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.warns(UserWarning, match="identical to the thin model"):
            train_naive_bayes(labels_path, tmp_path / "models")
