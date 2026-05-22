"""Unit tests for train_stage_c() in prefilter.stage_c.

Stage C training downloads ``intfloat/multilingual-e5-large`` from HuggingFace,
which is incompatible with the "no live network calls in tests" rule.  All
tests here monkeypatch ``sentence_transformers.SentenceTransformer`` with a
lightweight fake so that:

- The full artifact-write path (embed → centroid → FAISS index → calibration
  → atomic rename) is exercised.
- No network access occurs.
- Tests run fast even when the ``prefilter`` extras are installed.

Skipped entirely when ``faiss`` or ``sentence_transformers`` are not installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

_ = pytest.importorskip("sentence_transformers")  # also implies faiss from [prefilter] extras

from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet
from denbust.prefilter.stage_c import StageCModelMeta, train_stage_c
from tests.unit.prefilter._helpers import FakeSentenceTransformer, make_labeled_row

_PATCH_ST = "sentence_transformers.SentenceTransformer"


def _fake_st(*_args: Any, **_kwargs: Any) -> FakeSentenceTransformer:
    return FakeSentenceTransformer(dim=8)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_fixture(tmp_path: Path, n_per_class: int = 10) -> Path:
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
                    body="משחקי כדורגל" if split == "train" else None,
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
                    body="החשוד נעצר" if split == "train" else None,
                )
            )
            idx += 1
    labels_path = tmp_path / "labels.parquet"
    write_labels_parquet(rows, labels_path)
    return labels_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestTrainStageC:
    """train_stage_c() writes correct artifacts when SentenceTransformer is mocked."""

    @patch(_PATCH_ST, _fake_st)
    def test_returns_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_stage_c(labels_path, tmp_path / "models")
        assert isinstance(meta, StageCModelMeta)

    @patch(_PATCH_ST, _fake_st)
    def test_returns_stage_dir(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert stage_dir.is_dir()

    @patch(_PATCH_ST, _fake_st)
    def test_stage_dir_matches_returned_path(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert stage_dir == tmp_path / "models" / "stage_c"

    @patch(_PATCH_ST, _fake_st)
    def test_centroid_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert (stage_dir / "centroid.npy").exists()

    @patch(_PATCH_ST, _fake_st)
    def test_faiss_index_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert (stage_dir / "index.faiss").exists()

    @patch(_PATCH_ST, _fake_st)
    def test_calibration_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert (stage_dir / "calibration.json").exists()

    @patch(_PATCH_ST, _fake_st)
    def test_meta_json_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        assert (stage_dir / "meta.json").exists()

    @patch(_PATCH_ST, _fake_st)
    def test_meta_json_is_valid(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert len(raw["model_version"]) == 12
        assert isinstance(raw["n_train_positives"], int)
        assert isinstance(raw["n_val"], int)
        assert isinstance(raw["n_neighbors"], int)
        assert raw["base_model_id"] == "intfloat/multilingual-e5-large"

    @patch(_PATCH_ST, _fake_st)
    def test_meta_n_train_positives_matches_split(self, tmp_path: Path) -> None:
        n = 10
        labels_path = _write_fixture(tmp_path, n_per_class=n)
        meta, _ = train_stage_c(labels_path, tmp_path / "models")
        assert meta.n_train_positives == n

    @patch(_PATCH_ST, _fake_st)
    def test_meta_n_val_matches_split(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_stage_c(labels_path, tmp_path / "models")
        assert meta.n_val == 8  # 4 positive + 4 negative

    @patch(_PATCH_ST, _fake_st)
    def test_model_version_is_12_char_hex(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_stage_c(labels_path, tmp_path / "models")
        assert len(meta.model_version) == 12
        assert all(c in "0123456789abcdef" for c in meta.model_version)

    @patch(_PATCH_ST, _fake_st)
    def test_calibration_json_has_coef_and_intercept(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        raw = json.loads((stage_dir / "calibration.json").read_text(encoding="utf-8"))
        assert "coef" in raw
        assert "intercept" in raw
        assert isinstance(raw["coef"], float)
        assert isinstance(raw["intercept"], float)

    @patch(_PATCH_ST, _fake_st)
    def test_centroid_is_l2_normalised(self, tmp_path: Path) -> None:
        import numpy as np

        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models")
        centroid = np.load(str(stage_dir / "centroid.npy"))
        norm = float(np.linalg.norm(centroid))
        assert abs(norm - 1.0) < 1e-5

    @patch(_PATCH_ST, _fake_st)
    def test_atomic_write_no_tmp_dir_left_on_success(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_stage_c(labels_path, models_dir)
        tmp_dirs = list(models_dir.glob("stage_c.tmp.*"))
        assert tmp_dirs == [], f"Stale tmp dirs: {tmp_dirs}"

    @patch(_PATCH_ST, _fake_st)
    def test_second_retrain_replaces_artifacts(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir1 = train_stage_c(labels_path, models_dir)
        _, stage_dir2 = train_stage_c(labels_path, models_dir)
        assert stage_dir1 == stage_dir2

    @patch(_PATCH_ST, _fake_st)
    def test_custom_base_model_id_written_to_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        custom_id = "intfloat/multilingual-e5-small"
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models", base_model_id=custom_id)
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["base_model_id"] == custom_id

    @patch(_PATCH_ST, _fake_st)
    def test_custom_n_neighbors_written_to_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_stage_c(labels_path, tmp_path / "models", n_neighbors=3)
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["n_neighbors"] == 3

    @patch(_PATCH_ST, _fake_st)
    def test_raises_on_no_positive_training_rows(self, tmp_path: Path) -> None:
        rows = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="No positive training examples"):
            train_stage_c(labels_path, tmp_path / "models")

    @patch(_PATCH_ST, _fake_st)
    def test_warns_when_val_has_single_class(self, tmp_path: Path) -> None:
        """Calibration emits UserWarning when val split has only one label class."""
        rows = [
            make_labeled_row(i, "positive", "train", "עצור", "חשוד")  # type: ignore[arg-type]
            for i in range(10)
        ] + [
            # val split: only positives (single class → calibration fallback)
            make_labeled_row(i + 10, "positive", "val", "עצור", "חשוד")  # type: ignore[arg-type]
            for i in range(4)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.warns(UserWarning, match="fewer than 2 label classes"):
            train_stage_c(labels_path, tmp_path / "models")

    @patch(_PATCH_ST, _fake_st)
    def test_raises_import_error_when_sentence_transformers_not_available(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import builtins

        real_import = builtins.__import__

        def _mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("faiss", "sentence_transformers"):
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _mock_import)
        labels_path = _write_fixture(tmp_path)
        with pytest.raises(ImportError, match="prefilter extras"):
            train_stage_c(labels_path, tmp_path / "models")
