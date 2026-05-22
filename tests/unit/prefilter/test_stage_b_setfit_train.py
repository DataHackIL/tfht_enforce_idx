"""Unit tests for train_setfit() in prefilter.stage_b.

SetFit training downloads a large (~560 MB) sentence-encoder from HuggingFace,
which is incompatible with the project's "no live network calls in tests" rule.
All tests here monkeypatch ``SetFitModel.from_pretrained`` and
``SetFitTrainer`` with lightweight fakes so that:

- The full artifact-write path (train → save → atomic rename) is exercised.
- No network access occurs.
- The test suite runs fast even when the ``prefilter`` extras are installed.

Skipped entirely when ``setfit`` is not installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

setfit = pytest.importorskip("setfit")  # skip whole module if setfit not installed

from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet
from denbust.prefilter.stage_b import StageBModelMeta, train_setfit
from tests.unit.prefilter._helpers import make_labeled_row

# ---------------------------------------------------------------------------
# Fake SetFit model & trainer (avoids any network or GPU usage)
# ---------------------------------------------------------------------------


class _FakeSetFitModel:
    """Minimal SetFit model stub that persists/loads without real weights."""

    def __init__(self, p_negative: float = 0.2) -> None:
        self._p_negative = p_negative

    def predict_proba(self, texts: list[str]) -> Any:
        import numpy as np

        n = len(texts)
        result = np.zeros((n, 2), dtype=np.float64)
        result[:, 0] = self._p_negative
        result[:, 1] = 1.0 - self._p_negative
        return result

    def save_pretrained(self, path: str) -> None:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        # Write the files that _sha1_setfit_head looks for.
        (p / "config_setfit.json").write_text(
            json.dumps({"model_type": "fake_setfit", "p_negative": self._p_negative}),
            encoding="utf-8",
        )
        (p / "model_head.pkl").write_bytes(b"fake-head-bytes")
        (p / "config.json").write_text(
            json.dumps({"hidden_size": 4}),
            encoding="utf-8",
        )


def _fake_from_pretrained(*_args: Any, **_kwargs: Any) -> _FakeSetFitModel:
    return _FakeSetFitModel()


class _FakeSetFitTrainer:
    def __init__(self, **_kwargs: Any) -> None:
        pass

    def train(self) -> None:
        pass  # no-op — no real training


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_fixture(tmp_path: Path, n_per_class: int = 10) -> Path:
    rows: list[LabeledCandidate] = []
    idx = 0
    for split, count in [("train", n_per_class), ("val", 3), ("test", 3)]:
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
# All tests patch SetFit internals so no network is needed.
# ---------------------------------------------------------------------------

_PATCH_FROM_PRETRAINED = "setfit.SetFitModel.from_pretrained"
_PATCH_TRAINER = "denbust.prefilter.stage_b.SetFitTrainer"


@pytest.mark.slow
class TestTrainSetfit:
    """train_setfit() writes correct artifacts when SetFit is mocked."""

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_returns_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert isinstance(meta, StageBModelMeta)

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_returns_stage_dir(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        assert stage_dir.is_dir()

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_stage_dir_matches_returned_path(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        assert stage_dir == tmp_path / "models" / "stage_b_setfit"

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_meta_model_kind_is_setfit(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert meta.model_kind == "setfit"

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_meta_n_train_matches_split(self, tmp_path: Path) -> None:
        n = 10
        labels_path = _write_fixture(tmp_path, n_per_class=n)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert meta.n_train == n * 2  # n positive + n negative

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_meta_n_val_matches_split(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert meta.n_val == 6  # 3 positive + 3 negative

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thin_model_dir_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        assert (stage_dir / "thin_model").is_dir()

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_thick_model_dir_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        assert (stage_dir / "thick_model").is_dir()

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_meta_json_artifact_created(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        assert (stage_dir / "meta.json").exists()

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_meta_json_is_valid(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        _, stage_dir = train_setfit(labels_path, tmp_path / "models")
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["model_kind"] == "setfit"
        assert len(raw["model_version"]) == 12
        assert isinstance(raw["n_train"], int)
        assert isinstance(raw["n_val"], int)
        assert isinstance(raw["n_thick_with_body"], int)
        assert raw["base_model_id"] == "intfloat/multilingual-e5-large"

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_model_version_is_12_char_hex(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert len(meta.model_version) == 12
        assert all(c in "0123456789abcdef" for c in meta.model_version)

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_n_thick_with_body_counted_correctly(self, tmp_path: Path) -> None:
        n = 10
        labels_path = _write_fixture(tmp_path, n_per_class=n)
        meta, _ = train_setfit(labels_path, tmp_path / "models")
        assert meta.n_thick_with_body == n * 2  # all train rows have bodies

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_atomic_write_no_tmp_dir_left_on_success(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        train_setfit(labels_path, models_dir)
        tmp_dirs = list(models_dir.glob("stage_b_setfit.tmp.*"))
        assert tmp_dirs == [], f"Stale tmp dirs: {tmp_dirs}"

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_second_retrain_replaces_artifacts(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        models_dir = tmp_path / "models"
        _, stage_dir1 = train_setfit(labels_path, models_dir)
        _, stage_dir2 = train_setfit(labels_path, models_dir)
        assert stage_dir1 == stage_dir2

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_warns_when_all_bodies_none(self, tmp_path: Path) -> None:
        rows: list[LabeledCandidate] = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ] + [
            make_labeled_row(i + 10, "positive", "train", "עצור", "חשוד")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.warns(UserWarning, match="identical to the thin model"):
            train_setfit(labels_path, tmp_path / "models")

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_raises_on_empty_training_split(self, tmp_path: Path) -> None:
        rows = [
            make_labeled_row(0, "negative", "val", "ספורט", "כדורגל"),  # type: ignore[arg-type]
            make_labeled_row(1, "positive", "val", "עצור", "חשוד"),  # type: ignore[arg-type]
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="No training rows"):
            train_setfit(labels_path, tmp_path / "models")

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_raises_on_single_class_training(self, tmp_path: Path) -> None:
        rows = [
            make_labeled_row(i, "negative", "train", "ספורט", "כדורגל")  # type: ignore[arg-type]
            for i in range(10)
        ]
        labels_path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, labels_path)
        with pytest.raises(ValueError, match="only one label class"):
            train_setfit(labels_path, tmp_path / "models")

    def test_raises_import_error_when_setfit_not_available(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """train_setfit raises ImportError with a helpful message when setfit is missing."""
        import builtins

        real_import = builtins.__import__

        def _mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("datasets", "setfit"):
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _mock_import)
        labels_path = _write_fixture(tmp_path)
        with pytest.raises(ImportError, match="prefilter"):
            train_setfit(labels_path, tmp_path / "models")

    @patch(_PATCH_TRAINER, _FakeSetFitTrainer)
    @patch(_PATCH_FROM_PRETRAINED, _fake_from_pretrained)
    def test_custom_base_model_id_written_to_meta(self, tmp_path: Path) -> None:
        labels_path = _write_fixture(tmp_path)
        custom_id = "intfloat/multilingual-e5-small"
        _, stage_dir = train_setfit(labels_path, tmp_path / "models", base_model_id=custom_id)
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["base_model_id"] == custom_id
