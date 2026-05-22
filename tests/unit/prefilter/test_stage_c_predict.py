"""Unit tests for StageCScorer inference in prefilter.stage_c.

All tests monkeypatch ``sentence_transformers.SentenceTransformer`` with a
lightweight fake so no network access occurs and the test suite stays fast.

Skipped entirely when ``faiss`` or ``sentence_transformers`` are not installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

_ = pytest.importorskip("sentence_transformers")  # also implies faiss from [prefilter] extras

from denbust.prefilter.models import StageScore
from denbust.prefilter.stage_c import StageCScorer, train_stage_c
from tests.unit.prefilter._helpers import FakeCandidate, FakeSentenceTransformer, make_labeled_row

_PATCH_ST = "sentence_transformers.SentenceTransformer"


def _fake_st(*_args: Any, **_kwargs: Any) -> FakeSentenceTransformer:
    return FakeSentenceTransformer(dim=8)


# ---------------------------------------------------------------------------
# Module-scoped fixture: real artifacts produced by train_stage_c with the
# fake SentenceTransformer (no network, no GPU).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stage_c_models_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build Stage C artifacts once for the whole test module.

    Uses ``unittest.mock.patch`` as a context manager rather than a decorator
    because ``@patch`` does not compose reliably with ``scope="module"``
    fixtures in pytest.
    """
    from unittest.mock import patch as _patch

    from denbust.prefilter.labels import write_labels_parquet

    base = tmp_path_factory.mktemp("stage_c_models")
    models_dir = base / "models"

    rows = []
    idx = 0
    for split, count in [("train", 12), ("val", 4), ("test", 4)]:
        for _ in range(count):
            rows.append(
                make_labeled_row(
                    idx,
                    "negative",
                    split,  # type: ignore[arg-type]
                    title="ספורט ופנאי",
                    snippet="כדורגל ושחמט",
                    # Include idx so each training row has a unique embedding.
                    body=f"משחקי כדורגל וטניס {idx}" if split == "train" else None,
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
                    # Include idx so each training positive has a unique embedding.
                    # This ensures that knn_mean(k=1) ≠ knn_mean(k=5) in tests
                    # that verify n_neighbors has a genuine effect on the signal.
                    body=f"החשוד נעצר לאחר חקירה {idx}" if split == "train" else None,
                )
            )
            idx += 1

    labels_path = base / "labels.parquet"
    write_labels_parquet(rows, labels_path)
    with _patch(_PATCH_ST, _fake_st):
        train_stage_c(labels_path, models_dir)
    return models_dir


# ---------------------------------------------------------------------------
# Stub behaviour — no artifacts
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageCStoreScorerStub:
    """StageCScorer returns None when no artifacts are present."""

    def test_none_models_dir_returns_none_thick(self) -> None:
        scorer = StageCScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thick", body="some body") is None

    def test_none_models_dir_returns_none_thin(self) -> None:
        scorer = StageCScorer(models_dir=None)
        assert scorer.evaluate(FakeCandidate(), "thin") is None

    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        scorer = StageCScorer(models_dir=tmp_path / "nonexistent")
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None

    def test_empty_models_dir_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "stage_c").mkdir()
        scorer = StageCScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None

    def test_thin_pass_always_returns_none_even_when_loaded(self, stage_c_models_dir: Path) -> None:
        """Thin pass must return None regardless of whether artifacts are loaded."""
        with patch(_PATCH_ST, _fake_st):
            scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thin")
        assert result is None

    def test_sentence_transformers_not_installed_returns_none(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When sentence_transformers is missing the scorer logs a warning."""
        import builtins
        import logging

        # Build a minimal (but incomplete) stage_c dir to trigger the import path.
        stage_dir = tmp_path / "stage_c"
        stage_dir.mkdir()
        (stage_dir / "centroid.npy").write_bytes(b"fake")
        (stage_dir / "index.faiss").write_bytes(b"fake")
        (stage_dir / "calibration.json").write_text(
            json.dumps({"coef": 1.0, "intercept": 0.0}), encoding="utf-8"
        )

        real_import = builtins.__import__

        def _block(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("faiss", "sentence_transformers"):
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block)
        with caplog.at_level(logging.WARNING, logger="denbust.prefilter.stage_c"):
            scorer = StageCScorer(models_dir=tmp_path)
        assert scorer.evaluate(FakeCandidate(), "thick", body="body") is None
        assert any("prefilter" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Loaded model behaviour
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestStageCStoreScorerLoaded:
    """StageCScorer loaded from real artifacts (fake embedder) returns valid scores."""

    @patch(_PATCH_ST, _fake_st)
    def test_thick_returns_stage_score(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="החשוד נעצר")
        assert isinstance(result, StageScore)

    @patch(_PATCH_ST, _fake_st)
    def test_thick_without_body_falls_back_to_snippet(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body=None)
        assert isinstance(result, StageScore)

    @patch(_PATCH_ST, _fake_st)
    def test_stage_score_has_stage_c(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.stage == "C"

    @patch(_PATCH_ST, _fake_st)
    def test_p_negative_in_unit_interval(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="some body text")
        assert result is not None
        assert 0.0 <= result.p_negative <= 1.0

    @patch(_PATCH_ST, _fake_st)
    def test_reason_format(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.reason.startswith("embed/thick=centroid:")
        assert "knn_mean:" in result.reason

    @patch(_PATCH_ST, _fake_st)
    def test_threshold_propagated(self, stage_c_models_dir: Path) -> None:
        custom = 0.7
        scorer = StageCScorer(models_dir=stage_c_models_dir, threshold=custom)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.threshold == custom

    @patch(_PATCH_ST, _fake_st)
    def test_dropped_flag_consistent_with_threshold(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir, threshold=0.5)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.dropped == (result.p_negative >= 0.5)

    @patch(_PATCH_ST, _fake_st)
    def test_model_version_loaded_from_meta(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        assert len(scorer.model_version) == 12
        assert all(c in "0123456789abcdef" for c in scorer.model_version)

    @patch(_PATCH_ST, _fake_st)
    def test_model_version_propagated_to_stage_score(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert result is not None
        assert result.model_version == scorer.model_version

    @patch(_PATCH_ST, _fake_st)
    def test_base_model_id_loaded_from_meta(self, stage_c_models_dir: Path) -> None:
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        assert scorer.base_model_id == "intfloat/multilingual-e5-large"

    @patch(_PATCH_ST, _fake_st)
    def test_deterministic_same_input(self, stage_c_models_dir: Path) -> None:
        """Same input to the same scorer must produce the same p_negative."""
        scorer = StageCScorer(models_dir=stage_c_models_dir)
        cand = FakeCandidate(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        r1 = scorer.evaluate(cand, "thick", body="נעצר")
        r2 = scorer.evaluate(cand, "thick", body="נעצר")
        assert r1 is not None and r2 is not None
        assert r1.p_negative == r2.p_negative

    @patch(_PATCH_ST, _fake_st)
    def test_n_neighbors_override_stored(self, stage_c_models_dir: Path) -> None:
        """A caller-supplied n_neighbors overrides the value from meta.json."""
        scorer = StageCScorer(models_dir=stage_c_models_dir, n_neighbors=1)
        assert scorer._n_neighbors == 1
        result = scorer.evaluate(FakeCandidate(), "thick", body="body")
        assert isinstance(result, StageScore)

    @patch(_PATCH_ST, _fake_st)
    def test_n_neighbors_affects_score(self, stage_c_models_dir: Path) -> None:
        """n_neighbors=1 (nearest-neighbour cosine) differs from n_neighbors=5 (mean over 5).

        With the fixed implementation, knn_mean_cos is the mean cosine over k
        neighbours.  k=1 gives the cosine to the single nearest neighbour;
        k=5 averages over 5 neighbours — these are distinct signals unless all
        5 neighbours are equidistant (astronomically unlikely with hash-based
        fake embeddings and 12 distinct training positives).
        """
        cand = FakeCandidate(title="עצור חשוד ברצח", snippet="המשטרה עצרה חשוד")
        scorer1 = StageCScorer(models_dir=stage_c_models_dir, n_neighbors=1)
        scorer5 = StageCScorer(models_dir=stage_c_models_dir, n_neighbors=5)
        r1 = scorer1.evaluate(cand, "thick", body="נעצר לאחר חקירה")
        r5 = scorer5.evaluate(cand, "thick", body="נעצר לאחר חקירה")
        assert r1 is not None and r5 is not None
        assert r1.p_negative != r5.p_negative, (
            "n_neighbors=1 and n_neighbors=5 produced identical p_negative — "
            "the knn_mean signal is not actually using k neighbours"
        )
