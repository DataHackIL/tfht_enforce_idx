"""Shared pytest fixtures for prefilter unit tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from denbust.prefilter.labels import write_labels_parquet
from denbust.prefilter.stage_b import train_naive_bayes

# Re-export FakeCandidate so tests can import it from one place.
# (Importing from _helpers directly is equally valid.)
from tests.unit.prefilter._helpers import FakeCandidate as FakeCandidate  # noqa: PLC0414
from tests.unit.prefilter._helpers import make_labeled_row

# ---------------------------------------------------------------------------
# Shared trained Stage B fixture
#
# scope="module" — one training run per test MODULE, not per test function.
# The models are read-only after setup; there is no need to retrain for each
# of the ~35 tests that exercise the scorer.  tmp_path_factory creates a
# directory that persists for the lifetime of the module.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def trained_stage_b_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Train Stage B models on a small fixture dataset; return *models_dir*.

    Writes ``labels.parquet`` with 15 train rows per class (positive /
    negative) and 4 val + 4 test rows per class, then calls
    :func:`train_naive_bayes` and returns the ``models_dir`` path.

    The fixture is module-scoped so a single training run is shared across
    all tests in a module — training is idempotent and the artifacts are
    purely read-only after setup.
    """
    base = tmp_path_factory.mktemp("stage_b_trained")
    n_per_class = 15
    rows = []
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
                    body=("משחקי כדורגל וטניס וכדורסל הם ספורט פנאי" if split == "train" else None),
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
                    body=("החשוד נעצר לאחר חקירה ממושכת של המשטרה" if split == "train" else None),
                )
            )
            idx += 1

    labels_path = base / "labels.parquet"
    write_labels_parquet(rows, labels_path)
    models_dir = base / "models"
    train_naive_bayes(labels_path, models_dir)
    return models_dir
