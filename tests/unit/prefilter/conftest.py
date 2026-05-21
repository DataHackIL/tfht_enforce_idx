"""Shared pytest fixtures for prefilter unit tests."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from denbust.prefilter.labels import LabeledCandidate, write_labels_parquet
from denbust.prefilter.stage_b import train_naive_bayes

# ---------------------------------------------------------------------------
# Shared label-row builder
# ---------------------------------------------------------------------------

_LABELED_AT = "2026-01-01T00:00:00+00:00"


def make_labeled_row(
    idx: int,
    label: Literal["positive", "negative"],
    split: Literal["train", "val", "test"],
    title: str,
    snippet: str,
    body: str | None = None,
) -> LabeledCandidate:
    """Build a minimal :class:`LabeledCandidate` for fixture use."""
    return LabeledCandidate(
        candidate_id=f"cand-{idx:04d}",
        domain="example.co.il",
        url=f"https://example.co.il/article/{idx}",
        title=title,
        snippet=snippet,
        article_body=body,
        label=label,
        label_source="triage_manual",
        split=split,
        labeled_at=_LABELED_AT,
        decision_hash=f"hash{idx:04d}",
    )


# ---------------------------------------------------------------------------
# Shared trained Stage B fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def trained_stage_b_dir(tmp_path: Path) -> Path:
    """Train Stage B models on a small fixture dataset; return *models_dir*.

    Writes ``labels.parquet`` with 15 train rows per class (positive /
    negative) and 4 val + 4 test rows per class, then calls
    :func:`train_naive_bayes` and returns the ``models_dir`` path.
    """
    n_per_class = 15
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

    labels_path = tmp_path / "labels.parquet"
    write_labels_parquet(rows, labels_path)
    models_dir = tmp_path / "models"
    train_naive_bayes(labels_path, models_dir)
    return models_dir
