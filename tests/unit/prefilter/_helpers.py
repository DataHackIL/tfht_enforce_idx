"""Shared test helpers for prefilter unit tests.

Putting shared test utilities here (rather than duplicating them across test
modules) avoids the conftest import anti-pattern while keeping each test file
self-contained at the import level.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from denbust.prefilter.labels import LabeledCandidate

# ---------------------------------------------------------------------------
# Shared labeled-row builder
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
# Minimal CandidateView for testing
#
# Defined here so all prefilter test files share one implementation instead
# of copy-pasting an identical class into every module.
# ---------------------------------------------------------------------------


class FakeCandidate:
    """Minimal object satisfying the CandidateView protocol for test use."""

    def __init__(
        self,
        candidate_id: str = "cand-test",
        domain: str | None = "example.co.il",
        title: str | None = "עצור חשוד ברצח",
        snippet: str | None = "המשטרה עצרה חשוד",
        url: str | None = "https://example.co.il/article/1",
    ) -> None:
        self._candidate_id = candidate_id
        self._domain = domain
        self._title = title
        self._snippet = snippet
        self._url = url

    @property
    def candidate_id(self) -> str:
        return self._candidate_id

    @property
    def domain(self) -> str | None:
        return self._domain

    @property
    def title(self) -> str | None:
        return self._title

    @property
    def snippet(self) -> str | None:
        return self._snippet

    @property
    def url(self) -> str | None:
        return self._url


# ---------------------------------------------------------------------------
# Minimal SetFit model stub for SetFit-related tests
#
# Defined here so both test_stage_b_setfit_predict.py and
# test_stage_b_setfit_train.py share one implementation.  numpy is imported
# lazily inside predict_proba so this file stays importable even without the
# prefilter extras installed.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Minimal SentenceTransformer stub for Stage C tests
#
# Defined here so both test_stage_c_train.py and test_stage_c_predict.py
# share one implementation.  numpy and faiss are imported lazily so this
# file stays importable without the prefilter extras.
# ---------------------------------------------------------------------------


class FakeSentenceTransformer:
    """Minimal SentenceTransformer stub for Stage C tests.

    ``encode`` returns deterministic, text-dependent float32 embeddings of
    dimension *dim*.  When *normalize_embeddings=True* the rows are L2-
    normalised before returning (matching the real SentenceTransformer API).
    """

    def __init__(self, dim: int = 8) -> None:
        self._dim = dim

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = False,
        show_progress_bar: bool = False,  # noqa: ARG002
        batch_size: int = 32,  # noqa: ARG002
    ) -> Any:
        import hashlib

        import numpy as np

        result = np.zeros((len(texts), self._dim), dtype=np.float32)
        for i, text in enumerate(texts):
            raw = hashlib.md5(text.encode(), usedforsecurity=False).digest()  # noqa: S324
            for j in range(self._dim):
                result[i, j] = (raw[j % len(raw)] / 127.5) - 1.0  # in (−1, 1)

        if normalize_embeddings:
            norms = np.linalg.norm(result, axis=1, keepdims=True)
            norms = np.maximum(norms, 1e-9)
            result = result / norms

        return result


class FakeSetFitModel:
    """Minimal SetFit model stub for testing: no network, no GPU.

    ``predict_proba`` returns a constant probability matrix parameterised by
    *p_negative*.  ``save_pretrained`` writes the three files that
    :func:`~denbust.prefilter.stage_b._sha1_setfit_head` looks for, including
    *p_negative* in ``config_setfit.json`` so that different instances produce
    distinct SHA-1 hashes.
    """

    def __init__(self, p_negative: float = 0.2) -> None:
        self._p_negative = p_negative

    def predict_proba(self, texts: list[str]) -> Any:
        import numpy as np  # lazy: only needed when setfit extras are installed

        n = len(texts)
        result = np.zeros((n, 2), dtype=np.float64)
        result[:, 0] = self._p_negative
        result[:, 1] = 1.0 - self._p_negative
        return result

    def save_pretrained(self, path: str) -> None:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        (p / "config_setfit.json").write_text(
            json.dumps({"model_type": "fake_setfit", "p_negative": self._p_negative}),
            encoding="utf-8",
        )
        (p / "model_head.pkl").write_bytes(b"fake-head-bytes")
        (p / "config.json").write_text(json.dumps({"hidden_size": 4}), encoding="utf-8")
