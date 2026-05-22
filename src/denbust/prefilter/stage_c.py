"""Stage C — multilingual embedding centroid + FAISS kNN similarity.

:class:`StageCScorer`
    Centroid-cosine + FAISS-HNSW kNN-mean-cosine similarity scorer,
    sigmoid-calibrated on the validation split.
    Artifacts: ``models_dir/stage_c/{centroid.npy, index.faiss,
    calibration.json, meta.json}``.  Requires the ``prefilter`` extras
    (``pip install -e '.[dev,prefilter]'``).

    **Thick-pass only**: :meth:`~StageCScorer.evaluate` returns ``None``
    when *pass_kind* is ``"thin"`` so Stage C is silently skipped during the
    pre-scrape cascade pass.

Training entry point
--------------------
:func:`train_stage_c`
    Embeds training positives using
    :class:`sentence_transformers.SentenceTransformer`, builds a centroid
    vector and a FAISS HNSW index, fits sigmoid calibration on the validation
    split, then writes artifacts atomically.

Scoring
-------
For each thick-pass candidate the scorer computes two cosine-similarity
signals against the *positive* training set:

centroid_cos
    Cosine similarity between the candidate embedding and the L2-normalised
    mean embedding of all training positives.
knn_mean_cos
    Mean cosine similarity over the *n_neighbors* nearest training positives
    found via the FAISS HNSW index.  Larger values of *n_neighbors* smooth the
    signal over a broader neighbourhood; the parameter has a real effect on the
    output because the mean is taken over all *k* retrieved results, not just
    the closest one.

The combined raw signal is ``raw = max(centroid_cos, knn_mean_cos)``.  A
logistic-regression sigmoid calibration maps *raw* to ``p_positive``; the
final ``p_negative = 1 − p_positive``.

Text format
-----------
``intfloat/multilingual-e5-large`` requires a ``"passage: "`` prefix for
document embeddings (see the model card).  :func:`_build_text` applies this
prefix to *all* inputs — both at training time and at inference time — so the
embedding space is consistent across the two phases.  Changing this function
changes both training and inference; there is no separate logic to drift.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import math
import os
import shutil
import tempfile
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from denbust.prefilter.models import CandidateView, PassKind, StageScore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Artifact path constants
# ---------------------------------------------------------------------------

_STAGE_C_SUBDIR = "stage_c"
_CENTROID_FILE = "centroid.npy"
_INDEX_FILE = "index.faiss"
_CALIBRATION_FILE = "calibration.json"
_META_FILE = "meta.json"

_DEFAULT_BASE_MODEL = "intfloat/multilingual-e5-large"
_DEFAULT_N_NEIGHBORS = 5
_HNSW_M = 32  # connections per HNSW layer; 32 is a solid default for recall
_HNSW_EF_CONSTRUCTION = 200  # higher → better index quality at build time
_HNSW_EF_SEARCH = 64  # fixed search budget — must NOT scale with corpus size or
# HNSW degrades to O(n) traversal, defeating its purpose

# intfloat/multilingual-e5-large requires this prefix for document embeddings.
# See: https://huggingface.co/intfloat/multilingual-e5-large
_E5_PASSAGE_PREFIX = "passage: "


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class StageCModelMeta:
    """Provenance metadata for trained Stage C artifacts.

    Attributes
    ----------
    model_version:
        Short (12-char) SHA-1 prefix derived from the centroid, FAISS index,
        and calibration artifacts combined — changes whenever any artifact does.
    trained_at:
        ISO-8601 UTC timestamp of when the artifacts were written.
    n_train_positives:
        Number of positive training examples used to build the index.
    n_val:
        Total validation rows available at training time.
    base_model_id:
        HuggingFace model ID used for encoding.
    n_neighbors:
        Number of HNSW neighbours queried for the kNN-mean-cosine signal.
    """

    model_version: str
    trained_at: str
    n_train_positives: int
    n_val: int
    base_model_id: str
    n_neighbors: int


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _sigmoid(x: float) -> float:
    """Numerically-stable sigmoid."""
    if x >= 0.0:
        return 1.0 / (1.0 + math.exp(-x))
    ex = math.exp(x)
    return ex / (1.0 + ex)


def _l2_normalize(v: Any) -> Any:
    """Return an L2-normalised copy of 1-D array *v* (numpy)."""
    import numpy as np

    norm = float(np.linalg.norm(v))
    if norm < 1e-9:
        return v.copy()
    return v / norm


def _build_text(title: str | None, body_or_snippet: str | None) -> str:
    """Assemble the E5-format passage text for a candidate.

    This is the **single source of truth** for text assembly — called by both
    :func:`train_stage_c` (for training positives and validation rows) and
    :meth:`StageCScorer.evaluate` (for inference), guaranteeing train/serve
    consistency.

    ``intfloat/multilingual-e5-large`` requires a ``"passage: "`` prefix for
    all document embeddings; omitting it measurably degrades retrieval quality.
    Both *title* and *body_or_snippet* are stripped independently before
    concatenation so leading/trailing whitespace never leaks into the input.
    Falls back to title-only when *body_or_snippet* is absent or blank.
    """
    t = (title or "").strip()
    b = (body_or_snippet or "").strip()
    core = f"{t} {b}".strip() if b else t
    return f"{_E5_PASSAGE_PREFIX}{core}"


def _sha1_files(*paths: Path) -> str:
    """Return the first 12 hex chars of SHA-1 over the concatenated bytes of *paths*.

    Feed paths in a fixed caller-chosen order so the hash is deterministic.
    Hashing multiple artifacts together means the version changes when *any*
    of them changes — not just the first one.
    """
    h = hashlib.sha1(usedforsecurity=False)  # noqa: S324
    for path in paths:
        h.update(path.read_bytes())
    return h.hexdigest()[:12]


def _embed_texts(
    model: Any,
    texts: list[str],
) -> Any:
    """Encode *texts* with *model*, returning L2-normalised float32 embeddings."""
    import numpy as np

    emb: Any = model.encode(
        texts,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return emb.astype(np.float32)


def _build_hnsw_index(embeddings: Any) -> Any:
    """Build a FAISS HNSW index from L2-normalised *embeddings*.

    Parameters
    ----------
    embeddings:
        Float32 numpy array of shape ``(n, dim)`` — must already be
        L2-normalised.  L2 distance on normalised vectors is equivalent to
        cosine distance, so the nearest neighbour by L2 is the nearest
        neighbour by cosine similarity.

    Notes
    -----
    ``efSearch`` is set to :data:`_HNSW_EF_SEARCH` (64), a fixed constant.
    Setting it to ``n`` (the corpus size) would force HNSW to explore every
    node at query time — O(n) cost — making it worse than brute-force search.
    """
    import faiss
    import numpy as np

    arr: Any = np.ascontiguousarray(embeddings, dtype=np.float32)
    _n, dim = arr.shape
    index = faiss.IndexHNSWFlat(dim, _HNSW_M)
    index.hnsw.efConstruction = _HNSW_EF_CONSTRUCTION
    index.add(arr)
    index.hnsw.efSearch = _HNSW_EF_SEARCH
    return index


def _compute_similarity(
    query: Any,
    centroid: Any,
    index: Any,
    n_neighbors: int,
) -> tuple[float, float]:
    """Return ``(centroid_cos, knn_mean_cos)`` for *query*.

    Parameters
    ----------
    query:
        L2-normalised float32 embedding of shape ``(dim,)``.
    centroid:
        L2-normalised centroid embedding of shape ``(dim,)``.
    index:
        Populated FAISS HNSW index (L2 metric, normalised vectors).
    n_neighbors:
        Number of neighbours to retrieve.  The *mean* cosine over all *k*
        retrieved neighbours is returned as ``knn_mean_cos``, so this parameter
        has a genuine effect on the signal: larger values average over a
        broader neighbourhood rather than just the single closest point.

    Returns
    -------
    tuple[float, float]
        ``(centroid_cos, knn_mean_cos)`` both clipped to ``[−1, 1]``.
        ``knn_mean_cos`` is ``−1.0`` when the index is empty.
    """
    import numpy as np

    centroid_cos = float(np.dot(query, centroid))
    centroid_cos = max(-1.0, min(1.0, centroid_cos))

    k = min(n_neighbors, index.ntotal)
    if k == 0:
        return centroid_cos, -1.0

    q2d: Any = query.reshape(1, -1).astype(np.float32)
    dists, _ = index.search(q2d, k)
    # FAISS returns squared L2 for normalised vectors: d² = 2(1 − cos)
    # → cos = 1 − d²/2.  Take the mean over all k retrieved neighbours so
    # that n_neighbors controls the signal rather than being a no-op.
    cosines: Any = np.clip(1.0 - dists[0] / 2.0, -1.0, 1.0)
    knn_mean_cos = float(cosines.mean())

    return centroid_cos, knn_mean_cos


def _fit_calibration(
    raw_scores: list[float],
    labels: list[int],
) -> tuple[float, float]:
    """Fit logistic-regression sigmoid calibration.

    Returns ``(coef, intercept)`` for ``p_positive = sigmoid(coef*s + b)``.
    Falls back to ``(1.0, 0.0)`` when calibration is not possible (empty
    data or single class in labels).
    """
    if not raw_scores or len(set(labels)) < 2:
        if raw_scores and len(set(labels)) < 2:
            warnings.warn(
                "Stage C calibration: validation split has fewer than 2 label "
                "classes; using uncalibrated sigmoid (coef=1.0, intercept=0.0). "
                "Add more labeled data and retrain to improve calibration.",
                UserWarning,
                stacklevel=3,
            )
        return 1.0, 0.0

    import numpy as np
    from sklearn.linear_model import LogisticRegression

    X: Any = np.array(raw_scores, dtype=np.float64).reshape(-1, 1)
    y: Any = np.array(labels, dtype=np.int32)
    lr = LogisticRegression(C=1.0, max_iter=1000, solver="lbfgs")
    lr.fit(X, y)
    return float(lr.coef_[0, 0]), float(lr.intercept_[0])


# ---------------------------------------------------------------------------
# StageCScorer
# ---------------------------------------------------------------------------


class StageCScorer:
    """Stage C cascade scorer: embedding centroid + kNN mean similarity.

    Returns ``None`` for the thin pass (this stage is thick-pass only), and
    when no trained artifacts exist or the ``prefilter`` extras are missing.

    Parameters
    ----------
    models_dir:
        Root models directory (``PrefilterStatePaths.models_dir``).
        Artifacts expected under ``models_dir/stage_c/``.  When ``None`` or
        artifacts are absent the scorer returns ``None`` for every call.
    threshold:
        Drop threshold.  Candidates whose ``p_negative >= threshold`` are
        tagged ``dropped=True``.
    n_neighbors:
        Number of HNSW neighbours used for the kNN-mean-cosine signal.
        When ``None`` the value from ``meta.json`` is used; falls back to
        :data:`_DEFAULT_N_NEIGHBORS`.
    """

    def __init__(
        self,
        *,
        models_dir: Path | None = None,
        threshold: float = 0.90,
        n_neighbors: int | None = None,
    ) -> None:
        self._threshold = threshold
        self._n_neighbors: int = n_neighbors if n_neighbors is not None else _DEFAULT_N_NEIGHBORS
        self._centroid: Any = None  # np.ndarray, L2-normalised, shape (dim,)
        self._index: Any = None  # faiss.IndexHNSWFlat
        self._calib_coef: float = 1.0
        self._calib_intercept: float = 0.0
        self._embed_model: Any = None  # sentence_transformers.SentenceTransformer
        self.model_version: str = ""
        self.base_model_id: str = ""

        if models_dir is None:
            return

        stage_dir = models_dir / _STAGE_C_SUBDIR
        centroid_path = stage_dir / _CENTROID_FILE
        index_path = stage_dir / _INDEX_FILE
        calib_path = stage_dir / _CALIBRATION_FILE
        meta_path = stage_dir / _META_FILE

        if not (centroid_path.exists() and index_path.exists() and calib_path.exists()):
            return

        try:
            import faiss
            import numpy as np
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            logger.warning(
                "Stage C: required package missing (%s); install the prefilter extras "
                "(pip install -e '.[dev,prefilter]'). "
                "Scorer will return None for all candidates.",
                exc,
            )
            return

        try:
            self._centroid = np.load(str(centroid_path))
            self._index = faiss.read_index(str(index_path))

            calib_raw = json.loads(calib_path.read_text(encoding="utf-8"))
            self._calib_coef = float(calib_raw["coef"])
            self._calib_intercept = float(calib_raw["intercept"])

            if meta_path.exists():
                meta_raw = json.loads(meta_path.read_text(encoding="utf-8"))
                self.model_version = str(meta_raw.get("model_version", ""))
                self.base_model_id = str(meta_raw.get("base_model_id", _DEFAULT_BASE_MODEL))
                if n_neighbors is None:
                    self._n_neighbors = int(meta_raw.get("n_neighbors", _DEFAULT_N_NEIGHBORS))
            else:
                self.base_model_id = _DEFAULT_BASE_MODEL
                logger.warning(
                    "Stage C: meta.json missing from %s; model_version will be empty "
                    "in telemetry.  Re-run `denbust prefilter retrain --stage c` to fix.",
                    stage_dir,
                )

            model_id = self.base_model_id or _DEFAULT_BASE_MODEL
            self._embed_model = SentenceTransformer(model_id)

        except Exception as exc:  # noqa: BLE001
            # Artifacts are present but corrupt or incompatible — log at ERROR
            # so operators can distinguish this from a "not yet trained" state.
            logger.error(
                "Stage C: artifacts at %s are present but failed to load: %s — "
                "run `denbust prefilter retrain --stage c` to rebuild.",
                stage_dir,
                exc,
            )
            self._centroid = None
            self._index = None
            self._embed_model = None

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        candidate: CandidateView,
        pass_kind: PassKind,
        body: str | None = None,
    ) -> StageScore | None:
        """Evaluate *candidate* and return a :class:`StageScore`, or ``None``.

        Always returns ``None`` for the thin pass (Stage C is thick-pass only).
        Returns ``None`` when no trained artifacts are loaded.

        Parameters
        ----------
        candidate:
            Candidate to evaluate.
        pass_kind:
            ``"thin"`` → returns ``None`` unconditionally.
            ``"thick"`` → scores the candidate using body+title (or
            title+snippet when *body* is absent).
        body:
            Full article body text; only meaningful for the thick pass.
        """
        if pass_kind == "thin":
            return None  # Stage C is thick-pass only

        if self._centroid is None or self._index is None or self._embed_model is None:
            return None

        import numpy as np

        text = _build_text(candidate.title, body or candidate.snippet)
        emb_batch: Any = _embed_texts(self._embed_model, [text])
        query: Any = emb_batch[0]  # shape (dim,)

        centroid_cos, knn_mean_cos = _compute_similarity(
            query, self._centroid, self._index, self._n_neighbors
        )
        raw_score = max(centroid_cos, knn_mean_cos)

        p_positive = _sigmoid(self._calib_coef * raw_score + self._calib_intercept)
        p_negative = float(np.clip(1.0 - p_positive, 0.0, 1.0))

        dropped = p_negative >= self._threshold
        reason = f"embed/thick=centroid:{centroid_cos:.3f},knn_mean:{knn_mean_cos:.3f}"
        return StageScore(
            stage="C",
            p_negative=p_negative,
            threshold=self._threshold,
            dropped=dropped,
            reason=reason,
            model_version=self.model_version,
        )


# ---------------------------------------------------------------------------
# Training entry point
# ---------------------------------------------------------------------------


def train_stage_c(
    labels_path: Path,
    out_dir: Path,
    *,
    base_model_id: str = _DEFAULT_BASE_MODEL,
    n_neighbors: int = _DEFAULT_N_NEIGHBORS,
) -> tuple[StageCModelMeta, Path]:
    """Train Stage C embedding-similarity artifacts and write them atomically.

    Reads *labels_path*, extracts training positives and validation rows, then:

    1. Embeds training positives with :class:`sentence_transformers.SentenceTransformer`.
    2. Computes the L2-normalised centroid of positive embeddings.
    3. Builds a FAISS HNSW index over the positive embeddings.
    4. Scores every validation row (centroid-cosine + kNN-mean-cosine).
    5. Fits a sigmoid calibration (logistic regression) on the validation scores.
    6. Writes all artifacts atomically under ``out_dir/stage_c/``.

    Artifacts written
    -----------------
    ``centroid.npy``
        L2-normalised mean embedding of training positives.
    ``index.faiss``
        FAISS HNSW index of training positive embeddings.
    ``calibration.json``
        Sigmoid calibration parameters ``{"coef": float, "intercept": float}``.
    ``meta.json``
        :class:`StageCModelMeta` provenance record.

    Parameters
    ----------
    labels_path:
        Path to a ``labels.parquet`` from :mod:`denbust.prefilter.labels`.
    out_dir:
        Parent directory for ``stage_c/`` artifacts.
    base_model_id:
        HuggingFace model ID for the sentence encoder.
    n_neighbors:
        Number of HNSW neighbours for the kNN-mean-cosine signal.

    Returns
    -------
    tuple[StageCModelMeta, Path]
        ``(meta, stage_dir)`` — provenance metadata and the written artifact
        directory.

    Raises
    ------
    ImportError
        When ``sentence_transformers``, ``faiss``, or ``scikit-learn`` are not
        installed.
    ValueError
        When the training split contains no positive examples.
    """
    # Single import block: guard + acquire the names used throughout.
    try:
        import faiss
        import numpy as np
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise ImportError(
            "Stage C training requires the prefilter extras. "
            "Install with: pip install -e '.[dev,prefilter]'"
        ) from exc

    from denbust.prefilter.labels import read_labels_parquet

    all_rows = read_labels_parquet(labels_path)
    train_rows = [r for r in all_rows if r.split == "train"]
    val_rows = [r for r in all_rows if r.split == "val"]

    train_positives = [r for r in train_rows if r.label == "positive"]
    if not train_positives:
        raise ValueError(
            f"No positive training examples found in {labels_path}. "
            "Stage C requires at least one labeled positive to build the "
            "centroid and FAISS index."
        )

    # Build E5-format passage texts via _build_text — the same function used
    # at inference time, so training and serving share identical text assembly.
    pos_texts = [_build_text(r.title, r.article_body or r.snippet) for r in train_positives]
    model = SentenceTransformer(base_model_id)
    pos_embeddings = _embed_texts(model, pos_texts)  # shape (n_pos, dim)

    # Centroid: mean of L2-normalised embeddings, then re-normalise.
    centroid_raw: Any = np.mean(pos_embeddings, axis=0)
    centroid = _l2_normalize(centroid_raw).astype(np.float32)

    # FAISS HNSW index over positive embeddings.
    index = _build_hnsw_index(pos_embeddings)

    # Score validation rows to fit the sigmoid calibration.
    raw_scores: list[float] = []
    val_labels: list[int] = []

    if val_rows:
        val_texts = [_build_text(r.title, r.article_body or r.snippet) for r in val_rows]
        val_embeddings = _embed_texts(model, val_texts)

        for i, row in enumerate(val_rows):
            query = val_embeddings[i]
            c_cos, k_cos = _compute_similarity(query, centroid, index, n_neighbors)
            raw_scores.append(max(c_cos, k_cos))
            val_labels.append(1 if row.label == "positive" else 0)

    calib_coef, calib_intercept = _fit_calibration(raw_scores, val_labels)

    # Atomic write -----------------------------------------------------------
    out_dir.mkdir(parents=True, exist_ok=True)
    final_dir = out_dir / _STAGE_C_SUBDIR

    old_dir: Path | None = None
    tmp_dir = Path(tempfile.mkdtemp(dir=out_dir, prefix=f"{_STAGE_C_SUBDIR}.tmp."))
    try:
        centroid_path = tmp_dir / _CENTROID_FILE
        index_path = tmp_dir / _INDEX_FILE
        calib_path = tmp_dir / _CALIBRATION_FILE
        meta_path = tmp_dir / _META_FILE

        np.save(str(centroid_path), centroid)
        faiss.write_index(index, str(index_path))

        calib_path.write_text(
            json.dumps(
                {"coef": calib_coef, "intercept": calib_intercept},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        # Hash all three artifacts so model_version changes when any of them
        # does — not just the centroid.
        model_version = _sha1_files(centroid_path, index_path, calib_path)

        meta = StageCModelMeta(
            model_version=model_version,
            trained_at=datetime.now(UTC).isoformat(),
            n_train_positives=len(train_positives),
            n_val=len(val_rows),
            base_model_id=base_model_id,
            n_neighbors=n_neighbors,
        )
        meta_path.write_text(
            json.dumps(dataclasses.asdict(meta), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Replace old artifact dir using rename-aside so readers never see a
        # missing directory.  Both rename() calls are atomic at the filesystem
        # level; the old content is accessible in old_dir until cleanup below.
        if final_dir.exists():
            old_dir = out_dir / f"{_STAGE_C_SUBDIR}.old.{os.getpid()}"
            final_dir.rename(old_dir)
        tmp_dir.rename(final_dir)

    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        # Restore the displaced old artifacts so the scorer stays usable.
        if old_dir is not None and not final_dir.exists():
            try:
                old_dir.rename(final_dir)
                old_dir = None
            except OSError:
                pass
        raise

    # Clean up the displaced old artifacts on success.
    if old_dir is not None:
        shutil.rmtree(old_dir, ignore_errors=True)

    return meta, final_dir
