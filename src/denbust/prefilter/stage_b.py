"""Stage B — trained text classifier (Naive Bayes default).

Implements Stage B as a calibrated Complement Naive Bayes on character
n-grams (3–5).  Two separate model artifacts are trained and persisted:

thin_model.joblib
    Applied to ``title + " " + snippet`` in the thin (pre-scrape) pass.
thick_model.joblib
    Applied to ``article_body`` text in the thick (post-scrape) pass,
    falling back to title+snippet when body is absent or empty.
meta.json
    :class:`StageBModelMeta` — artifact provenance metadata.

Both models use the labeled-candidates parquet produced by LPF-PR-02.
The thin model is trained on title+snippet.  The thick model is trained on
``article_body`` when available and on title+snippet otherwise — so the
thick model improves automatically as more scraped labels accumulate.

When no trained artifacts exist the scorer returns ``None``, preserving the
stub behaviour so the cascade continues to Stage C unimpeded.

Full SetFit alternative lands in LPF-PR-05.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from denbust.prefilter.models import CandidateView, PassKind, StageScore

# ---------------------------------------------------------------------------
# Artifact path constants
# ---------------------------------------------------------------------------

_STAGE_B_SUBDIR = "stage_b"
_THIN_MODEL_FILE = "thin_model.joblib"
_THICK_MODEL_FILE = "thick_model.joblib"
_META_FILE = "meta.json"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class StageBModelMeta:
    """Provenance metadata for trained Stage B model artifacts.

    Attributes
    ----------
    model_kind:
        Always ``"naive_bayes"`` in LPF-PR-04.  SetFit support: LPF-PR-05.
    model_version:
        Short (12-char) SHA-1 of the thin model artifact file.
    trained_at:
        ISO-8601 UTC timestamp of when the artifacts were written.
    n_train:
        Number of training examples used to fit the models.
    n_val:
        Number of validation examples available at training time.
    """

    model_kind: Literal["naive_bayes"]
    model_version: str
    trained_at: str
    n_train: int
    n_val: int


# ---------------------------------------------------------------------------
# StageBScorer
# ---------------------------------------------------------------------------


class StageBScorer:
    """Stage B cascade scorer: calibrated Complement Naive Bayes.

    Returns ``None`` when no trained artifacts exist, preserving stub
    behaviour so the cascade continues to Stage C unimpeded.

    Parameters
    ----------
    models_dir:
        Root models directory (``PrefilterStatePaths.models_dir``).
        Artifacts expected under ``models_dir/stage_b/``.  When ``None`` or
        the artifact files are absent, returns ``None`` for every candidate.
    threshold:
        Drop threshold.  Candidates whose ``p_negative >= threshold`` are
        tagged ``dropped=True``.
    """

    def __init__(
        self,
        *,
        models_dir: Path | None = None,
        threshold: float = 0.95,
    ) -> None:
        self._threshold = threshold
        self._thin_model: Any = None
        self._thick_model: Any = None
        self._model_version: str = ""

        if models_dir is None:
            return

        stage_dir = models_dir / _STAGE_B_SUBDIR
        thin_path = stage_dir / _THIN_MODEL_FILE
        thick_path = stage_dir / _THICK_MODEL_FILE
        meta_path = stage_dir / _META_FILE

        if not (thin_path.exists() and thick_path.exists()):
            return

        import joblib  # lazy — scikit-learn/joblib are optional at import time

        _load: Any = joblib.load
        self._thin_model = _load(thin_path)
        self._thick_model = _load(thick_path)
        if meta_path.exists():
            meta_raw = json.loads(meta_path.read_text(encoding="utf-8"))
            self._model_version = str(meta_raw.get("model_version", ""))

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

        Returns ``None`` when no trained model is loaded so the cascade
        passes Stage B without a score.

        Parameters
        ----------
        candidate:
            Candidate to evaluate.
        pass_kind:
            ``"thin"`` → thin model applied to title+snippet.
            ``"thick"`` → thick model applied to *body*; falls back to thin
            text when *body* is absent or empty.
        body:
            Full article body text; only meaningful for the thick pass.
        """
        if self._thin_model is None:
            return None

        title = candidate.title or ""
        snippet = candidate.snippet or ""
        thin_text = (title + " " + snippet).strip()

        if pass_kind == "thick" and body and body.strip():
            text = body.strip()
            model = self._thick_model
        else:
            text = thin_text
            model = self._thin_model

        # CalibratedClassifierCV.predict_proba returns [[p(class_0), p(class_1)]].
        # Classes are sorted by integer label: y=0 → "negative", y=1 → "positive".
        # Therefore proba[0][0] == p("negative") == p_negative.
        proba: Any = model.predict_proba([text])
        p_negative = float(proba[0][0])

        dropped = p_negative >= self._threshold
        return StageScore(
            stage="B",
            p_negative=p_negative,
            threshold=self._threshold,
            dropped=dropped,
            reason=f"nb={p_negative:.3f}",
            model_version=self._model_version,
        )


# ---------------------------------------------------------------------------
# Artifact builder (used by CLI retrain command)
# ---------------------------------------------------------------------------


def train_naive_bayes(
    labels_path: Path,
    out_dir: Path,
    *,
    seed: int = 20260521,
) -> StageBModelMeta:
    """Train calibrated ComplementNB classifiers and write Stage B artifacts.

    Reads *labels_path*, restricts to the ``"train"`` split, then trains:

    - **thin model** on ``title + " " + snippet`` (all train rows)
    - **thick model** on ``article_body`` when present, else title+snippet

    Artifacts written to ``out_dir/stage_b/``:

    - ``thin_model.joblib``
    - ``thick_model.joblib``
    - ``meta.json``

    Parameters
    ----------
    labels_path:
        Path to a ``labels.parquet`` from :mod:`denbust.prefilter.labels`.
    out_dir:
        Parent directory for ``stage_b/`` artifacts.
    seed:
        Random seed for :class:`~sklearn.calibration.CalibratedClassifierCV`
        to make cross-validation fold assignment reproducible.

    Returns
    -------
    StageBModelMeta
        Provenance metadata for the written artifacts.

    Raises
    ------
    ValueError
        When the training split is empty or contains only one label class.
    ImportError
        When ``scikit-learn`` or ``joblib`` are not installed.
    """
    import joblib
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.model_selection import StratifiedKFold
    from sklearn.naive_bayes import ComplementNB
    from sklearn.pipeline import Pipeline

    from denbust.prefilter.labels import read_labels_parquet

    rows = read_labels_parquet(labels_path)
    train_rows = [r for r in rows if r.split == "train"]
    val_rows = [r for r in rows if r.split == "val"]

    if not train_rows:
        raise ValueError(f"No training rows found in {labels_path}")

    classes = {r.label for r in train_rows}
    if len(classes) < 2:
        raise ValueError(
            f"Training data contains only one label class ({classes}); "
            "both 'positive' and 'negative' labels are required."
        )

    # Label mapping: y=0 → "negative", y=1 → "positive".
    # sklearn sorts unique integer labels, so predict_proba columns are ordered
    # [p(class=0), p(class=1)] = [p_negative, p_positive].
    label_to_int = {"negative": 0, "positive": 1}

    thin_texts = [(r.title + " " + r.snippet).strip() for r in train_rows]
    # Thick model: use article_body when available, fall back to title+snippet.
    thick_texts = [(r.article_body or (r.title + " " + r.snippet)).strip() for r in train_rows]
    y_train = [label_to_int[r.label] for r in train_rows]

    def _build_pipeline() -> Any:
        vec = TfidfVectorizer(
            analyzer="char_wb",
            ngram_range=(3, 5),
            min_df=2,
            sublinear_tf=True,
        )
        base: Any = Pipeline([("vec", vec), ("clf", ComplementNB())])
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)
        return CalibratedClassifierCV(base, method="sigmoid", cv=cv)

    thin_model = _build_pipeline()
    thin_model.fit(thin_texts, y_train)

    thick_model = _build_pipeline()
    thick_model.fit(thick_texts, y_train)

    stage_dir = out_dir / _STAGE_B_SUBDIR
    stage_dir.mkdir(parents=True, exist_ok=True)

    thin_path = stage_dir / _THIN_MODEL_FILE
    thick_path = stage_dir / _THICK_MODEL_FILE
    meta_path = stage_dir / _META_FILE

    _dump: Any = joblib.dump
    _dump(thin_model, thin_path)
    _dump(thick_model, thick_path)

    model_version = _sha1_file(thin_path)[:12]

    meta = StageBModelMeta(
        model_kind="naive_bayes",
        model_version=model_version,
        trained_at=datetime.now(UTC).isoformat(),
        n_train=len(train_rows),
        n_val=len(val_rows),
    )
    meta_path.write_text(
        json.dumps(dataclasses.asdict(meta), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return meta


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _sha1_file(path: Path) -> str:
    """Return the full SHA-1 hex digest of *path*'s content."""
    return hashlib.sha1(path.read_bytes()).hexdigest()  # noqa: S324
