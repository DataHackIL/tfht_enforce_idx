"""Stage B — trained text classifier (Naive Bayes default, SetFit alternative).

Two scorer implementations are provided:

:class:`StageBScorer`
    Calibrated Complement Naive Bayes on character n-grams (3–5).
    Artifacts: ``models_dir/stage_b/{thin_model,thick_model}.joblib`` +
    ``meta.json``.  No heavy dependencies beyond scikit-learn/joblib.

:class:`StageBSetFitScorer`
    SetFit on ``intfloat/multilingual-e5-large``.
    Artifacts: ``models_dir/stage_b_setfit/{thin_model/,thick_model/}`` +
    ``meta.json``.  Requires the ``prefilter`` extras group
    (``pip install -e '.[dev,prefilter]'``).

Both implementations:

- Return ``None`` when no trained artifacts exist so the cascade continues
  to Stage C unimpeded (stub behaviour).
- Use separate thin (title + snippet) and thick (article body, with
  title+snippet fallback) model artifacts.
- Produce :class:`~denbust.prefilter.models.StageScore` with a
  ``reason`` tag that names the implementation and pass kind, e.g.
  ``"nb/thin=0.123"`` or ``"setfit/thick=0.456"``.

Training entry points
---------------------
:func:`train_naive_bayes`
    Trains both NB artifacts from a ``labels.parquet``; returns
    ``(StageBModelMeta, stage_dir)``.
:func:`train_setfit`
    Trains both SetFit artifacts; same return type.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import shutil
import tempfile
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from denbust.prefilter.models import CandidateView, PassKind, StageScore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Artifact path constants
# ---------------------------------------------------------------------------

_STAGE_B_SUBDIR = "stage_b"
_THIN_MODEL_FILE = "thin_model.joblib"
_THICK_MODEL_FILE = "thick_model.joblib"

_STAGE_B_SETFIT_SUBDIR = "stage_b_setfit"
_THIN_MODEL_DIRNAME = "thin_model"
_THICK_MODEL_DIRNAME = "thick_model"

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
        ``"naive_bayes"`` or ``"setfit"``.
    model_version:
        Short (12-char) SHA-1 derived from the primary trained artifact.
    trained_at:
        ISO-8601 UTC timestamp of when the artifacts were written.
    n_train:
        Number of training examples used to fit the models.
    n_val:
        Number of validation examples available at training time.
    n_thick_with_body:
        Train rows where the thick model used a real ``article_body`` rather
        than falling back to title+snippet.  Zero means thick == thin model
        at training time; increases as more scraped labels accumulate.
    """

    model_kind: Literal["naive_bayes", "setfit"]
    model_version: str
    trained_at: str
    n_train: int
    n_val: int
    n_thick_with_body: int


# ---------------------------------------------------------------------------
# NB pipeline builder (module-level so it can be imported and tested directly)
# ---------------------------------------------------------------------------


def _build_nb_pipeline(seed: int) -> Any:
    """Build a fresh calibrated ComplementNB pipeline.

    Uses lazy sklearn imports so the module stays importable without
    scikit-learn installed (the scorer falls back to stub mode in that case).

    Parameters
    ----------
    seed:
        Random state for :class:`~sklearn.model_selection.StratifiedKFold`
        inside :class:`~sklearn.calibration.CalibratedClassifierCV` to make
        cross-validation fold assignment reproducible.
    """
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.model_selection import StratifiedKFold
    from sklearn.naive_bayes import ComplementNB
    from sklearn.pipeline import Pipeline

    vec = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=2,
        sublinear_tf=True,
    )
    base: Any = Pipeline([("vec", vec), ("clf", ComplementNB())])
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)
    return CalibratedClassifierCV(base, method="sigmoid", cv=cv)


# ---------------------------------------------------------------------------
# StageBScorer — Naive Bayes
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
        self.model_version: str = ""

        if models_dir is None:
            return

        stage_dir = models_dir / _STAGE_B_SUBDIR
        thin_path = stage_dir / _THIN_MODEL_FILE
        thick_path = stage_dir / _THICK_MODEL_FILE
        meta_path = stage_dir / _META_FILE

        if not (thin_path.exists() and thick_path.exists()):
            return

        import joblib  # lazy — scikit-learn/joblib are optional at import time

        self._thin_model = joblib.load(thin_path)
        self._thick_model = joblib.load(thick_path)
        if meta_path.exists():
            meta_raw = json.loads(meta_path.read_text(encoding="utf-8"))
            self.model_version = str(meta_raw.get("model_version", ""))
        else:
            logger.warning(
                "Stage B NB: meta.json missing from %s; model_version will be empty "
                "in telemetry.  Re-run `denbust prefilter retrain --stage b` to fix.",
                stage_dir,
            )

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
            reason=f"nb/{pass_kind}={p_negative:.3f}",
            model_version=self.model_version,
        )


# ---------------------------------------------------------------------------
# StageBSetFitScorer — SetFit with multilingual-e5-large
# ---------------------------------------------------------------------------


class StageBSetFitScorer:
    """Stage B cascade scorer: SetFit on ``intfloat/multilingual-e5-large``.

    Returns ``None`` when no trained artifacts exist or when the ``setfit``
    package is not installed, preserving stub behaviour.

    Requires the ``prefilter`` extras group::

        pip install -e '.[dev,prefilter]'

    Parameters
    ----------
    models_dir:
        Root models directory.  Artifacts expected under
        ``models_dir/stage_b_setfit/{thin_model/,thick_model/}``.
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
        self.model_version: str = ""

        if models_dir is None:
            return

        stage_dir = models_dir / _STAGE_B_SETFIT_SUBDIR
        thin_path = stage_dir / _THIN_MODEL_DIRNAME
        thick_path = stage_dir / _THICK_MODEL_DIRNAME
        meta_path = stage_dir / _META_FILE

        if not (thin_path.is_dir() and thick_path.is_dir()):
            return

        try:
            from setfit import SetFitModel  # lazy — setfit is optional
        except ImportError:
            logger.warning(
                "Stage B SetFit: 'setfit' package is not installed; scorer returns None "
                "for all candidates.  Install with: pip install -e '.[dev,prefilter]'"
            )
            return

        self._thin_model = SetFitModel.from_pretrained(str(thin_path))
        self._thick_model = SetFitModel.from_pretrained(str(thick_path))
        if meta_path.exists():
            meta_raw = json.loads(meta_path.read_text(encoding="utf-8"))
            self.model_version = str(meta_raw.get("model_version", ""))
        else:
            logger.warning(
                "Stage B SetFit: meta.json missing from %s; model_version will be empty.",
                stage_dir,
            )

    def evaluate(
        self,
        candidate: CandidateView,
        pass_kind: PassKind,
        body: str | None = None,
    ) -> StageScore | None:
        """Evaluate *candidate* and return a :class:`StageScore`, or ``None``.

        Returns ``None`` when no model is loaded.

        Parameters
        ----------
        candidate:
            Candidate to evaluate.
        pass_kind:
            ``"thin"`` → thin model on title+snippet.
            ``"thick"`` → thick model on *body*; falls back to thin text.
        body:
            Full article body; only meaningful for the thick pass.
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

        # SetFitModel.predict_proba returns an ndarray of shape (n_texts, n_classes).
        # Classes follow the integer label order: 0 → "negative", 1 → "positive".
        # proba[0][0] = p(label=0) = p_negative.
        proba: Any = model.predict_proba([text])
        p_negative = float(proba[0][0])

        dropped = p_negative >= self._threshold
        return StageScore(
            stage="B",
            p_negative=p_negative,
            threshold=self._threshold,
            dropped=dropped,
            reason=f"setfit/{pass_kind}={p_negative:.3f}",
            model_version=self.model_version,
        )


# ---------------------------------------------------------------------------
# Artifact builder — Naive Bayes
# ---------------------------------------------------------------------------


def train_naive_bayes(
    labels_path: Path,
    out_dir: Path,
    *,
    seed: int = 20260521,
) -> tuple[StageBModelMeta, Path]:
    """Train calibrated ComplementNB classifiers and write Stage B artifacts.

    Reads *labels_path*, restricts to the ``"train"`` split, then trains:

    - **thin model** on ``title + " " + snippet`` (all train rows)
    - **thick model** on ``article_body`` when present, else title+snippet

    Artifacts written atomically to ``out_dir/stage_b/``:

    - ``thin_model.joblib``
    - ``thick_model.joblib``
    - ``meta.json``

    All three files are first written to a temporary sibling directory and
    then renamed into place as a unit, so a crash mid-write never leaves the
    ``stage_b/`` directory in a partially-written state.

    Parameters
    ----------
    labels_path:
        Path to a ``labels.parquet`` from :mod:`denbust.prefilter.labels`.
    out_dir:
        Parent directory for ``stage_b/`` artifacts.
    seed:
        Random seed passed to :func:`_build_nb_pipeline` for reproducible
        cross-validation fold assignment.

    Returns
    -------
    tuple[StageBModelMeta, Path]
        ``(meta, stage_dir)`` — provenance metadata and the path of the
        written artifact directory.

    Raises
    ------
    ValueError
        When the training split is empty or contains only one label class.
    ImportError
        When ``scikit-learn`` or ``joblib`` are not installed.
    """
    import joblib

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
    # Track how many rows actually have a real body — zero means thick == thin.
    n_thick_with_body = sum(1 for r in train_rows if r.article_body is not None)
    if n_thick_with_body == 0:
        warnings.warn(
            f"All {len(train_rows)} train rows have article_body=None; "
            "the thick model is identical to the thin model at this training. "
            "Scrape articles and retrain to improve thick-pass accuracy.",
            UserWarning,
            stacklevel=2,
        )
    thick_texts = [(r.article_body or (r.title + " " + r.snippet)).strip() for r in train_rows]
    y_train = [label_to_int[r.label] for r in train_rows]

    thin_model = _build_nb_pipeline(seed)
    thin_model.fit(thin_texts, y_train)

    thick_model = _build_nb_pipeline(seed)
    thick_model.fit(thick_texts, y_train)

    # Atomic write: write all artifacts to a temp sibling directory, then
    # rename it into place.  This ensures the stage_b/ directory is either
    # fully written or the old version survives intact.
    out_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp(dir=out_dir, prefix=f"{_STAGE_B_SUBDIR}.tmp."))
    try:
        thin_path = tmp_dir / _THIN_MODEL_FILE
        thick_path = tmp_dir / _THICK_MODEL_FILE
        meta_path = tmp_dir / _META_FILE

        joblib.dump(thin_model, thin_path)
        joblib.dump(thick_model, thick_path)

        model_version = _sha1_file(thin_path)[:12]

        meta = StageBModelMeta(
            model_kind="naive_bayes",
            model_version=model_version,
            trained_at=datetime.now(UTC).isoformat(),
            n_train=len(train_rows),
            n_val=len(val_rows),
            n_thick_with_body=n_thick_with_body,
        )
        # Serialize only the JSON-safe fields — exclude any future Path-typed fields.
        meta_dict = {
            "model_kind": meta.model_kind,
            "model_version": meta.model_version,
            "trained_at": meta.trained_at,
            "n_train": meta.n_train,
            "n_val": meta.n_val,
            "n_thick_with_body": meta.n_thick_with_body,
        }
        meta_path.write_text(
            json.dumps(meta_dict, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Atomic replace: remove old stage_b/ (if any) then rename tmp into place.
        stage_dir = out_dir / _STAGE_B_SUBDIR
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        tmp_dir.rename(stage_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    return meta, stage_dir


# ---------------------------------------------------------------------------
# Artifact builder — SetFit
# ---------------------------------------------------------------------------


def train_setfit(
    labels_path: Path,
    out_dir: Path,
    *,
    base_model_id: str = "intfloat/multilingual-e5-large",
    seed: int = 20260521,
) -> tuple[StageBModelMeta, Path]:
    """Train SetFit classifiers and write Stage B SetFit artifacts.

    Reads *labels_path*, restricts to the ``"train"`` split, then trains:

    - **thin model** on ``title + " " + snippet`` (all train rows)
    - **thick model** on ``article_body`` when present, else title+snippet

    Training configuration (matches LPF-PR-05 spec)::

        num_iterations=20, batch_size=16,
        body_learning_rate=2e-5, head_learning_rate=1e-2,
        num_epochs=1, seed=<seed>

    Artifacts written atomically to ``out_dir/stage_b_setfit/``:

    - ``thin_model/``   — SetFit model directory (sentence encoder + head)
    - ``thick_model/``  — SetFit model directory
    - ``meta.json``     — :class:`StageBModelMeta` provenance

    Parameters
    ----------
    labels_path:
        Path to a ``labels.parquet`` from :mod:`denbust.prefilter.labels`.
    out_dir:
        Parent directory for ``stage_b_setfit/`` artifacts.
    base_model_id:
        HuggingFace model identifier for the sentence-encoder backbone.
        Default: ``"intfloat/multilingual-e5-large"``.
    seed:
        Random seed for training reproducibility.

    Returns
    -------
    tuple[StageBModelMeta, Path]
        ``(meta, stage_dir)``

    Raises
    ------
    ImportError
        When ``setfit``, ``sentence-transformers``, or ``torch`` are not installed.
        Install with ``pip install -e '.[dev,prefilter]'``.
    ValueError
        When the training split is empty or contains only one label class.
    """
    try:
        import datasets as hf_datasets
        from setfit import SetFitModel, SetFitTrainer
        from setfit import TrainingArguments as SetFitTrainingArgs
    except ImportError as exc:
        raise ImportError(
            "SetFit training requires the 'prefilter' extras:\n  pip install -e '.[dev,prefilter]'"
        ) from exc

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

    label_to_int = {"negative": 0, "positive": 1}
    y_train = [label_to_int[r.label] for r in train_rows]

    n_thick_with_body = sum(1 for r in train_rows if r.article_body is not None)
    if n_thick_with_body == 0:
        warnings.warn(
            f"All {len(train_rows)} train rows have article_body=None; "
            "the SetFit thick model is identical to the thin model at this training.",
            UserWarning,
            stacklevel=2,
        )

    thin_texts = [(r.title + " " + r.snippet).strip() for r in train_rows]
    thick_texts = [(r.article_body or (r.title + " " + r.snippet)).strip() for r in train_rows]

    train_args = SetFitTrainingArgs(
        num_iterations=20,
        batch_size=16,
        body_learning_rate=2e-5,
        head_learning_rate=1e-2,
        num_epochs=1,
        seed=seed,
    )

    def _train_one(texts: list[str], labels: list[int]) -> Any:
        dataset = hf_datasets.Dataset.from_dict({"text": texts, "label": labels})
        model = SetFitModel.from_pretrained(base_model_id, labels=[0, 1])
        trainer = SetFitTrainer(model=model, train_dataset=dataset, args=train_args)
        trainer.train()
        return model

    thin_model = _train_one(thin_texts, y_train)
    thick_model = _train_one(thick_texts, y_train)

    # Atomic write
    out_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp(dir=out_dir, prefix=f"{_STAGE_B_SETFIT_SUBDIR}.tmp."))
    try:
        thin_path = tmp_dir / _THIN_MODEL_DIRNAME
        thick_path = tmp_dir / _THICK_MODEL_DIRNAME
        meta_path = tmp_dir / _META_FILE

        thin_model.save_pretrained(str(thin_path))
        thick_model.save_pretrained(str(thick_path))

        model_version = _sha1_setfit_head(thin_path)[:12]

        meta = StageBModelMeta(
            model_kind="setfit",
            model_version=model_version,
            trained_at=datetime.now(UTC).isoformat(),
            n_train=len(train_rows),
            n_val=len(val_rows),
            n_thick_with_body=n_thick_with_body,
        )
        meta_dict = {
            "model_kind": meta.model_kind,
            "model_version": meta.model_version,
            "trained_at": meta.trained_at,
            "n_train": meta.n_train,
            "n_val": meta.n_val,
            "n_thick_with_body": meta.n_thick_with_body,
            "base_model_id": base_model_id,
        }
        meta_path.write_text(
            json.dumps(meta_dict, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        stage_dir = out_dir / _STAGE_B_SETFIT_SUBDIR
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        tmp_dir.rename(stage_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    return meta, stage_dir


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _sha1_file(path: Path) -> str:
    """Return the full SHA-1 hex digest of *path*'s content."""
    return hashlib.sha1(path.read_bytes()).hexdigest()  # noqa: S324


def _sha1_setfit_head(model_dir: Path) -> str:
    """Return a SHA-1 derived from the SetFit classification head artifact(s).

    Hashes ``model_head.pkl`` and ``config_setfit.json`` when present
    (small files that capture what training changed), falling back to a
    recursive hash of all files in the directory.
    """
    h = hashlib.sha1()  # noqa: S324
    candidates = (
        list(model_dir.rglob("model_head.pkl"))
        + list(model_dir.rglob("config_setfit.json"))
        + list(model_dir.rglob("config.json"))
    )
    found_any = False
    for fpath in sorted(candidates):
        if fpath.is_file():
            h.update(fpath.read_bytes())
            found_any = True
    if not found_any:
        for fpath in sorted(model_dir.rglob("*")):
            if fpath.is_file():
                h.update(fpath.read_bytes())
    return h.hexdigest()
