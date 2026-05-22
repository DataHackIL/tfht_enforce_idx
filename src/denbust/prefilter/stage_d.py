"""Stage D — local SLM judge via MLX (Apple Silicon only).

:class:`StageDScorer`
    A thick-pass-only scorer that runs a local small language model (SLM)
    through the MLX inference backend.  For each candidate it formats a
    Hebrew prompt and reads ``p("כן")`` vs ``p("לא")`` at the final token
    position to derive ``p_negative``.

    **Thick-pass only**: :meth:`~StageDScorer.evaluate` returns ``None``
    when *pass_kind* is ``"thin"`` — Stage D is too slow for the pre-scrape
    cascade pass.

    **Graceful degradation**: when the ``mlx_lm`` package is not installed,
    the SLM cannot be loaded, the inference times out, or the circuit breaker
    is open, the scorer returns ``None`` so the cascade continues.

Baking entry point
------------------
:func:`bake_stage_d`
    Writes the prompt template and provenance metadata to ``stage_d/`` under
    the given output directory.  No ML training happens here — the SLM is
    loaded from HuggingFace at runtime by :class:`StageDScorer`.

Prompt template
---------------
The default prompt is written at bake time.  It must contain ``{title}`` and
``{body}`` placeholders which are substituted at inference time.  The prompt
is Hebrew and designed for :data:`_DEFAULT_BASE_MODEL_D`
(``dicta-il/dictalm2.0-instruct``); the fallback is
:data:`_FALLBACK_BASE_MODEL_D` (``Qwen/Qwen2.5-7B-Instruct``).

Timeout and circuit breaker
---------------------------
Each inference call is wrapped in a :class:`concurrent.futures.ThreadPoolExecutor`
with a per-candidate timeout of :data:`_DEFAULT_TIMEOUT_SECONDS` seconds.  If
inference times out, ``p_negative`` for that candidate is ``None`` (pass-through).
After :data:`_DEFAULT_CB_THRESHOLD` consecutive timeouts the circuit breaker
trips: the scorer skips inference entirely for all subsequent candidates and
returns ``None`` until the object is recreated.

Scoring
-------
``_mlx_score`` reads the log-probabilities for the first token of ``"כן"``
(yes) and ``"לא"`` (no) at the last input position.  Softmax-normalising
these two logits gives:

    ``p_negative = p("לא") / (p("כן") + p("לא"))``

A high ``p_negative`` means the model judged the article *not* relevant to
TFHT enforcement — the same semantics as other cascade stages.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import hashlib
import json
import logging
import math
import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from denbust.prefilter.models import CandidateView, PassKind, StageScore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STAGE_D_SUBDIR = "stage_d"
_PROMPT_FILE = "prompt.txt"
_META_FILE = "meta.json"

_DEFAULT_BASE_MODEL_D = "dicta-il/dictalm2.0-instruct"
_FALLBACK_BASE_MODEL_D = "Qwen/Qwen2.5-7B-Instruct"
_DEFAULT_TIMEOUT_SECONDS = 30.0
_DEFAULT_CB_THRESHOLD = 3  # consecutive timeouts before opening the circuit

# Default Hebrew prompt template.  Uses {title} and {body} as placeholders.
_DEFAULT_PROMPT_TEMPLATE = """\
[הוראה]
אתה עוזר לסווג כתבות עיתוניות. ענה בדיוק "כן" או "לא" בלבד.

[קלט]
כותרת: {title}
תוכן: {body}

האם הכתבה עוסקת באכיפת חוק נגד סחר בנשים וניצול מיני בישראל?

[פלט]
"""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class StageDModelMeta:
    """Provenance metadata for baked Stage D artifacts.

    Attributes
    ----------
    prompt_version:
        Short (12-char) SHA-1 prefix of ``prompt.txt`` — changes whenever the
        prompt template changes.
    baked_at:
        ISO-8601 UTC timestamp of when the artifacts were written.
    base_model_id:
        HuggingFace model ID to load at inference time.
    timeout_seconds:
        Per-candidate inference timeout.
    circuit_breaker_threshold:
        Number of consecutive timeouts that trip the circuit breaker.
    """

    prompt_version: str
    baked_at: str
    base_model_id: str
    timeout_seconds: float
    circuit_breaker_threshold: int


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _sha1_file(path: Path) -> str:
    """Return the first 12 hex chars of SHA-1 over *path*'s bytes."""
    h = hashlib.sha1(path.read_bytes(), usedforsecurity=False)  # noqa: S324
    return h.hexdigest()[:12]


def _mlx_score(model: Any, tokenizer: Any, prompt: str, ken_id: int, lo_id: int) -> float:
    """Run MLX inference and return ``p("לא") / (p("כן") + p("לא"))``.

    This is a module-level function (not a method) so it can be patched by
    tests without requiring a live MLX runtime.

    Parameters
    ----------
    model:
        An MLX language model loaded via ``mlx_lm.load``.
    tokenizer:
        The tokenizer paired with *model*.
    prompt:
        The formatted prompt string.
    ken_id:
        Token ID for ``"כן"`` (yes).
    lo_id:
        Token ID for ``"לא"`` (no).

    Returns
    -------
    float
        ``p_negative`` in ``[0, 1]``.  A high value means the model judged
        the article irrelevant to TFHT enforcement.
    """
    import mlx.core as mx

    tokens = tokenizer.encode(prompt, add_special_tokens=False)
    x = mx.array(tokens)[None]
    logits = model(x)
    mx.eval(logits)

    ken_logit = float(logits[0, -1, ken_id])
    lo_logit = float(logits[0, -1, lo_id])

    # Numerically stable softmax over the two relevant tokens.
    max_l = max(ken_logit, lo_logit)
    exp_ken = math.exp(ken_logit - max_l)
    exp_lo = math.exp(lo_logit - max_l)
    return exp_lo / (exp_ken + exp_lo)


def _get_token_id(tokenizer: Any, text: str) -> int:
    """Return the single token ID for *text*.

    Logs a warning and returns the first token when *text* tokenizes to
    multiple tokens (best-effort fallback; should not happen for short Hebrew
    words with a well-configured tokenizer).
    """
    ids: list[int] = tokenizer.encode(text, add_special_tokens=False)
    if len(ids) == 1:
        return ids[0]
    logger.warning("Stage D: '%s' tokenizes to %d tokens; using first token only.", text, len(ids))
    return ids[0] if ids else -1


# ---------------------------------------------------------------------------
# StageDScorer
# ---------------------------------------------------------------------------


class StageDScorer:
    """Stage D cascade scorer: local SLM judge via MLX.

    Returns ``None`` for the thin pass (this stage is thick-pass only), when
    no baked artifacts exist, when ``mlx_lm`` is not installed, when a
    per-candidate timeout fires, or when the circuit breaker is open.

    Parameters
    ----------
    models_dir:
        Root models directory (``PrefilterStatePaths.models_dir``).
        Artifacts expected under ``models_dir/stage_d/``.  When ``None`` or
        artifacts are absent the scorer returns ``None`` for every call.
    threshold:
        Drop threshold.  Candidates whose ``p_negative >= threshold`` are
        tagged ``dropped=True``.
    timeout_seconds:
        Per-candidate inference timeout in seconds.  When ``None`` the value
        from ``meta.json`` is used; falls back to :data:`_DEFAULT_TIMEOUT_SECONDS`.
    circuit_breaker_threshold:
        Number of consecutive timeouts before the circuit breaker opens.
        When ``None`` the value from ``meta.json`` is used; falls back to
        :data:`_DEFAULT_CB_THRESHOLD`.
    """

    def __init__(
        self,
        *,
        models_dir: Path | None = None,
        threshold: float = 0.90,
        timeout_seconds: float | None = None,
        circuit_breaker_threshold: int | None = None,
    ) -> None:
        self._threshold = threshold
        self._timeout_seconds: float = (
            timeout_seconds if timeout_seconds is not None else _DEFAULT_TIMEOUT_SECONDS
        )
        self._cb_threshold: int = (
            circuit_breaker_threshold
            if circuit_breaker_threshold is not None
            else _DEFAULT_CB_THRESHOLD
        )
        self._prompt_template: str = ""
        self._model: Any = None
        self._tokenizer: Any = None
        self._ken_id: int = -1
        self._lo_id: int = -1
        self.model_version: str = ""
        self.base_model_id: str = ""
        # Circuit-breaker state (mutable — not thread-safe, but cascade is single-threaded).
        self._consecutive_timeouts: int = 0
        self._circuit_open: bool = False

        if models_dir is None:
            return

        stage_dir = models_dir / _STAGE_D_SUBDIR
        prompt_path = stage_dir / _PROMPT_FILE
        meta_path = stage_dir / _META_FILE

        if not prompt_path.exists():
            return

        try:
            from mlx_lm import load as mlx_load
        except ImportError as exc:
            logger.warning(
                "Stage D: mlx_lm is not installed (%s); install the prefilter extras "
                "(pip install -e '.[dev,prefilter]'). "
                "Scorer will return None for all candidates.",
                exc,
            )
            return

        try:
            self._prompt_template = prompt_path.read_text(encoding="utf-8")

            if meta_path.exists():
                meta_raw = json.loads(meta_path.read_text(encoding="utf-8"))
                self.model_version = str(meta_raw.get("prompt_version", ""))
                self.base_model_id = str(meta_raw.get("base_model_id", _DEFAULT_BASE_MODEL_D))
                if timeout_seconds is None:
                    self._timeout_seconds = float(
                        meta_raw.get("timeout_seconds", _DEFAULT_TIMEOUT_SECONDS)
                    )
                if circuit_breaker_threshold is None:
                    self._cb_threshold = int(
                        meta_raw.get("circuit_breaker_threshold", _DEFAULT_CB_THRESHOLD)
                    )
            else:
                self.base_model_id = _DEFAULT_BASE_MODEL_D
                logger.warning(
                    "Stage D: meta.json missing from %s; model_version will be empty "
                    "in telemetry.  Re-run `denbust prefilter retrain --stage d` to fix.",
                    stage_dir,
                )

            model_id = self.base_model_id or _DEFAULT_BASE_MODEL_D
            # mlx_lm.load() returns Union[Tuple[model, tok], Tuple[model, tok, cfg]]
            # depending on return_config.  Index explicitly to satisfy mypy in both
            # environments (installed stubs vs. ignore_missing_imports fallback).
            _loaded: Any = mlx_load(model_id)
            self._model = _loaded[0]
            self._tokenizer = _loaded[1]
            self._ken_id = _get_token_id(self._tokenizer, "כן")
            self._lo_id = _get_token_id(self._tokenizer, "לא")

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Stage D: artifacts at %s failed to load: %s — "
                "run `denbust prefilter retrain --stage d` to rebuild.",
                stage_dir,
                exc,
            )
            self._model = None
            self._tokenizer = None
            self._prompt_template = ""

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

        Always returns ``None`` for the thin pass (Stage D is thick-pass only),
        when no artifacts are loaded, when inference times out, or when the
        circuit breaker is open.

        Parameters
        ----------
        candidate:
            Candidate to evaluate.
        pass_kind:
            ``"thin"`` → returns ``None`` unconditionally.
            ``"thick"`` → runs SLM inference.
        body:
            Full article body; falls back to ``candidate.snippet`` when absent.
        """
        if pass_kind == "thin":
            return None  # Stage D is thick-pass only

        if self._model is None or self._tokenizer is None or not self._prompt_template:
            return None

        if self._circuit_open:
            logger.debug(
                "Stage D: circuit breaker open; skipping inference for %s.",
                candidate.candidate_id,
            )
            return None

        title = (candidate.title or "").strip()
        content = (body or candidate.snippet or "").strip()
        prompt = self._prompt_template.format(title=title, body=content)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                _mlx_score,
                self._model,
                self._tokenizer,
                prompt,
                self._ken_id,
                self._lo_id,
            )
            try:
                p_negative = future.result(timeout=self._timeout_seconds)
                self._consecutive_timeouts = 0
            except concurrent.futures.TimeoutError:
                self._consecutive_timeouts += 1
                logger.warning(
                    "Stage D: inference timed out after %.1fs for candidate %s "
                    "(consecutive=%d/%d).",
                    self._timeout_seconds,
                    candidate.candidate_id,
                    self._consecutive_timeouts,
                    self._cb_threshold,
                )
                if self._consecutive_timeouts >= self._cb_threshold:
                    self._circuit_open = True
                    logger.error(
                        "Stage D: circuit breaker opened after %d consecutive timeouts.",
                        self._consecutive_timeouts,
                    )
                return None

        # Clamp to [0, 1] for safety.
        p_negative = max(0.0, min(1.0, p_negative))
        dropped = p_negative >= self._threshold
        reason = f"slm/thick={p_negative:.3f}"
        return StageScore(
            stage="D",
            p_negative=p_negative,
            threshold=self._threshold,
            dropped=dropped,
            reason=reason,
            model_version=self.model_version,
        )


# ---------------------------------------------------------------------------
# Baking entry point
# ---------------------------------------------------------------------------


def bake_stage_d(
    out_dir: Path,
    *,
    base_model_id: str = _DEFAULT_BASE_MODEL_D,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    circuit_breaker_threshold: int = _DEFAULT_CB_THRESHOLD,
    prompt_template: str = _DEFAULT_PROMPT_TEMPLATE,
) -> tuple[StageDModelMeta, Path]:
    """Write Stage D prompt and metadata artifacts atomically.

    No ML training happens here — the SLM is loaded from HuggingFace at
    runtime by :class:`StageDScorer`.  Baking is fast (milliseconds) and does
    not require MLX to be installed.

    Artifacts written
    -----------------
    ``prompt.txt``
        The prompt template, with ``{title}`` and ``{body}`` placeholders.
    ``meta.json``
        :class:`StageDModelMeta` provenance record.

    Parameters
    ----------
    out_dir:
        Parent directory for ``stage_d/`` artifacts.
    base_model_id:
        HuggingFace model ID that :class:`StageDScorer` will load.
    timeout_seconds:
        Per-candidate inference timeout stored in ``meta.json``.
    circuit_breaker_threshold:
        Consecutive-timeout threshold stored in ``meta.json``.
    prompt_template:
        Full prompt template string.  Must contain ``{title}`` and ``{body}``
        placeholder tokens.

    Returns
    -------
    tuple[StageDModelMeta, Path]
        ``(meta, stage_dir)`` — provenance metadata and the artifact directory.

    Raises
    ------
    ValueError
        When *prompt_template* does not contain both ``{title}`` and ``{body}``.
    """
    if "{title}" not in prompt_template or "{body}" not in prompt_template:
        raise ValueError("prompt_template must contain both '{title}' and '{body}' placeholders.")

    out_dir.mkdir(parents=True, exist_ok=True)
    final_dir = out_dir / _STAGE_D_SUBDIR

    old_dir: Path | None = None
    tmp_dir = Path(tempfile.mkdtemp(dir=out_dir, prefix=f"{_STAGE_D_SUBDIR}.tmp."))
    try:
        prompt_path = tmp_dir / _PROMPT_FILE
        meta_path = tmp_dir / _META_FILE

        prompt_path.write_text(prompt_template, encoding="utf-8")
        prompt_version = _sha1_file(prompt_path)

        meta = StageDModelMeta(
            prompt_version=prompt_version,
            baked_at=datetime.now(UTC).isoformat(),
            base_model_id=base_model_id,
            timeout_seconds=timeout_seconds,
            circuit_breaker_threshold=circuit_breaker_threshold,
        )
        meta_path.write_text(
            json.dumps(dataclasses.asdict(meta), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Atomic rename-aside: readers never see a half-written directory.
        if final_dir.exists():
            old_dir = out_dir / f"{_STAGE_D_SUBDIR}.old.{os.getpid()}"
            final_dir.rename(old_dir)
        tmp_dir.rename(final_dir)

    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if old_dir is not None and not final_dir.exists():
            try:
                old_dir.rename(final_dir)
                old_dir = None
            except OSError:
                pass
        raise

    if old_dir is not None:
        shutil.rmtree(old_dir, ignore_errors=True)

    return meta, final_dir
