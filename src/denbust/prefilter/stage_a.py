"""Stage A — lexicon scorer, domain reputation scorer, and URL heuristic scorer.

Implements three sub-scorers blended via the independence assumption:

    p_negative_A = 1 - (1 - p_lex) * (1 - p_dom) * (1 - p_url)

The combined score drives a single drop decision at the configured threshold.

Artifacts
---------
lexicon.json
    JSON array of LexiconEntry objects (term, log_weight_negative, k_neg, k_pos).
    Built by :func:`build_stage_a_artifacts` from the labeled-candidates parquet.
domain_reputation.parquet
    Parquet table of DomainReputation rows.  Schema matches :class:`DomainReputation`.

Scoring at inference
--------------------
All three sub-scorers return a probability in ``[0.0, 1.0]``.  ``0.0`` means the
scorer has no negative signal for this candidate.  The blend clips the result to
``[0.0, 1.0]`` before comparison against the threshold.

Default behaviour (no trained artifacts)
-----------------------------------------
When no ``models_dir`` is provided or the artifact files do not exist, the lexicon
falls back to :data:`_EXCLUDED_TITLE_TERMS` from ``candidate_filters.py`` with a
high default weight (approximating the existing hard-drop filter), and the domain
scorer returns ``0.0`` for all domains (no opinion).  URL heuristics are always
active.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from denbust.discovery.candidate_filters import globally_excluded_title_terms
from denbust.prefilter.models import CandidateView, PassKind, StageScore

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Number of chi-squared-selected unigrams+bigrams added to the lexicon on top
# of _EXCLUDED_TITLE_TERMS during retrain.
_N_CHI2_TERMS: int = 100

# Default k_neg assigned to every _EXCLUDED_TITLE_TERMS entry when no training
# data is available.  k_pos=0.  log((k_neg+1)/(0+1)) = log(k_neg+1).
# k_neg=98 → log(99) ≈ 4.6 → sigmoid ≈ 0.99, matching the existing hard-drop.
_DEFAULT_EXCLUDED_K_NEG: int = 98

# URL path segments that strongly indicate non-article content.
# These are bare segment roots — no trailing slash.  The boundary check is
# handled by :func:`_url_has_segment`, which ensures that e.g. ``/feed`` does
# not fire on ``/feedback/`` or ``/sitemap`` on ``/sitemapper/``.
_NEGATIVE_URL_SEGMENTS: tuple[str, ...] = (
    "/tag",
    "/category",
    "/topic",
    "/section",
    "/archive",
    "/sitemap",
    "/feed",
    "/rss",
    "/amp",
)

# File extensions that indicate non-HTML documents (PDFs, Office files, etc.)
_NEGATIVE_EXTENSIONS: tuple[str, ...] = (".pdf", ".doc", ".docx", ".xml", ".xls", ".xlsx")

# Candidates with more than this many query-string parameters are rarely articles.
_MAX_ARTICLE_QS_PARAMS: int = 3

# Wilson-score z-value for the 95th percentile (one-sided).
_Z95: float = 1.6449


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class LexiconEntry:
    """One weighted term in the Stage-A lexicon.

    Attributes
    ----------
    term:
        The exact term string (matched case-insensitively via ``casefold``).
    log_weight_negative:
        ``log((k_neg + 1) / (k_pos + 1))``.  Positive values indicate that
        the term is more common in negatives; large positive values → high
        ``p_term``.
    k_neg:
        Count of training negatives whose title+snippet contained this term.
    k_pos:
        Count of training positives whose title+snippet contained this term.
    """

    term: str
    log_weight_negative: float
    k_neg: int
    k_pos: int


@dataclasses.dataclass(frozen=True)
class DomainReputation:
    """Beta-Binomial posterior for one domain's negative rate.

    Attributes
    ----------
    domain:
        Normalised eTLD+1 host (casefolded).
    n:
        Total number of training examples from this domain.
    k_negative:
        Count of training negatives from this domain.
    p_post_mean:
        Posterior mean probability of being negative: ``(k_negative + 1) / (n + 2)``.
    p_post_upper_95:
        Wilson-score upper bound for the 95th percentile (one-sided, conservative).
    """

    domain: str
    n: int
    k_negative: int
    p_post_mean: float
    p_post_upper_95: float


# ---------------------------------------------------------------------------
# LexiconScorer
# ---------------------------------------------------------------------------


class LexiconScorer:
    """Score a candidate against a weighted term lexicon.

    Each matching term contributes an independent piece of negative evidence.
    The sub-scores are combined via the independence assumption:
    ``p_lex = 1 - product(1 - p_term for each matching term)``.

    ``p_term = sigmoid(log_weight_negative) = (k_neg+1) / (k_neg+k_pos+2)``.
    """

    def __init__(self, entries: list[LexiconEntry]) -> None:
        # Pre-casefold every term once at construction time to avoid repeated
        # casefold() calls on the hot scoring path (one per entry per candidate).
        self._entries: list[tuple[LexiconEntry, str]] = [(e, e.term.casefold()) for e in entries]

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: Path) -> LexiconScorer:
        """Load a :class:`LexiconScorer` from a JSON file produced by :func:`build_stage_a_artifacts`."""
        raw = json.loads(path.read_text(encoding="utf-8"))
        entries: list[LexiconEntry] = []
        for row in raw:
            log_w_raw = row["log_weight_negative"]
            try:
                log_w = float(log_w_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid log_weight_negative {log_w_raw!r} for term {row['term']!r}"
                ) from exc
            if not math.isfinite(log_w):
                raise ValueError(
                    f"Non-finite log_weight_negative {log_w!r} for term {row['term']!r}"
                )
            k_neg = int(row["k_neg"])
            k_pos = int(row["k_pos"])
            if k_neg < 0 or k_pos < 0:
                raise ValueError(
                    f"Negative count for term {row['term']!r}: k_neg={k_neg}, k_pos={k_pos}"
                )
            entries.append(
                LexiconEntry(
                    term=row["term"],
                    log_weight_negative=log_w,
                    k_neg=k_neg,
                    k_pos=k_pos,
                )
            )
        return cls(entries)

    def save(self, path: Path) -> None:
        """Persist this lexicon to *path* as a JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = [dataclasses.asdict(e) for e, _ in self._entries]
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score(self, title: str, snippet: str) -> float:
        """Return ``p_negative ∈ [0.0, 1.0]`` based on lexicon hits.

        ``0.0`` is returned when no entries match (lexicon has no opinion).
        """
        text = (title + " " + snippet).casefold()
        p_no_neg = 1.0
        for entry, term_cf in self._entries:
            if term_cf in text:
                p_term = _sigmoid(entry.log_weight_negative)
                p_no_neg *= 1.0 - p_term
        return 1.0 - p_no_neg


# ---------------------------------------------------------------------------
# DomainReputationScorer
# ---------------------------------------------------------------------------


class DomainReputationScorer:
    """Score a candidate domain against a Beta-Binomial reputation table.

    Domains with fewer than ``min_observations`` training examples return
    ``0.0`` (no opinion) regardless of the observed ratio.
    """

    def __init__(
        self,
        table: dict[str, DomainReputation],
        min_observations: int = 20,
    ) -> None:
        self._table = table
        self._min_observations = min_observations

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: Path, min_observations: int = 20) -> DomainReputationScorer:
        """Load a :class:`DomainReputationScorer` from a Parquet file."""
        import pyarrow.parquet as pq  # lazy — pyarrow is optional at import time

        read_fn: Callable[[Any], Any] = pq.read_table
        table = read_fn(path)
        reputation: dict[str, DomainReputation] = {}
        for row in table.to_pylist():
            rep = DomainReputation(
                domain=str(row["domain"]),
                n=int(row["n"]),
                k_negative=int(row["k_negative"]),
                p_post_mean=float(row["p_post_mean"]),
                p_post_upper_95=float(row["p_post_upper_95"]),
            )
            reputation[rep.domain] = rep
        return cls(reputation, min_observations)

    def save(self, path: Path) -> None:
        """Persist this reputation table to *path* as a Parquet file."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        path.parent.mkdir(parents=True, exist_ok=True)
        schema = pa.schema(
            [
                pa.field("domain", pa.string(), nullable=False),
                pa.field("n", pa.int64(), nullable=False),
                pa.field("k_negative", pa.int64(), nullable=False),
                pa.field("p_post_mean", pa.float64(), nullable=False),
                pa.field("p_post_upper_95", pa.float64(), nullable=False),
            ]
        )
        rows = [dataclasses.asdict(rep) for rep in self._table.values()]
        tbl = pa.Table.from_pylist(rows, schema=schema)
        write_fn: Callable[[Any, Any], None] = pq.write_table
        write_fn(tbl, path)

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score(self, domain: str) -> float:
        """Return ``p_negative ∈ [0.0, 1.0]`` for *domain*.

        Returns ``0.0`` when the domain is unknown or has fewer than
        ``min_observations`` training examples.

        The Wilson upper-95 bound is used rather than the posterior mean so
        that domains with small-but-entirely-negative evidence are penalised
        more aggressively — the right trade-off for a pre-filter where a
        missed drop is cheaper than a missed pass during shadow-mode
        validation.
        """
        rep = self._table.get((domain or "").casefold())
        if rep is None or rep.n < self._min_observations:
            return 0.0
        return rep.p_post_upper_95


# ---------------------------------------------------------------------------
# UrlHeuristicScorer
# ---------------------------------------------------------------------------


class UrlHeuristicScorer:
    """Score a candidate URL using hand-coded structural heuristics.

    Each heuristic contributes an independent piece of evidence that the URL
    points to a non-article page (taxonomy index, sitemap, document, etc.).
    Scores are combined via the independence assumption.
    """

    # Per-heuristic weights expressed as p_negative contributions.
    # These are hand-tuned conservative priors based on a qualitative
    # assessment of how reliably each heuristic identifies non-article URLs.
    # They should be recalibrated against validation-set precision/recall once
    # the cascade is running in shadow mode and telemetry is available.
    _W_SEGMENT: float = 0.70  # navigation/index path segment (tag, category, etc.)
    _W_EXTENSION: float = 0.85  # non-HTML document extension (.pdf, .doc, etc.)
    _W_TRAILING_SLASH: float = 0.55  # non-root path ending in '/' → likely directory/nav
    _W_EXCESS_QS: float = 0.60  # more than _MAX_ARTICLE_QS_PARAMS query parameters

    def score(self, url: str) -> float:
        """Return ``p_negative ∈ [0.0, 1.0]`` based on URL structure.

        Returns ``0.0`` when the URL is empty or malformed.
        """
        if not url:
            return 0.0
        try:
            parsed = urlparse(url)
        except ValueError:
            return 0.0

        path = (parsed.path or "/").casefold()
        query = parsed.query or ""

        components: list[float] = []

        # Heuristic 1 — navigation/index path segments.
        # Uses _url_has_segment so that e.g. "/feed" does not fire on
        # "/feedback/" and "/sitemap" does not fire on "/sitemapper/".
        if any(_url_has_segment(path, seg) for seg in _NEGATIVE_URL_SEGMENTS):
            components.append(self._W_SEGMENT)

        # Heuristic 2 — non-HTML document extensions.
        # Path.suffix is reliable and handles edge cases like versioned paths
        # (/api/v2.0/report.pdf) that naive rsplit would mis-parse.
        ext = Path(parsed.path).suffix.lower()
        if ext in _NEGATIVE_EXTENSIONS:
            components.append(self._W_EXTENSION)

        # Heuristic 3 — trailing slash (directory / nav page)
        if path != "/" and path.endswith("/"):
            components.append(self._W_TRAILING_SLASH)

        # Heuristic 4 — excessive query-string parameters
        if query:
            n_params = len([p for p in query.split("&") if p])
            if n_params > _MAX_ARTICLE_QS_PARAMS:
                components.append(self._W_EXCESS_QS)

        if not components:
            return 0.0
        # Independence blend
        p_no_neg = math.prod(1.0 - p for p in components)
        return min(1.0 - p_no_neg, 0.99)  # cap at 0.99 per spec


# ---------------------------------------------------------------------------
# StageAScorer — combines all three sub-scorers
# ---------------------------------------------------------------------------

_STAGE_A_LEXICON_FILE = "lexicon.json"
_STAGE_A_DOMAIN_FILE = "domain_reputation.parquet"
_STAGE_A_SUBDIR = "stage_a"


class StageAScorer:
    """Stage A cascade scorer: lexicon + domain reputation + URL heuristics.

    Parameters
    ----------
    models_dir:
        Root models directory (``PrefilterStatePaths.models_dir``).  When
        ``None`` or the artifact files are absent, falls back to
        :func:`_default_lexicon` (hard-weights from ``_EXCLUDED_TITLE_TERMS``)
        and an empty domain reputation table.
    threshold:
        Drop threshold.  Candidates whose ``p_negative_A >= threshold`` are
        tagged ``dropped=True``.
    min_domain_observations:
        Minimum number of training examples required before a domain's
        reputation is used.  Domains below this count contribute ``0.0``.
    _lexicon_scorer:
        **For testing only.** When provided, bypasses artifact loading
        entirely and uses this scorer directly.  The leading underscore
        signals that callers outside tests should never set this.
    """

    def __init__(
        self,
        *,
        models_dir: Path | None = None,
        threshold: float = 0.95,
        min_domain_observations: int = 20,
        _lexicon_scorer: LexiconScorer | None = None,
    ) -> None:
        self._threshold = threshold
        self._url_scorer = UrlHeuristicScorer()

        if _lexicon_scorer is not None:
            # Testing shortcut: skip all artifact I/O.
            self._lexicon_scorer = _lexicon_scorer
            self._model_version = "injected"
            self._domain_scorer = DomainReputationScorer({}, min_domain_observations)
            return

        stage_dir = models_dir / _STAGE_A_SUBDIR if models_dir is not None else None

        # Lexicon — prefer trained artifact; fall back to defaults
        lex_path = stage_dir / _STAGE_A_LEXICON_FILE if stage_dir is not None else None
        if lex_path is not None and lex_path.exists():
            self._lexicon_scorer = LexiconScorer.from_file(lex_path)
            self._model_version = _sha1_file(lex_path)
        else:
            self._lexicon_scorer = _default_lexicon()
            self._model_version = "default"

        # Domain reputation — prefer trained artifact; fall back to empty
        dom_path = stage_dir / _STAGE_A_DOMAIN_FILE if stage_dir is not None else None
        if dom_path is not None and dom_path.exists():
            self._domain_scorer = DomainReputationScorer.from_file(
                dom_path, min_domain_observations
            )
        else:
            self._domain_scorer = DomainReputationScorer({}, min_domain_observations)

    def evaluate(
        self,
        candidate: CandidateView,
        _pass_kind: PassKind,
        _body: str | None = None,
    ) -> StageScore:
        """Evaluate *candidate* and return a :class:`StageScore`.

        Unlike stub stages, Stage A always returns a score (never ``None``),
        so every candidate gets a logged probability even when no signal fires.
        The *body* and *pass_kind* parameters are accepted for interface
        uniformity but not used — Stage A scores thin-pass features only.
        """
        title = candidate.title or ""
        snippet = candidate.snippet or ""
        domain = candidate.domain or ""
        url = candidate.url or ""

        p_lex = self._lexicon_scorer.score(title, snippet)
        p_dom = self._domain_scorer.score(domain)
        p_url = self._url_scorer.score(url)

        p_negative = 1.0 - (1.0 - p_lex) * (1.0 - p_dom) * (1.0 - p_url)
        p_negative = min(max(p_negative, 0.0), 1.0)

        dropped = p_negative >= self._threshold

        reason_parts: list[str] = []
        if p_lex > 0.0:
            reason_parts.append(f"lex={p_lex:.3f}")
        if p_dom > 0.0:
            reason_parts.append(f"dom={p_dom:.3f}")
        if p_url > 0.0:
            reason_parts.append(f"url={p_url:.3f}")
        reason = "+".join(reason_parts) if reason_parts else "no_signal"

        return StageScore(
            stage="A",
            p_negative=p_negative,
            threshold=self._threshold,
            dropped=dropped,
            reason=reason,
            model_version=self._model_version,
        )


# ---------------------------------------------------------------------------
# Artifact builder (used by CLI retrain command)
# ---------------------------------------------------------------------------


def build_stage_a_artifacts(
    labels_path: Path,
    out_dir: Path,
    *,
    n_chi2_terms: int = _N_CHI2_TERMS,
    min_domain_observations: int = 20,
) -> tuple[Path, Path]:
    """Build Stage-A artifacts from a labeled-candidates parquet.

    Reads ``labels_path``, restricts to ``split == "train"`` rows, then:

    1. Counts ``k_neg`` / ``k_pos`` for every term in
       :func:`~denbust.discovery.candidate_filters.globally_excluded_title_terms`.
    2. Selects the top *n_chi2_terms* additional word unigrams + bigrams by
       chi-squared score (pure-Python implementation, no sklearn dependency).
    3. Computes Beta-Binomial domain reputation statistics.

    Parameters
    ----------
    labels_path:
        Path to a ``labels.parquet`` produced by :mod:`denbust.prefilter.labels`.
    out_dir:
        Parent directory for ``stage_a/``.  Artifacts are written to
        ``out_dir/stage_a/lexicon.json`` and
        ``out_dir/stage_a/domain_reputation.parquet``.
    n_chi2_terms:
        Maximum number of chi-squared-selected unigrams/bigrams to add.
    min_domain_observations:
        Minimum training examples per domain for reputation to be stored.

    Returns
    -------
    tuple[Path, Path]
        ``(lexicon_path, domain_reputation_path)``.
    """
    from denbust.prefilter.labels import read_labels_parquet

    rows = read_labels_parquet(labels_path)
    train_rows = [r for r in rows if r.split == "train"]
    if not train_rows:
        raise ValueError(f"No training rows found in {labels_path}")

    # Build per-term counts over title+snippet
    excluded_terms = globally_excluded_title_terms()

    # Corpora split by label.  Guard against None title/snippet (Optional[str]
    # in LabeledCandidate) so the training path behaves like evaluate().
    pos_texts = [
        (r.title or "") + " " + (r.snippet or "") for r in train_rows if r.label == "positive"
    ]
    neg_texts = [
        (r.title or "") + " " + (r.snippet or "") for r in train_rows if r.label == "negative"
    ]

    # Pre-casefold corpora once to avoid O(terms × docs) repeated casefold()
    # calls in the inner-loop term counting below.
    pos_texts_cf = [t.casefold() for t in pos_texts]
    neg_texts_cf = [t.casefold() for t in neg_texts]

    # --- Lexicon: _EXCLUDED_TITLE_TERMS with computed weights ---
    entries: list[LexiconEntry] = []
    for term in excluded_terms:
        term_cf = term.casefold()
        k_neg = sum(1 for t in neg_texts_cf if term_cf in t)
        k_pos = sum(1 for t in pos_texts_cf if term_cf in t)
        log_w = math.log((k_neg + 1) / (k_pos + 1))
        entries.append(LexiconEntry(term=term, log_weight_negative=log_w, k_neg=k_neg, k_pos=k_pos))

    # --- Lexicon: chi-squared top-N additional terms ---
    existing_casefolded = {e.term.casefold() for e in entries}
    chi2_terms = _chi2_top_terms(pos_texts_cf, neg_texts_cf, n=n_chi2_terms, min_df=3)
    for term, k_neg_chi, k_pos_chi in chi2_terms:
        if term.casefold() in existing_casefolded:
            continue
        log_w = math.log((k_neg_chi + 1) / (k_pos_chi + 1))
        entries.append(
            LexiconEntry(term=term, log_weight_negative=log_w, k_neg=k_neg_chi, k_pos=k_pos_chi)
        )

    lex_scorer = LexiconScorer(entries)
    stage_dir = out_dir / _STAGE_A_SUBDIR
    lex_path = stage_dir / _STAGE_A_LEXICON_FILE
    lex_scorer.save(lex_path)

    # --- Domain reputation ---
    domain_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [n, k_neg]
    for row in train_rows:
        d = (row.domain or "").casefold()
        if not d:
            continue
        domain_counts[d][0] += 1
        if row.label == "negative":
            domain_counts[d][1] += 1

    reputation: dict[str, DomainReputation] = {}
    for domain, (n, k_neg_d) in domain_counts.items():
        if n < min_domain_observations:
            continue
        p_mean = (k_neg_d + 1) / (n + 2)
        p_upper = _wilson_upper_95(k_neg_d, n)
        reputation[domain] = DomainReputation(
            domain=domain,
            n=n,
            k_negative=k_neg_d,
            p_post_mean=p_mean,
            p_post_upper_95=p_upper,
        )

    dom_scorer = DomainReputationScorer(reputation, min_domain_observations)
    dom_path = stage_dir / _STAGE_A_DOMAIN_FILE
    dom_scorer.save(dom_path)

    return lex_path, dom_path


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _sigmoid(x: float) -> float:
    """Numerically stable sigmoid: 1 / (1 + exp(-x))."""
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    ex = math.exp(x)
    return ex / (1.0 + ex)


def _default_lexicon() -> LexiconScorer:
    """Return a :class:`LexiconScorer` built from :data:`_EXCLUDED_TITLE_TERMS`.

    Uses ``k_neg = _DEFAULT_EXCLUDED_K_NEG, k_pos = 0`` for every term, which
    produces ``p_term ≈ 0.99`` — matching the existing hard-drop behaviour.
    """
    entries = [
        LexiconEntry(
            term=term,
            log_weight_negative=math.log(_DEFAULT_EXCLUDED_K_NEG + 1),
            k_neg=_DEFAULT_EXCLUDED_K_NEG,
            k_pos=0,
        )
        for term in globally_excluded_title_terms()
    ]
    return LexiconScorer(entries)


def _sha1_file(path: Path) -> str:
    """Return a short SHA-1 hex digest of *path*'s content."""
    digest = hashlib.sha1(path.read_bytes()).hexdigest()  # noqa: S324
    return digest[:12]


def _wilson_upper_95(k: int, n: int) -> float:
    """Wilson-score upper bound for p(negative) at the 95th percentile (one-sided).

    Falls back to 1.0 when n == 0.
    """
    if n == 0:
        return 1.0
    p_hat = k / n
    z = _Z95
    z2 = z * z
    numerator = p_hat + z2 / (2 * n) + z * math.sqrt(p_hat * (1 - p_hat) / n + z2 / (4 * n * n))
    denominator = 1 + z2 / n
    return min(numerator / denominator, 1.0)


def _url_has_segment(path: str, segment: str) -> bool:
    """Return True if *segment* is a complete URL path component in *path*.

    A component is complete when the character immediately after the matched
    segment is ``/``, ``.``, or end-of-string.  This prevents ``/feed`` from
    matching ``/feedback/`` and ``/sitemap`` from matching ``/sitemapper/``.

    Parameters
    ----------
    path:
        The URL path string (already casefolded by the caller).
    segment:
        Bare segment root starting with ``/``, e.g. ``"/feed"``.  Must not
        have a trailing slash (the boundary check handles that).
    """
    start = 0
    while True:
        idx = path.find(segment, start)
        if idx == -1:
            return False
        after = idx + len(segment)
        if after >= len(path) or path[after] in (".", "/"):
            return True
        start = idx + 1


def _tokenize(text: str) -> list[str]:
    """Tokenize *text* into whitespace-separated tokens (works for Hebrew).

    Returns an empty list for blank input to prevent empty-string tokens from
    polluting chi-squared term counts.
    """
    stripped = text.strip()
    if not stripped:
        return []
    return re.split(r"\s+", stripped)


def _word_ngrams(tokens: list[str], n: int) -> list[str]:
    """Return word *n*-grams from *tokens*."""
    return [" ".join(tokens[i : i + n]) for i in range(max(0, len(tokens) - n + 1))]


def _chi2_top_terms(
    pos_texts: list[str],
    neg_texts: list[str],
    *,
    n: int,
    min_df: int = 3,
) -> list[tuple[str, int, int]]:
    """Return the top-*n* word unigrams+bigrams by chi-squared score.

    Uses a pure-Python implementation so no sklearn dependency is required.

    Returns a list of ``(term, k_neg, k_pos)`` tuples sorted by chi-squared
    score descending.  Terms with document frequency < *min_df* are excluded.
    """
    # Count per-term document occurrences in each class
    neg_term_counts: dict[str, int] = defaultdict(int)
    pos_term_counts: dict[str, int] = defaultdict(int)

    for text in neg_texts:
        tokens = _tokenize(text.casefold())
        seen: set[str] = set()
        for ng in (1, 2):
            for term in _word_ngrams(tokens, ng):
                if term and term not in seen:
                    neg_term_counts[term] += 1
                    seen.add(term)

    for text in pos_texts:
        tokens = _tokenize(text.casefold())
        seen = set()
        for ng in (1, 2):
            for term in _word_ngrams(tokens, ng):
                if term and term not in seen:
                    pos_term_counts[term] += 1
                    seen.add(term)

    all_terms = set(neg_term_counts) | set(pos_term_counts)
    n_neg = len(neg_texts)
    n_pos = len(pos_texts)
    N = n_neg + n_pos

    results: list[tuple[str, float, int, int]] = []
    for term in all_terms:
        k_neg_t = neg_term_counts.get(term, 0)
        k_pos_t = pos_term_counts.get(term, 0)
        if k_neg_t + k_pos_t < min_df:
            continue
        # Contingency table: a=neg_with, b=pos_with, c=neg_without, d=pos_without
        a = k_neg_t
        b = k_pos_t
        c = n_neg - a
        d = n_pos - b
        denom = (a + b) * (c + d) * (a + c) * (b + d)
        if denom == 0:
            continue
        chi2 = N * (a * d - b * c) ** 2 / denom
        results.append((term, chi2, k_neg_t, k_pos_t))

    # Retain only terms that are more common in negatives: their log_weight is
    # positive so sigmoid > 0.5, which correctly increases p_negative when they
    # match.  Positive-skewed terms would have log_weight < 0 (sigmoid < 0.5),
    # meaning matching them would *reduce* p_negative and silently hurt recall.
    results = [(t, chi2, kn, kp) for t, chi2, kn, kp in results if kn > kp]
    results.sort(key=lambda x: -x[1])
    return [(term, k_neg_t, k_pos_t) for term, _, k_neg_t, k_pos_t in results[:n]]
