# Local Pre-Classification Filter Cascade — Implementation Plan

> Companion design report: [docs/local_prefilter_cascade_design.md](./local_prefilter_cascade_design.md).
> Planning identifier prefix: `LPF-PR-XX`. These are planning identifiers, not GitHub PR numbers.
> Audience: an implementer with Sonnet-level reasoning. The plan is intentionally prescriptive — file paths, function signatures, dependency versions, acceptance numbers — to minimize required design judgment during implementation.

## 0. Guiding principles

- Each PR is independently mergeable and ships in `mode: off` until `LPF-PR-09` switches the default to `mode: shadow`.
- No PR may regress existing pipeline behaviour while the cascade mode is `off`.
- Each PR ships its own tests; broken CI blocks the merge.
- Hebrew text everywhere: never lowercase or strip non-ASCII; use `casefold()` only when the existing filter code already does it.
- All disk artifacts (`labels.parquet`, model directories, decisions JSONL) live under
  `data/<dataset_name>/<job_name>/prefilter/...` using the existing
  `denbust.discovery.state_paths.resolve_discovery_state_paths` family of helpers — do **not** invent new path roots.
- All public functions are fully type-annotated; mypy strict mode must pass for every PR.
- Ruff format and lint must pass; the project uses `ruff format .` and `ruff check .` as the formatters/linters of record.
- Do not commit any model weights, embeddings, or labeled-data parquets to git; these live under `data/` which is git-ignored.

## 1. Module layout

A new package `src/denbust/prefilter/` is introduced.

```
src/denbust/prefilter/
    __init__.py
    models.py                  # PrefilterDecision, PrefilterMode, StageScore
    config.py                  # PrefilterConfig (pydantic)
    state_paths.py             # path helpers for prefilter artifacts (uses discovery state paths)
    labels.py                  # label-dataset assembly + train/val/test split
    telemetry.py               # PrefilterDecision writer (state-repo + supabase)
    cascade.py                 # CascadeOrchestrator
    stage_a.py                 # LexiconScorer, DomainReputationScorer, UrlHeuristicScorer
    stage_b.py                 # TextClassifier (NB default, SetFit alt)
    stage_c.py                 # EmbeddingSimilarityScorer
    stage_d.py                 # SlmJudge
    cli.py                     # subcommands wired into denbust CLI
    calibration.py             # threshold sweep + persistence
    eval.py                    # batch eval helpers used by tests + CLI
```

`src/denbust/prefilter/__init__.py` exports only the public surface:

```python
from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.models import PrefilterDecision, StageScore
```

Insertion points:

- Pre-scrape thin pass: `src/denbust/discovery/scrape_queue.py` — call `CascadeOrchestrator.evaluate_thin(candidate)` when selecting candidates to scrape. Drop candidates whose `verdict == "drop"` from the scrape queue when `mode == enforce`.
- Post-scrape thick pass: `src/denbust/news_items/ingest.py` — after scrape completes and before the Claude classifier call, call `CascadeOrchestrator.evaluate_thick(candidate, article_text)`. Drop candidates whose `verdict == "drop"` from the Claude queue when `mode == enforce`.

In both insertion points, always invoke the orchestrator regardless of mode; the orchestrator decides whether to actually drop based on `mode` and writes decisions to telemetry either way.

## 2. Config schema additions

Add a top-level `prefilter:` section to dataset configs (e.g. `agents/news/local_search.yaml`). Parsed by `denbust.prefilter.config.PrefilterConfig`.

```yaml
prefilter:
  enabled: true
  mode: off               # off | shadow | enforce
  model_cache_dir: ~/.cache/denbust/prefilter
  stages:
    a:
      enabled: true
      threshold: 0.95        # drop if p_negative >= threshold
    b:
      enabled: true
      model: naive_bayes     # naive_bayes | setfit
      threshold: 0.95
    c:
      enabled: true
      model: multilingual-e5-large
      threshold: 0.95
    d:
      enabled: true
      model: dictalm2.0-instruct   # or qwen2.5-7b-instruct
      backend: mlx                  # mlx | llama_cpp
      batch_size: 4
      threshold: 0.95
      timeout_seconds: 5
  recall_floor_per_stage: 0.99
  shadow_min_days_before_enforce: 7
  refresh:
    domain_reputation_min_observations: 20
    domain_reputation_recompute_every_days: 7
```

`PrefilterConfig` validates ranges (`0 ≤ threshold ≤ 1`, `recall_floor_per_stage ∈ (0, 1)`), and converts `model_cache_dir` to an absolute `Path` with `~` expansion.

Config normalization lives in `src/denbust/config.py` — extend the existing config loader to accept the new section. Do not introduce a separate config-loading entry point.

## 3. CLI surface

A new top-level `prefilter` subcommand group on the existing `denbust` CLI (typer). Implemented in `src/denbust/prefilter/cli.py` and wired in `src/denbust/cli/__init__.py` (or wherever the typer app is composed).

```
denbust prefilter assemble-labels --config CFG [--out PATH]
denbust prefilter retrain         --stage {a,b,c,d} --config CFG
denbust prefilter calibrate       --stage {a,b,c,d} --config CFG [--recall-floor 0.99]
denbust prefilter evaluate        --config CFG [--split test] [--report-path PATH]
denbust prefilter decision        --candidate-id ID [--mode {thin,thick}]    # ad-hoc inspection
denbust prefilter summary         --config CFG                                # latest decision counts
```

Every subcommand reads the dataset's `prefilter:` config and persists artifacts under the prefilter state path. No subcommand requires network access except `retrain` (which may need to download a model the first time).

## 4. PR sequence

Each PR section below specifies: goal, scope, files added/changed, dependencies to add, tests, acceptance criteria, and explicit out-of-scope.

---

### LPF-PR-01 — Foundation: package, models, config, telemetry stubs

**Goal.** Land the empty cascade package with type-safe models, config parsing, state-path helpers, and a no-op telemetry writer. After merge, the cascade is invocable but always returns `verdict="pass"`.

**Scope.**

- Create `src/denbust/prefilter/` package with the layout in § 1, populated as follows:
  - `models.py`:

    ```python
    from __future__ import annotations
    from dataclasses import dataclass
    from typing import Literal

    Verdict = Literal["pass", "drop"]
    StageName = Literal["A", "B", "C", "D"]
    StoppedAt = Literal["A", "B", "C", "D", "passed_all"]

    @dataclass(frozen=True)
    class StageScore:
        stage: StageName
        p_negative: float
        threshold: float
        dropped: bool
        reason: str
        model_version: str

    @dataclass(frozen=True)
    class PrefilterDecision:
        candidate_id: str
        pass_kind: Literal["thin", "thick"]
        verdict: Verdict
        stopped_at_stage: StoppedAt
        stage_scores: tuple[StageScore, ...]
        decided_at: str           # ISO-8601 UTC
        config_hash: str          # sha1 of resolved PrefilterConfig
    ```

  - `config.py`: `PrefilterConfig`, `StageConfig`, `PrefilterMode` enum (`OFF | SHADOW | ENFORCE`).
  - `state_paths.py`:

    ```python
    @dataclass(frozen=True)
    class PrefilterStatePaths:
        root: Path                  # data/<dataset>/<job>/prefilter
        labels_path: Path           # root/labels.parquet
        models_dir: Path            # root/models
        decisions_dir: Path         # root/decisions
        calibration_dir: Path       # root/calibration

    def resolve_prefilter_state_paths(
        state_root: Path, dataset_name: DatasetName, job_name: JobName
    ) -> PrefilterStatePaths: ...
    ```

    Reuse `resolve_discovery_state_paths` to anchor the dataset/job sub-path; only the `prefilter/...` leaf differs.

  - `telemetry.py`: `PrefilterDecisionWriter` with `append(decision: PrefilterDecision) -> None` and `flush() -> None`. Writes JSONL to `decisions/<utc_date>.jsonl`. No Supabase integration yet.
  - `cascade.py`: `CascadeOrchestrator` with `evaluate_thin` and `evaluate_thick`, both returning a `PrefilterDecision` with `verdict="pass"` and an empty `stage_scores` tuple. The orchestrator delegates to stage classes, but in this PR all stages are no-op stubs.
  - `stage_a.py` / `stage_b.py` / `stage_c.py` / `stage_d.py`: each defines a class with `evaluate(candidate) -> StageScore | None`, returning `None` (= skip stage).
  - `cli.py`: register the typer subcommand group with only a `denbust prefilter summary` command that prints "no decisions yet" when nothing exists.
- Extend `src/denbust/config.py` to parse the new `prefilter:` block (use pydantic; default `PrefilterConfig` is `enabled=False, mode=OFF`).
- No insertion into `scrape_queue.py` or `ingest.py` yet.

**Dependencies to add to `pyproject.toml`.** None new (only stdlib + pydantic which is already present).

**Tests.** Add `tests/unit/prefilter/`:

- `test_models.py` — `PrefilterDecision` and `StageScore` are frozen, hashable, and JSON-serializable via `dataclasses.asdict`.
- `test_config.py` — default config parses cleanly; an out-of-range threshold raises `pydantic.ValidationError`; unknown `mode` strings raise; YAML round-trip works against a tiny inline YAML.
- `test_state_paths.py` — `resolve_prefilter_state_paths` returns paths under `data/<dataset>/<job>/prefilter/...`; nothing is auto-created on resolve.
- `test_telemetry.py` — writer appends one JSONL line per decision; multiple decisions in one day go to the same file; second writer instance does not overwrite.
- `test_cascade_noop.py` — `evaluate_thin` and `evaluate_thick` return `verdict="pass"`; one decision is written to telemetry per call.

**Acceptance criteria.**

- `pip install -e .[dev]` succeeds.
- `ruff format .` and `ruff check .` clean.
- `mypy src/` strict-clean.
- `pytest -q tests/unit/prefilter/` passes.
- `denbust prefilter summary` runs and prints "no decisions yet".

**Out of scope.**

- Any stage logic.
- Any integration with `scrape_queue.py` or `ingest.py`.
- Supabase migrations.

---

### LPF-PR-02 — Labeled-candidates dataset assembly

**Goal.** Produce a canonical, versioned labeled-candidates dataset (`labels.parquet`) by merging:

- manual triage decisions from `triage_decisions.jsonl`,
- candidate snapshots from the latest `persistent_candidates` file in the state repo,
- past Claude classifier outputs from the operational store (Supabase if configured, else state-repo JSONL).

Stratify into train / validation / test splits. Persist split assignments inside the parquet so every downstream PR sees the same split.

**Scope.**

- Add `src/denbust/prefilter/labels.py`:

  ```python
  @dataclass(frozen=True)
  class LabelSource:
      name: Literal["triage_manual", "triage_auto", "claude_classifier"]

  @dataclass(frozen=True)
  class LabeledCandidate:
      candidate_id: str
      domain: str
      url: str
      title: str
      snippet: str
      article_body: str | None       # may be None pre-scrape
      label: Literal["positive", "negative"]
      label_source: LabelSource
      split: Literal["train", "val", "test"]
      labeled_at: str                # ISO-8601 UTC
      decision_hash: str             # sha1 of the source row to detect dedup conflicts

  def assemble_labels(
      state_paths: DiscoveryStatePaths,
      operational_store: OperationalStore | None,
      seed: int = 20260521,
      val_fraction: float = 0.15,
      test_fraction: float = 0.15,
  ) -> list[LabeledCandidate]: ...

  def write_labels_parquet(rows: list[LabeledCandidate], out_path: Path) -> None: ...
  def read_labels_parquet(path: Path) -> list[LabeledCandidate]: ...
  ```

- Label-mapping rules (apply in this order; first match wins):
  - `triage_decisions.jsonl` `action == "exclude"` (manual) → `negative`, `label_source = triage_manual`.
  - `triage_decisions.jsonl` `action == "prioritize"` (manual) → `positive`, `label_source = triage_manual`.
  - `triage_decisions.jsonl` `action == "exclude"` with `auto: true` → `negative`, `label_source = triage_auto`.
  - `triage_decisions.jsonl` `action == "reset"` → drop the candidate from the labeled set (ambiguous).
  - Claude classifier output `is_relevant == true` (whatever the operational store calls the equivalent column) → `positive`, `label_source = claude_classifier`.
  - Claude classifier output `is_relevant == false` → `negative`, `label_source = claude_classifier`.
  - `label_source` priority for conflicts: `triage_manual > claude_classifier > triage_auto`.
- Stratified split on `(label, label_source)` using a deterministic seed. `train_fraction = 1 - val_fraction - test_fraction`.
- Wire CLI: `denbust prefilter assemble-labels --config CFG [--out PATH]`. Writes to `state_paths.labels_path` by default.

**Dependencies to add to `pyproject.toml`.**

- `pandas` already present (used elsewhere) — confirm and reuse. If absent, add `pandas>=2.0`.
- `pyarrow` already present (used by parquet release path). Reuse.

**Tests.** Under `tests/unit/prefilter/`:

- `test_labels_priority.py` — conflicts resolve in the documented priority order.
- `test_labels_split.py` — splits are deterministic given the seed; class balance within each split is within ±2 pp of the global balance per `label_source`.
- `test_labels_round_trip.py` — `write_labels_parquet` then `read_labels_parquet` returns identical rows.
- `test_labels_cli.py` — invoking `denbust prefilter assemble-labels` against a fixture state-repo creates the parquet with the expected row count.

Fixtures: hand-crafted small JSONL (≤ 30 rows) covering the priority-conflict cases.

**Acceptance criteria.**

- Running against the actual `data/news_items/discover/candidates/triage_decisions.jsonl` produces a labels parquet with at least 10 k rows.
- Train / val / test class counts logged to stdout for human sanity-check.
- All criteria from LPF-PR-01 still pass.

**Out of scope.**

- Supabase reads in local-only operator setups should be allowed but not required (fail soft: warn and skip).
- Active learning (LPF-PR-12).

---

### LPF-PR-03 — Stage A: lexicon + domain reputation + URL heuristics

**Goal.** Implement Stage A as three sub-scorers blended into one calibrated probability.

**Scope.**

- Add `src/denbust/prefilter/stage_a.py`:

  ```python
  @dataclass(frozen=True)
  class LexiconEntry:
      term: str
      log_weight_negative: float   # log P(neg | term has hit)

  @dataclass(frozen=True)
  class DomainReputation:
      domain: str
      n: int
      k_negative: int
      p_post_mean: float
      p_post_upper_95: float

  class LexiconScorer:
      def __init__(self, entries: list[LexiconEntry]) -> None: ...
      def score(self, title: str, snippet: str) -> float: ...   # p_negative

  class DomainReputationScorer:
      def __init__(self, table: dict[str, DomainReputation], min_observations: int) -> None: ...
      def score(self, domain: str) -> float: ...

  class UrlHeuristicScorer:
      def score(self, url: str) -> float: ...

  class StageAScorer:
      def __init__(self, lex: LexiconScorer, dom: DomainReputationScorer, url: UrlHeuristicScorer) -> None: ...
      def evaluate(self, candidate: CandidateView) -> StageScore: ...
  ```

  where `CandidateView` is a small protocol over the candidate fields the cascade actually reads.

- Lexicon construction:
  - For every term in `_EXCLUDED_TITLE_TERMS` (from `candidate_filters.py`), compute `log_weight_negative = log( (k_neg + 1) / (k_pos + 1) )` on the train split; also include the top-N most informative unigrams + bigrams discovered from the train split via chi-squared selection on character-aware tokens (call into the same tokenizer Stage B uses).
  - Aggregate terms into a single artifact `lexicon.json`.

- Domain reputation:
  - Group train-split rows by normalized domain, compute Beta-Binomial posterior with prior `Beta(1, 1)`.
  - Persist as `domain_reputation.parquet`.

- URL heuristics: hand-coded probability lookup based on path features:
  - `/tag/`, `/category/`, `/topic/`, `/section/`, `/archive/`, `/sitemap`, file extensions `.pdf`/`.doc`/`.xml`, paths ending in `/`, paths with > 3 query-string params: each adds a non-zero negative weight.
  - Return a clipped probability ∈ `[0.0, 0.99]`.

- Combine via independence assumption:
  `p_negative_A = 1 - (1 - p_lex) * (1 - p_dom) * (1 - p_url)`.

- Wire `StageAScorer.evaluate` into `CascadeOrchestrator.evaluate_thin` and `evaluate_thick`.

- CLI: `denbust prefilter retrain --stage a` rebuilds both artifacts from the latest labels parquet.

**Dependencies.** None new.

**Tests.** Under `tests/unit/prefilter/`:

- `test_stage_a_lexicon.py` — a known title containing a high-weight term yields `p_negative >= 0.95`.
- `test_stage_a_domain.py` — a domain with `k=20, n=20` known negatives yields `p_post_upper_95 < 0.05`; an unseen domain yields a wide posterior (`p_post_upper_95 > 0.5`).
- `test_stage_a_url.py` — tag-index URLs score high; clean article-style URLs score low.
- `test_stage_a_blend.py` — combined score equals the documented independence formula on a small example.
- `test_stage_a_cli_retrain.py` — retrain CLI produces artifacts under `models/stage_a/`.

**Acceptance criteria.**

- On a held-out validation set, calibrated `θ_A` yields recall ≥ 0.99 and drops ≥ 30% of validation negatives. (Logged to stdout in `denbust prefilter evaluate --split val`; this PR adds the metric, calibrate command lands in LPF-PR-08/10.)
- All previous PRs' criteria still pass.

**Out of scope.**

- Inserting Stage A into the live pipeline (`enforce` mode default stays `off`).

---

### LPF-PR-04 — Stage B: trained text classifier (Naive Bayes default)

**Goal.** Implement Stage B as a calibrated Multinomial Naive Bayes on character n-grams (3–5) over title + snippet (thin pass) or article body (thick pass).

**Scope.**

- Add `src/denbust/prefilter/stage_b.py`:

  ```python
  @dataclass(frozen=True)
  class StageBModelMeta:
      model_kind: Literal["naive_bayes", "setfit"]
      model_version: str   # sha1 of trained artifact + label data
      trained_at: str
      n_train: int
      n_val: int

  class StageBScorer:
      def __init__(self, model_kind: Literal["naive_bayes"], model_dir: Path) -> None: ...
      def evaluate(self, candidate: CandidateView, pass_kind: Literal["thin", "thick"]) -> StageScore: ...

  def train_naive_bayes(
      labels_path: Path, out_dir: Path, seed: int = 20260521
  ) -> StageBModelMeta: ...
  ```

- Implementation details:
  - `sklearn.feature_extraction.text.TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=2, sublinear_tf=True)`.
  - `sklearn.naive_bayes.ComplementNB()` (handles class imbalance better than MultinomialNB).
  - Wrap in `sklearn.calibration.CalibratedClassifierCV(method="sigmoid", cv=5)` for Platt-scaled probabilities.
  - Persist via `joblib.dump` under `models/stage_b/<version>/model.joblib`.
  - Separate models for thin (title+snippet) and thick (article body) passes — train both in one `denbust prefilter retrain --stage b` call.

- Wire `StageBScorer` into the orchestrator after Stage A.

**Dependencies to add to `pyproject.toml`.**

- `scikit-learn>=1.5`
- `joblib>=1.4`

**Tests.** Under `tests/unit/prefilter/`:

- `test_stage_b_train.py` — training on a tiny labeled fixture produces a model file and metadata; `n_train` and `n_val` match split counts.
- `test_stage_b_predict.py` — loaded model returns a probability ∈ `[0, 1]`; deterministic on the same input.
- `test_stage_b_calibration.py` — calibration curve on validation data has Brier score below an uncalibrated baseline.
- `test_stage_b_integration_with_cascade.py` — orchestrator returns a `StageScore` for stage B; `pass_kind="thick"` uses the body-trained model.

**Acceptance criteria.**

- On the validation split, calibrated `θ_B` yields recall ≥ 0.99 and end-to-end (Stage A + B) drop rate ≥ 50% of validation negatives.
- Stage B inference latency: ≤ 5 ms median on M4 Max for a batch of 64 (measure in a microbenchmark test).

**Out of scope.**

- SetFit (deferred to LPF-PR-05).

---

### LPF-PR-05 — Stage B alternative: SetFit model

**Goal.** Add a SetFit-on-`multilingual-e5-large` alternative for Stage B; selectable via `config.prefilter.stages.b.model: setfit`.

**Scope.**

- Extend `src/denbust/prefilter/stage_b.py` to support `model_kind="setfit"`.
- Implementation:
  - Use the `setfit` library; base model `intfloat/multilingual-e5-large`.
  - Training arguments: `num_iterations=20`, `batch_size=16`, `body_learning_rate=2e-5`, `head_learning_rate=1e-2`, `num_epochs=1`, `seed=20260521`.
  - Persist the SetFit model under `models/stage_b/<version>/setfit_model/`.
- A/B harness in `denbust prefilter evaluate` that, when given `--compare-b naive_bayes,setfit`, prints precision/recall/Brier for both at a fixed validation recall target.

**Dependencies.**

- `setfit>=1.0`
- `sentence-transformers>=3.0`
- `torch>=2.3` (CPU/MPS wheel — explicitly do not pull CUDA wheels). Pin in optional extras `prefilter`.

Add an optional extras group in `pyproject.toml`:

```toml
[project.optional-dependencies]
prefilter = [
  "scikit-learn>=1.5",
  "joblib>=1.4",
  "setfit>=1.0",
  "sentence-transformers>=3.0",
  "torch>=2.3",
  "faiss-cpu>=1.8",
  "mlx-lm>=0.18 ; sys_platform == 'darwin'",
]
```

Document `pip install -e ".[dev,prefilter]"` in `AGENTS.md` under the Environment section (this is the one cross-PR doc touch this PR must do).

**Tests.**

- `test_stage_b_setfit_train.py` — trains on a tiny fixture and produces a SetFit model directory.
- `test_stage_b_setfit_predict.py` — loaded model returns probabilities; deterministic across two predict calls.
- Gated with `@pytest.mark.slow` and only run when the `prefilter` extra is installed (use `pytest.importorskip("setfit")`).

**Acceptance criteria.**

- A/B on validation: report whichever Stage B variant wins by F1 at the recall-floor threshold; the loser is not the default but stays available.
- Existing Naive Bayes tests still pass.

**Out of scope.**

- LoRA fine-tuning (LPF-PR-11).

---

### LPF-PR-06 — Stage C: embedding similarity (centroid + FAISS kNN)

**Goal.** Implement Stage C using `intfloat/multilingual-e5-large` embeddings with both centroid-cosine and FAISS-kNN-max-cosine signals.

**Scope.**

- Add `src/denbust/prefilter/stage_c.py`:

  ```python
  class StageCScorer:
      def __init__(self, embedder: SentenceEmbedder, centroid_path: Path, faiss_index_path: Path,
                   sigmoid_temperature: float) -> None: ...
      def evaluate(self, candidate: CandidateView, pass_kind: Literal["thin","thick"]) -> StageScore: ...

  def rebuild_positive_artifacts(labels_path: Path, out_dir: Path) -> StageCArtifactMeta: ...
  ```

- `SentenceEmbedder` is a thin wrapper around `sentence_transformers.SentenceTransformer`; loads with `device="mps"` when available, else `"cpu"`.

- Embed positives once at retrain time:
  - Compute embeddings of `(title + " " + snippet)` for every `label="positive"` row in train split.
  - Centroid = mean (L2-normalized after averaging).
  - FAISS HNSW index built on the positives (`faiss.IndexHNSWFlat(dim, M=32)`).

- At inference time:
  - Embed candidate's `(title + " " + snippet)` (thin) or first 512 tokens of body (thick).
  - `s = max(cosine(emb, centroid), kNN_max_cosine(emb, faiss_index, k=5))`.
  - `p_positive = sigmoid(s / sigmoid_temperature)`. `sigmoid_temperature` fitted on val pairs in `calibration.py`.
  - `p_negative = 1 - p_positive`.

- For thick pass, truncate the body to the first 512 BPE tokens of the underlying tokenizer (`embedder.tokenizer.model_max_length`).

- Wire `StageCScorer` into the orchestrator only for the thick pass by default. Add a config flag to optionally enable it in the thin pass (default `false`).

**Dependencies.** Already in `prefilter` extras (LPF-PR-05).

**Tests.**

- `test_stage_c_artifacts.py` — `rebuild_positive_artifacts` produces a centroid `.npy` and a FAISS index file; both round-trippable.
- `test_stage_c_predict.py` — a candidate identical to a positive example scores `p_positive > 0.9`; a random Hebrew Wikipedia paragraph scores `p_positive < 0.3`. Use 5 known positives + 5 known negatives in fixtures.
- `test_stage_c_truncation.py` — thick-pass scoring truncates correctly; no exception on very long bodies.

**Acceptance criteria.**

- Stage C inference latency: ≤ 30 ms median per candidate at batch size 32 on MPS.
- On validation, calibrated `θ_C` yields recall ≥ 0.99 and end-to-end (A + B + C) drop rate ≥ 65% of validation negatives.

**Out of scope.**

- Cross-encoder reranking (Method M8 in the design report — explicitly skipped).

---

### LPF-PR-07 — Stage D: local SLM judge via MLX (logprob scoring)

**Goal.** Implement Stage D using DictaLM-2.0-Instruct (default) or Qwen2.5-7B-Instruct (fallback) via MLX, scoring `p("כן" | prompt)` vs `p("לא" | prompt)`.

**Scope.**

- Add `src/denbust/prefilter/stage_d.py`:

  ```python
  @dataclass(frozen=True)
  class StageDPrompt:
      version: str
      system: str
      user_template: str       # contains {title} and {body} placeholders
      yes_token: str           # "כן"
      no_token: str            # "לא"

  class SlmJudge:
      def __init__(self, model_id: str, backend: Literal["mlx","llama_cpp"],
                   prompt: StageDPrompt, batch_size: int, timeout_seconds: float,
                   temperature: float) -> None: ...
      def evaluate(self, candidate: CandidateView, body: str | None) -> StageScore: ...
      def evaluate_batch(self, items: list[tuple[CandidateView, str | None]]) -> list[StageScore]: ...
  ```

- MLX backend implementation (`stage_d_mlx.py` private module):
  - Load model with `mlx_lm.load(model_id)`.
  - Construct the chat prompt using the model's tokenizer chat template, ending exactly at the position where the assistant should emit `כן` or `לא`.
  - Forward-pass once; read logits at the answer position.
  - Compute `p_positive = softmax([logit(tok("כן")), logit(tok("לא"))])[0]`.
  - Apply `temperature` (default 1.0; tunable via `calibration.py`).
  - Hard timeout via `concurrent.futures.ThreadPoolExecutor.submit(...).result(timeout=...)`. On timeout, return a `StageScore` with `reason="stage_d_timeout"` and `p_negative=0.5` (i.e. do not drop).

- llama.cpp fallback backend: defer the actual implementation but reserve the codepath. If `backend="llama_cpp"` is configured, raise `NotImplementedError` until LPF-PR-11.

- Prompt template (pin `version="2026-05-21-v1"`):

  ```
  system:
    אתה מסייע סיווג עברי. ענה רק "כן" או "לא".
  user:
    האם הכתבה הבאה היא ידיעה חדשותית מישראל על אכיפה נגד זנות, סחר בבני אדם, או תעשיית המין?
    כותרת: {title}
    קטע: {body}
    תשובה (כן/לא):
  ```

  Stored as a frozen `StageDPrompt`; persisted to `models/stage_d/prompt_v<version>.json` for auditability.

- Wire `SlmJudge` into the orchestrator's thick pass only (Stage D never runs in the thin pass).

- Add a circuit-breaker in `cascade.py`: if Stage D timeouts exceed 10% over the last 100 calls, the cascade skips Stage D for the next 5 minutes and logs a warning. Skipped candidates pass through to Claude.

**Dependencies.** `mlx-lm>=0.18` (already in `prefilter` extras).

**Tests.**

- `test_stage_d_prompt_versioning.py` — `StageDPrompt` round-trips through `json.dumps(asdict(...))`.
- `test_stage_d_mlx_smoke.py` — `@pytest.mark.slow` `@pytest.mark.requires_mlx`; loads the model and scores 3 known positives + 3 known negatives; positives score `p_positive > 0.6`, negatives `p_positive < 0.4`. Use a small model variant (`dictalm2.0-instruct-Q4_K_M-MLX` or whatever is available); document the exact model id in `AGENTS.md`.
- `test_stage_d_timeout.py` — patching the MLX call to sleep > timeout returns a `StageScore` with `reason="stage_d_timeout"` and `p_negative=0.5`.
- `test_stage_d_circuit_breaker.py` — forcing 11 consecutive timeouts triggers the circuit breaker and the next call returns `reason="stage_d_skipped_circuit_breaker"`.

**Acceptance criteria.**

- Stage D inference latency: ≤ 800 ms median per candidate at batch_size=4 on M4 Max with DictaLM-2.0 Q4. (Measured in `tests/integration/prefilter/test_stage_d_latency.py`, gated with `@pytest.mark.slow`.)
- Stage D recall on a 100-row labeled subset: ≥ 0.99 at the calibrated threshold.

**Out of scope.**

- llama.cpp backend implementation.
- Fine-tuning the SLM.

---

### LPF-PR-08 — Cascade orchestrator + pipeline integration

**Goal.** Implement the full cascade orchestrator with calibrated per-stage thresholds, and insert it into `scrape_queue.py` (thin pass) and `news_items/ingest.py` (thick pass). Default config still ships with `mode: off`.

**Scope.**

- Replace the stub `CascadeOrchestrator` with the real implementation:

  ```python
  class CascadeOrchestrator:
      def __init__(self, config: PrefilterConfig, stages: CascadeStages,
                   writer: PrefilterDecisionWriter, config_hash: str) -> None: ...

      def evaluate_thin(self, candidate: CandidateView) -> PrefilterDecision: ...
      def evaluate_thick(self, candidate: CandidateView, body: str) -> PrefilterDecision: ...
  ```

- Sequencing:
  - Thin: A → B (title+snippet model). If `verdict=="drop"` and `mode==enforce`, the caller drops the candidate from the scrape queue. Telemetry is always recorded.
  - Thick: A (recheck on canonical domain) → B (body model) → C → D. Same drop semantics.
- Each stage's `evaluate` returns a `StageScore`; orchestrator builds the `PrefilterDecision` tuple and decides verdict.
- If any stage raises, log a structured warning and record a `StageScore` with `reason="stage_<x>_error: <type>"` and `p_negative=0.5`; do not propagate.

- Add `src/denbust/prefilter/calibration.py`:
  - `denbust prefilter calibrate --stage {a,b,c,d} --recall-floor 0.99` sweeps thresholds and updates `models/<stage>/threshold.json`.
  - At orchestrator init, prefer `threshold.json` over the value in `PrefilterConfig` if both exist; log which source is used.

- Pipeline integration:
  - `src/denbust/discovery/scrape_queue.py`: at the candidate-selection boundary, build a `CandidateView` and call `orchestrator.evaluate_thin(...)`. If `mode == enforce` and `verdict == "drop"`, transition the candidate's status to `suppressed` with `suppression_reason="prefilter_thin"` and skip it. Otherwise enqueue as usual.
  - `src/denbust/news_items/ingest.py`: after the scrape returns article text and before the Claude classifier call, call `orchestrator.evaluate_thick(...)`. If `mode == enforce` and `verdict == "drop"`, persist a `provisional_internal_only` row with `prefilter_dropped=true` and **do not** call Claude. Otherwise proceed to Claude.

- Wire `prefilter_summary.json` writes alongside existing `classifier_summary.json` / scrape summary writers.

**Dependencies.** None new.

**Tests.**

- `test_orchestrator_thin.py` / `test_orchestrator_thick.py` — both endpoints return decisions with the documented stage sequence; an early-stop at Stage A short-circuits.
- `test_orchestrator_stage_error.py` — a stage raising an exception yields a logged warning and `verdict="pass"` overall.
- `test_orchestrator_mode_off.py` — in `mode=off`, decisions are still produced but no pipeline action is taken.
- `test_orchestrator_mode_shadow.py` — in `mode=shadow`, decisions are produced and `drop` decisions do not actually drop candidates from the scrape queue or Claude queue (integration test against an in-memory queue stub).
- `test_orchestrator_mode_enforce.py` — in `mode=enforce`, `drop` decisions remove candidates from the scrape queue and skip Claude invocation.
- Existing scrape-queue and ingest tests still pass.

**Acceptance criteria.**

- A bounded end-to-end run with `mode=shadow` on a fixture state-repo produces a non-empty `prefilter_summary.json` with per-stage drop counts.
- No regression in classifier output count when `mode=off`.

**Out of scope.**

- Switching the default mode to `shadow` (done in LPF-PR-09).

---

### LPF-PR-09 — Shadow-mode telemetry harness + Supabase migration

**Goal.** Promote the default `mode` to `shadow` for `agents/news/local_search.yaml`; persist `PrefilterDecision` to Supabase; add a `denbust prefilter shadow-report` command for daily auditing.

**Scope.**

- Add a Supabase migration `migrations/<timestamp>_prefilter_decisions.sql`:

  ```sql
  create table if not exists prefilter_decisions (
    decision_id        uuid primary key default gen_random_uuid(),
    candidate_id       text not null,
    pass_kind          text not null check (pass_kind in ('thin','thick')),
    verdict            text not null check (verdict in ('pass','drop')),
    stopped_at_stage   text not null,
    stage_scores       jsonb not null,
    decided_at         timestamptz not null,
    config_hash        text not null,
    inserted_at        timestamptz not null default now()
  );
  create index if not exists prefilter_decisions_candidate_idx on prefilter_decisions (candidate_id);
  create index if not exists prefilter_decisions_decided_at_idx on prefilter_decisions (decided_at);
  ```

- Extend `PrefilterDecisionWriter` to optionally push to Supabase using the existing operational-store abstraction. If Supabase is unavailable, continue writing JSONL only (no run failure).

- Add `denbust prefilter shadow-report --since <iso> --until <iso>` that:
  - aggregates per-stage drop counts,
  - computes recall on the labeled subset (joining `prefilter_decisions` × `labels.parquet`),
  - emits a markdown report under `data/<dataset>/<job>/prefilter/reports/<run_id>.md`.

- Bump default `prefilter.mode` to `shadow` in `agents/news/local_search.yaml` (only — leave other configs untouched).

**Dependencies.** None new.

**Tests.**

- `test_writer_supabase_optional.py` — writer skips Supabase when the operational store is null.
- `test_shadow_report_recall.py` — fixture-based recall computation matches a hand-computed expected value.
- Integration: existing `agents/news/local.yaml` tests must still pass with the default `prefilter` block, which means `local.yaml` does **not** opt into `shadow` (keeps `mode: off`).

**Acceptance criteria.**

- A 7-day shadow window on local data produces a report with non-trivial drop counts and a recall metric ≥ 0.98 on the labeled subset. (This may be demonstrated in the PR description rather than CI.)
- The Supabase migration is idempotent (`create if not exists`); rerunning the migration is a no-op.

**Out of scope.**

- Switching default to `enforce` (operator action, not a code change).

---

### LPF-PR-10 — Calibration tooling + golden-set regression CI

**Goal.** Lock the cascade into a measurable, regression-tested state. Introduce a frozen golden evaluation set, a CI job that runs `denbust prefilter evaluate --split test` against it, and a fail-on-recall-drop assertion.

**Scope.**

- Add `tests/golden/prefilter/golden_eval.parquet` — a small (≤ 2 000 row) curated subset of the labels parquet, stratified, committed to the repo. This is the only labeled artifact we commit.
- Add a `denbust prefilter evaluate --golden tests/golden/prefilter/golden_eval.parquet` mode that:
  - loads the golden parquet,
  - runs the full cascade (Stages A–D) with model artifacts from the configured model cache,
  - asserts `recall >= 0.98`, `drop_rate >= 0.5`, both configurable.
- Wire into `.github/workflows/ci-test.yml` as a job `prefilter-eval` that runs only when files under `src/denbust/prefilter/` or `tests/golden/prefilter/` change. The job:
  - installs `.[dev,prefilter]`,
  - downloads pinned model artifacts to a cache step,
  - runs the evaluate command,
  - fails the build on the assertion.
- Document the calibration workflow in a new `docs/local_prefilter_calibration.md` (operator-facing): how to retrain, how to recalibrate, how to bump the golden set.

**Dependencies.** None new.

**Tests.**

- `test_evaluate_golden_pass.py` — synthetic small golden parquet with known optimal threshold passes the assertion.
- `test_evaluate_golden_fail.py` — synthetic case where a stage is broken fails the assertion.

**Acceptance criteria.**

- CI `prefilter-eval` job is green on `main` at merge time.
- The doc explains the threshold-sweep methodology and how to refresh model artifacts.

**Out of scope.**

- Adding more eval datasets (one is enough to lock the contract).

---

### LPF-PR-11 — Claude-distilled student model (LoRA on DictaBERT)

**Goal.** Train a LoRA adapter on top of DictaBERT-large, using past Claude classifications as labels, to act as an even stronger Stage B replacement.

**Scope.**

- Add `src/denbust/prefilter/stage_b_distilled.py` selectable via `config.prefilter.stages.b.model: distilled_dictabert`.
- Training pipeline (`denbust prefilter retrain --stage b --kind distilled_dictabert`):
  - Pull all `label_source=claude_classifier` rows from `labels.parquet`.
  - Tokenize with the DictaBERT tokenizer.
  - Train a LoRA adapter (`peft` library; `r=8`, `alpha=16`, `dropout=0.1`); freeze the base model.
  - Persist as `models/stage_b/distilled_dictabert/<version>/`.
- llama.cpp backend for Stage D unlocks here if the operator wants a non-MLX path.

**Dependencies.** Add to `prefilter` extras: `transformers>=4.45`, `peft>=0.13`, `accelerate>=1.0`, `bitsandbytes` only if explicitly opted into via a separate `prefilter-cuda` extra (we do not enable on Apple Silicon by default).

**Tests.**

- Mark as `@pytest.mark.slow @pytest.mark.requires_torch`. Train on 200 fixture rows for 1 epoch; assert convergence (loss decreases monotonically over the first 5 steps).
- A/B against Naive Bayes + SetFit on the golden eval; if distilled wins by F1, document that in the PR description but do **not** change the default model.

**Acceptance criteria.**

- Cascade still passes the LPF-PR-10 golden assertion with `distilled_dictabert` selected.
- Operator can choose between three Stage B implementations via config.

**Out of scope.**

- Distilling Stages C or D.

---

### LPF-PR-12 — Active learning loop + BERTopic cluster filter

**Goal.** Two operator-facing tools that improve the labeled dataset (not the runtime cascade):

1. **Active learning**: surface highest-uncertainty candidates in the triage workbench.
2. **BERTopic cluster filter**: cluster unreviewed candidates with multilingual embeddings; surface candidate cluster-level exclusions to the operator.

**Scope.**

- Active learning:
  - Add `denbust prefilter rank-for-review --top-n 200` that scores all unreviewed candidates with Stage B, sorts by `|p - 0.5|` ascending (most uncertain first), writes a list to `data/<dataset>/<job>/prefilter/review_queue/<utc_date>.jsonl`.
  - Extend `triage_app/serve.py` to load this file when it exists and prioritize it in the unreviewed view (behind a `?queue=active` query param).
- BERTopic cluster filter:
  - Add `denbust prefilter cluster --min-cluster-size 30` that runs BERTopic on unreviewed candidates' title+snippet embeddings; outputs a markdown report listing each cluster, its top 5 representative titles, and the fraction of triaged-excluded items in the cluster.
  - The report is operator-facing; bulk-exclude decisions remain a human action via the triage workbench.

**Dependencies.** Add `bertopic>=0.16` and `hdbscan>=0.8` to the `prefilter` extras.

**Tests.** Lightweight — only smoke tests that the commands run and produce well-formed outputs on a tiny fixture.

**Acceptance criteria.**

- Both CLIs run on a fixture state-repo in < 60 s.
- Triage workbench loads the active-learning queue when present and falls back to the default ordering otherwise.

**Out of scope.**

- Anything that drops candidates automatically (this PR is operator-assist only).

---

## 5. Cross-cutting test plan

- **Unit tests** live under `tests/unit/prefilter/`. Every PR adds its tests there.
- **Integration tests** live under `tests/integration/prefilter/`. Add a single integration test in LPF-PR-08 that exercises the orchestrator end-to-end against a fixture scrape queue + fake Claude.
- **Slow / model-dependent tests** are marked `@pytest.mark.slow` and excluded from the default `pytest -q` run; CI has a separate job `pytest -q -m slow` that runs them when prefilter files change.
- **Golden regression** test gate is the LPF-PR-10 CI job.

## 6. Operational rollout

| Day | Action |
| --- | --- |
| Day 0 | Merge LPF-PR-01 through LPF-PR-08 sequentially. All ship with `mode: off`. |
| Day 1 | Merge LPF-PR-09. `agents/news/local_search.yaml` flips to `mode: shadow`. |
| Day 1–8 | Run regular backfill / discovery / scrape / ingest. Inspect `denbust prefilter shadow-report` daily. |
| Day 8 | If shadow recall on labeled subset ≥ 0.98 and Claude-drop rate is meaningful, operator flips `mode: enforce` in the config and commits the change. Document the attestation in `docs/local_prefilter_cascade_design.md` change log. |
| Ongoing | Weekly retrain via `denbust prefilter retrain` cron; weekly recalibrate via `denbust prefilter calibrate`. |

## 7. Rollback plan

- The cascade is a single config flag: `mode: enforce → mode: off` (or `shadow`) restores baseline behaviour without code changes.
- If a Supabase migration causes operational issues, the JSONL fallback continues to function; revert the migration via a follow-up SQL.
- Model artifacts are versioned under `models/<stage>/<version>/`; the orchestrator always loads the latest version unless `prefilter.stages.<x>.model_version` overrides. Pinning to an older version is a config-only revert.

## 8. Estimated effort

| PR | Complexity | Estimate |
| --- | --- | --- |
| LPF-PR-01 Foundation | Low | 4 h |
| LPF-PR-02 Label assembly | Medium | 6 h |
| LPF-PR-03 Stage A | Medium | 6 h |
| LPF-PR-04 Stage B (NB) | Medium | 6 h |
| LPF-PR-05 Stage B (SetFit) | Medium | 6 h |
| LPF-PR-06 Stage C | Medium | 6 h |
| LPF-PR-07 Stage D | High | 10 h |
| LPF-PR-08 Orchestrator + integration | High | 10 h |
| LPF-PR-09 Shadow + Supabase | Medium | 6 h |
| LPF-PR-10 Calibration + CI gate | Medium | 6 h |
| **Total (core, LPF-PR-01..10)** | | **~66 h** |
| LPF-PR-11 Claude distill | High | 12 h (optional) |
| LPF-PR-12 Active learning + clusters | Medium | 8 h (optional) |

## 9. Cross-document touch list

When opening each PR, also update:

- `.agent-plan.md` — flip the relevant Task Ledger entry from `[later]` to `[next]` when starting; to `[done]` on merge. Keep exactly one `[next]` item.
- `docs/IMPLEMENTATION_PLAN.md` — tick the corresponding task in Phase 3.
- `README.md` — only for LPF-PR-08 (insertion points become user-visible) and LPF-PR-09 (default mode changes for the local search config).
- `docs/local_prefilter_cascade_design.md` — append a dated entry under a new "Change log" section when calibration thresholds, default models, or recall floors change in production.
