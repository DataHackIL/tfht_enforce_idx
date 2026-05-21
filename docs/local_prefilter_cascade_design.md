# Local Pre-Classification Filter Cascade — Design Report

> Planning identifier prefix: `LPF-PR-XX` (Local Pre-Filter).
> Companion implementation plan: [docs/local_prefilter_cascade_implementation_plan.md](./local_prefilter_cascade_implementation_plan.md).

## 1. Executive summary

This document proposes inserting a **local, non-LLM-API-based filtering cascade** between the existing discovery/triage layer and the Claude-Sonnet relevance classifier, in order to drop high-confidence true negatives before they consume paid LLM budget. The cascade is composed of four stages of increasing compute cost and decreasing throughput; each stage is independently calibrated to a high-recall threshold (target ≥ 99% recall on a held-out validation set) so that no stage on its own can drop a true positive that downstream Claude would have flagged as relevant.

The four stages, lightest-first, are:

| Stage | Method | Latency / candidate | Purpose |
| --- | --- | --- | --- |
| **A** | Scored lexicon + domain reputation posterior | < 100 µs | Reject obvious blacklist hits and historically empty domains |
| **B** | Trained text classifier (Naive Bayes / TF-IDF or SetFit) | ~1–5 ms | Reject candidates a small supervised model is confident about |
| **C** | Multilingual sentence-embedding similarity to known positives | ~10–30 ms | Reject candidates topically far from any known positive |
| **D** | Local SLM (DictaLM-2.0 7B or Qwen2.5-7B-Instruct) via MLX, scored by token logprobs | ~300–800 ms | Last-line semantic check before Claude |

Expected outcome at steady state: **70–90% reduction in Claude calls** on noise candidates, with held-out recall preserved within a configurable tolerance of the no-cascade baseline. The cascade runs entirely on the local MacBook Pro M4 Max; no external API spend.

This report:

- frames the design constraints (§ 2–4),
- enumerates 25 candidate methods we evaluated (§ 5),
- specifies the recommended cascade architecture in detail (§ 6),
- documents what is intentionally out-of-scope (§ 7),
- lists open questions and risks (§ 8),
- defines success metrics (§ 9).

The implementation plan in the companion document breaks the build into ten merge-sized PRs (`LPF-PR-01` through `LPF-PR-10`) plus two optional follow-ups.

---

## 2. Problem statement

The current pipeline is:

```
discover (search engines + source-native) → triage → scrape_candidates → ingest (Claude classifier) → release
```

The `ingest` step calls **Claude Sonnet** on each scraped article and pays per-token cost. The discovery layer is intentionally high-recall: it pulls in tens of thousands of candidates per backfill window, the majority of which are not actually about prostitution / trafficking / sex-work enforcement in Israel. Today only two filters sit in front of Claude:

1. The keyword/domain blacklists in `src/denbust/discovery/candidate_filters.py` (binary, hand-curated).
2. The manual triage workbench in `triage_app/` (slow, requires human attention).

That leaves a long tail of obviously-irrelevant candidates flowing into Claude — fundable but wasteful. We have ~14.6 k manually-or-auto excluded candidates and only ~50 explicitly-prioritized positives in the current state-repo snapshot; past Claude classifications in the operational store add more labels on both sides.

**Goal:** an automated pre-LLM filter that meaningfully reduces the Claude-bound candidate volume while preserving recall on the (small) positive class, that runs entirely on local hardware, and that is auditable and incrementally improvable.

**Non-goals:**

- Replacing Claude as the final classifier.
- Replacing the human triage workbench (it stays useful for label generation and edge-case review).
- Running anything non-local (no OpenAI, no remote inference).

---

## 3. Pipeline context

Insertion point: **between `scrape_candidates` and `ingest`**, conceptually:

```
discover → triage → scrape_candidates → [local_prefilter (NEW)] → ingest (Claude) → release
```

Two questions follow:

1. **Pre-scrape or post-scrape?** Pre-scrape we have only title, snippet, domain, URL, and the search query that found the candidate. Post-scrape we additionally have the full article text. We will run the cascade in **both** positions: a "thin" pass on pre-scrape signals (Stages A + B) inside the existing scrape-queue selection, and a "thick" pass on full article text (Stages C + D) after a successful scrape. Pre-scrape filtering also saves scrape bandwidth and queue capacity; post-scrape filtering catches noise that title/snippet hide.
2. **Hard reject or score?** Each stage produces a **calibrated probability** of being a true negative; the cascade orchestrator drops only candidates whose probability exceeds a per-stage threshold tuned to a recall floor on a held-out validation set. Borderline candidates pass through to the next stage. Only Stage D's threshold is the final guard before Claude.

The cascade exposes a `PrefilterDecision` to the rest of the pipeline:

```python
@dataclass(frozen=True)
class PrefilterDecision:
    verdict: Literal["pass", "drop"]
    stopped_at_stage: Literal["A", "B", "C", "D", "passed_all"]
    stage_scores: dict[str, float]   # per-stage P(negative)
    stage_thresholds: dict[str, float]
    reason: str                       # human-readable, e.g. "stage_A:domain_reputation=0.992>=0.95"
    model_versions: dict[str, str]    # which trained/snapshot models participated
```

Decisions are written to the operational store as a new `prefilter_decisions` table (or state-repo JSONL in local mode) so we can audit and recompute downstream metrics.

---

## 4. Design constraints

### 4.1 Signals

| Source | Where available | Useful for |
| --- | --- | --- |
| Title | pre + post-scrape | All stages |
| Snippet | pre + post-scrape | Stages A–D |
| Domain (eTLD+1 and host) | pre + post-scrape | Stage A reputation; Stage B features |
| URL path + query string | pre + post-scrape | Stage A heuristics |
| Originating search query | pre + post-scrape | Stage B as a feature |
| Producer kind (source-native vs search) | pre + post-scrape | Cascade routing |
| Full article body | post-scrape only | Stages B–D "thick" pass |
| Byline, publish date, JSON-LD metadata | post-scrape only | Optional features for Stage B |
| Historical scrape attempts on this URL | both | Tie-breaker for ambiguous cases |

### 4.2 Recall / precision asymmetry

Dropping a true positive is materially worse than passing a true negative through to Claude: positives are rare (estimated 1–5% of post-triage candidates), each one matters for the dataset, and Claude is fast enough on a single candidate that some excess pass-through is tolerable. Therefore every stage is **threshold-tuned for high recall**, accepting modest precision. We target ≥ 99% recall per stage at first; the orchestrator's combined-recall target is ≥ 98% vs. the current Claude-only baseline on a held-out evaluation set.

### 4.3 Labeled data inventory

| Source | Approximate count | Quality |
| --- | --- | --- |
| Manual triage `exclude` decisions (`triage_decisions.jsonl`) | ~14,600 | High (human-curated) but biased toward obvious noise — they reflect the current blacklists |
| Manual triage `prioritize` decisions | ~50 | High |
| Past Claude classifier outputs (`news_items` operational store) | Several hundred to low thousands; needs counted | High; closest proxy for "what Claude would do" |
| Auto-exclude via current `_EXCLUDED_TITLE_TERMS` / `_IRRELEVANT_CONTENT_DOMAINS` | Several thousand | Lower — same lexical-rule decision the cascade would re-learn |

`LPF-PR-02` consolidates these into a single canonical labeled-candidates dataset (`labels.parquet` under the state repo), de-duplicated and stratified for train/val/test splits. Stage models train only on this dataset; the cascade evaluates on the held-out test split.

### 4.4 Hardware budget

Target machine: **Apple MacBook Pro, M4 Max, ≥ 64 GB unified memory**.

Reference capabilities we plan around:

- Sentence-transformer batch embeddings on MPS / MLX: ≥ 10 k titles/sec.
- 1.5–3 B parameter SLMs (Q4 or Q5 quantized via MLX): 100–500 tokens/sec single-batch.
- 7 B SLMs (Q4 quantized via MLX): 30–80 tokens/sec; logprob extraction is single-forward-pass, so cost is dominated by prompt length.
- BERT-class fine-tuning with LoRA (DictaBERT / AlephBERT): minutes per epoch on a 10 k-row dataset.

We rule out anything requiring CUDA-specific libraries or large remote training jobs.

### 4.5 Hebrew-language constraints

This is a **Hebrew-language** corpus. Several modeling choices are constrained:

- Hebrew BERT family options: AlephBERT (BGU), HeBERT (TAU), DictaBERT (DICTA, large variant is the current SOTA for Hebrew). Default to **DictaBERT-large** when a Hebrew encoder is needed.
- Hebrew sentence-embedding options: `intfloat/multilingual-e5-large` and `BAAI/bge-m3` both have good Hebrew transfer. Default to **`multilingual-e5-large`** for compactness and well-known calibration behaviour.
- Hebrew SLM options: **DictaLM-2.0-Instruct (7 B)** for in-language reasoning; **Qwen2.5-7B-Instruct** as a strong multilingual fallback. Run via **MLX** to use the M4 GPU. Avoid models known to have very poor Hebrew (e.g. Phi-3.5-mini).
- Avoid sklearn whitespace-only tokenization where possible; prefer character n-grams (3–5) or HebPipe for robustness to Hebrew morphology.

---

## 5. Method survey

The 25 methods we evaluated, grouped into five tiers. For each we note: how it works, the data it needs, an order-of-magnitude latency on M4 Max, and whether we adopt it. The cascade in § 6 incorporates the methods marked **Adopted**.

### Tier 1 — Cheap and immediate

**M1. Scored lexicon with calibrated thresholds. _Adopted (Stage A)._**
Replace the current binary keyword/domain blacklists with a scored lexicon: each term carries a weight equal to `log P(exclude | term)` measured on the labeled dataset. Sum log-weights across hits; threshold on a calibrated probability. Pure Python, microsecond per candidate. Extends but does not replace `_EXCLUDED_TITLE_TERMS`.

**M2. Domain reputation posterior. _Adopted (Stage A)._**
For every domain, compute `Beta(α, β)` posterior over the per-domain probability of being a true positive, using labels from past triage + Claude outcomes (Laplace prior `Beta(1,1)`). Reject domains whose 95% upper-credible bound is below a chosen positive-rate floor and whose evidence count exceeds `n_min`. Naturally widens to no-reject for unseen domains.

**M3. URL / path heuristics. _Adopted (Stage A, as one channel)._**
Tiny rule set on URL shape: tag-index pages, category landing pages, paginated archives, file extensions, query-string complexity. Implemented as a hand-written function that returns a probability of being non-article. Cheap, interpretable.

**M4. Multinomial Naive Bayes on character n-grams (3–5). _Adopted (Stage B default)._**
Robust to Hebrew morphology without tokenization. Trains in seconds. Calibrated via Platt scaling. Sub-millisecond inference. Best ROI / effort on this list.

**M5. TF-IDF + linear SVM or logistic regression. _Adopted as Stage B alternative._**
Equivalent compute footprint to M4 but uses word-level features. Useful as an ensemble channel alongside M4 if either ends up under-performing alone.

### Tier 2 — Embedding-based (no training)

**M6. Multilingual sentence embeddings + centroid similarity. _Adopted (Stage C)._**
Embed title + snippet with `intfloat/multilingual-e5-large`; cosine-compare to a centroid built from known-positive titles. Drop candidates below threshold. Default Stage C model.

**M7. Hebrew-specialized BERT embeddings + classifier head. _Considered, deferred._**
DictaBERT-large embeddings + MLP head trained on labels. Likely higher quality than M6 for Hebrew, but adds a per-stage model artifact and depends on a strong fine-tuning loop. Park behind `LPF-PR-11` distillation work.

**M8. Cross-encoder reranker. _Rejected for cascade._**
A cross-encoder scoring (canonical_query, candidate) is too slow at our throughput (~100 ms/candidate × tens of thousands). May resurface as a borderline-case rescorer if Stage D proves insufficient.

**M9. FAISS dense retrieval over known-positives. _Adopted as Stage C variant (k-NN max-similarity)._**
Build a FAISS index of all known-positive titles; for each candidate, take `max_sim` over top-k. Different signal from centroid (M6). Either M6 or M9 can serve as Stage C; we implement both and pick by validation.

**M10. Multilingual NLI zero-shot classifier. _Considered, deferred._**
`MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli` scoring "the article is about prostitution-enforcement news in Israel" as entailment. Off-the-shelf, no training. Defer — its compute footprint (~50 ms/candidate) overlaps Stage C with worse calibration than M6/M9.

### Tier 3 — Local SLMs as zero/few-shot classifiers

**M11. Qwen 2.5 1.5B / 3B Instruct via MLX. _Considered, used inside SetFit comparison._**
Tiny SLM, sub-second per candidate. Probably under-powered for Hebrew nuance but worth a baseline.

**M12. Phi-3.5-mini or Gemma 2 2B. _Rejected._**
Both have weak Hebrew. Skip.

**M13. DictaLM-2.0-Instruct (7 B). _Adopted (Stage D default)._**
Hebrew-specialized 7 B instruction-tuned model from DICTA. Best per-parameter Hebrew comprehension we can run locally. Run via MLX (or llama.cpp with Metal) at Q4.

**M14. Qwen 2.5 7B Instruct via MLX. _Adopted (Stage D fallback)._**
Strong multilingual SLM. Use as a fallback if DictaLM-2.0 is unavailable or under-performs.

**M15. SLM-as-judge using token logprobs, not generation. _Adopted (Stage D inference convention)._**
For Stage D, compute `logprob("כן" | prompt)` vs. `logprob("לא" | prompt)` after a fixed prompt that ends in the answer slot; convert to a calibrated probability via temperature-scaled sigmoid. This is the only stable way to use an SLM as a binary classifier. Implementation note: requires an MLX or llama.cpp backend that exposes per-token logits at the answer position.

### Tier 4 — Trained / fine-tuned

**M16. SetFit on multilingual-e5. _Adopted as Stage B+ option._**
Contrastive fine-tuning of a sentence encoder with a small labeled set. Trains in minutes. Often beats LLM zero-shot for narrow domains. Bundled into `LPF-PR-05` as an A/B against the M4 baseline.

**M17. LoRA fine-tune on DictaBERT / AlephBERT. _Adopted (LPF-PR-11)._**
Fine-tune a small LoRA adapter on a Hebrew BERT, using the consolidated labels. Expected to dominate every zero-shot option. Deferred to LPF-PR-11 because it needs the dataset assembly (LPF-PR-02) to be solid first.

**M18. Distill Claude into a local student. _Adopted (LPF-PR-11)._**
Mine all past Claude classifications as soft labels, fine-tune the LoRA from M17 on those soft labels. Builds a local model whose decision boundary mimics Claude's. One-time cost, perpetual savings. Shipped together with M17.

**M19. XGBoost / LightGBM on engineered features. _Considered, deferred._**
Tabular boosted-tree blender across (text-feature scores, domain rep, URL features, length stats). Strong default for heterogeneous signal. Defer to the orchestrator step (`LPF-PR-08`); for now the orchestrator combines stage probabilities through a simple cascade rather than a learned blender.

**M20. Stacked ensemble (logistic-regression blender). _Adopted (LPF-PR-08, optional)._**
Once the cascade is running in shadow mode, fit a small logistic blender on stage scores to optionally replace the per-stage thresholds with a single calibrated combined score. Use as a tuning-time aid, not a hard requirement.

### Tier 5 — Structural / clever

**M21. Topic modeling for cluster-level filtering. _Adopted (LPF-PR-12)._**
Run BERTopic over unreviewed candidates with multilingual embeddings; inspect clusters dominated by triaged-excluded items; surface candidate cluster-level exclusions to the operator. This is an offline operator-assist, not part of the runtime cascade.

**M22. Active learning loop. _Adopted (LPF-PR-12, lightweight version)._**
Use the Stage B classifier's uncertainty to drive what the manual triage UI surfaces next, so each manual decision maximally improves the dataset.

**M23. Weak supervision (Snorkel-style). _Rejected for first release._**
Powerful but heavyweight; we already have lots of labels and don't need to synthesize them. Reconsider only if labeled data quality plateaus.

**M24. Translate-then-classify cascade. _Rejected for first release._**
Adds latency without much value once we have Hebrew-native models in Stages C and D.

**M25. Post-scrape, pre-classify filter. _Adopted as the "thick" cascade pass._**
The cascade orchestrator already runs in both pre-scrape (Stages A + B only, on title/snippet/domain) and post-scrape (Stages A–D on full article text) positions, per § 3.

---

## 6. Recommended architecture

### 6.1 The cascade at a glance

```
              ┌──────────────────────────────────────┐
              │ scrape_candidates (existing)         │
              └──────────────────┬───────────────────┘
                                 │ candidate selected for scrape
                                 ▼
              ┌──────────────────────────────────────┐
              │ Thin pass (pre-scrape)               │
              │   Stage A: lexicon + domain rep      │
              │   Stage B: NB / SetFit on title+snip │
              │   → drop or scrape                   │
              └──────────────────┬───────────────────┘
                                 │ kept
                                 ▼
              ┌──────────────────────────────────────┐
              │ scrape page (existing)               │
              └──────────────────┬───────────────────┘
                                 │ article text available
                                 ▼
              ┌──────────────────────────────────────┐
              │ Thick pass (post-scrape)             │
              │   Stage A': domain rep recheck       │
              │   Stage B': NB on article body       │
              │   Stage C : embedding centroid / kNN │
              │   Stage D : DictaLM-2.0 logprob      │
              │   → drop or send to Claude           │
              └──────────────────┬───────────────────┘
                                 │ passed
                                 ▼
              ┌──────────────────────────────────────┐
              │ ingest (Claude Sonnet) (existing)    │
              └──────────────────────────────────────┘
```

Both passes write a `PrefilterDecision` to the operational store. The thick pass's decision takes precedence; the thin pass's decision is recorded for downstream telemetry even when overridden.

### 6.2 Stage specifications

#### Stage A — Lexicon + domain reputation + URL heuristics

**Inputs:** title, snippet, domain, URL.
**Output:** `p_negative_A ∈ [0, 1]`, plus a structured `reason` string.
**Model artifacts:**

- `lexicon.json` — list of `(term, weight)` entries.
- `domain_reputation.parquet` — `(domain, n, k, p_post_mean, p_post_upper_95)`.
- `url_heuristics.py` — hand-coded rule scorer.

**Decision rule:**

```
p_negative_A = 1 - clip(P_pos_lexicon * P_pos_domain * P_pos_url, ε, 1-ε)
drop if p_negative_A >= θ_A
```

`θ_A` calibrated to ≥ 99% per-stage recall on validation set. Typical θ around 0.95.

**Latency target:** < 100 µs per candidate, in-process.
**Refresh:** weekly batch job recomputes lexicon weights and domain posteriors from the latest labels.

#### Stage B — Trained text classifier

**Inputs:** title + snippet (thin pass), article body (thick pass).
**Output:** `p_negative_B`.
**Default model:** Multinomial Naive Bayes on character n-grams (3–5) with TF-IDF transform, Platt-scaled.
**Alternative (LPF-PR-05):** SetFit on `intfloat/multilingual-e5-large`, plus a logistic head.
**Model artifacts:** sklearn or `setfit_model/` directory pinned under `data/<dataset>/<job>/prefilter/models/stage_b/<version>/`.

**Decision rule:**

```
drop if p_negative_B >= θ_B
```

`θ_B` calibrated to ≥ 99% per-stage recall on validation set.

**Latency target:** < 5 ms per candidate.
**Refresh:** weekly batch retrain triggered by `denbust prefilter retrain --stage B`.

#### Stage C — Embedding similarity

**Inputs:** title + snippet (thin), full body truncated to 512 tokens (thick).
**Output:** `p_negative_C`.
**Default model:** `intfloat/multilingual-e5-large` via sentence-transformers.
**Method:** maintain two artifacts:

- `centroid.npy` — mean embedding of known-positive titles.
- `positives.index` — FAISS HNSW index of positive embeddings.

For each candidate, compute both `cos(emb, centroid)` and `max_k cos(emb, positives.index)`. Convert the larger of the two similarities to `p_positive_C` via a sigmoid fit on validation pairs; `p_negative_C = 1 - p_positive_C`.

**Decision rule:**

```
drop if p_negative_C >= θ_C
```

`θ_C` calibrated to ≥ 99% per-stage recall on validation set.

**Latency target:** < 30 ms per candidate at batch size 32; embeddings batch-friendly.
**Refresh:** centroid + FAISS index rebuilt whenever the labeled-positive set grows by ≥ 5%.

#### Stage D — Local SLM logprob judge

**Inputs:** title + first 512 tokens of body (thick pass only). Stage D does **not** run in the thin pass.
**Output:** `p_negative_D`.
**Default model:** `dicta-il/dictalm2.0-instruct` quantized to Q4 via MLX.
**Fallback model:** `Qwen/Qwen2.5-7B-Instruct` quantized to Q4 via MLX.
**Method:** fixed prompt template ending with the literal token slot for "כן" / "לא". Read logits at the answer position. Compute:

```
p_positive_D = softmax([logit("כן"), logit("לא")])[0]
p_negative_D = 1 - p_positive_D
```

Optionally temperature-scale via a small calibration set.

**Decision rule:**

```
drop if p_negative_D >= θ_D
```

`θ_D` calibrated to ≥ 99% per-stage recall on validation set. Stage D is the last gate before Claude, so the orchestrator may use a stricter `θ_D` if combined-recall metrics permit.

**Latency target:** < 1 s per candidate; can be batched 4–8 at a time on M4 Max via MLX.
**Refresh:** model artifact is downloaded once; the prompt template is the only versioned input. Bump prompt version with any change.

### 6.3 Calibration protocol

Per stage, on the held-out validation set, sweep θ from 1.0 down toward 0; pick the largest θ such that recall on the positive class is ≥ the per-stage recall floor (default 0.99). Record:

- `recall_at_threshold`
- `precision_at_threshold`
- `negative_drop_rate`
- `n_validation_positives`

These are persisted alongside the model artifact for auditability. Re-calibrate whenever a model artifact is retrained or whenever the validation set grows materially.

### 6.4 Telemetry

Each cascade run emits a structured record per candidate to:

- the operational store as a `prefilter_decisions` row (Supabase), and
- `data/<dataset>/<job>/prefilter/decisions/<run_id>.jsonl` in the state repo.

Aggregated per-batch summary written to `prefilter_summary.json` alongside existing scrape and classifier summaries, including: per-stage drop counts, per-stage recall on labeled subset, end-to-end pass rate vs. baseline.

### 6.5 Operational modes

The cascade ships with three operational modes, switched via config:

- **`off`** — cascade not invoked; pipeline is unchanged.
- **`shadow`** — cascade runs and records `PrefilterDecision` rows, but the orchestrator does not actually drop any candidate from the Claude queue. This is the **default** for the first three sprints after enabling, so we can measure recall/precision without losing data.
- **`enforce`** — cascade drops candidates as configured.

Switching from `shadow` to `enforce` requires an explicit operator action and an attestation that shadow-mode recall on the labeled subset is ≥ the enforcement recall floor for ≥ 7 days.

---

## 7. Out-of-scope / deferred

- **Cross-encoder reranking (M8).** Too slow at our throughput.
- **NLI zero-shot (M10).** Overlaps Stage C with worse calibration.
- **Translate-then-classify (M24).** Adds latency without clear gain over Hebrew-native models.
- **Weak supervision (M23).** Heavyweight for the labeling problem we have.
- **XGBoost blender as primary cascade (M19).** Cascade-with-thresholds is simpler to reason about; revisit blender as a stretch goal in `LPF-PR-08`.
- **Replacing Claude.** Out of scope — Claude remains the final classifier of record.
- **Multi-language coverage.** Cascade is Hebrew-first; if the dataset ever expands to other languages, Stages A–D need per-language model artifacts.

---

## 8. Open questions / risks

| Risk | Mitigation |
| --- | --- |
| Labeled-positive set is small (~50 prioritized + a few hundred Claude positives). Stage C centroid + Stage D calibration both depend on it. | `LPF-PR-02` mines all historical Claude positives, not just current state-repo. If still small, fall back to oversampling and trust Stage A/B more. |
| Stage D latency could starve scrape throughput if mis-sized. | Stage D runs only on candidates that survived A + B + C, so its volume is small. Add a hard timeout per candidate and a circuit-breaker that bypasses Stage D under load. |
| Calibration drifts over time as new domains/topics appear. | Weekly retrain job + a regression test on a frozen golden set that fails CI if recall drops below baseline. |
| MLX availability / model artifact gaps on operator machines. | Cascade gracefully degrades: if Stage D's model can't load, the cascade still runs Stages A–C and writes an explicit `stage_d_unavailable` reason. |
| Risk of silently dropping a true positive in `enforce` mode. | Mandatory `shadow` period before enforce; shadow telemetry includes per-positive trace so operators can inspect any false-drop. |
| Local model artifacts can be large (DictaLM-2.0 Q4 ≈ 4–5 GB). | Store under a configurable `prefilter.model_cache_dir` outside the repo; document disk requirements. |

---

## 9. Success metrics

The cascade is successful if, measured over a 7-day enforcement window:

- **Recall preserved:** Cascade-passed positives / (cascade-passed positives + cascade-dropped positives on labeled subset) ≥ 0.98.
- **Claude call reduction:** ≥ 50% fewer Claude-classifier calls than a `shadow`-mode baseline run over the same window.
- **No silent failures:** Every dropped candidate has a non-empty `reason`, a `stopped_at_stage`, and a model-version provenance.
- **Auditable rollback:** A single config flip (`mode: enforce → mode: shadow`) restores baseline behaviour without code changes.
- **Operator confidence:** The triage workbench surfaces shadow-cascade decisions so the operator can spot-check disagreements.

---

## 10. References

- Existing filter code: `src/denbust/discovery/candidate_filters.py`
- Triage workbench: `triage_app/serve.py`
- Pipeline overview: [README.md](../README.md) — "Discovery Layer Foundation"
- Companion: [docs/local_prefilter_cascade_implementation_plan.md](./local_prefilter_cascade_implementation_plan.md)
