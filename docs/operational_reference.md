# TFHT Enforcement Index Operational Reference

This document preserves the detailed operator and implementation reference that previously lived in
the repository README. Start with the concise [repository README](../README.md) for first-time
orientation, then use this file for deeper operational context.

## Previous README Reference

מדד האכיפה - Enforcement Index

`denbust` is evolving from a single-purpose news scanner into a small multi-dataset platform for TFHT
data jobs. Phase A introduced the shared platform spine. Phase B turns the first real dataset,
`news_items`, into an end-to-end operational flow with:

- normalized metadata records
- Supabase operational persistence
- privacy/review/suppression gating
- weekly public release bundle generation
- publication hooks for Kaggle and Hugging Face
- latest-backup upload hooks for Google Drive and S3-compatible object storage

Today, the implemented dataset/jobs are:

- `news_items / discover` (source-native + Brave + Exa + Google CSE candidate persistence, plus taxonomy-targeted and Facebook-targeted social discovery queries)
- `news_items / backfill_discover`
- `news_items / backfill_scrape`
- `news_items / ingest`
- `news_items / monthly_report`
- `news_items / release`
- `news_items / backup`

Planned future datasets:

- `docs_metadata`
- `open_docs_fulltext`
- `events`

## What it does now

- Scans Israeli news sources for enforcement activity: raids, arrests, closures, trafficking cases
- Uses RSS and browser-backed scrapers
- Classifies relevance with an LLM
- Deduplicates the same story across multiple sources
- Emits unified items via CLI or SMTP email for the ingest workflow
- Persists normalized `news_items` operational rows
- Builds metadata-only weekly release bundles
- Builds monthly Markdown/JSON report bundles for public-facing TFHT reporting
- Publishes release bundles to Kaggle and Hugging Face when configured
- Uploads the latest release bundle to Google Drive and S3-compatible object storage when configured
- Persists dataset/job-scoped seen state and per-run JSON snapshots
- Scaffolds a persistent discovery/candidacy layer with dedicated Supabase tables and state-repo
  paths under `news_items/discover/`
- Runs Brave, Exa, and Google CSE as external discovery engines feeding the durable candidate layer
- Adds taxonomy-targeted discovery queries from the packaged TFHT taxonomy for broader
  search-engine recall
- Adds source-targeted taxonomy queries for each configured news domain so search-backed discovery
  and capped historical backfill can compensate when source-native pages produce zero recent matches
- Expands source-native recall terms for Walla archive filtering and ICE search so diagnostics and
  discovery exercise targeted Hebrew phrases beyond the coarse operator keyword sample
- Protects the Ynet source-targeted taxonomy search path with fixture-backed recall coverage for a
  known February 12, 2026 article, including candidate provenance and pre-classification handoff
- Keeps Ynet RSS as the primary משפט ופלילים source while adding a non-browser category-page
  backstop for source-native recall
- Emits Facebook-targeted `social_targeted` search queries and retains those results as non-scrapeable reference candidates
- Plans historical backfill windows and persists durable `backfill_batches` metadata for slow-drain
  discovery/scrape work
- Drains one historical backfill batch at a time with oldest-window-first scrape prioritization
- Refreshes backfill batch aggregate counts through the discovery persistence layer, including
  Supabase exact-count requests, without materializing full candidate rows in the pipeline
- Provides an opt-in `agents/news/local_search.yaml` config for local source-native + Brave + Exa
  + Google CSE discovery experiments
- Provides `agents/news/local_search_brave_exa.yaml` for local Brave+Exa wet tests when Google CSE
  returns `403 PERMISSION_DENIED` / no API access; Google CSE remains supported in code and in the
  full local search config
- Retains obvious non-article search-result noise as provenance while marking social profiles,
  app-store detail pages, dictionary, translation, and reference-utility candidates
  `unsupported_source` before they can consume scrape-drain budget; post-like social URLs and
  non-app store paths remain eligible, and diagnostics expose durable filter reason counts
- Writes discovery overlap/queue/conversion diagnostics artifacts and exposes
  `denbust diagnose-discovery`
- Reports queue-drain selection order, attempted and remaining eligible source mix, configured
  candidate cap, persisted scrape-attempt count, and inferred stop reason for bounded candidate
  scrape passes
- Breaks down partial-page diagnostics so operators can distinguish retained candidate-fallback
  operational rows, metadata-only partials, search-result-only generic fetch failures, source vs
  generic partial attempts, generic partials after source-adapter attempts, dominant partial
  domains/sources, and visible current-candidate classifier/taxonomy risk signals, including
  low-confidence fallback counts by source, domain, taxonomy label, and confidence field
- Persists run-level classifier parser warning counts in run debug summaries under
  `classifier_summary.warning_counts`, including JSON parse failures and invalid taxonomy pairs,
  and exposes fallback-only scrape/backfill context under `fallback_classifier_summary`, without
  changing classifier prompts, taxonomy policy, or scrape selection behavior
- Documents the first post-persistence warning-count interpretation pass: the bounded January 1-7
  Brave+Exa/no-Google scrape/backfill run saw 4 parse failures and 1 invalid taxonomy pair across
  100 fallback classifier inputs, and compact summaries now retain `fallback_classifier_summary`
  for future fallback-only drains
- Keeps classifier output robustness evidence-driven: the retained Phase C parse-failure artifacts
  do not expose raw malformed response shapes, so representative non-JSON outputs are covered only
  as current rejection-policy examples and parser recovery is deferred until sanitized shape
  evidence is persisted safely
- Persists sanitized classifier parse-failure shape diagnostics in run debug summaries under
  `classifier_summary.parse_failure_diagnostics` and, for fallback-only scrape/backfill drains,
  `fallback_classifier_summary.parse_failure_diagnostics`; compact summaries retain the same
  bounded category counts, sanitized JSON error kinds, and structural samples without storing raw
  classifier response text, article text, secrets, or generated data artifacts
- Interprets the first post-capture bounded Phase C parse-failure evidence pass: five fallback
  parse failures across 100 fallback classifier inputs were all `object_like_non_json`, with
  one-line, no-code-fence samples and `missing_property_name` at line 1 column 2, so parser recovery
  stayed deferred until a follow-up evidence pass could prove recovery safety
- Interprets the follow-up parse-failure structure evidence pass: the same five-failure pattern was
  consistently balanced double-wrapped valid-inner-JSON classifier output, so the next parser slice
  can add fixture-backed recovery for that exact shape while keeping prompts, taxonomy policy, queue
  behavior, scraper behavior, source support, and generated-data boundaries unchanged
- Recovers only the proven classifier double-wrapper shape where the normalized response starts
  with exactly two opening object braces, ends with exactly two closing object braces, has balanced
  braces outside strings, and trimming one outer wrapper exposes a valid JSON object; recovered
  objects still use the normal taxonomy validation path and increment
  `double_wrapper_recovery_count`, while pseudo-JSON, unbalanced wrappers, non-object JSON,
  code-fenced malformed JSON, and invalid taxonomy pairs remain rejected
- Interprets the first post-recovery bounded Phase C evidence pass: four double-wrapper recoveries
  replaced the prior five-parse-failure pattern with zero parse failures across 100 fallback
  classifier inputs, invalid taxonomy warnings stayed at one, and no retained fallback row carried
  an invalid taxonomy pair. One retained low-confidence partial fallback row had only a legacy
  category and no TFHT taxonomy leaf, so parser-output hardening pauses while the next Phase C
  classifier item investigates fallback retention without usable taxonomy.
- Keeps source-suggestion scrape diagnostics evidence-driven by reporting generic partial
  recoveries separately from definite scrape failures, without otherwise changing source-suggestion
  ranking
- Reports self-heal-eligible candidate backlog and structured scrape-failure groups for future
  repair workflows, without running automatic AI repair or selector rewriting
- Writes source-suggestion diagnostics artifacts for repeated unseen non-social domains
- Flags candidate-only `sport1.maariv.co.il` pressure in source-suggestion diagnostics without
  changing scrape eligibility or Maariv source-family support
- Lints the tracked classifier validation CSV with `denbust validation-lint`
- Shares validation taxonomy/category/index-relevance row-integrity rules between
  `denbust validation-lint` and reviewed-row finalize/import
- Runs tracked classifier/live-source scenarios with `denbust live-check`
- Runs scheduled GitHub Actions discovery separately from candidate-driven ingest
- Exposes manual GitHub Actions workflows for historical `backfill_discover` and `backfill_scrape`
- Reviews the latest daily ingest artifacts and can open GitHub issues for suspicious runs

## Quick Start

```bash
pip install -e ".[dev]"
python -m playwright install chromium
denbust scan --config agents/news/local.yaml
DENBUST_BACKFILL_DATE_FROM=2026-01-23T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-04-22T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml
DENBUST_BACKFILL_BATCH_ID=batch-optional \
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
denbust report monthly --month 2026-03 --config agents/news/local.yaml
denbust diagnose-discovery --config agents/news/local.yaml
denbust validation-lint --validation-set validation/news_items/classifier_validation.csv
denbust release --config agents/release/news_items.yaml
denbust backup --config agents/backup/news_items.yaml
```

To send reports by email, set `output.format: email` in your config and provide SMTP env vars
from `.env.example`.

Mako scraping uses a headless Chromium browser. After installing dependencies on a new machine, run
`python -m playwright install chromium` once before your first live scan.

## Dataset And Job Model

Phase A introduces explicit dataset and job identity in config and run snapshots:

- `dataset_name`
- `job_name`

Current defaults remain:

- `dataset_name: news_items`
- `job_name: ingest`

`denbust scan` is preserved as a compatibility alias for `news_items / ingest`.

Future-facing commands now exist as real `news_items` jobs:

```bash
denbust run --dataset news_items --job discover --config agents/news/local.yaml
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
denbust run --dataset news_items --job scrape_candidates --config agents/news/local.yaml
denbust run --dataset news_items --job ingest --config agents/news/local.yaml
denbust run --dataset news_items --job monthly_report --config agents/news/local.yaml
denbust release --dataset news_items --config agents/release/news_items.yaml
denbust backup --dataset news_items --config agents/backup/news_items.yaml
```

## Persistence Modes

### Local mode

Local runs now use dataset/job-namespaced defaults under the repo-local state root:

- seen store: `data/news_items/ingest/seen.json`
- run snapshots: `data/news_items/ingest/runs/`
- publication scaffold dir: `data/news_items/ingest/publication/`

Example:

```bash
denbust scan --config agents/news/local.yaml
```

You can override the persistence layout without changing YAML by setting:

- `DENBUST_STATE_ROOT`
- `DENBUST_STORE_PATH`
- `DENBUST_RUNS_DIR`

Precedence rules:

1. `DENBUST_STORE_PATH` / `DENBUST_RUNS_DIR`
2. `DENBUST_STATE_ROOT`
3. explicit YAML store paths / `store.state_root`
4. local default root `data/`

### GitHub Actions + state repo mode

Scheduled GitHub Actions runs use this repo as the code runner and a separate repo,
`tfht_enforce_idx_state`, as the canonical mutable state store.

The workflow family:

- checks out this repo
- checks out the state repo into `state_repo/`
- sets dataset/job env such as `DATASET_NAME=news_items` and `JOB_NAME=discover` or `ingest`
- runs the job and persists state through the shared `scripts/state-run.sh` wrapper (see below),
  e.g. `bash scripts/state-run.sh --subtree news_items/<job>/... --message "..." -- denbust run
  --dataset news_items --job <job> --config agents/news/local_search_brave_exa.yaml --overlay
  agents/news/ci.overlay.yaml` (the shared base config plus the CI overlay; the overlay is the
  single auditable place CI differs — Supabase store, emailed report, leaner reach)

#### The `scripts/state-run.sh` wrapper

Every state-writing workflow (and local runs) funnels the pull -> run -> commit -> push cycle
through one wrapper so that logic lives in exactly one place instead of being copy-pasted into each
workflow. Given `--subtree` paths, a `--message`, and a command after `--`, it:

- brings the state repo to canonical HEAD (clone if missing, else fetch + reset to
  `origin/<branch>`) so a run always starts from the single source of truth;
- runs the command with `DENBUST_STATE_ROOT` pointed at the state repo;
- stages only the named subtrees and commits **only when they actually changed**;
- does a plain `git push` (which already refuses to clobber a concurrent commit — a
  non-fast-forward is rejected), and on rejection **refetches, rebases its commit onto the new
  tip, and retries**, so a race recovers instead of failing the run;
- persists partial state even when the command fails, then exits with the command's status code.

Concurrency is handled in layers: a portable same-machine lock (atomic `mkdir`, with stale-lock
recovery via the holder's PID, so a crashed run does not wedge future ones) serializes local
writers; the GitHub Actions `state-run` concurrency group serializes CI; and the fetch-before-run
plus push-retry handle a local-vs-CI race (different machines, where the lock cannot reach).
`--force-with-lease` is deliberately **not** used — the wrapper never wants to overwrite the
remote, and a stale lease over a shallow fetch could.

Configuration is via env (`STATE_REPO_DIR`, `STATE_REPO_URL`/`STATE_REPO_SLUG`/`STATE_REPO_TOKEN`,
`STATE_REPO_BRANCH`). In CI the repo is already checked out by `actions/checkout`, so the wrapper
reuses that configured remote — no token plumbing needed (a local `STATE_REPO_TOKEN` is passed via
an in-memory `http.extraheader`, never baked into the repo URL). Locally, `--offline` skips the
fetch/push for fast iteration against an existing checkout. `scripts/state-run.sh --help` documents
the full interface. The auth/lock/canonical-checkout helpers live in
`scripts/lib/state_repo_common.sh`, shared with the squash job below.

#### History bounding: `scripts/state-squash.sh`

State files are plain JSONL so per-run blobs stay tiny, but the *number* of commits grows one per
run. The `state-repo-squash` workflow (intended cadence: weekly) periodically flattens history via
`scripts/state-squash.sh`: it replaces the branch with a single fresh root commit holding the
current tree, so a clone never pays for the full run-by-run history. It is a force-push by nature
(a history rewrite) but safe against concurrent writers — it holds the same same-machine lock and
shares the `state-run` GitHub Actions concurrency group, and a state-run that races a squash simply
resets to the squashed root on its next run. `--dry-run` builds the squashed commit without pushing;
a branch that is already a single commit is left untouched.

Operational workflow ownership after `DL-PR-11`:

- `news-items-discover` owns scheduled discovery writes under `news_items/discover/`
- `daily-state-run` and `weekly-state-run` own candidate-driven ingest under `news_items/ingest/`
- `news-items-backfill-discover` and `news-items-backfill-scrape` are manual operator workflows
  for historical recovery
- `news-items-daily-review` still follows `daily-state-run`

Required secrets for GitHub-run mode:

- `ANTHROPIC_API_KEY`
- `STATE_REPO_PAT`
- `DENBUST_SUPABASE_URL`
- `DENBUST_SUPABASE_SERVICE_ROLE_KEY`
- `DENBUST_EMAIL_SMTP_HOST`
- `DENBUST_EMAIL_SMTP_PORT`
- `DENBUST_EMAIL_SMTP_USERNAME`
- `DENBUST_EMAIL_SMTP_PASSWORD`
- `DENBUST_EMAIL_FROM`
- `DENBUST_EMAIL_TO`
- `DENBUST_EMAIL_USE_TLS`
- `DENBUST_EMAIL_SUBJECT`

Optional discovery-engine secrets:

- `DENBUST_BRAVE_SEARCH_API_KEY`
- `DENBUST_EXA_API_KEY`
- `DENBUST_GOOGLE_CSE_API_KEY`
- `DENBUST_GOOGLE_CSE_ID`

Expected `tfht_enforce_idx_state` structure:

```text
tfht_enforce_idx_state/
└── news_items/
    ├── ingest/
    │   ├── seen.json
    │   ├── runs/
    │   ├── logs/
    │   └── publication/
    ├── release/
    │   ├── runs/
    │   └── publication/
    ├── monthly_report/
    │   ├── runs/
    │   └── publication/
    └── backup/
        ├── runs/
        └── publication/
```

The new discovery/candidacy foundation adds a separate candidate-layer namespace alongside the
existing ingest/release/backup state:

```text
tfht_enforce_idx_state/
└── news_items/
    └── discover/
        ├── runs/
        ├── candidates/
        │   ├── backfill_queue.jsonl
        │   ├── latest_candidates.jsonl
        │   ├── retry_queue.jsonl
        │   └── scrape_attempts.jsonl
        ├── backfill_batches/
        │   ├── latest_backfill_batches.jsonl
        │   └── <batch-id>.json
        └── metrics/
            ├── engine_overlap_latest.json
            ├── discovery_diagnostics_latest.json
            └── source_suggestions_latest.json
```

State files are kept as **plain JSONL** on purpose — do not pre-compress them (e.g. to
`*.jsonl.gz`) for storage in the state repo. Git already delta+zlib-compresses blobs, so
gzipping first is redundant on any single snapshot (~1.01x vs git's own packing) and actively
harmful across runs: pre-compressed blobs cannot be delta-compressed against the previous
revision, which was measured to grow the candidate store's history roughly **15x**. Gzip also
embeds a wall-clock mtime, so byte-identical state produces a different blob every run, breaking
"skip the commit when nothing changed". The repo is bounded instead by the `state-run` wrapper
(shallow/treeless clone + commit-only-on-change) and a periodic orphan-squash.

Bootstrap notes:

- `seen.json` may be absent initially; it is created once a run marks at least one URL as seen
- `runs/` and `publication/` directories are created automatically by the workflows when needed
- `logs/` is created automatically once ingest debug artifacts are written
- a small `README.md` in the state repo is fine but optional
- see [docs/discovery_operations.md](discovery_operations.md) for the detailed discover /
  ingest / backfill runbook and migration checklist
- the one-time `C-8` catch-up path is a manual 90-day `backfill_discover` plus `backfill_scrape`
  run using the existing 7-day backfill window slicing

## Architecture Direction

Phase A introduces shared platform primitives so future dataset jobs can reuse them:

- `src/denbust/models/`
  - dataset/job identity
  - run snapshot model
  - policy enums for rights, privacy, review, and publication status
- `src/denbust/store/state_paths.py`
  - centralized dataset/job state path resolution
- `src/denbust/datasets/`
  - explicit dataset/job registry
- `src/denbust/ops/storage.py`
  - operational-store abstraction with a null implementation
- `src/denbust/publish/`
  - release/export abstractions
  - backup abstractions

What is implemented now:

- `news_items / ingest`
  - live source ingestion
  - canonical URL normalization
  - LLM relevance classification
  - one-sentence summary generation
  - privacy/review/publication/takedown status assignment
  - operational persistence through the configured store
- `news_items / release`
  - reads operational rows
  - filters to publicly releasable metadata-only rows
  - writes `news_items.parquet`, `news_items.csv`, `MANIFEST.json`, `SCHEMA.json`, `SCHEMA.md`, `README.md`, and `checksums.txt`
  - publishes to Kaggle and Hugging Face when configured
- `news_items / backup`
  - finds the latest release bundle
  - uploads it to Google Drive and S3-compatible object storage when configured

Still intentionally deferred:

- additional dataset implementations beyond `news_items`
- richer human review tooling / admin UI
- more advanced privacy policies beyond the current pragmatic gate

## Discovery Layer Foundation

DL-PR-06 now builds on the earlier discovery milestones:

- `src/denbust/discovery/` with durable candidate, provenance, scrape-attempt, and discovery-run
  models
- config sections for `discovery`, `source_discovery`, `candidates`, and `backfill`
- durable `backfill_batches` metadata mirrored to the state repo and Supabase
- explicit state-repo path helpers for candidate-layer snapshots and queue files
- source-native candidate normalization and merge/upsert persistence
- a real `news_items / discover` job that persists source-native candidates and Brave-discovered
  plus Exa-discovered and Google CSE-discovered candidates into the same durable substrate
- Brave query building for broad and source-targeted discovery searches
- taxonomy-targeted query building from packaged TFHT discovery terms
- capped source-targeted taxonomy query building for historical backfill via
  `backfill.max_source_targeted_taxonomy_queries_per_window`
- fixture-backed Ynet recall coverage for source-targeted taxonomy search candidates, durable
  candidate normalization, source-adapter materialization, and pre-classification ingest handling
- Exa query execution for the same broad and source-targeted discovery searches
- Google CSE query execution for the same broad and source-targeted discovery searches
- dedicated `DENBUST_BRAVE_SEARCH_API_KEY`, `DENBUST_EXA_API_KEY`, and
  `DENBUST_GOOGLE_CSE_API_KEY` / `DENBUST_GOOGLE_CSE_ID` configuration paths for external
  discovery engines
- candidate selection / queueing helpers for retryable scrape work
- scrape-attempt persistence and candidate status transitions underneath the ingest path
- a real `news_items / scrape_candidates` job that drains queued candidates into article ingest
- a real `news_items / backfill_discover` job that requires
  `DENBUST_BACKFILL_DATE_FROM` / `DENBUST_BACKFILL_DATE_TO`, creates a durable batch record,
  and runs historical search-engine discovery plus capability-based source-native discovery
- a real `news_items / backfill_scrape` job that drains one historical batch at a time through the
  existing scrape-to-ingest path
- persistence-layer aggregate count API used by backfill batch status refreshes, with Supabase
  exact-count requests and state-repo streaming counts preserving existing aggregate semantics
- `news_items / ingest` now uses the candidate scrape layer for source-native candidates while
  preserving the existing direct-fetch convenience flow as a compatibility fallback
- Supabase migrations for:
  - `discovery_runs`
  - `persistent_candidates`
  - `backfill_batches`
  - `candidate_provenance`
  - `scrape_attempts`

The current daily monitoring flow remains operational, and the durable candidate substrate now
accepts source-native discovery plus Brave, Exa, and Google CSE search results.

Local operators can use `agents/news/local_search_brave_exa.yaml` for wet tests when the Google
Programmable Search API is unavailable locally. That config disables only the Google CSE engine and
keeps Brave and Exa enabled; browser-backed scraping still uses `DENBUST_BROWSER_MODE=chrome_cdp`
and `DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222` when the run needs Chrome CDP.

DL-PR-08 extends that substrate with fallback retention for imperfect scraping:

- source-adapter successes now persist `content_basis = full_article_page`
- generic fetch fallback can retain `content_basis = partial_page` when page metadata is recoverable
- failed full scrapes can retain `content_basis = search_result_only` using discovery metadata
- retained fallback candidates can materialize provisional internal-only `news_items` rows for
  monitoring and review, while staying excluded from public release by default
- discovery diagnostics now report candidate-basis counts so operators can distinguish full-page,
  partial-page, and search-result-only retention
- Ynet source-health diagnostics now report separate RSS and category-page checks, distinguishing
  RSS low coverage, category HTTP failure, category parse-zero, and category keyword-zero cases
- source-health diagnostics now include a report-level `source_zero_summary` for the Phase C
  4+ hard affected-source guardrail, keep keyword-zero outcomes visible through separate
  report-level counts and per-source warnings, and include Mako `failure_mode` details for missing
  Chromium, navigation timeout, context teardown, redirect/anti-bot, selector drift, parse-zero, and
  stale/keyword-zero outcomes
- the 2026-05-03 Phase C source-health triage pass, run with an isolated
  `DENBUST_STATE_ROOT` and Chromium installed before Mako probing, showed Mako and Haaretz healthy
  while the then-current 4-source guardrail still fired for Ynet, Walla, Maariv, and ICE; the #72
  follow-up narrows that guardrail to hard source failures and makes #71/#74 duplicate or stale Mako
  runtime hygiene unless Mako regresses again
- the 2026-05-03T13:13:10Z source-specific Mako follow-up reran the Chromium-backed probe under
  `data/may_26_followup/20260503T131309Z/state`; Mako again returned `ok`, so #71/#74 are closed as
  stale/duplicate runtime hygiene without changing scraper behavior
- PR #108 was squash-merged as `dea6406` and left no open GitHub issue backlog; the post-#108
  artifact-only reset under `data/may_26_followup/20260503T134102Z/state` correctly showed an empty
  isolated diagnostics baseline rather than a new code defect
- PR #109 was squash-merged as `201c247`; the 2026-05-03 candidate-drain evidence pass under
  `data/may_26_followup/20260503T153123Z/state` persisted 63 candidates, spent all 30 scrape
  attempts on ICE, left 33 candidates from Haaretz, ICE, Maariv, Mako, and Walla never scraped, and
  produced no scrape failures, retry backlog, or self-heal backlog. Artifact-only source-health was
  inconclusive because that diagnostic path skipped `scrape_candidates` debug summaries
- PR #110 was squash-merged as `8c89d91`; `diagnose-discovery` now emits `queue_drain` diagnostics
  for bounded candidate scrape passes, including persisted attempted order, actual-attempt source
  mix, remaining eligible order/source mix, configured candidate cap, persisted scrape-attempt
  count, and inferred stop reason, without changing queue prioritization or fairness behavior
- `SRC-PR-GLOBES-THEMARKER` adds bounded generic-fetch source-family support for `globes.co.il` and
  `themarker.com`: diagnostics and fallback provenance now group search-discovered article URLs as
  `globes`/`themarker`, article metadata and JSON-LD are preferred over page titles for partial
  metadata extraction, and source-targeted discovery/backfill queries now cover both domains while
  source-suggestion diagnostics can still surface weak conversion or candidate-only backlog. This is
  intentionally not a browser scraper or source-native discovery adapter
- `SRC-PR-ISRAELHAYOM` adds bounded generic-fetch source-family recognition for main-domain
  `israelhayom.co.il` article URLs: search-discovered Israel Hayom article URLs can be grouped as
  `israelhayom`, while source-targeted discovery/backfill fanout, Israel Hayom subdomains, browser
  scrapers, source-native adapters, and queue-prioritization changes remain out of scope until
  stronger extraction evidence exists
- `SRC-PR-KAN` adds low-confidence generic-fetch diagnostic labeling for official Kan news article
  paths under `kan.org.il/content/kan-news/`; this is not source-targeted fanout, a source-native
  adapter, a browser scraper, or broad support for non-article `kan.org.il` pages and unrelated
  Kan-named domains
- `SRC-PR-NEWS1` adds low-confidence generic-fetch diagnostic labeling for main-domain News1
  archive article paths under `news1.co.il/Archive/`; this is not source-targeted fanout, a
  source-native adapter, a browser scraper, or broad support for non-archive News1 pages
- `CLASSIFIER-PR-WARNING-EVIDENCE-INTERPRETATION` records the first post-warning-counter Phase C
  interpretation pass under generated local root `data/may_26_followup/20260514T182934Z/`: 100
  fallback classifier inputs produced 4 parse failures, 1 invalid taxonomy pair, no invalid legacy
  pairs, and no relevant-without-usable-taxonomy warnings. It also keeps
  `fallback_classifier_summary` in compact run summaries going forward and sets the next PR to
  inspect parse-failure shape evidence before any bounded parser robustness change. It does not
  recommend prompt, taxonomy-policy, queue, scraper, or source-family changes
- `DL-PR-12` adds explicit future self-heal hooks while preserving current behavior:
  `self_heal_eligible` is visible in queue diagnostics, failed scrape attempts are grouped by
  attempt kind/status/error/source/domain, source-adapter and generic-fetch failures carry stable
  failure-stage diagnostics, and code can select eligible failed candidates for a later
  `self_heal_retry` orchestration pass without implementing AI repair.

### Local pre-classification filter cascade (LPF-PR-01–LPF-PR-12)

A four-stage, fully-local, non-LLM-API-based filter cascade is planned for insertion between the
discovery/triage layer and the Claude Sonnet relevance classifier, to drop high-confidence true
negatives before they consume paid LLM budget. The cascade composes:

- Stage A: scored lexicon + Beta-Binomial domain reputation + URL heuristics (microseconds per
  candidate);
- Stage B: trained text classifier (Naive Bayes on character n-grams, with a SetFit option)
  (~5 ms per candidate);
- Stage C: multilingual sentence-embedding similarity using `intfloat/multilingual-e5-large`
  with centroid-cosine and FAISS-HNSW kNN-max-cosine signals (~30 ms per candidate);
- Stage D: local SLM (`dicta-il/dictalm2.0-instruct` via MLX) scored by token logprobs of
  `כן` / `לא` at the answer slot (~800 ms per candidate).

Each stage is calibrated to ≥ 99% per-stage recall on a held-out validation set; the cascade
ships in `mode: off` by default, is promoted to `mode: shadow` in `LPF-PR-09`, and only flips to
`mode: enforce` after a documented 7-day shadow window meets the configured recall floor.

See [docs/local_prefilter_cascade_design.md](docs/local_prefilter_cascade_design.md) for the
design and [docs/local_prefilter_cascade_implementation_plan.md](docs/local_prefilter_cascade_implementation_plan.md)
for the per-PR rollout plan (`LPF-PR-01` through `LPF-PR-10` for the core cascade plus optional
`LPF-PR-11` Claude distillation and `LPF-PR-12` active-learning / cluster-filter follow-ups).

## Config Layout

Preferred checked-in config layout:

```text
agents/
  news/
    local.yaml
    github.yaml
  release/
    news_items.yaml
  backup/
    news_items.yaml
```

Backward-compatible shims are still present:

- `agents/news.yaml`
- `agents/news-github.yaml`

Current intent:

- `agents/news/...` drives ingest jobs
- `agents/release/...` drives release jobs
- `agents/backup/...` drives backup jobs

The `release` and `backup` commands still accept any compatible config path you pass explicitly, but
their default paths now point at dedicated config files instead of reusing the ingest config.

## Workflow Parameterization

The current GitHub Actions layer is still news-items-first, but it is now parameterized around shared
dataset/job env variables:

- `DATASET_NAME`
- `JOB_NAME`
- `JOB_CONFIG_PATH`
- `STATE_JOB_DIR`

This keeps the current scheduled news ingest behavior unchanged while making the workflow files easier
to extend for future dataset/job combinations.

## `news_items` public dataset

The public `news_items` dataset is metadata-only. Each public row contains:

- deterministic row id
- source name and source domain
- original and canonical URL
- publication and retrieval timestamps
- title
- category and sub-category
- one-sentence factual summary
- geographic fields when available
- organizations and topic tags
- rights / privacy / review / publication / takedown status
- release version

The public dataset intentionally excludes:

- article full text
- cached HTML
- page screenshots or snapshots
- private ingestion diagnostics

Rows are excluded from public release when they are:

- suppressed by a takedown/suppression rule
- marked `internal_only`
- still pending privacy review
- otherwise non-public under the shared policy enums

## Release bundle contents

Each `news_items` release currently writes:

- `news_items.parquet` as the canonical export
- `news_items.csv`
- `MANIFEST.json`
- `SCHEMA.json`
- `SCHEMA.md`
- `README.md`
- `checksums.txt`

Release versions use a UTC date string such as `2026-03-22`.

## Operational behavior matrix

| Mode | Reads from | Writes to | External integrations |
|---|---|---|---|
| Local ingest | live sources + local seen store | local namespaced state + local JSON operational store (from `agents/news/local.yaml`) | Anthropic, optional SMTP |
| GitHub ingest | live sources + shared state repo seen store | shared state repo + Supabase | Anthropic, Supabase, optional SMTP |
| Weekly release | Supabase `news_items` rows | release bundle under `news_items/release/publication` + release run snapshot | optional Kaggle, optional Hugging Face |
| Weekly backup | latest built release bundle under `news_items/release/publication` | backup run snapshot under `news_items/backup/runs` | optional Google Drive, optional S3-compatible object storage |

In other words:

- local ingest uses local state plus the local JSON operational store by default
- GitHub ingest uses the shared state repo plus Supabase
- release reads releasable rows from Supabase, builds the bundle, and only publishes to public targets when they are configured
- backup does not rebuild the release; it uploads the latest already-built bundle when backup targets are configured

## Required environment variables

### Ingest

- `ANTHROPIC_API_KEY`
- `DENBUST_SUPABASE_URL` for GitHub/Supabase-backed ingest
- `DENBUST_SUPABASE_SERVICE_ROLE_KEY` for GitHub/Supabase-backed ingest
- SMTP variables when email output is enabled

### Release

- `DENBUST_SUPABASE_URL`
- `DENBUST_SUPABASE_SERVICE_ROLE_KEY`
- `DENBUST_KAGGLE_DATASET` to enable Kaggle publishing
- `KAGGLE_USERNAME`
- `KAGGLE_KEY`
- `DENBUST_HUGGINGFACE_REPO_ID` to enable Hugging Face publishing
- `HF_TOKEN`

### Backup

- `DENBUST_DRIVE_SERVICE_ACCOUNT_JSON`
- `DENBUST_DRIVE_FOLDER_ID`
- `DENBUST_OBJECT_STORE_BUCKET`
- `DENBUST_OBJECT_STORE_PREFIX` (optional; defaults to `news_items/latest`)
- `DENBUST_OBJECT_STORE_ENDPOINT_URL`
- `DENBUST_OBJECT_STORE_ACCESS_KEY_ID`
- `DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY`

## Integration activation and failure semantics

The Phase B integrations are intentionally split into:

- required backends for a given job
- optional targets that are only activated when explicitly configured

### Supabase

- `DENBUST_SUPABASE_URL` and `DENBUST_SUPABASE_SERVICE_ROLE_KEY` are required for the checked-in GitHub ingest config and for the checked-in release config
- the service-role key is used because ingest, release, and suppression-aware export assembly all need privileged operational access
- if the selected config uses `operational.provider: supabase` and these variables are missing, the job fails

### Kaggle

- Kaggle publishing is activated only when `DENBUST_KAGGLE_DATASET` is set
- if `DENBUST_KAGGLE_DATASET` is not set, the release job still builds the bundle and skips Kaggle publication
- if `DENBUST_KAGGLE_DATASET` is set but `KAGGLE_USERNAME` or `KAGGLE_KEY` is missing, the release job fails

### Hugging Face

- Hugging Face publication is activated only when `DENBUST_HUGGINGFACE_REPO_ID` is set
- if `DENBUST_HUGGINGFACE_REPO_ID` is not set, the release job still builds the bundle and skips Hugging Face publication
- if `DENBUST_HUGGINGFACE_REPO_ID` is set but `HF_TOKEN` is missing, the release job fails

### Google Drive backup

- Google Drive backup is activated when the backup config enables the target or when `DENBUST_DRIVE_FOLDER_ID` is present
- the checked-in backup config keeps the target disabled for local safety; in GitHub Actions the folder-id secret can activate it implicitly
- if the target is inactive, backup skips Google Drive cleanly
- if the target is active but `DENBUST_DRIVE_SERVICE_ACCOUNT_JSON` is missing, the backup job fails

### Object-storage backup

- object-storage backup is activated when the backup config enables the target or when `DENBUST_OBJECT_STORE_BUCKET` is present
- `DENBUST_OBJECT_STORE_PREFIX` is optional and defaults to `news_items/latest`
- if the target is inactive, backup skips object storage cleanly
- if the target is active but `DENBUST_OBJECT_STORE_ACCESS_KEY_ID` or `DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY` is missing, the backup job fails

### Partial success behavior

- release is considered successful if the bundle is built; skipped public targets are surfaced as warnings in logs/run snapshots
- backup is considered successful if the command completes; zero configured targets is treated as a warning, not a failure
- if a configured publication or backup target is missing required credentials, that target currently fails the job rather than silently skipping

## Daily AI Review Workflow

The repository also includes a workflow that reviews the latest `daily-state-run` artifacts and can
open GitHub issues when the latest ingest looks suspicious:

- `news-items-daily-review.yml`

It runs automatically after `daily-state-run` completes successfully, and it also supports
`workflow_dispatch` for manual review. It reads the latest matching files from:

- `news_items/ingest/runs/`
- `news_items/ingest/logs/`

The workflow uses Anthropic to turn those artifacts into candidate engineering issues, then creates
only new issues by embedding a hidden fingerprint marker in each issue body.

## GitHub Actions secret/setup matrix

| Workflow | Required secrets | Optional secrets |
|---|---|---|
| `daily-state-run.yml` / `weekly-state-run.yml` | `STATE_REPO_PAT`, `ANTHROPIC_API_KEY`, `DENBUST_SUPABASE_URL`, `DENBUST_SUPABASE_SERVICE_ROLE_KEY` | SMTP/email secrets if email output is enabled |
| `news-items-daily-review.yml` | `STATE_REPO_PAT`, `ANTHROPIC_API_KEY` | `DENBUST_REVIEW_MODEL`, `DENBUST_REVIEW_ISSUE_LABELS` |
| `news-items-release.yml` | `STATE_REPO_PAT`, `DENBUST_SUPABASE_URL`, `DENBUST_SUPABASE_SERVICE_ROLE_KEY` | `DENBUST_KAGGLE_DATASET`, `KAGGLE_USERNAME`, `KAGGLE_KEY`, `DENBUST_HUGGINGFACE_REPO_ID`, `HF_TOKEN` |
| `news-items-backup.yml` | `STATE_REPO_PAT` | `DENBUST_DRIVE_FOLDER_ID`, `DENBUST_DRIVE_SERVICE_ACCOUNT_JSON`, `DENBUST_OBJECT_STORE_BUCKET`, `DENBUST_OBJECT_STORE_PREFIX`, `DENBUST_OBJECT_STORE_ENDPOINT_URL`, `DENBUST_OBJECT_STORE_ACCESS_KEY_ID`, `DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY` |

The release and backup workflows both support `workflow_dispatch` for manual runs and weekly schedules for automated runs.

Recommended GitHub Environment mapping:

- `news-items-ingest` for `daily-state-run.yml` and `weekly-state-run.yml`
- `news-items-ingest` for `news-items-daily-review.yml`
- `news-items-release` for `news-items-release.yml`
- `news-items-backup` for `news-items-backup.yml`

The code reads generic env vars at runtime, so the same variable names can safely have different
values per GitHub Environment.

## Supabase setup

Phase B adds SQL migrations under:

```text
supabase/migrations/
```

Apply the `news_items` migration before running Supabase-backed jobs. The schema includes:

- `news_items`
- `ingestion_runs`
- `release_runs`
- `backup_runs`
- `suppression_rules`

`suppression_rules` is the minimal takedown/suppression path. Add rows there by canonical URL or row
id to block future public releases.

## Local development modes

### Local end-to-end mode

The checked-in local ingest config uses the local JSON operational store:

```bash
denbust scan --config agents/news/local.yaml
denbust release --config agents/release/news_items.yaml
denbust backup --config agents/backup/news_items.yaml
```

To run release locally without Supabase, either:

- switch `operational.provider` to `local_json` in the release config, or
- provide a custom config path with that override

### GitHub operational mode

GitHub ingest reuses the shared base config and layers the CI overlay on top via
`--overlay`. The overlay is the single, auditable place where CI differs from local
runs — it flips the operational store to Supabase, adds the emailed report, pins
CI's leaner cost surface (`max_articles`, source-targeted-only backfill), and sets
`scraping_enabled: false`, inheriting everything else (sources, keywords, budget
caps, prefilter) unchanged:

- base: `agents/news/local_search_brave_exa.yaml`
- overlay: `agents/news/ci.overlay.yaml`

`scraping_enabled: false` is the **GH-never-scrapes** guardrail: GitHub's datacenter
IPs are bot-blocked by Israeli news sites, so CI never fetches article bodies or
source pages. Every source-site fetch path no-ops when it is false:

- candidate materialization — `scrape_candidates()` (the ingest source-native,
  `scrape_candidates`, and `backfill_scrape` paths all route through it);
- the ingest job's `fetch_all_sources` call;
- **source-native discovery** in the `discover` and `backfill_discover` jobs
  (`_run_source_native_discovery` / `_run_source_native_backfill_discovery`) — these
  fetch source sites even though the job is "discovery", so they are skipped too while
  the Brave/Exa search engines still run.

The canonical state is scraped by local runs; GH still searches (Brave/Exa), classifies,
and runs the deterministic phases (prefilter, gates, balanced selection, budget math).
The `daily-review` workflow runs `diagnose-sources --artifacts-only`, which does no live
source fetch, so it does not scrape on GH either. Because the operational workflows are
still `workflow_dispatch`-only, this is a guardrail that takes effect when they are enabled.

The overlay also sets `discovery.search_backstop_only: true` — the **search backstop**.
Brave/Exa are a paid, ~1,000-free-queries/month resource, so the search budget should be
spent once. With this flag, the `discover` job issues open-web search queries only when the
search-budget ledger shows **no search recorded in the prior 24h** (by any run, local or CI).
A rolling 24h window (rather than a calendar day) means GH defers to a recent local search
regardless of clock-time ordering — as long as local runs at least daily, GH always skips and
only searches once local has been idle for more than a day. **Schedule GH discover to run at
least daily** so the backstop is timely. When it skips, the job completes normally (non-fatal)
with `search_backstop_skipped=true` in its warnings; source-native discovery and the
deterministic phases are unaffected.

Release and backup jobs rely on dedicated configs:

- `agents/release/news_items.yaml`
- `agents/backup/news_items.yaml`

For backup specifically:

- the checked-in YAML keeps both targets disabled for local safety
- `DENBUST_DRIVE_FOLDER_ID` auto-enables Google Drive backup at config-load time
- `DENBUST_OBJECT_STORE_BUCKET` auto-enables object-storage backup at config-load time
- because the backup config no longer hardcodes `store.publication_dir`, it reads the latest release bundle from the current state root under `news_items/release/publication`

## Current limitations

- privacy/risk gating is intentionally lightweight and conservative, not a substitute for legal review
- publication and backup integrations require external credentials and cannot be fully exercised in CI
- only `news_items` is implemented end to end in this phase

## Example Output

```
📍 פשיטה על בית בושת ברמת גן
תאריך: 2026-02-15
קטגוריה: בית בושת

תקציר: המשטרה פשטה על דירה ברמת גן...

מקורות:
• Ynet: https://ynet.co.il/...
• Mako: https://mako.co.il/...
```

## Documentation

- [Agent Plan](../.agent-plan.md) - Current operational priority pointer
- [Repo Plan Summary](../PLAN.md) - Human-friendly map of the main plan and active sub-plans
- [Discovery Operations](discovery_operations.md) - Local and GitHub Actions runbook for discover/ingest/backfill
- [Product Definition](product_def.md) - Full project background (Hebrew)
- [MVP Spec](MVP_SPEC.md) - Phase 1 technical scope
- [Implementation Plan](IMPLEMENTATION_PLAN.md) - Task breakdown
- [Discovery Layer Rollout Plan](tfht_discovery_layer_implementation_plan.md) - `DL-PR-*` sequence

## Planning Workflow

- When a PR is opened against a tracked plan item, the PR should update `.agent-plan.md`,
  `README.md`, and the relevant human-facing plan document(s) in the same branch.
- Those documentation updates should describe the state the repo is expected to be in after the PR
  is merged, not the pre-merge state from before the work landed.
- `.agent-plan.md` is written as mainline truth using `Mainline Status` and `Task Ledger`; on a
  branch it acts as the merge contract, and on `main` the same text is read as present-tense fact.
- `.agent-plan.md` task ledger entries use only `[done]`, `[next]`, `[later]`, and `[blocked]`,
  with exactly one `[next]` item at any time.
- Plan-tracked PRs should not leave planning and docs surfaces one merge behind the code.

## Roadmap

- **Phase A** (current): multi-dataset platform spine + working `news_items / ingest`
- **Phase B**: `news_items` dataset evolution and release/backup implementation
- **Later**: docs metadata, open-docs fulltext, events, and downstream analytics
