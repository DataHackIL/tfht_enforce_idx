# denbust

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

- `news_items / discover` (source-native candidate persistence only)
- `news_items / ingest`
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
- Publishes release bundles to Kaggle and Hugging Face when configured
- Uploads the latest release bundle to Google Drive and S3-compatible object storage when configured
- Persists dataset/job-scoped seen state and per-run JSON snapshots
- Scaffolds a persistent discovery/candidacy layer with dedicated Supabase tables and state-repo
  paths under `news_items/discover/`
- Reviews the latest daily ingest artifacts and can open GitHub issues for suspicious runs

## Quick Start

```bash
pip install -e ".[dev]"
python -m playwright install chromium
denbust scan --config agents/news/local.yaml
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
denbust run --dataset news_items --job scrape_candidates --config agents/news/local.yaml
denbust run --dataset news_items --job ingest --config agents/news/local.yaml
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

The workflow:

- checks out this repo
- checks out the state repo into `state_repo/`
- sets dataset/job env such as `DATASET_NAME=news_items` and `JOB_NAME=ingest`
- runs `denbust scan --config agents/news/github.yaml`
- points persistence at the checked-out state repo via `DENBUST_STATE_ROOT=state_repo`
- commits and pushes the updated namespaced state files only if files changed

Required secrets for GitHub-run mode:

- `ANTHROPIC_API_KEY`
- `STATE_REPO_PAT`
- `DENBUST_EMAIL_SMTP_HOST`
- `DENBUST_EMAIL_SMTP_PORT`
- `DENBUST_EMAIL_SMTP_USERNAME`
- `DENBUST_EMAIL_SMTP_PASSWORD`
- `DENBUST_EMAIL_FROM`
- `DENBUST_EMAIL_TO`
- `DENBUST_EMAIL_USE_TLS`
- `DENBUST_EMAIL_SUBJECT`

Expected `tfht_enforce_idx_state` structure:

```text
tfht_enforce_idx_state/
└── news_items/
    ├── ingest/
    │   ├── seen.json
    │   ├── runs/
    │   └── publication/
    ├── release/
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
        │   ├── latest_candidates.jsonl
        │   ├── retry_queue.jsonl
        │   └── backfill_queue.jsonl
        └── metrics/
            └── engine_overlap_latest.json
```

Bootstrap notes:

- `seen.json` may be absent initially; it is created once a run marks at least one URL as seen
- `runs/` and `publication/` directories are created automatically by the workflows when needed
- `logs/` is created automatically once ingest debug artifacts are written
- a small `README.md` in the state repo is fine but optional

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
- operational wiring of the new multi-engine discovery layer into production ingest
- richer human review tooling / admin UI
- more advanced privacy policies beyond the current pragmatic gate

## Discovery Layer Foundation

PR 3 of the persistent multi-engine discovery work now builds on the PR 1 foundation and the PR 2
source-native persistence slice:

- `src/denbust/discovery/` with durable candidate, provenance, scrape-attempt, and discovery-run
  models
- config sections for `discovery`, `source_discovery`, `candidates`, and `backfill`
- explicit state-repo path helpers for candidate-layer snapshots and queue files
- source-native candidate normalization and merge/upsert persistence
- a real `news_items / discover` job that fetches source-native candidates only and persists them
- candidate selection / queueing helpers for retryable scrape work
- scrape-attempt persistence and candidate status transitions underneath the ingest path
- a real `news_items / scrape_candidates` job that drains queued candidates into article ingest
- `news_items / ingest` now uses the candidate scrape layer for source-native candidates while
  preserving the existing direct-fetch convenience flow as a compatibility fallback
- Supabase migrations for:
  - `discovery_runs`
  - `persistent_candidates`
  - `candidate_provenance`
  - `scrape_attempts`

This PR still does not call Brave, Exa, or Google CSE, and the generic fetch/extract fallback is
only scaffolded structurally for retry bookkeeping. The current daily monitoring flow remains
operational, but candidate selection, scrape attempts, and retryable failure state now exist as a
real substrate under source-native ingest.

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

The checked-in GitHub ingest config uses the Supabase operational store:

- `agents/news/github.yaml`

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

- [Product Definition](docs/product_def.md) - Full project background (Hebrew)
- [MVP Spec](docs/MVP_SPEC.md) - Phase 1 technical scope
- [Implementation Plan](docs/IMPLEMENTATION_PLAN.md) - Task breakdown

## Roadmap

- **Phase A** (current): multi-dataset platform spine + working `news_items / ingest`
- **Phase B**: `news_items` dataset evolution and release/backup implementation
- **Later**: docs metadata, open-docs fulltext, events, and downstream analytics
