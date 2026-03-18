# denbust

מדד האכיפה - Enforcement Index

`denbust` is evolving from a single-purpose news scanner into a small multi-dataset platform for TFHT
data jobs. Phase A keeps the current news ingest pipeline working while introducing shared dataset/job
identity, namespaced state paths, shared policy models, and scaffolding for future release and backup
flows.

Today, the only fully implemented dataset/job is:

- `news_items / ingest`

Scaffolded but not yet fully implemented:

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
- Emits unified items via CLI or SMTP email
- Persists dataset/job-scoped seen state and per-run JSON snapshots

## Quick Start

```bash
pip install -e ".[dev]"
python -m playwright install chromium
denbust scan --config agents/news/local.yaml
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

Future-facing commands now exist as scaffolding:

```bash
denbust run --dataset news_items --job ingest --config agents/news/local.yaml
denbust release --dataset news_items --config agents/news/local.yaml
denbust backup --dataset news_items --config agents/news/local.yaml
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

Bootstrap notes:

- `seen.json` may be absent initially; it is created once a run marks at least one URL as seen
- `runs/` and `publication/` directories are created automatically by the workflows when needed
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

- `news_items / ingest` end to end
- dataset/job-aware run snapshots
- dataset/job-scoped local and state-repo persistence
- CLI scaffolding for `run`, `release`, and `backup`
- placeholder release/backup workflows

What remains scaffolded for later phases:

- real operational persistence backends
- Parquet release generation
- remote backup uploads
- additional dataset handlers beyond `news_items / ingest`

## Config Layout

Preferred checked-in config layout:

```text
agents/
  news/
    local.yaml
    github.yaml
```

Backward-compatible shims are still present:

- `agents/news.yaml`
- `agents/news-github.yaml`

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
