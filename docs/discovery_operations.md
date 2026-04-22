# Discovery Operations

This document describes how the `news_items` discovery layer is operated locally and in GitHub
Actions after `DL-PR-11`.

## Local Run Path

Use the tracked local config for operator runs:

```bash
denbust run --dataset news_items --job discover --config agents/news/local.yaml
denbust run --dataset news_items --job ingest --config agents/news/local.yaml
DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-01-31T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml
DENBUST_BACKFILL_BATCH_ID=batch-optional \
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
```

`discover` writes into the candidate-layer namespace under `news_items/discover/`.
`ingest` remains the article-processing job and still owns `news_items/ingest/`.
Backfill jobs also read and write the shared `news_items/discover/` candidate-layer state.
The search-engine side of `discover` and `backfill_discover` now emits `taxonomy_targeted` queries
from the packaged TFHT taxonomy in addition to the coarse operator keyword list.

## GitHub Actions Run Path

The operational workflow split is:

- `news-items-discover`
  Runs daily at `03:00 UTC` and on manual dispatch.
- `daily-state-run`
  Runs daily candidate-driven ingest and keeps the daily email path intact.
- `weekly-state-run`
  Runs weekly catch-up candidate-driven ingest.
- `news-items-backfill-discover`
  Manual-only historical discovery with explicit request window inputs.
- `news-items-backfill-scrape`
  Manual-only scrape drain for one backfill batch or the oldest eligible batch.

All of these workflows:

- check out `DataHackIL/tfht_enforce_idx_state` into `state_repo/`
- set `DENBUST_STATE_ROOT=state_repo`
- use `.github/actions/setup-denbust-state-job`
- commit only namespaced state outputs back to the state repo

## Required And Optional Secrets

Common required secrets for discover and ingest family workflows:

- `STATE_REPO_PAT`
- `ANTHROPIC_API_KEY`
- `DENBUST_SUPABASE_URL`
- `DENBUST_SUPABASE_SERVICE_ROLE_KEY`

Optional discovery-engine secrets:

- `DENBUST_BRAVE_SEARCH_API_KEY`
- `DENBUST_EXA_API_KEY`
- `DENBUST_GOOGLE_CSE_API_KEY`
- `DENBUST_GOOGLE_CSE_ID`

Optional email-output secrets for ingest-family jobs:

- `DENBUST_EMAIL_SMTP_HOST`
- `DENBUST_EMAIL_SMTP_PORT`
- `DENBUST_EMAIL_SMTP_USERNAME`
- `DENBUST_EMAIL_SMTP_PASSWORD`
- `DENBUST_EMAIL_FROM`
- `DENBUST_EMAIL_TO`
- `DENBUST_EMAIL_USE_TLS`
- `DENBUST_EMAIL_SUBJECT`

Backfill workflows reuse the same discovery and ingest secrets. They do not require any extra
credential surface beyond the explicit `workflow_dispatch` inputs.

## State Repo Layout And Workflow Ownership

Discovery-family workflows own:

```text
state_repo/news_items/discover/
├── runs/
├── candidates/
├── metrics/
└── backfill_batches/
```

Ingest-family workflows own:

```text
state_repo/news_items/ingest/
├── seen.json
├── runs/
├── logs/
└── publication/
```

Backfill job namespaces also appear in the state repo:

```text
state_repo/news_items/backfill_discover/
├── runs/
└── logs/

state_repo/news_items/backfill_scrape/
├── seen.json
├── runs/
└── logs/
```

- `news-items-backfill-discover` owns the `backfill_discover/` run-artifact namespace and also
  updates the shared `discover/` candidate-layer namespace.
- `news-items-backfill-scrape` owns the `backfill_scrape/` run-artifact namespace and may also
  update both `discover/` and `ingest/` because it drains discovery candidates through the existing
  ingest pipeline.

## Retry Semantics

- `discover` is additive and durable; it updates candidate-layer files without scraping article
  bodies.
- `ingest` consumes queued candidates through the normal scrape/classify/persist flow.
- `backfill_discover` creates or extends durable historical batches using
  `DENBUST_BACKFILL_DATE_FROM` and `DENBUST_BACKFILL_DATE_TO`.
- `backfill_scrape` drains one historical batch at a time and can be constrained with
  `DENBUST_BACKFILL_BATCH_ID`.

`backfill_scrape` remains manual in this phase because historical drain rates and retry backlog are
operationally sensitive. Operators should choose when to spend scrape budget on historical work
instead of scheduling it blindly.

## One-Time 90-Day Re-Scan

`C-8` does not add a dedicated workflow. The catch-up run uses the existing backfill jobs with the
default 7-day backfill slicing:

```bash
DENBUST_BACKFILL_DATE_FROM=2026-01-23T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-04-22T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml

DENBUST_BACKFILL_BATCH_ID=batch-optional \
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
```

Use `DENBUST_BACKFILL_BATCH_ID` only if operators want to drain one batch explicitly; otherwise
`backfill_scrape` can drain the oldest eligible batch.

## Migration And Setup Checklist

1. Ensure the `news-items-ingest` environment already contains the existing ingest secrets.
2. Add the discovery-engine secrets if Brave, Exa, or Google CSE should run in Actions.
3. Enable the new workflows in GitHub Actions:
   - `news-items-discover`
   - `news-items-backfill-discover`
   - `news-items-backfill-scrape`
4. Confirm `daily-state-run` and `weekly-state-run` still point at `agents/news/github.yaml`.
5. Run one manual `news-items-discover` dispatch to verify `news_items/discover/` state is written.
6. If historical recovery is needed, launch `news-items-backfill-discover` with an explicit window,
   then drain it with `news-items-backfill-scrape`.
7. Keep `.agent-plan.md`, `README.md`, and planning docs aligned to the post-merge mainline state
   whenever workflow coverage changes.
