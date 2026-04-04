# TFHT Enforcement Index — State of the Project

This document is a quick orientation guide for new contributors to `tfht_enforce_idx`.

## What this repository is

`tfht_enforce_idx` started as a focused news scanner for Israeli anti-brothel-law enforcement news, and has since evolved into a small dataset/job platform centered around TFHT’s enforcement-index work.

Today, the repository’s implemented first-class dataset is:

- `news_items`

And the implemented job types are:

- `news_items / ingest`
- `news_items / release`
- `news_items / backup`

The current system can:

- scrape and ingest relevant Israeli news items from multiple sources
- classify relevance with an LLM
- deduplicate overlapping coverage
- normalize results into operational `news_items` records
- persist those records to Supabase
- emit daily review output through CLI/email
- build metadata-only public release bundles
- publish those bundles to Kaggle and Hugging Face when configured
- upload latest backups to Google Drive and S3-compatible object storage when configured
- keep dataset/job-scoped seen-state and run snapshots
- evaluate classifier variants against a maintained validation set

Planned future datasets already reflected in the architecture include:

- `docs_metadata`
- `open_docs_fulltext`
- `events`

## Current project shape

The project is now best understood as a **small data-jobs platform** with:

1. **source ingestion**
2. **LLM-assisted classification**
3. **deduplication and normalization**
4. **operational persistence**
5. **public release generation**
6. **publication and backup**
7. **validation and regression checking**
8. **GitHub Actions automation**

## Current operational flow

### 1. Ingest (`news_items / ingest`)
The ingest job:
- pulls candidate items from configured news sources
- classifies relevance/categories
- deduplicates overlapping stories
- enriches them into normalized `news_items` operational records
- writes operational records to the configured store
- updates seen-state and run snapshots
- emits CLI/email output for daily review workflows

### 2. Release (`news_items / release`)
The release job:
- reads operational rows from Supabase
- applies publication/privacy/suppression gating
- generates metadata-only public records
- builds a release bundle, centered on Parquet
- writes a manifest, schema docs, and checksums
- optionally publishes to Kaggle and Hugging Face

### 3. Backup (`news_items / backup`)
The backup job:
- locates the latest built release bundle
- uploads the latest release contents to configured backup targets
- currently supports:
  - Google Drive
  - S3-compatible object storage

### 4. Validation
The validation subsystem:
- maintains a permanent classifier validation CSV
- supports reviewed-example collection/normalization
- evaluates classifier variants using a configurable matrix
- provides regression coverage for relevance/category behavior

## Repository layout

```text
.github/
  actions/
    setup-denbust-state-job/
      action.yml
  skills/
    ci-and-agent-integrations/
      SKILL.md
    news-sources/
      SKILL.md
  workflows/
    ci-test.yml
    codecov-yaml-validate.yml
    daily-state-run.yml
    news-items-backup.yml
    news-items-daily-review.yml
    news-items-release.yml
    pr-agent-context-refresh.yml
    pyproject-validate.yml
    weekly-state-run.yml
  copilot-instructions.md
  pr-agent-context-template.md

agents/
  backup/
    news_items.yaml
  news/
    github.yaml
    local.yaml
  release/
    news_items.yaml
  validation/
    classifier_variants.yaml
  news-github.yaml
  news.yaml

data/
  runs/
    ...
  .gitkeep

docs/
  articles_examples.md
  IMPLEMENTATION_PLAN.md
  LEGAL_CONV_EN.md
  LEGAL_CONV_HE.md
  MVP_SPEC.md
  product_def.md

src/
  denbust/
    classifier/
    datasets/
    dedup/
    models/
    news_items/
    ops/
    output/
    publish/
    sources/
    store/
    validation/
    cli.py
    config.py
    data_models.py
    pipeline.py

supabase/
  migrations/
    20260319_news_items_phase_b.sql

tests/
  fixtures/
  integration/
  smoke/
  unit/

validation/
  news_items/
    classifier_validation.csv

.env.example
AGENTS.md
pyproject.toml
README.md
```

## How to read the repo

### `src/denbust/cli.py`
Main CLI entrypoint.  
Start here to understand the exposed commands:

- `denbust scan`
- `denbust run`
- `denbust release`
- `denbust backup`
- validation-related commands

This is the fastest way to understand how the repository is meant to be operated.

### `src/denbust/pipeline.py`
Core orchestration entrypoint.  
This is where config, dataset/job dispatch, state handling, output, and run snapshots come together.

### `src/denbust/config.py`
Central config models and loading behavior.  
Important for understanding:
- local vs GitHub config
- operational store selection
- release/backup config
- env-var interactions

### `src/denbust/datasets/`
Dataset/job registration layer introduced in Phase A.

- `registry.py` maps dataset/job pairs to handlers
- `jobs.py` defines the dataset/job execution surface

This is the architectural seam that future datasets should plug into.

### `src/denbust/news_items/`
The main dataset-specific implementation.

Key files:

- `ingest.py` — builds normalized operational records from unified items
- `models.py` — dataset-specific models for operational/public rows and related objects
- `normalize.py` — normalization/canonicalization helpers
- `enrich.py` — dataset-specific enrichment logic
- `policy.py` — privacy/publication/suppression gating helpers
- `release.py` — public release bundle generation
- `publication.py` — publication integration helpers
- `backup.py` — backup upload logic
- `daily_review.py` — daily review/reporting support

If you want to work on the current production dataset, this is the most important directory.

### `src/denbust/sources/`
News source adapters and scraper logic.

Current source modules include:
- `haaretz.py`
- `ice.py`
- `maariv.py`
- `mako.py`
- `rss.py`
- `walla.py`

This is where source-specific fetching logic lives.

### `src/denbust/classifier/`
LLM-assisted classification logic.  
Currently centered on relevance/category classification over fetched candidate items.

### `src/denbust/dedup/`
Story similarity and deduplication helpers.  
Important for collapsing multiple source articles into unified items.

### `src/denbust/ops/`
Operational persistence abstraction and implementations.

Important files:
- `storage.py` — abstract operational store layer
- `factory.py` — store construction
- `supabase.py` — real Supabase-backed operational store implementation

This is the layer that turns the project from “scraper + email” into a real operational data system.

### `src/denbust/publish/`
Cross-dataset release and backup abstractions introduced in Phase A and concretized in Phase B.

### `src/denbust/store/`
State persistence utilities:
- seen-store
- state path resolution
- run snapshots

This supports both local usage and GitHub Actions state-repo workflows.

### `src/denbust/validation/`
Validation and evaluation subsystem.

Important files:
- `collect.py`
- `dataset.py`
- `evaluate.py`
- `models.py`
- `common.py`

This area is increasingly important as the project becomes more typology-driven and feedback-driven.

## Important configuration directories

### `agents/news/`
Config for the ingest job.

- `local.yaml` — local/dev ingest flow
- `github.yaml` — GitHub Actions ingest flow

### `agents/release/`
Config for release jobs.

- `news_items.yaml`

### `agents/backup/`
Config for backup jobs.

- `news_items.yaml`

### `agents/validation/`
Validation/evaluation matrix config.

- `classifier_variants.yaml`

## Database layer

### `supabase/migrations/`
Contains the operational schema for Phase B.

Current migration:
- `20260319_news_items_phase_b.sql`

This creates the main `news_items` operational table and related run/release/suppression-oriented tables.

If you are touching operational persistence or schema, start here together with `src/denbust/ops/supabase.py`.

## Tests

The repo has a solid multi-layer test layout.

### `tests/unit/`
Fast tests for:
- config
- CLI
- models
- snapshots
- policy models
- dedup
- formatter/output
- validation
- dataset jobs
- Phase B `news_items` behavior

### `tests/integration/`
Broader workflow behavior:
- pipeline execution
- scraper behavior

### `tests/smoke/`
Basic smoke coverage for the repo

### `tests/fixtures/`
Representative source/article/RSS/HTML fixtures used by tests

New contributors should check fixtures before changing scrapers or classification logic.

## Automation / GitHub Actions

Key workflows:

- `news-items-daily-review.yml` — daily ingest/review path
- `news-items-release.yml` — weekly release/publication path
- `news-items-backup.yml` — backup path
- `ci-test.yml` — test suite
- `daily-state-run.yml` / `weekly-state-run.yml` — state repo support and automation scaffolding

These workflows are important because the repository is meant to run operationally, not just locally.

## Current strengths of the project

The repo is already in a strong place in several ways:

- clear dataset/job architecture
- real end-to-end `news_items` implementation
- proper operational persistence
- metadata-only public release design
- release/publication/backup separation
- validation subsystem exists instead of being an afterthought
- state-repo + run-snapshot operational traceability
- reasonably organized config/workflow structure

## Current likely next area of development

The next major evolution is likely to be **typology / validation / product alignment** with TFHT’s richer category hierarchy, manual examples, report template, and minisite needs.

That likely means future work around:
- closed-set TFHT taxonomy assets
- manual overrides and reviewed examples import
- richer validation/evaluation
- monthly report generation
- minisite-facing exports
- eventual additional datasets

## Recommended onboarding path for new contributors

A good way to get oriented is:

1. Read `README.md`
2. Read `src/denbust/cli.py`
3. Read `src/denbust/pipeline.py`
4. Read `src/denbust/news_items/`
5. Read `src/denbust/ops/supabase.py`
6. Read `src/denbust/validation/evaluate.py`
7. Skim `agents/` configs
8. Skim `tests/unit/test_news_items_phase_b.py`
9. Skim the relevant GitHub workflow files

That path gives a good picture of:
- what exists
- what runs in production
- where future contributions should land

## Suggested contributor mental model

When making changes, think in this order:

1. **Source acquisition**
2. **Classification**
3. **Dedup/unification**
4. **Normalization into dataset-specific records**
5. **Operational persistence**
6. **Public release filtering**
7. **Publication / backup**
8. **Validation / regression safety**

That is the current backbone of the repository.

## Bottom line

This is no longer just a scraper project.

It is now a small but serious data-jobs repository with:
- one implemented production dataset (`news_items`)
- a generalizable platform spine
- operational automation
- public dataset publication
- backup/export flows
- and the beginnings of a durable validation system

New contributors should treat it as a **dataset platform with one mature dataset**, not as a one-off script.
