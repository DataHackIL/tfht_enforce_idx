# Implementation Plan: Persistent Multi-Engine Discovery & Candidacy Layer

This plan breaks the discovery/candidacy feature into a sequence of well-scoped discovery-layer PRs for `tfht_enforce_idx`.

It assumes the following design document exists in `docs/`:

- `docs/tfht_discovery_layer_design_amended.md`

## Guiding principles

- Keep each PR mergeable and low-risk.
- Preserve the current working ingest/release/backup flow while introducing the new layer underneath.
- Prefer additive changes and compatibility shims over disruptive rewrites.
- Make candidacy durable before making discovery smarter.
- Introduce engines incrementally.
- Keep source-native candidacy as a first-class producer throughout.
- Ensure each PR leaves the repo in a coherent, testable state.

## Discovery-layer PR labeling

To avoid confusion with GitHub pull request numbers, the discovery-layer rollout uses the prefix
`DL-PR-XX` in planning documents:

- `DL-PR-01`
- `DL-PR-02`
- `DL-PR-03`
- ...

These labels are planning identifiers, not GitHub PR numbers.

---

## DL-PR-01 — Discovery/Candidacy foundation (models, config, persistence scaffolding)

### Goal
Introduce the durable candidate layer without changing the production ingest behavior yet.

### Scope
- Add `src/denbust/discovery/` package scaffolding
- Add core models:
  - `DiscoveryQuery`
  - `DiscoveredCandidate`
  - `PersistentCandidate`
  - `CandidateProvenance`
  - `ScrapeAttempt`
- Add config models/sections for:
  - `discovery`
  - `source_discovery`
  - `candidates`
  - optional `backfill`
- Add persistence abstractions/interfaces for:
  - candidate store
  - provenance store
  - scrape-attempt store
- Add Supabase migration(s) for:
  - `discovery_runs`
  - `candidate_provenance`
  - `persistent_candidates`
  - `scrape_attempts`
- Add state-repo path conventions and snapshot file helpers for candidate-layer files
- Add unit tests for models/config/path resolution
- Add docs update describing the new layer and tables

### Out of scope
- no engine implementation yet
- no source-native integration yet
- no scrape queue yet
- no workflow changes beyond placeholders if needed

### Deliverable
A mergeable persistence/config/model foundation that does not yet affect production runs.

---

## DL-PR-02 — Source-native candidacy persistence

### Goal
Make existing source-native discovery write durable candidates.

### Scope
- Introduce a `SourceCandidateProducer` abstraction or equivalent
- Adapt current source-native candidacy flow to emit `DiscoveredCandidate`
- Normalize source-native candidates into:
  - `candidate_provenance`
  - `persistent_candidates`
- Add candidate merge/upsert logic by canonical URL where possible
- Preserve existing immediate ingest path behavior
- Add provenance fields showing source-native origin
- Add tests covering:
  - source-native candidate creation
  - canonical merge/upsert
  - repeat discovery of same item
- Add minimal CLI/internal path to run “discover from source-native only”

### Out of scope
- no Brave/Exa/Google yet
- no separate scrape queue yet
- no retry scheduler yet

### Deliverable
The existing source-native system now persists durable candidates without breaking current ingest.

---

## DL-PR-03 — Separate scrape-attempt layer and candidate-driven ingest

### Goal
Introduce candidate-driven scraping and retryable scrape state.

### Scope
- Add candidate selection / queueing logic
- Add scrape-attempt writer/update path
- Add candidate status transitions:
  - `new`
  - `queued`
  - `scrape_pending`
  - `scrape_in_progress`
  - `scrape_succeeded`
  - `scrape_failed`
  - `partially_scraped`
  - `unsupported_source`
- Add `ScrapeAttempt` persistence
- Refactor ingest pipeline so it can consume candidates and produce `news_items`
- Preserve existing combined convenience behavior:
  - current ingest path can still run end-to-end
- Add fallback handling for:
  - source adapter
  - generic fetch/extract
- Add tests for:
  - candidate → scrape attempt → news_item success
  - candidate retained after failure
  - repeated scrape attempt bookkeeping

### Out of scope
- no external search engines yet
- no backfill scheduling yet
- no self-healing yet

### Deliverable
A working durable candidate queue with retryable scrape state underneath the existing ingest flow.

---

## DL-PR-04 — Brave engine integration

### Goal
Add the first search-engine discovery path.

### Scope
- Implement `DiscoveryEngine` abstraction if not already done
- Add Brave adapter under `src/denbust/discovery/engines/brave.py`
- Add query builder support for:
  - broad web queries
  - source-targeted queries
- Normalize Brave results into `DiscoveredCandidate`
- Persist Brave candidates into candidate store
- Merge Brave candidates with source-native candidates
- Add engine diagnostics and run metrics
- Add config/docs for Brave API key and limits
- Add unit tests with mocked Brave responses

### Out of scope
- no Exa yet
- no Google yet
- no backfill yet

### Deliverable
Search-engine discovery begins contributing durable candidates, starting with Brave.

---

## DL-PR-05 — Exa engine integration

### Goal
Add Exa as a semantic/AI-native discovery engine.

### Status
Implemented.

### Scope
- Add Exa adapter under `src/denbust/discovery/engines/exa.py`
- Support:
  - standard query search
  - optional “find similar” mode scaffold
- Normalize Exa results into `DiscoveredCandidate`
- Persist/merge into durable candidate layer
- Add config/docs for Exa API key and usage controls
- Add tests with mocked Exa responses
- Add engine-level observability fields where useful

### Out of scope
- no Google yet
- no backfill yet
- no self-healing yet

### Deliverable
A second complementary discovery engine is integrated cleanly.

---

## DL-PR-06 — Google CSE integration

### Goal
Add recall-oriented search discovery via Google CSE.

### Status
Implemented.

### Scope
- Add Google CSE adapter under `src/denbust/discovery/engines/google_cse.py`
- Support:
  - broad Hebrew queries
  - source-targeted `site:`-style queries where appropriate
  - query-budget controls
- Normalize Google results into `DiscoveredCandidate`
- Persist/merge into durable candidate layer
- Add config/docs for:
  - API key
  - programmable search engine ID
  - budget controls
- Add tests with mocked Google responses

### Out of scope
- no backfill yet
- no self-healing yet

### Deliverable
The initial three-engine discovery layer is complete:
- source-native
- Brave
- Exa
- Google CSE

---

## DL-PR-07 — Discovery observability and overlap reporting

### Goal
Make the new layer measurable and debuggable.

### Status
Implemented.

### Scope
- Add engine overlap report generation
- Add source-native vs search-engine recall reporting
- Add candidate-to-news-item conversion metrics
- Add queue health reporting:
  - new candidates
  - stale candidates
  - failed candidates
  - retry backlog
- Add state-repo metrics files such as:
  - `engine_overlap_latest.json`
- Add CLI command(s) or report helpers for discovery diagnostics
- Add tests around report generation logic

### Out of scope
- no backfill yet
- no self-healing yet

### Deliverable
You can now tell whether the discovery layer is actually helping.

---

## DL-PR-08 — Search-result-only fallback rows and partial retention

### Goal
Retain value from promising candidates even when full scraping fails.

### Status
Implemented.

### Scope
- Add explicit handling for:
  - `content_basis = search_result_only`
  - `content_basis = partial_page`
- Add candidate/publication review flags for lower-confidence rows
- Ensure public release excludes these by default unless policy changes later
- Add operational support for:
  - monitoring
  - queue review
  - event-reference eligibility later
- Add tests for:
  - retention on failed scrape
  - default exclusion from public release

### Out of scope
- no backfill scheduler yet
- no self-healing yet

### Deliverable
The system can retain useful low-confidence candidates without pretending they are fully scraped articles.

---

## DL-PR-09 — Backfill foundation

### Goal
Enable slow, systematic historical gap-closing from the candidate layer.

### Status
Implemented.

### Scope
- Add backfill models/config:
  - `backfill_batches`
  - candidate `backfill_batch_id`
- Add historical query generation over configurable windows
- Add jobs/commands for:
  - `news_items / backfill_discover`
  - `news_items / backfill_scrape`
- Add batch-aware queue prioritization
- Add daily-drain behavior limits:
  - max candidates per run
  - max scrape attempts per run
- Add docs and tests for backfill scheduling and queue behavior

### Implemented notes
- `news_items / backfill_discover` now requires
  `DENBUST_BACKFILL_DATE_FROM` / `DENBUST_BACKFILL_DATE_TO`, creates one durable batch per
  invocation, and plans contiguous historical windows using `backfill.batch_window_days`
- backfill batches are mirrored into the state repo and persisted into Supabase via
  `backfill_batches`
- historical search-engine discovery is live for Brave, Exa, and Google CSE
- source-native historical discovery is capability-based: sources that do not implement explicit
  window fetching are skipped with warnings rather than treated as fatal
- `news_items / backfill_scrape` drains one historical batch at a time with oldest-window-first
  ordering and reuses the existing scrape-to-ingest path

### Out of scope
- no self-healing yet
- no automatic source addition yet

### Deliverable
Historical discovery becomes operationally possible without overwhelming the pipeline.

---

## DL-PR-10 — Source suggestion and social-targeted discovery support

### Goal
Lay the groundwork for future source expansion and social/reference discovery.

### Scope
- Add source-suggestion reporting from candidate provenance:
  - repeated unseen domains
  - scrape success/failure signals
- Add a persisted source-suggestion diagnostics artifact and include it in rendered discovery diagnostics
- Add `social_targeted` query support in the live and backfill query builders
- Make `social_targeted` part of the default discovery query-kind set
- Allow candidate persistence for Facebook search results via search discovery
- Keep social candidates as candidate/reference-first by default
- Persist `social_targeted` search results as `social_search` provenance/candidates and mark them non-scrapeable by default
- Add docs on intended use and limitations
- Add tests for source suggestion logic and social query handling

### Out of scope
- no full social scraping
- no automatic source creation yet
- no event inference yet

### Deliverable
The candidate layer now supports future expansion into new sources and social/reference evidence without treating Facebook discovery as scrapeable source content.

---

## DL-PR-11 — Workflow and operations rollout

### Goal
Expose the new layer safely in GitHub Actions and local operations.

### Scope
- Add/update workflows for:
  - `discover`
  - candidate-driven ingest
  - optional backfill jobs
- Keep current daily email reporting intact
- Add clear env/secret docs for:
  - Brave
  - Exa
  - Google CSE
- Add operational docs:
  - local run path
  - GitHub path
  - candidate tables/files
  - retry semantics
- Add migration/setup checklist

### Out of scope
- no self-healing implementation yet
- no event table yet

### Deliverable
The feature is operationally usable in CI/GitHub Actions.

---

## Optional DL-PR-12 — Self-healing scaffolding hooks

### Goal
Add explicit hooks for the future AI-based self-healing phase without implementing it yet.

### Scope
- add `self_heal_eligible` plumbing where still missing
- add structured scrape-failure diagnostics
- add explicit self-heal retry attempt kind
- add docs for future self-heal workflow

### Deliverable
A clean on-ramp for the next large feature, but not the feature itself.

---

## Recommended merge order

The recommended order is:

1. DL-PR-01 — foundation
2. DL-PR-02 — source-native candidacy persistence
3. DL-PR-03 — scrape-attempt layer
4. DL-PR-04 — Brave
5. DL-PR-05 — Exa
6. DL-PR-06 — Google CSE
7. DL-PR-07 — observability
8. DL-PR-08 — fallback rows
9. DL-PR-09 — backfill foundation
10. DL-PR-10 — source suggestion + social-targeted support
11. DL-PR-11 — workflow rollout
12. DL-PR-12 — self-healing hooks (optional)

---

## What should already be usable after each stage

### After DL-PR-03
- durable candidate queue exists
- failed scraping is retryable
- current system can be migrated onto the new substrate

### After DL-PR-06
- the core multi-engine discovery feature is functionally implemented

### After DL-PR-08
- candidate retention is mature enough for real-world imperfect scraping
- partial-page and search-result-only fallbacks can be retained without leaking into public release

### After DL-PR-09
- historical backfill becomes feasible
- the earlier `C-8` vs `DL-PR-09` sequencing choice is resolved, and `C-8` is now explicitly deferred until after the full `DL-PR-*` sequence completes

### After DL-PR-11
- the feature can be used operationally in CI/jobs

---

## What should explicitly wait until later

These should not be folded into the core discovery PR series unless truly needed:

- full AI-based self-healing implementation
- automatic source creation
- event-level unification/inference
- full social-network scraping
- major release/publication redesign

They should build on top of the candidate substrate, not be mixed into its initial rollout.

---

## Success criteria for the feature as a whole

This feature is “done enough” when:

- source-native candidacy and search-engine discovery both feed the same persistent candidate layer
- candidates survive failed scraping and can be retried
- Brave, Exa, and Google CSE all work as candidate producers
- scrape attempts are tracked durably
- overlap/recall/conversion reporting exists
- backfill can be scheduled incrementally
- workflows/docs make the feature usable in practice
