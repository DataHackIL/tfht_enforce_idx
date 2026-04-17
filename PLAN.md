# PLAN.md

This repo currently has one main plan and two important sub-plans.

## Main Plan

- [`docs/CHATGPT_26_04_PLAN.md`](docs/CHATGPT_26_04_PLAN.md) is the primary implementation roadmap.
- It describes the high-level product and data-model evolution of the project across the major milestones.
- When there is a question about overall sequencing or intended end-state, this is the source of truth.

## Milestone 3 Validation Sub-Plan

- [`docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md`](docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md) is a branch plan under Milestone 3 of the main plan.
- It exists because Milestone 3 validation work exposed enough complexity and implementation issues to justify splitting that milestone into smaller reviewable PRs.
- It should be read as a delivery breakdown for Milestone 3, not as a separate product roadmap.

## Discovery/Candidacy Architecture Sub-Plan

- [`docs/tfht_discovery_layer_implementation_plan.md`](docs/tfht_discovery_layer_implementation_plan.md) is a separate sub-plan for the discovery-layer architecture update.
- Its `DL-PR-*` series covers the separation of discovery and candidacy concerns from scraping/ingest concerns, along with rollout of the new operational model.
- It is related to the main plan as enabling architecture, but it is not a replacement for the main milestone roadmap.

## Practical Reading Order

1. Read `docs/CHATGPT_26_04_PLAN.md` for the overall roadmap.
2. Read `docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md` when working specifically on Milestone 3 validation follow-through.
3. Read `docs/tfht_discovery_layer_implementation_plan.md` when advancing the discovery/candidacy architecture work in the `DL-PR-*` series.

## Current Next Focus: `DL-PR-07`

`DL-PR-07` is the next planned discovery-layer step. Its purpose is to make the discovery/candidacy layer measurable and debuggable, building on the existing `DL-PR-01` through `DL-PR-06` foundation.

### What already exists

- Candidate persistence, scrape attempts, and queue state already exist under `src/denbust/discovery/`.
- A lightweight overlap artifact already exists at `state_repo/.../metrics/engine_overlap_latest.json`.
- Discovery overlap/diagnostics generation already flows through `src/denbust/diagnostics/discovery.py`; the pipeline no longer keeps separate per-engine candidate ID sets.
- The repo already has a diagnostics pattern in `src/denbust/diagnostics/source_health.py` and `src/denbust/cli.py`.

### What `DL-PR-07` still needs

1. Introduce a dedicated discovery diagnostics/reporting module under `src/denbust/diagnostics/` for discovery-layer observability.
2. Define typed report models for:
   - engine overlap
   - source-native vs search-engine recall/coverage
   - candidate-to-news-item conversion
   - queue health
3. Expand discovery metrics outputs beyond `engine_overlap_latest.json` to include a coherent report artifact set under `src/denbust/discovery/state_paths.py`.
4. Add queue-health calculations from persisted candidates and scrape attempts:
   - new candidates
   - stale candidates
   - failed candidates
   - retry backlog
5. Add conversion metrics from candidate state and downstream ingest results:
   - scrape succeeded
   - scrape failed
   - partially scraped
   - unsupported source
   - candidate-to-news-item conversion counts/rates
6. Add source-native vs search-engine coverage reporting using current candidate provenance and discovered-via metadata.
7. Add a CLI entry point in `src/denbust/cli.py` for discovery diagnostics, following the same text/JSON pattern used by `diagnose-sources`.
8. Add unit tests for report generation, artifact writing, and CLI behavior.

### Likely code touchpoints

- `src/denbust/pipeline.py`
- `src/denbust/discovery/state_paths.py`
- `src/denbust/discovery/storage.py`
- `src/denbust/discovery/models.py`
- `src/denbust/diagnostics/__init__.py`
- `src/denbust/diagnostics/source_health.py` as a structural reference only
- `src/denbust/cli.py`
- `tests/unit/test_pipeline_core.py`
- `tests/unit/test_cli.py`
- new diagnostics-focused unit tests under `tests/unit/`

### Scope guardrails

- Do not add backfill logic yet; that belongs to `DL-PR-09`.
- Do not add self-healing or event-table work yet.
- Keep this PR focused on observability/reporting, not on changing scrape semantics or public-release policy.
