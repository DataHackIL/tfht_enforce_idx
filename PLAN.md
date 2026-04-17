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

## Current Next Focus: Post-`DL-PR-07` Discovery Work

`DL-PR-07` is now merged via PR `#82`. The next default discovery-layer step in the rollout plan is `DL-PR-08`, while `DL-PR-09` remains queued immediately after it unless priorities are explicitly reordered.

### What `DL-PR-07` delivered

- Candidate persistence, scrape attempts, and queue state already exist under `src/denbust/discovery/`.
- A lightweight overlap artifact already exists at `state_repo/.../metrics/engine_overlap_latest.json`.
- Discovery overlap/diagnostics generation already flows through `src/denbust/diagnostics/discovery.py`; the pipeline no longer keeps separate per-engine candidate ID sets.
- The repo already has a diagnostics pattern in `src/denbust/diagnostics/source_health.py` and `src/denbust/cli.py`.
- Discovery diagnostics now also emit a fuller `discovery_diagnostics_latest.json` artifact and a `denbust diagnose-discovery` CLI entry point.

### What comes next

1. `DL-PR-08` is the default next discovery PR in the rollout plan:
   - search-result-only fallback rows
   - partial retention
   - lower-confidence review/publication handling
2. `DL-PR-09` remains the next queued follow-up after that:
   - backfill batches
   - historical query generation
   - backfill discover/scrape jobs
3. Phase C follow-through is still open in parallel, so `.agent-plan.md` should be treated as the operational priority pointer when sequencing work.

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

- Do not fold `DL-PR-09` backfill work into the next PR unless priorities are explicitly changed.
- Do not add self-healing or event-table work yet.
- Keep the next discovery PR focused on its slice rather than mixing in workflow rollout or later-stage architecture work.

## Planning Workflow

- When a PR is opened against a tracked plan item, the PR itself should update `.agent-plan.md`, `README.md`, and any relevant human-facing plan document so the repository reflects the state expected after that PR is merged.
- Plan-tracked PRs should land with both implementation and planning/docs state aligned in the same merge.
