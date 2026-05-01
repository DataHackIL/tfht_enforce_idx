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

## Current Next Focus: May 2026 Experiment Follow-Through

PR `#95` added the May 2026 local experiment plan. PR `#96` hardened that plan's execution path so
local validation data problems and Anthropic provider failures fail visibly before operators trust
the resulting metrics. The first source-recall follow-up adds a Ynet משפט ופלילים category-page
backstop while keeping the RSS feed as the primary Ynet source.

### What is already in place

- Candidate persistence, scrape attempts, queue state, fallback retention, and backfill jobs already
  exist under `src/denbust/discovery/` and `src/denbust/pipeline.py`.
- Discovery diagnostics already flow through `src/denbust/diagnostics/discovery.py` and
  `denbust diagnose-discovery`.
- Source-health diagnostics already cover selector drift, parse-zero, stale-result, and keyword-zero
  cases.
- Ynet source-health diagnostics now split RSS and category-page checks so RSS low coverage,
  category HTTP failure, category parse-zero, and category keyword-zero outcomes are visible.
- Validation evaluation already reports stage-wise relevance, enforcement, taxonomy, and index
  metrics.

### What comes next

1. Use the hardened May experiment path to rerun validation and live checks locally.
2. Prioritize search-backed discovery and backfill gaps from those outputs.
3. Treat optional self-healing scaffolding as later work unless the hardened experiment data shows it
   is the highest-leverage next step.

### Likely code touchpoints

- `src/denbust/pipeline.py`
- `src/denbust/discovery/state_paths.py`
- `src/denbust/discovery/storage.py`
- `src/denbust/discovery/models.py`
- `src/denbust/diagnostics/__init__.py`
- `src/denbust/diagnostics/source_health.py`
- `src/denbust/cli.py`
- `tests/unit/test_pipeline_core.py`
- `tests/unit/test_cli.py`
- new diagnostics-focused unit tests under `tests/unit/`

### Scope guardrails

- Do not commit local experiment output bundles under `data/`.
- Do not hide missing search credentials in local configs; explicit search configs should surface
  discovery errors when required env vars are absent.
- Keep the next implementation PR focused on the evidence from the hardened local run.

## Planning Workflow

- When a PR is opened against a tracked plan item, the PR itself should update `.agent-plan.md`, `README.md`, and any relevant human-facing plan document so the repository reflects the state expected after that PR is merged.
- Plan-tracked PRs should land with both implementation and planning/docs state aligned in the same merge.
