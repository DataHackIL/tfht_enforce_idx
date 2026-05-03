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

## Current Next Focus: #72 Source-Native Reliability Follow-Through

PR `#95` added the May 2026 local experiment plan. PR `#96` hardened that plan's execution path so
local validation data problems and Anthropic provider failures fail visibly before operators trust
the resulting metrics. The first source-recall follow-up adds a Ynet משפט ופלילים category-page
backstop while keeping the RSS feed as the primary Ynet source. The Phase C source-health
follow-through added a report-level source-zero guardrail and explicit Mako browser/navigation
failure-mode details. The current #72 follow-through reproduced systemic source-zero for Ynet,
Walla, Maariv, and ICE, then expanded search-backed discovery/backfill so taxonomy recall terms are
also queried against each configured news domain. The #66 follow-up added fixture-backed Ynet recall
coverage over that source-targeted taxonomy path. The #97 validation follow-up now shares
taxonomy/category/index-relevance row-integrity checks between validation lint and finalize/import,
so permanent-set preflight and reviewed-row ingestion enforce the same semantic invariants.
`DL-PR-12` now adds the smallest self-healing on-ramp: scrape failures are grouped into structured
diagnostic buckets, the queue reports self-heal-eligible candidates, generic fetch/source-adapter
attempts carry stable failure-stage diagnostics, and future orchestration can select
self-heal-eligible failed candidates without running AI repair.

A fresh Phase C source-health triage pass on 2026-05-03 used an isolated
`data/may_26_followup/20260503T074131Z/state` root and Chromium installed through Playwright before
live Mako probing. The all-source run showed Mako `ok` and Haaretz `ok`; Ynet, Walla, Maariv, and
ICE still produced source-zero, stale-result, or keyword-zero diagnostics, so
`source_zero_summary.systemic_source_zero_suspected` remains true at the 4-source threshold. The
per-source Mako run also passed, which makes #71/#74 duplicate or stale Mako runtime hygiene rather
than the next correctness fix. #72 remains active as the narrow source-native reliability follow-up.
#88 remains a later persistence optimization because this diagnostic pass did not exercise or expose
backfill aggregate-count slowness. The auditable evidence summary is checked in at
[`docs/phase_c_source_health_triage_2026_05_03.md`](docs/phase_c_source_health_triage_2026_05_03.md).

### What is already in place

- Candidate persistence, scrape attempts, queue state, fallback retention, and backfill jobs already
  exist under `src/denbust/discovery/` and `src/denbust/pipeline.py`.
- Discovery diagnostics already flow through `src/denbust/diagnostics/discovery.py` and
  `denbust diagnose-discovery`.
- Source-health diagnostics already cover selector drift, parse-zero, stale-result, and keyword-zero
  cases.
- Source-health diagnostics include a `source_zero_summary` that flags the 4+ affected-source
  guardrail used to decide whether a run is systemic rather than source-specific.
- Discovery diagnostics include structured scrape-failure groups keyed by attempt kind, fetch
  status, error code, source adapter, and domain, including self-heal-eligible counts.
- Candidate scrape failures mark durable candidates as `self_heal_eligible`, and the candidate queue
  exposes a future self-heal selector without changing current ingest/backfill behavior.
- Mako live diagnostics distinguish missing browser runtime, navigation timeout, context destroyed,
  redirect/anti-bot, selector drift, parse-zero, and stale/keyword-zero failure modes where the
  rendered state supports that classification.
- Ynet source-health diagnostics now split RSS and category-page checks so RSS low coverage,
  category HTTP failure, category parse-zero, and category keyword-zero outcomes are visible.
- Search-backed discovery and backfill now emit source-targeted taxonomy queries for every enabled
  configured news domain, giving the durable candidate layer a domain-constrained fallback when
  source-native probes zero out.
- A fixture-backed Ynet regression protects the source-targeted taxonomy search path for the known
  February 12, 2026 article `https://www.ynet.co.il/news/article/bkcarhip11g`, including candidate
  normalization, preferred-domain/query provenance, source-adapter materialization, and
  pre-classification ingest handoff.
- Validation evaluation already reports stage-wise relevance, enforcement, taxonomy, and index
  metrics.
- Validation lint and reviewed-row finalize/import now share row-level taxonomy pair/version,
  legacy category compatibility, and `index_relevant` checks.

### What comes next

1. Implement a narrow #72 source-native reliability PR for Ynet, Walla, Maariv, and ICE using the
   fresh Phase C diagnostic evidence.
2. Treat #71/#74 as duplicate or near-duplicate Mako runtime/navigation diagnostic hygiene unless a
   future live Mako run fails after Chromium is installed.
3. Keep #88 lower priority unless bounded backfill evidence shows aggregate-count updates are a real
   local bottleneck.
4. Keep full AI repair, selector rewriting, and automatic source creation out of scope until a later
   self-heal implementation PR has fresh failure evidence.

### Likely code touchpoints

- `src/denbust/pipeline.py`
- `src/denbust/discovery/state_paths.py`
- `src/denbust/discovery/storage.py`
- `src/denbust/discovery/models.py`
- `src/denbust/discovery/scrape_queue.py`
- `src/denbust/diagnostics/__init__.py`
- `src/denbust/diagnostics/discovery.py`
- `src/denbust/diagnostics/source_health.py`
- `src/denbust/cli.py`
- `tests/unit/test_pipeline_core.py`
- `tests/unit/test_ynet_search_recall_fixture.py`
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
