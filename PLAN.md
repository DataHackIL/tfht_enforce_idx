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

## Current Next Focus: Post-#109 Phase C Queue-Drain Follow-Up

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

The #72 source-native reliability follow-through keeps the fix intentionally narrow. Walla archive
filtering and ICE search now use targeted supplemental Hebrew recall terms, matching the
source-native relaxation already present for Ynet and Maariv. Source-health diagnostics now keep
keyword-zero visible both per source and in separate report-level keyword-zero counts, while
`source_zero_summary.systemic_source_zero_suspected` is reserved for hard
source-zero/stale/fetch/parse failures so healthy pages with no sampled keyword hit no longer
masquerade as systemic source outage evidence.

The #88 backfill reliability/performance follow-up keeps the backfill behavior unchanged while
moving aggregate candidate counts behind the discovery persistence boundary. Batch status refreshes
now ask persistence for merged and scrape-eligible counts directly; the Supabase backend uses
PostgREST exact-count metadata, and the state-repo backend streams candidate JSONL count fields
without hydrating full `PersistentCandidate` models for this pipeline path.

A fresh Phase C source-health triage pass on 2026-05-03 used an isolated
`data/may_26_followup/20260503T074131Z/state` root and Chromium installed through Playwright before
live Mako probing. The all-source run showed Mako `ok` and Haaretz `ok`; Ynet, Walla, Maariv, and
ICE still produced source-zero, stale-result, or keyword-zero diagnostics under the then-current
guardrail. The per-source Mako run also passed, which makes #71/#74 duplicate or stale Mako runtime
hygiene rather than the next correctness fix. #72 is now addressed by the narrow source-native
recall/guardrail follow-up. #88 is now addressed by the narrow aggregate-count persistence API; no
broader backfill storage refactor or source-health work is included. The auditable Phase C evidence
summary remains checked in at
[`docs/phase_c_source_health_triage_2026_05_03.md`](docs/phase_c_source_health_triage_2026_05_03.md).

A fresh source-specific Mako follow-up on 2026-05-03T13:13:10Z used
`data/may_26_followup/20260503T131309Z/state`, installed Chromium through Playwright first, and
reran the same sampled Mako diagnostic probe. Mako again returned `ok`, with parsed
keyword-matching search results for `זנות` and `בית בושת`, zero affected sources in the
source-zero summary, and no reproduction of the original runtime/navigation failures in the local
Chromium-backed diagnostic path. #71/#74 are therefore closed as stale/duplicate Mako runtime
hygiene; no scraper behavior, selector rewrite, retry path, or live-network-dependent test is added
for that closure.

PR `#108` was squash-merged into `main` as `dea6406` and closed #71/#74. A fresh 2026-05-03 GitHub
issue search through the repo connector returned no open issues. The post-#108 local evidence reset
used `data/may_26_followup/20260503T134102Z/state` and ran artifact-only diagnostics there:
`diagnose-discovery` found no persisted candidates or scrape attempts, and `diagnose-sources
--artifacts-only` skipped every configured source because the isolated root had no ingest debug
summary. That is useful isolation evidence, but it does not identify a source-health, backfill, or
self-healing code defect by itself.

PR `#109` was squash-merged into `main` as `201c247`. A fresh 2026-05-03 GitHub issue check returned
zero open issues. The bounded candidate-drain evidence pass under
`data/may_26_followup/20260503T153123Z/state` ran `discover`, `scrape_candidates`,
`diagnose-discovery`, and `diagnose-sources --artifacts-only` against `agents/news/local.yaml`.
The first `scrape_candidates` command failed fast because the Anthropic key was not in the shell
environment; rerunning with the local `.env.local` environment completed. The pass persisted 63
latest candidates, recorded 30 scrape attempts, marked 30 ICE candidates as `scrape_succeeded`, and
left 33 candidates from Haaretz, ICE, Maariv, Mako, and Walla as never scraped. It produced no
scrape-failure groups, retry backlog, self-heal-eligible candidates, or relevant classified items.
`diagnose-sources --artifacts-only` was inconclusive for source health because that diagnostic path
expects ingest debug summaries and this run produced `scrape_candidates` debug summaries. Because
the first pass produced scrapeable candidates, the conditional latest-seven-complete-UTC-days
backfill window was not used.

Durable reset summary:

| Signal | Value |
|---|---|
| Open GitHub issues | `0` from repo-connector search |
| `diagnose-discovery` candidate files | absent under the isolated state root |
| `diagnose-discovery` queue health | all candidate counts `0` |
| `diagnose-discovery` scrape failures | no failure groups |
| `diagnose-sources --artifacts-only` source results | six `skip` results because no ingest debug summary exists |
| Implementation recommendation | no code PR from empty-state evidence alone |

Candidate-drain summary:

| Signal | Value |
|---|---:|
| Open GitHub issues | `0` |
| Evidence root | `data/may_26_followup/20260503T153123Z/` |
| Persisted latest candidates | `63` |
| Scrape attempts | `30` |
| Scrape-succeeded candidates | `30`, all from `ice` |
| Never-scraped candidates | `33` across `haaretz`, `ice`, `maariv`, `mako`, and `walla` |
| Scrape failures / retry backlog / self-heal eligible | `0` / `0` / `0` |
| `diagnose-sources --artifacts-only` source results | inconclusive: six `skip` results because this path expects ingest debug summaries, while the run produced `scrape_candidates` debug summaries |
| Conditional backfill window | not used because the first pass produced scrapeable candidates |
| Implementation recommendation | queue-drain diagnostic PR scoped to candidate selection visibility and contract validation |

### What is already in place

- Candidate persistence, scrape attempts, queue state, fallback retention, and backfill jobs already
  exist under `src/denbust/discovery/` and `src/denbust/pipeline.py`.
- Backfill batch aggregate refreshes use persistence-layer candidate counts instead of listing all
  batch candidates in the pipeline.
- Discovery diagnostics already flow through `src/denbust/diagnostics/discovery.py` and
  `denbust diagnose-discovery`.
- Source-health diagnostics already cover selector drift, parse-zero, stale-result, and keyword-zero
  cases.
- Source-health diagnostics include a `source_zero_summary` that flags the 4+ hard affected-source
  guardrail used to decide whether a run is systemic rather than source-specific; keyword-zero
  outcomes stay visible through separate report-level keyword-zero counts and individual source
  checks without counting as hard source-zero evidence.
- Discovery diagnostics include structured scrape-failure groups keyed by attempt kind, fetch
  status, error code, source adapter, and domain, including self-heal-eligible counts.
- Candidate scrape failures mark durable candidates as `self_heal_eligible`, and the candidate queue
  exposes a future self-heal selector without changing current ingest/backfill behavior.
- Mako live diagnostics distinguish missing browser runtime, navigation timeout, context destroyed,
  redirect/anti-bot, selector drift, parse-zero, and stale/keyword-zero failure modes where the
  rendered state supports that classification.
- Walla and ICE source-native paths use targeted supplemental Hebrew recall terms for diagnostics
  and discovery, matching the source-specific relaxation already present for Ynet and Maariv.
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

1. Treat #71/#74 as closed stale/duplicate Mako runtime/navigation diagnostic hygiene unless a
   future live Mako run fails after Chromium is installed.
2. Implement a narrow queue-drain diagnostic follow-up from the candidate-drain evidence. The first
   target should report candidate selection order, source mix, and budget-cap behavior so operators
   can validate whether the current queue contract is behaving as intended before changing
   prioritization or fairness behavior.
3. Keep full AI repair, selector rewriting, and automatic source creation out of scope until a later
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
