# Phase C Source-Health Triage Evidence: 2026-05-03

This note preserves the durable evidence summary for the Phase C source-health triage decision.
Raw live diagnostic artifacts were generated under ignored `data/` paths and are not committed.

## Run Context

- Timestamp: `2026-05-03T07:41:32.027408Z`
- Config: `agents/news/local.yaml`
- Isolated state root: `data/may_26_followup/20260503T074131Z/state`
- Browser setup: `python -m playwright install chromium`
- All-source diagnostic command:

```bash
DENBUST_STATE_ROOT="data/may_26_followup/20260503T074131Z/state" \
denbust diagnose-sources \
  --config agents/news/local.yaml \
  --live-only \
  --sample-keyword "זנות" \
  --sample-keyword "בית בושת" \
  --sample-keyword "סחר בבני אדם" \
  --format json \
  --output "data/may_26_followup/20260503T074131Z/artifacts/diagnose_sources_live_all.json"
```

In the local shell used for this run, commands were invoked through `.venv/bin/...` because bare
`python` was not on `PATH`. The portable commands above assume the project environment is activated.

Source-specific diagnostics used the same state root and sample keywords with repeated
`--source <source>` runs for `ynet`, `walla`, `mako`, `maariv`, `haaretz`, and `ice`.

## Source-Zero Summary

| Field | Value |
|---|---:|
| threshold | 4 |
| enabled sources | 6 |
| selected sources | 6 |
| affected sources | 4 |
| systemic source-zero suspected | true |

Affected sources: `ynet`, `walla`, `maariv`, `ice`.

## Source Results

| Source | Status | Failure bucket | Evidence |
|---|---|---|---|
| `ynet` | `warn` | `keyword_filter_zeroed_results` | RSS returned HTTP 200 with 30 entries and 0 sampled keyword matches; category page returned HTTP 200, parsed 40 articles, and had 0 sampled keyword matches. |
| `walla` | `warn` | `keyword_filter_zeroed_results` | May 2026 archive URLs for sections 1 and 10 returned 404; April 2026 archive pages returned HTTP 200 with 50 recent entries each and 0 sampled keyword matches. |
| `mako` | `ok` | none | Search probes for `זנות` and `בית בושת` returned parsed keyword-matching articles; `סחר בבני אדם` rendered but parsed zero; section page parsed 30 articles with 0 sampled keyword matches. |
| `maariv` | `warn` | `keyword_filter_zeroed_results` | Live probe returned HTTP 200, parsed 13 articles, and had 0 sampled keyword matches. |
| `haaretz` | `ok` | none | Search probe for `זנות` returned HTTP 200 with 2 recent entries and 1 keyword match; other sampled searches returned no current keyword match. |
| `ice` | `warn` | `stale_results` | All sampled search pages returned HTTP 200 but only stale candidates outside the cutoff window. |

## Issue Decisions

- #71: close as duplicate or stale Mako runtime hygiene unless a future Chromium-backed Mako probe
  regresses.
- #74: close as duplicate or stale with #71 unless a future Chromium-backed Mako probe regresses.
- #72: addressed by the later source-native reliability follow-up, which expanded Walla/ICE recall
  terms and narrowed the hard source-zero guardrail so keyword-zero evidence stays visible without
  counting as systemic source outage evidence.
- #88: addressed by PR #107, which moved backfill aggregate candidate counts behind the discovery
  persistence boundary.

## Mako Runtime Hygiene Follow-Up

- Timestamp: `2026-05-03T13:13:10.260820Z`
- Config: `agents/news/local.yaml`
- Isolated state root: `data/may_26_followup/20260503T131309Z/state`
- Browser setup: `.venv/bin/python -m playwright install chromium`
- Source-specific diagnostic command:

```bash
DENBUST_STATE_ROOT="data/may_26_followup/20260503T131309Z/state" \
.venv/bin/denbust diagnose-sources \
  --config agents/news/local.yaml \
  --live-only \
  --source mako \
  --sample-keyword "זנות" \
  --sample-keyword "בית בושת" \
  --sample-keyword "סחר בבני אדם" \
  --format json \
  --output "data/may_26_followup/20260503T131309Z/artifacts/diagnose_sources_live_mako.json"
```

Result: Mako returned `ok`; `source_zero_summary.affected_source_count` was `0` for the selected
source. The `זנות` and `בית בושת` search probes returned parsed keyword-matching articles. The
`סחר בבני אדם` search probe rendered but parsed zero articles, and the `men-men_news` section page
parsed 30 articles with zero sampled keyword matches; those warnings did not indicate browser
runtime, navigation, context-destruction, redirect/anti-bot, or selector-drift regression.

This confirms #71/#74 are stale/duplicate Mako runtime hygiene after Chromium-backed verification.
No scraper behavior, selector rewrite, retry path, or live-network-dependent regression test is
needed for their closure.

## Post-#108 Planning Reset

- PR #108 was squash-merged into `main` as `dea6406` and closed #71/#74.
- A fresh 2026-05-03 GitHub issue search through the repo connector returned zero open issues.
- Artifact-only diagnostics were rerun under
  `data/may_26_followup/20260503T134102Z/state`:
  - `diagnose-discovery` found no persisted candidates, scrape attempts, or operational records.
  - `diagnose-sources --artifacts-only` skipped all six configured sources because no ingest debug
    summary existed under the isolated state root.
- Durable evidence summary:
  - open GitHub issues: `0`;
  - discovery candidate files: absent under the isolated state root;
  - queue-health candidate counts: all `0`;
  - scrape-failure diagnostic groups: none;
  - source artifact results: six `skip` results because no ingest debug summary exists.
- Recommendation: do not open another implementation PR from this empty-state evidence alone. The
  next bounded task should produce `reports/candidate_drain_summary.md` from a fresh ignored state
  root after running `discover`, `scrape_candidates`, `diagnose-discovery`, and
  `diagnose-sources --artifacts-only` against all sources in `agents/news/local.yaml`. Add one
  latest-seven-complete-UTC-days backfill discover/scrape window only if that first pass produces no
  scrapeable candidates.

## Candidate-Drain Evidence Follow-Up

- PR #109 was squash-merged into `main` as `201c247`.
- A fresh 2026-05-03 GitHub issue check returned zero open issues.
- Evidence root: `data/may_26_followup/20260503T153123Z/`.
- Config: `agents/news/local.yaml`.
- Commands run under `DENBUST_STATE_ROOT=data/may_26_followup/20260503T153123Z/state`:
  - `discover`;
  - `scrape_candidates`;
  - `diagnose-discovery --format json`;
  - `diagnose-sources --artifacts-only --format json`.
- The first `scrape_candidates` command failed fast with `fatal: missing anthropic api key`; the
  rerun sourced local `.env.local` variables inside the subprocess and completed.
- `diagnose-discovery` reported 63 total candidates, 30 scrape-succeeded candidates, 33
  never-scraped candidates, 0 scrape-failed candidates, 0 retry-backlog candidates, and 0
  self-heal-eligible candidates.
- The 30 scrape attempts all landed on ICE candidates; 33 candidates remained `new` /
  `candidate_only`: 1 Haaretz, 11 ICE, 1 Maariv, 14 Mako, and 6 Walla.
- `scrape_candidates` classified 30 unseen ICE articles and rejected all 30 as `not_relevant`,
  producing no unified items.
- `diagnose-sources --artifacts-only` returned six `skip` results because this diagnostic path
  looks for ingest debug summaries, while the bounded pass produced `scrape_candidates` debug
  summaries. Its report-level `source_zero_summary` did not trigger:
  `affected_source_count = 0`, `keyword_zero_source_count = 0`, and
  `systemic_source_zero_suspected = false`.
- The latest-seven-complete-UTC-days backfill window was not run because the first pass produced
  scrapeable candidates and successful scrape attempts.
- Triage outcome: do not open a source-health reliability PR or self-healing orchestration PR from
  this evidence. The next narrow implementation PR should be a backfill/queue reliability follow-up
  focused on candidate-drain selection visibility or fairness.

Use this triage matrix for the next implementation choice:

| Evidence outcome | Next PR |
|---|---|
| Any Mako browser/runtime failure mode, any hard source fetch/parse/stale failure for a configured source, or `source_zero_summary.systemic_source_zero_suspected = true` | Source-health reliability PR scoped to the failing source or guardrail |
| Source health has no hard failures, but scrape-eligible or retry backlog is present and batch status refresh timing is the bottleneck or noisy part of the run | Backfill/queue reliability PR scoped to the measured bottleneck |
| `scrape_failed_candidates >= 10`, or at least 25% of attempted candidates fail, and `self_heal_eligible_candidates > 0` in repeated structured failure groups across at least two domains | Self-healing selection/orchestration PR, still without AI repair or selector rewriting |
| No candidates, no hard source failures, and no scrape-failure groups | Evidence gap only: adjust the evidence run design or source/search inputs before opening an implementation PR |

## Validation

- `.venv/bin/python scripts/validate_agent_plan.py`
- `.venv/bin/ruff format .`
- `.venv/bin/ruff check .`
- `.venv/bin/mypy src/`
- `.venv/bin/pytest -q tests/unit/test_validate_agent_plan.py`
- `.venv/bin/pytest -q tests/unit/test_source_health.py tests/unit/test_cli.py -k 'diagnose_sources or source_zero or mako or ynet'`
