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
- #72: keep active and use as the next narrow source-native reliability follow-up because the
  4-source guardrail still fires for Ynet, Walla, Maariv, and ICE.
- #88: keep as later optimization; this source-health triage did not exercise or expose backfill
  aggregate-count slowness.

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

## Validation

- `.venv/bin/python scripts/validate_agent_plan.py`
- `.venv/bin/ruff format .`
- `.venv/bin/ruff check .`
- `.venv/bin/mypy src/`
- `.venv/bin/pytest -q tests/unit/test_validate_agent_plan.py`
- `.venv/bin/pytest -q tests/unit/test_source_health.py tests/unit/test_cli.py -k 'diagnose_sources or source_zero or mako or ynet'`
