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

- #71: recommend closing as duplicate or stale Mako runtime hygiene unless a future
  Chromium-backed Mako probe regresses.
- #74: recommend closing as duplicate or stale with #71 unless a future Chromium-backed Mako probe
  regresses.
- #72: addressed by the narrow source-native reliability follow-up that expands Walla/ICE targeted
  recall terms and refines the report-level guardrail so keyword-zero remains visible per source
  without counting as hard systemic source-zero evidence.
- #88: keep as later optimization; this source-health triage did not exercise or expose backfill
  aggregate-count slowness.

## Validation

- `.venv/bin/python scripts/validate_agent_plan.py`
- `.venv/bin/ruff format .`
- `.venv/bin/ruff check .`
- `.venv/bin/mypy src/`
- `.venv/bin/pytest -q tests/unit/test_validate_agent_plan.py`
- `.venv/bin/pytest -q tests/unit/test_source_health.py tests/unit/test_cli.py -k 'diagnose_sources or source_zero or mako or ynet'`
