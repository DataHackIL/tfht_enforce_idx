# January 2026 Backfill Wet-Test Plan

Date authored: 2026-05-03

## Purpose

This plan defines the first bounded January 2026 backfill evidence pass. It is a wet test, not a
full historical recovery run. The goal is to prove that the January backfill path can produce and
drain candidates while the new queue-drain diagnostics explain selection order, source mix, budget
cap behavior, scrape failures, and remaining eligible backlog.

The first attempt should use the first seven complete UTC days of January 2026. Expand to the full
month only after the 7-day evidence pass looks sane.

## Preconditions

- Work from latest `main`.
- Load local secrets before model-backed scrape/classification:

```bash
eval "$(direnv export bash)"
```

- If the project environment is not activated, prefix commands with `.venv/bin/`.
- Use a real local Google Chrome instance for browser-backed local scraping. This mode does not
  require Playwright's downloaded test Chromium.
- Confirm Chrome is already running with a DevTools endpoint before the scrape phase:

```bash
curl -fsS http://127.0.0.1:9222/json/version
```

- If no attachable Chrome is running, start normal Google Chrome with remote debugging and a
  dedicated wet-test profile, then pause so the operator can log into source accounts and handle
  any browser challenges before continuing:

```bash
open -na "Google Chrome" --args \
  --remote-debugging-port=9222 \
  --user-data-dir="$HOME/.config/denbust/chrome-wet-test"
```

- Do not write this wet-test output into the default `data/` state root. Use a fresh ignored
  experiment root.

## Evidence Root

Create one timestamped follow-up root:

```bash
export FOLLOWUP_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export FOLLOWUP_ROOT="data/may_26_followup/${FOLLOWUP_ID}"
mkdir -p "${FOLLOWUP_ROOT}"/{logs,artifacts,reports,summaries}
```

Every command should write stdout/stderr to `logs/` and machine-readable output to `artifacts/`
where supported. Do not commit the generated `data/` bundle.

Do not export the wet-test `DENBUST_STATE_ROOT` until Phase 1. Phase 0 runs tests that intentionally
write fixture candidate state, and exporting the wet-test state root too early can pollute the
empty-state baseline.

`DENBUST_BROWSER_MODE=chrome_cdp` applies only to browser-backed source scrapers such as Mako and
Haaretz. HTTP fallback fetching and API-backed discovery/search engines continue to use their
existing non-browser paths.

Use one config path consistently across the run. The default source-native local path is:

```bash
export DENBUST_CONFIG=agents/news/local.yaml
```

When intentionally exercising local Brave+Exa search without Google CSE, use:

```bash
export DENBUST_CONFIG=agents/news/local_search_brave_exa.yaml
```

The Brave+Exa/no-Google config is for local wet tests where Google CSE returns
`403 PERMISSION_DENIED` / no API access. It does not remove Google CSE support from code or from
`agents/news/local_search.yaml`.

## Phase 0 - Static Guardrails

Run the narrow static checks that prove the checked-out code and plan format are healthy before live
work:

```bash
.venv/bin/python scripts/validate_agent_plan.py \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/00_validate_agent_plan.log"

.venv/bin/ruff check . \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/01_ruff_check.log"

.venv/bin/pytest -q tests/unit/test_discovery_diagnostics.py tests/unit/test_discovery_scrape_queue.py tests/unit/test_cli.py -k 'diagnose_discovery or queue_drain or select_candidates_for_scrape' \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/02_queue_diagnostics_tests.log"
```

Stop if any static guardrail fails.

## Phase 1 - Empty-State Baseline

Capture diagnostics before writing January candidates:

```bash
export DENBUST_STATE_ROOT="${FOLLOWUP_ROOT}/state"
export DENBUST_BROWSER_MODE=chrome_cdp
export DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222

.venv/bin/denbust diagnose-discovery \
  --config "${DENBUST_CONFIG}" \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_discovery_before.json" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/03_diagnose_discovery_before.log"

.venv/bin/denbust diagnose-sources \
  --config "${DENBUST_CONFIG}" \
  --artifacts-only \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_sources_artifact_only_before.json" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/04_diagnose_sources_artifact_only_before.log"
```

Expected baseline:

- `diagnose-discovery` reports no persisted candidates or attempts under the fresh root.
- `diagnose-sources --artifacts-only` may skip sources because no ingest debug summaries exist.
  Treat this as artifact-shape evidence, not source-health evidence.

## Phase 2 - Bounded January Discovery

Run only January 1-7 first:

```bash
export DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00
export DENBUST_BACKFILL_DATE_TO=2026-01-07T23:59:59+00:00

.venv/bin/denbust run \
  --dataset news_items \
  --job backfill_discover \
  --config "${DENBUST_CONFIG}" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/05_backfill_discover_2026_01_01_07.log"
```

Then capture discovery diagnostics:

```bash
.venv/bin/denbust diagnose-discovery \
  --config "${DENBUST_CONFIG}" \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_discovery_after_discover.json" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/06_diagnose_discovery_after_discover.log"
```

Record:

- total persisted candidates;
- candidates by source and producer;
- backfill batch id(s);
- queue health and scrape-eligible count;
- whether source-targeted taxonomy queries were capped as expected.

Stop before scrape if discovery produces zero candidates and no useful diagnostic clue. In that case,
write a short report explaining whether the next action should be source/search input adjustment
rather than scrape behavior work.

## Phase 3 - First Bounded Scrape Drain

Drain one eligible backfill batch with the default scrape cap:

```bash
curl -fsS "${DENBUST_CHROME_CDP_URL}/json/version" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/07a_chrome_cdp_preflight.log"

.venv/bin/denbust run \
  --dataset news_items \
  --job backfill_scrape \
  --config "${DENBUST_CONFIG}" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/07_backfill_scrape_2026_01_01_07.log"
```

Then capture the queue-drain diagnostics:

```bash
.venv/bin/denbust diagnose-discovery \
  --config "${DENBUST_CONFIG}" \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_discovery_after_scrape.json" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/08_diagnose_discovery_after_scrape.log"
```

The key evidence is the `queue_drain` object:

- `max_candidate_budget`;
- `persisted_attempted_candidate_count`;
- `persisted_scrape_attempt_count`;
- `attempted_source_mix`;
- `remaining_eligible_candidate_count`;
- `remaining_eligible_source_mix`;
- `persisted_attempted_candidate_order`;
- `remaining_eligible_candidate_order`;
- `inferred_stop_reason`.

For source-family follow-up slices, inspect the same persisted state with the current
`denbust diagnose-discovery` implementation before coding. The post-scrape diagnostic should be
used to compare:

- `source_suggestions.suggestions` for repeated candidate-only unsupported domains;
- `partial_page_diagnostics` for domains that generic fetch already reached as partial pages;
- `queue_drain.remaining_eligible_candidate_order` and the candidate JSONL for domain-specific
  backlog shape.

For `SRC-PR-ISRAELHAYOM`, this evidence favored bounded generic-fetch source-family support:
`israelhayom.co.il` appeared as a repeated source suggestion with 25 candidate-only main-domain
URLs across two runs, local state contained 33 Israel Hayom-family candidate-only URLs, and no
Israel Hayom candidate had attempted-scrape or partial-page evidence. That supports main-domain
generic fallback source labeling, not source-targeted discovery/backfill fanout, broad subdomain
matching, a browser scraper, queue fairness change, or Mako/Haaretz browser behavior change.

## Phase 4 - Source Artifact Check

Run artifact-only source diagnostics after the scrape pass:

```bash
.venv/bin/denbust diagnose-sources \
  --config "${DENBUST_CONFIG}" \
  --artifacts-only \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_sources_artifact_only_after_scrape.json" \
  2>&1 | tee "${FOLLOWUP_ROOT}/logs/09_diagnose_sources_artifact_only_after_scrape.log"
```

Record whether this path can read the produced artifacts. If it skips because the run produced
`backfill_scrape` or `scrape_candidates` summaries rather than ingest summaries, treat that as an
artifact compatibility gap, not as source-health evidence.

## Phase 5 - Human Summary

Write a short Markdown report:

```bash
cat > "${FOLLOWUP_ROOT}/reports/january_2026_backfill_wet_test_summary.md" <<'EOF'
# January 2026 Backfill Wet-Test Summary

## Window

- From: 2026-01-01T00:00:00+00:00
- To: 2026-01-07T23:59:59+00:00

## Commands

- Static guardrails:
- Empty-state diagnostics:
- Backfill discover:
- Backfill scrape:
- Post-scrape diagnostics:

## Discovery Results

- Persisted candidates:
- Candidate source mix:
- Backfill batch id(s):
- Scrape-eligible candidates:

## Scrape Results

- Persisted attempted candidates:
- Persisted scrape attempts:
- Attempted source mix:
- Scrape succeeded:
- Scrape failed:
- Retry backlog:
- Self-heal eligible:
- Remaining eligible candidates:
- Remaining eligible source mix:
- Inferred stop reason:

## Interpretation

- Queue contract appears sane / ambiguous / wrong:
- Evidence for or against prioritization/fairness change:
- Source-health or artifact-diagnostic gaps:

## Decision

- Expand to the full January 2026 month:
- Run another 7-day slice first:
- Open a code PR:
- Do not proceed until:
EOF
```

Fill the report from the JSON artifacts before deciding whether to expand.

## Expansion Decision

Expand from the 7-day wet test to the full January 2026 window only if all of these are true:

- `backfill_discover` produced candidates or produced a clearly understood zero-candidate result;
- `backfill_scrape` completed without fatal credential/provider/runtime failures, and browser-backed
  Mako/Haaretz activity either attached to Chrome over CDP or reported the CDP endpoint as
  unavailable before live scraping proceeded;
- `queue_drain.inferred_stop_reason` is explainable from the configured candidate cap and remaining
  eligible queue;
- attempted and remaining source mix do not show an obviously pathological single-source drain that
  contradicts the intended queue contract;
- scrape failures are either low-volume or grouped into actionable diagnostics;
- no self-heal or retry backlog pattern suggests a code fix should happen before more live work.

If those conditions pass, run the full month with the same config mode used for the 7-day pass.
For the default source-native path:

```bash
export DENBUST_CONFIG=agents/news/local.yaml
export DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00
export DENBUST_BACKFILL_DATE_TO=2026-01-31T23:59:59+00:00
export DENBUST_BROWSER_MODE=chrome_cdp
export DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222

.venv/bin/denbust run --dataset news_items --job backfill_discover --config "${DENBUST_CONFIG}"
.venv/bin/denbust run --dataset news_items --job backfill_scrape --config "${DENBUST_CONFIG}"
.venv/bin/denbust diagnose-discovery --config "${DENBUST_CONFIG}" --format json
```

For the Brave+Exa/no-Google local search fallback:

```bash
export DENBUST_CONFIG=agents/news/local_search_brave_exa.yaml
export DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00
export DENBUST_BACKFILL_DATE_TO=2026-01-31T23:59:59+00:00
export DENBUST_BROWSER_MODE=chrome_cdp
export DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222

.venv/bin/denbust run --dataset news_items --job backfill_discover --config "${DENBUST_CONFIG}"
.venv/bin/denbust run --dataset news_items --job backfill_scrape --config "${DENBUST_CONFIG}"
.venv/bin/denbust diagnose-discovery --config "${DENBUST_CONFIG}" --format json
```

If the 7-day pass is ambiguous, run a second adjacent slice before changing code:

```bash
export DENBUST_BACKFILL_DATE_FROM=2026-01-08T00:00:00+00:00
export DENBUST_BACKFILL_DATE_TO=2026-01-14T23:59:59+00:00
```

## Code-Change Decision Rules

Open a follow-up code PR only when the wet test shows one of these concrete defects:

- queue diagnostics cannot explain candidate selection order or remaining eligible backlog;
- `inferred_stop_reason` is inconsistent with persisted candidates, attempts, and configured cap;
- attempted source mix repeatedly drains one source while older or higher-priority eligible
  candidates from other sources remain contrary to the queue contract;
- scrape failures cluster by source/domain/error code with enough volume to justify source-health or
  self-heal selection work;
- artifact-only diagnostics cannot consume the produced backfill/scrape debug summaries and that gap
  blocks operator interpretation.

Do not implement queue fairness, prioritization, selector repair, or AI self-healing from a single
successful bounded wet test.

## Follow-Up PR Map

Use stable planning IDs for the post-wet-test slices so they are not confused with GitHub pull
request numbers. The current map is three simple cross-cutting PRs followed by four source-family
PRs, for seven planned follow-ups total:

1. `WET-PR-EVIDENCE-CONFIG` - planning/config evidence only.
   Add a tracked Brave+Exa/no-Google local search config, document that local operators may bypass
   Google CSE when the API returns `403 PERMISSION_DENIED`, check in a concise January 1-7
   Chrome-CDP wet-test evidence summary, and update `.agent-plan.md`.
2. `DISC-PR-NOISE-FILTERS` - discovery candidate quality.
   Implemented as a central persistence-time search-result noise filter: obvious non-article
   surfaces are retained with provenance but marked `unsupported_source` before scrape-drain budget
   can select them. Existing unattempted candidate-only noise is demoted on rediscovery; attempted
   or content-bearing candidates keep their status. Covered classes include profile-like `x.com` /
   Twitter and other social pages, Google Play / Apple app detail links,
   dictionary/translation/reference pages, and other metadata-poor utility pages. Post-like social
   URLs and non-app store paths remain scrape-eligible. Diagnostics expose durable reason counts,
   and queue fairness/prioritization intentionally remain unchanged.
3. `SCRAPE-PR-PARTIAL-DIAGNOSTICS` - scrape interpretation.
   Implemented as a diagnostics-only `partial_page_diagnostics` section in
   `denbust diagnose-discovery`. Operators can now separate retained candidate-fallback
   operational rows from metadata-only partials, blocked/failed/time-out generic fetches from
   usable generic metadata extraction, true source-adapter partial attempts from generic partials
   after a source-adapter miss/failure, dominant partial domains/source hints, and persisted
   current-candidate classifier/taxonomy warning signals. Queue fairness, source prioritization,
   generic fetch behavior, and source-family scraper support are unchanged.
4. `SRC-PR-GLOBES-THEMARKER` - source-family expansion.
   Implemented as bounded generic-fetch source-family support, not as a browser scraper. The
   checked-in January 1-7 evidence showed `globes.co.il` as a repeated source suggestion and
   showed TheMarker pages returning HTTP 200 with usable partial metadata through generic fetch.
   Globes/TheMarker URLs are now mapped to source-family labels for diagnostics/fallback
   provenance, source-targeted discovery/backfill queries now cover `www.globes.co.il` and
   `www.themarker.com`, generic metadata extraction prefers article metadata/JSON-LD over page
   titles, and source-suggestion diagnostics remain available when candidate-only or weak
   conversion evidence still justifies stronger scraper work.
5. `SRC-PR-ISRAELHAYOM` - source-family expansion.
   Implemented as bounded main-domain generic-fetch source-family recognition, not as a browser
   scraper or source-targeted query expansion. Fresh diagnostics over the January 1-7 persisted
   state showed `israelhayom.co.il` as a repeated source suggestion with 25 candidate-only
   main-domain URLs across two runs, 33 Israel Hayom-family candidate-only URLs in local state, and
   no Israel Hayom attempted-scrape or partial-page evidence. Main-domain Israel Hayom URLs are now
   mapped to a source-family label for diagnostics/fallback provenance; subdomains and recurring
   source-targeted discovery/backfill queries remain out of scope until stronger extraction
   evidence exists.
6. `SRC-PR-KAN` - source-family expansion.
   Implemented as low-confidence generic-fetch diagnostic labeling, not as a browser scraper or
   source-targeted query expansion. Fresh diagnostics and candidate-state inspection over the
   January 1-7 persisted state showed two official `kan.org.il` candidate-only URLs under
   `/content/kan-news/` and no attempted-scrape or partial-page evidence. Official Kan news article
   paths can now be mapped to a source-family label for diagnostics/fallback provenance when future
   generic fetches recover metadata. Unrelated Kan-named domains such as `kanisrael.co.il`,
   `kan-ashkelon.co.il`, Facebook posts linking to those domains, and non-article `kan.org.il`
   pages remain out of scope, as do recurring source-targeted discovery/backfill queries and any
   browser/source-native scraper.
7. `SRC-PR-NEWS1` - source-family expansion.
   Implemented as low-confidence generic-fetch diagnostic labeling, not as a browser scraper,
   source-targeted query expansion, or generic metadata hardening. Candidate-state inspection over
   the January 1-7 persisted state showed three main-domain News1 archive candidate-only URLs under
   `/Archive/`, all discovered by Exa, all still scrape-eligible after the bounded drain, and no
   News1 attempted-scrape or partial-page evidence. The post-scrape diagnostic did not emit News1
   among the top source suggestions. News1 archive paths can now be mapped to a source-family label
   for diagnostics/fallback provenance when future generic fallback records either partial page
   metadata or a retained search-result-only fallback.
   Non-archive News1 pages, recurring source-targeted discovery/backfill queries, generic metadata
   hardening, and any browser/source-native scraper remain out of scope.

Do not bundle the source-family work into one large scraper PR. Promote each source-family PR only
after the latest diagnostics justify it.
