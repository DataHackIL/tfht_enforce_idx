# Discovery Operations

This document describes how the `news_items` discovery layer is operated locally and in GitHub
Actions after `DL-PR-11`.

## Local Run Path

Load local secrets through `direnv` before Anthropic-backed validation or search-enabled runs:

```bash
eval "$(direnv export bash)"
```

For isolated experiments, clear any personal state overrides before choosing a fresh output root:

```bash
unset DENBUST_RUNS_DIR
unset DENBUST_STORE_PATH
```

Use the tracked safe local config for default operator runs:

```bash
denbust run --dataset news_items --job discover --config agents/news/local.yaml
denbust run --dataset news_items --job ingest --config agents/news/local.yaml
DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-01-31T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml
DENBUST_BACKFILL_BATCH_ID=batch-optional \
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
```

`discover` writes into the candidate-layer namespace under `news_items/discover/`.
`ingest` remains the article-processing job and still owns `news_items/ingest/`.
Backfill jobs also read and write the shared `news_items/discover/` candidate-layer state.
Backfill batch status updates refresh merged and scrape-eligible candidate counts through the
discovery persistence layer; Supabase-backed runs use server-side exact-count metadata, while
state-repo runs stream the candidate JSONL count fields without hydrating full candidate models for
that aggregate path.
The search-engine side of `discover` and `backfill_discover` now emits `taxonomy_targeted` queries
from the packaged TFHT taxonomy in addition to the coarse operator keyword list.
When source-targeted taxonomy expansion is enabled for backfill, each window is capped by
`backfill.max_source_targeted_taxonomy_queries_per_window` so historical recovery does not
silently multiply search-engine quota use across every source and taxonomy term.

Use `agents/news/local_search.yaml` only when intentionally exercising source-native discovery
plus Brave, Exa, and Google CSE locally:

```bash
denbust run --dataset news_items --job discover --config agents/news/local_search.yaml
DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-01-31T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local_search.yaml
```

`local_search.yaml` requires:

- `DENBUST_BRAVE_SEARCH_API_KEY`
- `DENBUST_EXA_API_KEY`
- `DENBUST_GOOGLE_CSE_API_KEY`
- `DENBUST_GOOGLE_CSE_ID`

Missing search keys are surfaced by the discovery run errors. They are not hidden by the config.

If local Google CSE setup returns `403 PERMISSION_DENIED` / no API access, use the tracked
Brave+Exa/no-Google config instead of a one-off `/tmp` YAML file:

```bash
denbust run --dataset news_items --job discover --config agents/news/local_search_brave_exa.yaml
DENBUST_BACKFILL_DATE_FROM=2026-01-01T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-01-07T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local_search_brave_exa.yaml
```

`local_search_brave_exa.yaml` keeps Brave and Exa enabled, disables Google CSE, and requires only:

- `DENBUST_BRAVE_SEARCH_API_KEY`
- `DENBUST_EXA_API_KEY`

This is an operator-local wet-test mode. It does not remove Google CSE support from code or from
`local_search.yaml`.

The local Google Programmable Search Engine backing `DENBUST_GOOGLE_CSE_ID` includes the current
repo-supported sources, Facebook public search, and additional Israeli news domains that should be
eligible for Google CSE-backed discovery. The current domain set is:

- `www.ynet.co.il/*`
- `news.walla.co.il/*`
- `www.mako.co.il/*`
- `www.maariv.co.il/*`
- `www.haaretz.co.il/*`
- `www.ice.co.il/*`
- `www.facebook.com/*`
- `www.kan.org.il/*`
- `www.n12.co.il/*`
- `www.13tv.co.il/*`
- `www.calcalist.co.il/*`
- `www.globes.co.il/*`
- `www.themarker.com/*`
- `www.israelhayom.co.il/*`
- `www.jpost.com/*`
- `www.timesofisrael.com/*`
- `www.i24news.tv/*`
- `www.0404.co.il/*`
- `www.srugim.co.il/*`
- `www.kikar.co.il/*`
- `www.bhol.co.il/*`
- `www.inn.co.il/*`
- `www.davar1.co.il/*`
- `www.zman.co.il/*`
- `www.mekomit.co.il/*`
- `www.ha-makom.co.il/*`
- `shakuf.co.il/*`
- `www.news1.co.il/*`

The pipeline currently builds source-targeted Google CSE queries for configured source domains and
Facebook social discovery. Future discovery expansion can and should use this same CSE to query the
broader listed Israeli news domains through the API before adding new source-specific scrapers.

Before model-backed validation, lint the tracked validation CSV without credentials:

```bash
denbust validation-lint --validation-set validation/news_items/classifier_validation.csv
```

Then run evaluation and live checks:

```bash
denbust validation-evaluate \
  --validation-set validation/news_items/classifier_validation.csv \
  --variants agents/validation/classifier_variants.yaml \
  --output data/may_26_followup/<timestamp>/validation_evaluate.json

denbust live-check \
  --config agents/live_checks/classifier_issue_48.yaml \
  --output-root data/may_26_followup/<timestamp>/live_checks
```

Generated `data/may_26_followup/` bundles are local experiment artifacts and should remain
untracked.

For Phase C source-health triage, activate the project environment, install Chromium before probing
Mako, and isolate state under a fresh ignored follow-up root:

```bash
export FOLLOWUP_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export FOLLOWUP_ROOT="data/may_26_followup/${FOLLOWUP_ID}"
export DENBUST_STATE_ROOT="${FOLLOWUP_ROOT}/state"
mkdir -p "${FOLLOWUP_ROOT}"/{logs,artifacts}

python -m playwright install chromium

denbust diagnose-sources \
  --config agents/news/local.yaml \
  --live-only \
  --sample-keyword "זנות" \
  --sample-keyword "בית בושת" \
  --sample-keyword "סחר בבני אדם" \
  --format json \
  --output "${FOLLOWUP_ROOT}/artifacts/diagnose_sources_live_all.json"

for source in ynet walla mako maariv haaretz ice; do
  denbust diagnose-sources \
    --config agents/news/local.yaml \
    --live-only \
    --source "${source}" \
    --sample-keyword "זנות" \
    --sample-keyword "בית בושת" \
    --sample-keyword "סחר בבני אדם" \
    --format json \
    --output "${FOLLOWUP_ROOT}/artifacts/diagnose_sources_live_${source}.json"
done
```

If the project environment is not activated but the repo-local `.venv` exists, prefix the commands
with `.venv/bin/`.

The 2026-05-03 triage run used this shape under
`data/may_26_followup/20260503T074131Z/`. Mako passed in both all-source and source-specific runs,
Haaretz passed, and the then-current `source_zero_summary.systemic_source_zero_suspected` stayed
true because Ynet, Walla, Maariv, and ICE were affected by zero, stale, or keyword-zero evidence.
After the #72 follow-up, keyword-zero remains a per-source warning but no longer counts toward the
report-level hard source-zero guardrail; it is tracked separately in the source-zero summary as
keyword-zero recall evidence. Operators should treat #71/#74 as duplicate or stale Mako runtime
hygiene unless a Chromium-backed Mako probe regresses. The #88 aggregate-count path is addressed by
the narrow persistence count API; choose further backfill work only from fresh bottleneck evidence.
The durable evidence summary is checked in at
[phase_c_source_health_triage_2026_05_03.md](phase_c_source_health_triage_2026_05_03.md).

The 2026-05-03T13:13:10Z Mako-only follow-up used the same isolated-root pattern under
`data/may_26_followup/20260503T131309Z/`, but only reran the Mako source-specific probe after
`.venv/bin/python -m playwright install chromium`. Mako returned `ok`, with parsed
keyword-matching search results for `זנות` and `בית בושת`; the selected-source
`source_zero_summary` had zero affected sources. That closes #71/#74 as stale/duplicate Mako
runtime hygiene unless a later Chromium-backed probe regresses.

After PR #108 was squash-merged as `dea6406`, the 2026-05-03T13:41:02Z planning reset used
`data/may_26_followup/20260503T134102Z/state` for artifact-only diagnostics. With a fresh isolated
root, `denbust diagnose-discovery` reported no persisted candidates, scrape attempts, or
operational records, and `denbust diagnose-sources --artifacts-only` skipped all configured sources
because no ingest debug summary existed under that root. Treat this as a clean empty-state baseline,
not as source-health evidence.

After PR #109 was squash-merged as `201c247`, the 2026-05-03T15:31:23Z candidate-drain evidence
pass used `data/may_26_followup/20260503T153123Z/state`. It persisted 63 candidates, recorded 30
successful ICE scrape attempts, left 33 candidates from Haaretz, ICE, Maariv, Mako, and Walla never
scraped, and produced no scrape failures, retry backlog, or self-heal backlog. Artifact-only
source-health was inconclusive because the diagnostic path skipped the `scrape_candidates` debug
summaries. Because the first pass produced scrapeable candidates, no latest-seven-complete-UTC-days
backfill window was added.

After PR #110 was squash-merged as `8c89d91`, `denbust diagnose-discovery` reports bounded
queue-drain diagnostics in its JSON and text output. The `queue_drain` section includes the
configured candidate cap, persisted attempted-candidate order, persisted scrape-attempt count,
attempted source mix derived from actual scrape attempts, remaining eligible candidate order,
remaining eligible source mix, and the inferred stop reason. Use those fields in the next bounded
candidate-drain evidence pass before changing queue prioritization or fairness behavior.

After PR #117 was squash-merged as `576e05f`, the January 1-7 wet-test follow-up used a local
Brave+Exa/no-Google search mode because Google CSE returned `403 PERMISSION_DENIED` / no API access
in local setup. The Chrome-CDP retry scrape used:

```bash
export DENBUST_BROWSER_MODE=chrome_cdp
export DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222
```

The durable checked-in evidence summary is
[january_2026_backfill_wet_test_evidence_2026_05_13.md](january_2026_backfill_wet_test_evidence_2026_05_13.md).
It records 3,116 persisted candidates, 100 attempted candidates, 189 scrape attempts, 28 retained
provisional operational rows, 88 partial pages, 12 scrape failures, 2,689 remaining eligible
candidates, and `budget_cap_reached`. The first no-CDP scrape attempt was aborted/reset and should
not be used as valid scrape evidence.

After `DISC-PR-NOISE-FILTERS`, search-engine results that are obvious non-article surfaces are
still retained with candidate provenance, but new matches are marked `unsupported_source` before
they can consume scrape-drain budget. Existing unattempted candidate-only matches are demoted the
same way when they are rediscovered, while attempted or content-bearing candidates keep their
current status. The filter covers profile-like `x.com` / Twitter, Facebook, Instagram, LinkedIn,
TikTok, and YouTube pages; Google Play / Apple app detail pages; and
dictionary/translation/reference utility domains such as Morfix, Reverso, Wiktionary, and Pealim.
Post-like social URLs and non-app store paths are left scrape-eligible so the filter does not become
a broad domain denylist. Queue fairness and source prioritization are intentionally unchanged; the
filter only removes known low-value search-result surfaces from scrape eligibility. Durable
candidate metadata stores `unsupported_source_filter=search_noise`,
`unsupported_source_reason`, and `unsupported_source_domain`, and `denbust diagnose-discovery`
reports `queue_health.search_noise_filter_reason_counts` so operators can distinguish app-store,
social-profile, and unsupported utility-domain noise.

After `SCRAPE-PR-PARTIAL-DIAGNOSTICS`, `denbust diagnose-discovery` adds a
`partial_page_diagnostics` JSON object and a matching text section. Use it when a wet test reports
many `partial_page` candidates:

- `partial_candidate_count` is the candidate-layer partial count. It includes candidates with
  `content_basis=partial_page` or `candidate_status=partially_scraped`.
- `retained_operational_record_candidate_count` and `retained_operational_record_count` show how
  many partial candidates produced retained candidate-fallback operational rows.
- `metadata_only_partial_candidate_count` is the partial-candidate count that did not match a
  retained operational row, so those pages are still metadata-only from the operator perspective.
- `search_result_only_candidate_count`, `blocked_generic_fetch_candidate_count`,
  `failed_generic_fetch_candidate_count`, and `timeout_generic_fetch_candidate_count` separate
  poor or blocked generic fetch outcomes from usable metadata extraction.
- `generic_fetch_partial_candidate_count`, `source_adapter_partial_candidate_count`,
  `partial_attempts_by_kind`, and `partial_attempts_by_source_adapter` show whether partials came
  from generic fallback or a source adapter.
- `partial_candidates_by_domain`, `partial_candidates_by_source`, and
  `generic_fetch_error_code_counts` identify the domains, source hints, and generic fetch errors
  dominating partial outcomes.
- `classifier_warning_signals` summarizes what discovery diagnostics can infer from persisted
  candidate-fallback operational rows: fallback rows, partial fallback rows, low-confidence
  fallback rows, missing taxonomy pairs, and invalid taxonomy pairs. Run-level classifier parse
  warnings that are not persisted in candidate or operational state still need the matching run log
  or debug summary.

This diagnostic slice is interpretation-only. It does not change queue fairness, source
prioritization, scrape caps, generic fetch behavior, or source-family scraper support.

## GitHub Actions Run Path

The operational workflow split is:

- `news-items-discover`
  Runs daily at `03:00 UTC` and on manual dispatch.
- `daily-state-run`
  Runs daily candidate-driven ingest and keeps the daily email path intact.
- `weekly-state-run`
  Runs weekly catch-up candidate-driven ingest.
- `news-items-backfill-discover`
  Manual-only historical discovery with explicit request window inputs.
- `news-items-backfill-scrape`
  Manual-only scrape drain for one backfill batch or the oldest eligible batch.

All of these workflows:

- check out `DataHackIL/tfht_enforce_idx_state` into `state_repo/`
- set `DENBUST_STATE_ROOT=state_repo`
- use `.github/actions/setup-denbust-state-job`
- commit only namespaced state outputs back to the state repo

## Required And Optional Secrets

Common required secrets for discover and ingest family workflows:

- `STATE_REPO_PAT`
- `ANTHROPIC_API_KEY`
- `DENBUST_SUPABASE_URL`
- `DENBUST_SUPABASE_SERVICE_ROLE_KEY`

Optional discovery-engine secrets:

- `DENBUST_BRAVE_SEARCH_API_KEY`
- `DENBUST_EXA_API_KEY`
- `DENBUST_GOOGLE_CSE_API_KEY`
- `DENBUST_GOOGLE_CSE_ID`

Optional email-output secrets for ingest-family jobs:

- `DENBUST_EMAIL_SMTP_HOST`
- `DENBUST_EMAIL_SMTP_PORT`
- `DENBUST_EMAIL_SMTP_USERNAME`
- `DENBUST_EMAIL_SMTP_PASSWORD`
- `DENBUST_EMAIL_FROM`
- `DENBUST_EMAIL_TO`
- `DENBUST_EMAIL_USE_TLS`
- `DENBUST_EMAIL_SUBJECT`

Backfill workflows reuse the same discovery and ingest secrets. They do not require any extra
credential surface beyond the explicit `workflow_dispatch` inputs.

## State Repo Layout And Workflow Ownership

Discovery-family workflows own:

```text
state_repo/news_items/discover/
├── runs/
├── candidates/
├── metrics/
└── backfill_batches/
```

Ingest-family workflows own:

```text
state_repo/news_items/ingest/
├── seen.json
├── runs/
├── logs/
└── publication/
```

Backfill job namespaces also appear in the state repo:

```text
state_repo/news_items/backfill_discover/
├── runs/
└── logs/

state_repo/news_items/backfill_scrape/
├── seen.json
├── runs/
└── logs/
```

- `news-items-backfill-discover` owns the `backfill_discover/` run-artifact namespace and also
  updates the shared `discover/` candidate-layer namespace.
- `news-items-backfill-scrape` owns the `backfill_scrape/` run-artifact namespace and may also
  update both `discover/` and `ingest/` because it drains discovery candidates through the existing
  ingest pipeline.

## Retry Semantics

- `discover` is additive and durable; it updates candidate-layer files without scraping article
  bodies.
- `ingest` consumes queued candidates through the normal scrape/classify/persist flow.
- `backfill_discover` creates or extends durable historical batches using
  `DENBUST_BACKFILL_DATE_FROM` and `DENBUST_BACKFILL_DATE_TO`.
- `backfill_scrape` drains one historical batch at a time and can be constrained with
  `DENBUST_BACKFILL_BATCH_ID`.

`backfill_scrape` remains manual in this phase because historical drain rates and retry backlog are
operationally sensitive. Operators should choose when to spend scrape budget on historical work
instead of scheduling it blindly.

## Self-Heal Scaffolding

`DL-PR-12` does not run AI repair. It only makes the future repair backlog explicit:

- failed scrape candidates can be marked `self_heal_eligible`
- `denbust diagnose-discovery` reports the self-heal-eligible queue count
- the persisted discovery diagnostics artifact includes structured scrape-failure groups by attempt
  kind, fetch status, error code, source adapter, and domain
- source-adapter and generic-fetch attempts include stable `failure_stage` diagnostics where the
  failure path is known
- future orchestration can select eligible failed candidates and record a `self_heal_retry` scrape
  attempt after a repair strategy exists

Do not treat these hooks as permission to rewrite selectors, create sources, or call an AI repair
loop automatically. Those behaviors need a separate implementation PR with live failure evidence and
fixture-backed regression tests.

Classifier provider/API failures are fatal for `ingest`, `scrape_candidates`, and
`backfill_scrape`. A run that cannot reach Anthropic records a sanitized
`classifier_provider_error=...`, sets `fatal=true`, returns `fatal: classifier provider error`, and
does not mark articles as seen. Live checks record the same condition as a per-case `error` rather
than fabricating `actual=not_relevant`.

## One-Time 90-Day Re-Scan

`C-8` does not add a dedicated workflow. The catch-up run uses the existing backfill jobs with the
default 7-day backfill slicing:

```bash
DENBUST_BACKFILL_DATE_FROM=2026-01-23T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-04-22T23:59:59+00:00 \
denbust run --dataset news_items --job backfill_discover --config agents/news/local.yaml

DENBUST_BACKFILL_BATCH_ID=batch-optional \
denbust run --dataset news_items --job backfill_scrape --config agents/news/local.yaml
```

Use `DENBUST_BACKFILL_BATCH_ID` only if operators want to drain one batch explicitly; otherwise
`backfill_scrape` can drain the oldest eligible batch.

## Migration And Setup Checklist

1. Ensure the `news-items-ingest` environment already contains the existing ingest secrets.
2. Add the discovery-engine secrets if Brave, Exa, or Google CSE should run in Actions.
3. Enable the new workflows in GitHub Actions:
   - `news-items-discover`
   - `news-items-backfill-discover`
   - `news-items-backfill-scrape`
4. Confirm `daily-state-run` and `weekly-state-run` still point at `agents/news/github.yaml`.
5. Run one manual `news-items-discover` dispatch to verify `news_items/discover/` state is written.
6. If historical recovery is needed, launch `news-items-backfill-discover` with an explicit window,
   then drain it with `news-items-backfill-scrape`.
7. Keep `.agent-plan.md`, `README.md`, and planning docs aligned to the post-merge mainline state
   whenever workflow coverage changes.
