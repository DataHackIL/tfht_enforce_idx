# 2026 Backfill, Release, And Daily Online Acquisition Report

Date reported: 2026-05-15

## Scope

This report covers the `BACKFILL-PR-2026-RELEASE-DAILY-ONLINE` pass for `news_items`.
The intended historical range was 2026-01-01 through 2026-05-15. Public Kaggle/Hugging Face
publication was intentionally disabled for this pass.

## Result

The run did not complete the full 2026-01-01 through 2026-05-15 range. It stopped during the
2026-03-19 through 2026-03-25 scrape/classification window because Anthropic returned a workspace
API usage-limit error:

```text
You have reached your specified workspace API usage limits. You will regain access on 2026-06-01 at 00:00 UTC.
```

The harness recorded the failure, appended an `abort` ledger row, and skipped release generation.
No public publication was attempted.

## Implemented Run Safety

- Added a budget-guarded backfill/release harness:
  `scripts/run_2026_backfill_release_daily.py`.
- Added `DENBUST_RELEASE_PUBLISH=false` support and `denbust release --no-publish` so release
  bundles can be built without publishing to public destinations.
- Added `agents/release/news_items_no_publish.yaml`.
- Added Supabase schema-completion migrations for discovery annotations and operational
  `news_items` columns.
- Hardened Supabase discovery persistence against provider text containing NUL characters.
- Added Brave backfill date-window handling with `freshness=YYYY-MM-DDtoYYYY-MM-DD`.
- Added local preferred-domain enforcement for Brave source-targeted queries.
- Added backfill-only query-kind override and configured the long run to use source-targeted
  Brave discovery only.
- Made backfill candidate scraping skip current-window source-adapter browser searches and use
  candidate-page generic fetches.
- Hardened backfill retention so undated or out-of-window fallback rows are not silently retained
  as 2026 records.
- Added diagnostics output-directory creation for scripted runs.

## Supabase State

The final attempted full run wrote operational state to Supabase and then stopped on classifier
quota.

- Completed through diagnostics: 2026-03-12 through 2026-03-18.
- Failed window: 2026-03-19 through 2026-03-25.
- Backfill batch statuses at stop: 8 `discovered`, 4 `partial`.
- Persistent candidates in `backfill-2026-*` batches: 1,471.
- Scrape attempts in `backfill-2026-*` batches: 1,174.
- Retained `news_items`: 9.
- Retained rows by source: Walla 4, ICE 2, Ynet 2, TheMarker 1.

## Provider Cost Ledger

Primary artifact:

```text
data/2026_backfill_run_20260515T_full_source_targeted_domain/ledger.md
```

The harness-estimated cumulative spend at abort was 14.64 USD:

- 12 discovery windows started.
- 11 scrape windows completed.
- The 12th scrape window failed on Anthropic quota.
- The run remained below the configured 50 USD hard budget and 45 USD soft budget.

## Key Evidence And Decisions

Early proof runs showed three issues before the full run was attempted:

- Brave-only discovery without date handling returned stale/out-of-window candidates.
- Broad and taxonomy-targeted historical queries produced too much web noise for unattended
  backfill.
- Brave sometimes returned off-domain results for source-targeted `site:` queries.

The PR therefore narrowed historical discovery to source-targeted Brave queries, added local
date/domain enforcement, and kept broad/taxonomy discovery available for non-backfill discovery.

The first clean source-targeted/domain-enforced proof window for 2026-01-01 through 2026-01-07
completed with:

- 144 Brave requests.
- 109 persisted candidates.
- 100 scrape attempts.
- 0 retained rows.
- No Supabase persistence failures.

That result justified scaling the run while preserving conservative release boundaries.

## Release Status

Release generation did not run for the final full-range pass because the harness stops after a
failed step and skips release after abort. There is no release-ready bundle for 2026-01-01 through
2026-05-15 from this run.

The next run should resume from 2026-03-19 through 2026-03-25 after classifier quota or replacement
credentials are available, then generate the no-publication release bundle.

## Supabase Security Advisory

Supabase reported Row Level Security disabled on operational tables during setup. This PR does not
enable RLS because doing so can break service-role/API behavior without a policy design. The
advisory should be handled as a separate operator-approved security/configuration pass.

## Validation

Targeted validations were run during implementation for:

- release config and CLI no-publication behavior
- backfill harness abort/ledger behavior
- Brave date/domain filtering
- backfill query-kind override
- backfill candidate selection and candidate-only scrape behavior
- Supabase discovery persistence and NUL sanitization
- backfill publication-window retention guards

Full final validation should be run before merge after any report/doc updates.
