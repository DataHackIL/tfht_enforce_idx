# 2026 Backfill, Release-Ready Bundle, And Daily Online Acquisition Plan

Date planned: 2026-05-15

Status update: the first full-range attempt stopped during the 2026-03-19 through 2026-03-25
window because Anthropic returned a workspace API usage-limit error. The run remained under budget,
release generation was skipped, and public publication was not attempted. See
[2026_backfill_release_daily_online_report_2026_05_15.md](2026_backfill_release_daily_online_report_2026_05_15.md).

## Goal

Deliver one large operational PR that moves `news_items` from diagnostic slices to a backfilled,
release-ready dataset. The target historical range is 2026-01-01 through 2026-05-15. The work should
also prove the daily online acquisition path enough for operator use, but it should stop before
public Kaggle or Hugging Face publication.

## Operating Decisions

- Backfill range: 2026-01-01 through 2026-05-15.
- Public publication: build and validate the release bundle, but stop before public publication.
- Operational store: local runs may write to Supabase.
- State repo: generated state commits may be pushed to `DataHackIL/tfht_enforce_idx_state` as long
  as each checkpoint is reversible and clearly documented.
- Provider budget: keep Brave, Exa, Anthropic, and other paid API usage under 50 USD total, with a
  soft stop near 40 USD.
- Article adjudication: the agent may inspect public articles directly, decide inclusion/taxonomy,
  and record concise rationale.
- Scheduled daily verification: no need to wait for the next cron; a manual/local equivalent is
  enough for this pass.
- Code-repo PR policy: ship as one large PR in `tfht_enforce_idx`; do not split into diagnostic
  micro-PRs.

## Cost Guard

The run should maintain a provider usage ledger in generated local artifacts. Use request counts and
provider pricing to estimate spend after every batch window. Stop before the projected total crosses
50 USD and pause for operator input if the soft stop near 40 USD is exceeded.

Initial budget assumptions:

- Brave Search API Search: approximately 5 USD per 1,000 requests.
- Exa Search: approximately 7 USD per 1,000 search requests, with possible extra cost for content
  retrieval depending on request shape.
- Anthropic classifier calls are also part of the budget and should be tracked from fallback/full
  classifier input counts when exact provider billing telemetry is not available.

Provider strategy:

- The checked-in run harness defaults to Brave-only backfill discovery to keep the full 2026 window
  within budget.
- Exa is used selectively with `--search-mode brave-exa` when cost headroom remains or when
  Brave-only windows are low-yield.
- Google CSE remains disabled unless the existing entitlement blocker is resolved.

## Workstreams

### 1. Long-Run Harness And Safety

- Create a branch from latest `main`: `codex/2026-backfill-release-daily-online`.
- Add a generated run ledger outside tracked source files.
- Add or update orchestration tooling so weekly discovery/scrape windows can run without manual
  command reconstruction.
- Add an explicit release dry-run/no-publication guard if the existing release command can publish
  when credentials are present.
- Record the initial state-repo commit before pushing generated state changes.

### 2. Backfill Execution

Run weekly windows from 2026-01-01 through 2026-05-15.

For each window:

1. Run `backfill_discover`.
2. Run `diagnose-discovery` after discovery.
3. Run `backfill_scrape` repeatedly until the window is exhausted, blocked, or intentionally
   deferred by budget/time risk.
4. Run `diagnose-discovery` after scrape drains.
5. Record discovered, attempted, retained, rejected, partial, failed, unsupported, remaining
   eligible, provider request counts, and estimated cost.
6. Push reversible state-repo checkpoints when state changes are ready to preserve.

Generated artifacts under `data/` and state repo outputs are not committed to the code repo.

### 3. Article-Level Adjudication

For retained rows, high-impact candidates, missing-taxonomy rows, and ambiguous partials, inspect
public source articles directly. Record concise decisions without copying article text:

- candidate id or canonical URL
- include/exclude decision
- TFHT taxonomy category/subcategory when included
- report relevance
- public release eligibility
- rationale
- remaining uncertainty, if any

Ambiguous rows should be treated conservatively for public release.

### 4. Dataset Quality Gate

Before release bundle generation:

- Verify Supabase row counts and run provenance.
- Verify public release rows do not include internal-only, invalid-taxonomy, missing-taxonomy,
  search-result-only, suppressed, or privacy-unsafe rows unless policy explicitly allows them.
- Summarize counts by date, source, taxonomy, confidence, content basis, review status, and
  publication status.
- Decide whether fallback rows without usable TFHT taxonomy should be suppressed or reported
  separately.

### 5. Release-Ready Bundle

- Build the release bundle with public publication disabled.
- Validate generated CSV/JSON schemas, row counts, metadata, and exclusion behavior.
- Record the local or state-repo bundle path.
- Stop before Kaggle or Hugging Face publication.

### 6. Daily Online Acquisition Readiness

Prove a manual/local equivalent of the daily path:

- discovery writes candidates
- scrape/ingest consumes candidates
- Supabase rows are written
- diagnostics and run artifacts are emitted
- daily review/reporting handoff is either working or explicitly documented as follow-up

If workflow or config changes are required, include them in this PR.

## Final PR Contents

The single PR should include:

- orchestration, guardrail, or pipeline code needed for the long run
- tests for any changed behavior
- workflow/config updates for daily acquisition or release dry-run behavior
- docs/runbook updates
- `.agent-plan.md` updated in mainline semantics
- a detailed final report with commands, artifacts, state repo commits, cost estimates, counts,
  article adjudication summary, release bundle path, daily acquisition proof, and remaining risks

## Definition Of Done

- The 2026-01-01 through 2026-05-15 backfill has a documented status for every window.
- Operational rows intended to persist are written to Supabase or explicitly staged for operator
  approval.
- The release bundle is built and validated but not publicly published.
- The daily acquisition path is proven by manual/local equivalent.
- Generated artifacts and raw article content remain out of the code repo.
- One non-draft PR is open against `main` with Phase C milestone, labels, and a detailed body.
