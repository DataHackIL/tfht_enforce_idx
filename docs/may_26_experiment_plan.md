# May 2026 Local Experiment Plan

Date authored: 2026-05-01

Branch: `may_26_experiment`

## Purpose

This plan describes a local, artifact-heavy experiment to answer four questions before choosing the
next implementation PR:

1. What parts of the checked-in roadmap still describe the real project state?
2. Which GitHub issues still represent active work rather than historical symptoms?
3. Which discovery, scraping, classification, validation, reporting, and release paths still work
   locally?
4. What is missing from the repo to make these answers repeatable without hand inspection?

The experiment should prefer measurement over assumptions. It should produce a timestamped bundle of
logs, JSON artifacts, summaries, and decisions that can be used to update the roadmap and choose the
next PR.

## Survey Baseline

### Planning and design docs reviewed

| Document | Current signal for this experiment |
|---|---|
| `.agent-plan.md` | Main operational pointer. It says the next planned PR is optional `DL-PR-12` self-healing scaffolding hooks, with no current blockers. |
| `PLAN.md` | Human plan map, but currently stale. It still describes "Post-`DL-PR-07`" and says `DL-PR-08` is next even though later discovery PRs have shipped. Treat as a doc-drift signal, not as source of truth. |
| `README.md` | Most complete current user-facing description of implemented jobs: discover, backfill discover/scrape, ingest, monthly report, release, backup, diagnostics, daily AI review. |
| `docs/CHATGPT_26_04_PLAN.md` | Main product/data roadmap. It frames the next major work as taxonomy, validation, product alignment, manual corrections, monthly reporting, and minisite export. Much of this appears partly implemented and needs verification. |
| `docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md` | Validation sub-plan. It says PR 3.4 typology-aware evaluation reports is the current next PR, which conflicts with `.agent-plan.md`. Treat as a stale or parallel sub-plan until measured. |
| `docs/PHASE_C_PLAN.md` | Phase C plan. C-8 is marked implemented and the 90-day catch-up remains an operator-run backfill. Use it to verify taxonomy keyword/backfill assumptions. |
| `docs/tfht_discovery_layer_design_amended.md` | Discovery architecture design. It defines the durable candidate layer, backfill, diagnostics, self-healing fields, and the key evaluation methodology. |
| `docs/tfht_discovery_layer_implementation_plan.md` | Discovery rollout plan. It says `DL-PR-12` is optional self-healing scaffolding only, not full self-healing. |
| `docs/discovery_operations.md` | Local and GitHub runbook after `DL-PR-11`. Use its commands as the starting point for local experiments. |
| `docs/2026_04_04_tfht_state_of_project_report.md` | Orientation report. It correctly predicts the next major axis as typology, validation, and product alignment. |
| `docs/articles_examples.md` | Known source/article watchlist. It should become a machine-readable coverage fixture or feed one. |
| `docs/MVP_SPEC.md` and `docs/IMPLEMENTATION_PLAN.md` | Historical MVP plans. Use only to understand original source expectations and logging requirements. |
| `docs/product_def.md` | Product intent in Hebrew. It confirms the public-facing enforcement index needs counts, visible case information, enforcement categories, and ongoing manual/legal follow-up. |
| `docs/LEGAL_CONV_EN.md` and `docs/LEGAL_CONV_HE.md` | Legal/data handling context. Keep public outputs metadata-only and avoid full-text publication. |

### Issue survey

The repo currently has 21 issues returned by the repo-specific GitHub MCP. Seven are open.

| Issue | State | Experiment treatment |
|---|---:|---|
| #10 Tighten classification to reduce non-enforcement false positives | Closed | Historical regression. Keep the celebrity/sex-work negative fixture in validation and live checks. |
| #17 Change pr-agent-context-refresh publish_mode to append | Closed | CI maintenance, not relevant to local pipeline health. |
| #30 Three news sources returning zero results consistently | Closed | Historical symptom. Re-test through source diagnostics because #72 is the current generalized version. |
| #32 Classifier rejecting highly relevant prostitution/trafficking articles | Closed | Historical classifier false negatives. Covered by #48-style fixtures. |
| #35 Potential false negatives: relevant Walla articles misclassified | Closed | Historical symptom. Verify Walla source and classifier fixture coverage. |
| #36 Classifier rejecting clearly relevant prostitution/trafficking articles | Closed | Historical symptom. Verify classifier fixture coverage. |
| #37 Classifier missing highly relevant articles | Closed | Historical symptom. Verify classifier fixture coverage. |
| #39 Classifier rejecting highly relevant prostitution articles | Closed | Historical symptom. Verify classifier fixture coverage. |
| #40 Classifier potentially missing relevant articles before classification | Closed | Historical symptom. Verify raw-to-classifier accounting in ingest debug logs. |
| #45 Classifier rejecting all articles despite relevant content | Closed | Historical symptom. Verify classifier fixture coverage. |
| #46 Pre-classifier filter silently dropping relevant articles | Closed | Important acceptance criterion. The experiment must prove skip/drop reasons are visible in debug artifacts. |
| #47 Source scrapers returning zero articles | Closed | Historical symptom. Re-test source health and zero-result visibility. |
| #48 LLM classifier miscalibrated | Closed | Main classifier regression cluster. Run validation evaluation and live-check scenario. |
| #52 Expose diagnostic-safe public probe helpers for Maariv scraper | Open | Active design debt. The experiment should detect whether diagnostics still depend on private Maariv helpers and whether that blocks source-health trust. |
| #53 Expose diagnostic-safe public probe helpers for ICE scraper | Open | Active design debt. Same treatment as #52 for ICE. |
| #65 Add Ynet category-page backstop for משפט ופלילים discovery | Open | Active source recall issue. Test whether Ynet RSS alone is still shallow and whether category-page probing is needed now. |
| #66 Add fixture-based Ynet end-to-end regression test after web-search source exists | Open | Active but dependent. Decide whether web-search source exists enough now or remains blocked by #65/search path work. |
| #71 Mako source completely failing due to browser navigation issues | Open | Active high-priority source-health issue. Live Mako diagnostics must be run with browser runtime installed. |
| #72 Major Israeli news sources returning zero results | Open | Active system-level health issue. This is the central local experiment target. |
| #74 Mako source experiencing complete failure with browser navigation | Open | Active but likely duplicate/continuation of #71. Verify whether both can be collapsed after the experiment. |
| #88 Optimize backfill batch aggregate counts in discovery persistence | Open | Active optimization, not a correctness blocker. Measure batch/candidate counts but do not prioritize unless local runs show aggregation is materially slow. |

### Initial local-state warning

The current untracked `data/news_items/` tree contains discovery artifacts with many repeated
`example.com` candidates and scrape attempts. That is useful as a fixture/test artifact but not as a
clean operational baseline. All May experiment commands below should use a fresh
`DENBUST_STATE_ROOT`, not the default `data/` state root.

The tracked validation CSV also deserves scrutiny before it is used as a source of truth. A quick
local read shows only 5 rows and no populated taxonomy IDs; one row appears suspicious when parsed
with a standard CSV reader. The experiment should validate this file explicitly before drawing
classifier conclusions from it.

## Experiment Outputs

Create one timestamped run root:

```bash
export EXPERIMENT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export EXPERIMENT_ROOT="data/may_26_experiment/${EXPERIMENT_ID}"
export DENBUST_STATE_ROOT="${EXPERIMENT_ROOT}/state"
mkdir -p "${EXPERIMENT_ROOT}"/{logs,artifacts,reports,summaries,inputs,tmp}
```

Every command should write:

- stdout/stderr to `logs/<step>.log`
- machine JSON where supported to `artifacts/<step>.json`
- human Markdown or text summaries to `reports/<step>.md` or `reports/<step>.txt`
- a one-line status record to `summaries/command_manifest.jsonl`

Do not store secrets in the bundle. The manifest should record whether required environment
variables were present, but values must be redacted.

## Phase 0 - Make the Experiment Reproducible

Before running long live jobs, add small local helper scripts if the manual command set becomes hard
to repeat. These scripts are not product features; they are experiment harnesses.

Recommended scripts:

1. `scripts/may_26_run_experiment.py`
   - Runs named steps as subprocesses.
   - Applies `DENBUST_STATE_ROOT`.
   - Captures stdout/stderr, duration, exit code, and redacted environment availability.
   - Writes `summaries/command_manifest.jsonl`.
2. `scripts/may_26_summarize_state.py`
   - Reads `runs/`, `logs/`, candidate JSONL files, diagnostics JSON, validation reports, and live-check output.
   - Produces a single `reports/final_experiment_summary.md`.
3. `scripts/may_26_issue_matrix.py`
   - Consumes a checked-in issue snapshot JSON exported from the GitHub MCP or `gh` fallback.
   - Produces a table of `active`, `stale`, `duplicate`, `blocked`, and `converted-to-test` decisions.
4. `scripts/may_26_known_url_watchlist.py`
   - Converts `docs/articles_examples.md` and issue-linked URLs into a machine-readable known-URL watchlist.
   - Runs discovery/source checks against that watchlist and writes recall metrics.

If time is short, skip script creation and run the shell commands manually with `tee`; do not skip
the artifact layout.

## Phase 1 - Environment and Static Health

Goal: prove the local environment can run the repo and identify obvious doc/config drift before live
network work.

Commands:

```bash
python --version 2>&1 | tee "${EXPERIMENT_ROOT}/logs/01_python_version.log"
python -m pip install -e ".[dev]" 2>&1 | tee "${EXPERIMENT_ROOT}/logs/02_install_dev.log"
python -m playwright install chromium 2>&1 | tee "${EXPERIMENT_ROOT}/logs/03_playwright_install.log"
python scripts/validate_agent_plan.py 2>&1 | tee "${EXPERIMENT_ROOT}/logs/04_validate_agent_plan.log"
ruff format --check . 2>&1 | tee "${EXPERIMENT_ROOT}/logs/05_ruff_format_check.log"
ruff check . 2>&1 | tee "${EXPERIMENT_ROOT}/logs/06_ruff_check.log"
mypy src/ 2>&1 | tee "${EXPERIMENT_ROOT}/logs/07_mypy.log"
pytest -q tests/unit/test_validate_agent_plan.py tests/unit/test_cli.py tests/unit/test_source_health.py tests/unit/test_discovery_diagnostics.py 2>&1 | tee "${EXPERIMENT_ROOT}/logs/08_targeted_static_tests.log"
```

Static decisions to record:

- Whether `.agent-plan.md` passes validation.
- Whether `PLAN.md` should be updated because it still points at `DL-PR-08`.
- Whether `docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md` still accurately says PR 3.4 is current next.
- Whether any tests already fail before live work.

## Phase 2 - Artifact-Only Baseline

Goal: understand what the current checked-out local state says without touching live sites.

Commands:

```bash
denbust diagnose-discovery \
  --config agents/news/local.yaml \
  --format json \
  --output "${EXPERIMENT_ROOT}/artifacts/diagnose_discovery_artifact_only.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/09_diagnose_discovery_artifact_only.log"

denbust diagnose-sources \
  --config agents/news/local.yaml \
  --artifacts-only \
  --format json \
  --output "${EXPERIMENT_ROOT}/artifacts/diagnose_sources_artifact_only.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/10_diagnose_sources_artifact_only.log"
```

Because `DENBUST_STATE_ROOT` points at a fresh experiment directory, these diagnostics should show
an empty or missing-state baseline. If they unexpectedly read the repo-default `data/`, that is a
configuration isolation bug.

Record:

- empty-state behavior
- whether diagnostics fail clearly when artifacts are absent
- whether source-health diagnostics can be trusted without live probes

## Phase 3 - Live Source Health Probes

Goal: answer open issues #71, #72, #74 and re-check historical zero-result issues.

Run all sources first:

```bash
denbust diagnose-sources \
  --config agents/news/local.yaml \
  --live-only \
  --sample-keyword "זנות" \
  --sample-keyword "בית בושת" \
  --sample-keyword "סחר בבני אדם" \
  --format json \
  --output "${EXPERIMENT_ROOT}/artifacts/diagnose_sources_live_all.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/11_diagnose_sources_live_all.log"
```

Then run source-specific probes so one slow source does not hide another:

```bash
for source in ynet walla mako maariv haaretz ice; do
  denbust diagnose-sources \
    --config agents/news/local.yaml \
    --live-only \
    --source "${source}" \
    --sample-keyword "זנות" \
    --sample-keyword "בית בושת" \
    --sample-keyword "סחר בבני אדם" \
    --format json \
    --output "${EXPERIMENT_ROOT}/artifacts/diagnose_sources_live_${source}.json" \
    2>&1 | tee "${EXPERIMENT_ROOT}/logs/12_diagnose_sources_live_${source}.log"
done
```

Record per source:

- HTTP/browser success or failure
- zero raw results vs parse failure vs keyword miss
- whether debug state includes enough detail to explain the failure
- whether diagnostics rely on private helper methods, especially for #52 and #53
- whether Mako failures look like missing Chromium, timeout, anti-bot, selector drift, or navigation instability
- whether Ynet RSS looks too shallow and needs the category-page backstop from #65

Decision rules:

- If 4+ configured sources return zero or hard failures, #72 remains active and should become the next correctness PR.
- If Mako alone fails with browser/navigation errors, merge #71 and #74 conceptually and fix Mako first.
- If Ynet returns recent RSS items but not known relevant category items, #65 remains active.
- If diagnostics cannot explain why a source returned zero, `DL-PR-12`-style structured failure diagnostics become more urgent.

## Phase 4 - Fresh Discovery Run

Goal: measure current candidate production from source-native and search engines in a clean state.

First run source-native plus any configured external engines. If search-engine keys are absent, this
still measures source-native discovery.

```bash
denbust run \
  --dataset news_items \
  --job discover \
  --config agents/news/local.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/13_discover.log"

denbust diagnose-discovery \
  --config agents/news/local.yaml \
  --format json \
  --output "${EXPERIMENT_ROOT}/artifacts/diagnose_discovery_after_discover.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/14_diagnose_discovery_after_discover.log"
```

Optional if search keys are available:

```bash
env | rg '^DENBUST_(BRAVE_SEARCH_API_KEY|EXA_API_KEY|GOOGLE_CSE_API_KEY|GOOGLE_CSE_ID)=' \
  > "${EXPERIMENT_ROOT}/summaries/search_engine_env_present.txt"
```

Record:

- candidate count by producer: source-native, Brave, Exa, Google CSE
- taxonomy-targeted query count
- Facebook/social-targeted candidate count
- overlap matrix
- unsupported-source candidates and source suggestions
- candidate status distribution
- whether source-native discovery is dominated by a single source

Decision rules:

- If search engines produce zero because keys are missing, mark that as environment-limited, not code-broken.
- If keys are present and search engines still produce zero, discovery engine health becomes active work.
- If all candidates come from ICE only, #72 remains active.

## Phase 5 - Candidate Scrape and Ingest Accounting

Goal: verify the raw -> unseen -> classified -> relevant -> unified accounting and prove that drops
are visible with reason codes.

Run candidate scrape first:

```bash
denbust run \
  --dataset news_items \
  --job scrape_candidates \
  --config agents/news/local.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/15_scrape_candidates.log"
```

Then run normal ingest:

```bash
denbust run \
  --dataset news_items \
  --job ingest \
  --config agents/news/local.yaml \
  --days 21 \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/16_ingest_21d.log"
```

Then re-run diagnostics:

```bash
denbust diagnose-discovery \
  --config agents/news/local.yaml \
  --format json \
  --output "${EXPERIMENT_ROOT}/artifacts/diagnose_discovery_after_ingest.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/17_diagnose_discovery_after_ingest.log"
```

Record:

- raw articles by source
- unseen articles by source
- classified articles by source
- relevant and rejected articles by source
- all skip/drop reasons between raw discovery and classifier input
- seen-store count before and after
- scrape success, partial-page, search-result-only, and scrape-failed counts
- top scrape failure source/domain/error code
- whether fallback rows remain internal-only as intended

Decision rules:

- If articles disappear before classification without explicit reason codes, reopen or supersede #46.
- If source adapter successes are not reflected in candidate statuses, the candidate/ingest handoff is active work.
- If classifier costs block the run, rerun source/candidate phases without classification and mark classifier verification as environment-limited.

## Phase 6 - Bounded Backfill

Goal: test the historical recovery path without launching a full 90-day scrape budget.

Start with a 7-day backfill window ending today:

```bash
DENBUST_BACKFILL_DATE_FROM=2026-04-24T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-05-01T23:59:59+00:00 \
denbust run \
  --dataset news_items \
  --job backfill_discover \
  --config agents/news/local.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/18_backfill_discover_7d.log"

denbust run \
  --dataset news_items \
  --job backfill_scrape \
  --config agents/news/local.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/19_backfill_scrape_7d.log"
```

If the 7-day run is clean and useful, run a 30-day discovery-only window:

```bash
DENBUST_BACKFILL_DATE_FROM=2026-04-01T00:00:00+00:00 \
DENBUST_BACKFILL_DATE_TO=2026-05-01T23:59:59+00:00 \
denbust run \
  --dataset news_items \
  --job backfill_discover \
  --config agents/news/local.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/20_backfill_discover_30d.log"
```

Do not run the full 90-day scrape until the 7-day scrape and 30-day discovery outputs show sane
source and candidate distributions.

Record:

- backfill batch count and status
- candidates by batch
- oldest-first scrape selection behavior
- source-native historical support warnings
- whether #88 aggregate-count performance is noticeable
- whether C-8 taxonomy-targeted queries materially increase candidate diversity

Decision rules:

- If the backfill queue grows but cannot drain, prioritize scrape diagnostics/retry health before wider backfill.
- If batch aggregate updates are slow or noisy, #88 becomes more relevant.
- If backfill discover produces no historical candidates from major sources, source discovery remains the main blocker.

## Phase 7 - Classifier and Validation Checks

Goal: determine whether the closed classifier issues are truly covered by tests and fixtures.

Run the tracked validation evaluation:

```bash
denbust validation-evaluate \
  --validation-set validation/news_items/classifier_validation.csv \
  --variants agents/validation/classifier_variants.yaml \
  --output "${EXPERIMENT_ROOT}/artifacts/validation_evaluate.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/21_validation_evaluate.log"
```

Run the existing issue-48 live-check scenario through the Python API, since there is no CLI command
for it yet:

```bash
python - <<'PY' 2>&1 | tee "${EXPERIMENT_ROOT}/logs/22_live_check_issue_48.log"
import os
from pathlib import Path
from denbust.live_checks.runner import run_live_check_scenario_sync

report = run_live_check_scenario_sync(
    Path("agents/live_checks/classifier_issue_48.yaml"),
    output_root=Path(os.environ["EXPERIMENT_ROOT"]) / "live_checks",
)
print(report.model_dump_json(indent=2))
PY
```

Record:

- validation-set row count and parse health
- number of taxonomy-labeled examples
- relevance, enforcement-related, index-relevance, category, and subcategory metrics
- whether issue #10 and #48 fixtures pass
- whether the validation report is human-readable enough to satisfy PR 3.4
- whether a first-class `denbust live-check` CLI should be added

Decision rules:

- If validation CSV quality is poor, fix validation data before trusting classifier metrics.
- If fixtures pass but live source coverage fails, prioritize source discovery over classifier prompts.
- If fixture regression fails, classifier calibration becomes active again despite closed issues.
- If reports are machine-rich but not human-readable, PR 3.4 may still be relevant.

## Phase 8 - Known URL and Coverage Watchlist

Goal: convert known examples and issue URLs into recall measurements.

Inputs:

- `docs/articles_examples.md`
- URLs named in issues #10, #35, #36, #37, #39, #40, #45, #48, #65, and #66
- `tests/fixtures/articles/*.json`
- any URLs discovered by the 7-day and 30-day backfill runs that should become future fixtures

Minimum manual process:

1. Build `inputs/known_urls.csv` with columns:
   - `url`
   - `source_name`
   - `expected_topic`
   - `expected_relevant`
   - `expected_enforcement_related`
   - `issue_or_doc_source`
   - `notes`
2. Compare each canonical URL to:
   - `latest_candidates.jsonl`
   - `candidate_provenance.jsonl`
   - ingest raw articles
   - classified articles
   - final operational records
3. Write `reports/known_url_recall.md`.

Recommended code if this is too tedious:

```bash
python scripts/may_26_known_url_watchlist.py \
  --input "${EXPERIMENT_ROOT}/inputs/known_urls.csv" \
  --state-root "${DENBUST_STATE_ROOT}" \
  --output "${EXPERIMENT_ROOT}/reports/known_url_recall.md" \
  --json-output "${EXPERIMENT_ROOT}/artifacts/known_url_recall.json"
```

Decision rules:

- Known URL found by source/discovery but not classified: ingestion handoff or seen/filter issue.
- Known URL not found by source-native but found by search engine: source-native gap.
- Known URL not found anywhere: discovery/query/source gap.
- Known URL classified incorrectly: classifier/validation gap.

## Phase 9 - Monthly Report, Release, and Public Contract Smoke

Goal: make sure product-facing outputs still work after local ingest/backfill.

Commands:

```bash
denbust report monthly \
  --month 2026-04 \
  --config agents/news/local.yaml \
  --output "${EXPERIMENT_ROOT}/reports/monthly_2026_04.md" \
  --json-output "${EXPERIMENT_ROOT}/artifacts/monthly_2026_04.json" \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/23_monthly_report.log"

denbust release \
  --dataset news_items \
  --config agents/release/news_items.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/24_release.log"

denbust backup \
  --dataset news_items \
  --config agents/backup/news_items.yaml \
  2>&1 | tee "${EXPERIMENT_ROOT}/logs/25_backup.log"
```

Record:

- whether monthly report has cases or correctly reports empty month
- whether report JSON matches minisite/export expectations
- whether release remains metadata-only
- whether release/backup local configs accidentally require external services
- whether manual missing-item/correction imports are needed for product completeness

Decision rules:

- If monthly report cannot produce useful output from local operational rows, product alignment is incomplete.
- If release contains full text or unsafe data, stop and fix public-output policy.
- If report shape is not close to the minisite contract, revive that part of the main roadmap.

## Phase 10 - Synthesis

Create `reports/final_experiment_summary.md` with these sections:

1. `Run Context`
   - commit SHA, branch, timestamp, Python version, config paths, state root, secrets-present booleans.
2. `Planning Drift`
   - list stale docs and exact recommended updates.
3. `Issue Decisions`
   - for each of the 21 issues: `still active`, `stale`, `duplicate`, `blocked`, `converted to test`, or `needs new issue`.
4. `Source Health`
   - per source: live status, raw count, failure mode, recommended next action.
5. `Discovery Health`
   - producer counts, overlap, candidate status, queue health, source suggestions.
6. `Ingest and Classification Health`
   - raw/unseen/classified/relevant accounting, skip reasons, classifier metrics.
7. `Backfill Health`
   - batch status, historical candidate yield, scrape drain behavior.
8. `Product Output Health`
   - monthly report, release, backup, public contract status.
9. `Missing Tooling`
   - scripts, CLI commands, fixture assets, diagnostics fields, docs updates.
10. `Recommended Next PR`
   - one concrete next PR with labels/milestone suggestions and acceptance criteria.

## Likely Outcomes to Validate

The experiment should not assume these are true, but these are the current hypotheses:

1. `DL-PR-12` is not the only plausible next step. If source failures are still widespread, a
   source-health correctness PR should come before optional self-healing scaffolding.
2. `PLAN.md` and `docs/MILESTONE_3_VALIDATION_PR_BREAKDOWN.md` likely need status updates after the
   experiment, regardless of what implementation work comes next.
3. Mako issues #71 and #74 are probably duplicates or near-duplicates and can be consolidated after
   a fresh Mako probe.
4. #72 is the central open reliability question: either the source-zero problem still exists, or the
   open issue can be closed with evidence.
5. #65 remains likely relevant because Ynet RSS is intentionally shallow even after moving to the
   better crime feed.
6. #52 and #53 are not user-facing bugs, but they matter if source diagnostics are going to become
   the basis for self-healing.
7. #88 should stay lower priority unless bounded backfill demonstrates real local slowness.
8. The repo needs a first-class experiment or live-check CLI if these checks are going to be run more
   than once.

## Completion Criteria

The local experiment is complete only when:

- A fresh `DENBUST_STATE_ROOT` was used.
- Every source has a live diagnostic result.
- Discovery, ingest, and at least one bounded backfill were attempted or explicitly marked blocked.
- Validation evaluation and issue-48 live checks were attempted or explicitly marked blocked by
  missing `ANTHROPIC_API_KEY`.
- A final summary exists with issue-by-issue decisions.
- The next PR is selected from evidence, not from the stale planning docs alone.
- Any follow-up implementation proposal includes exact acceptance criteria and artifact paths from
  this experiment.
