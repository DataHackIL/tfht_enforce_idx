# January 1-7, 2026 Backfill Wet-Test Evidence

Date summarized: 2026-05-13, with 2026-05-14 Phase C diagnostic addenda appended.

This is the checked-in evidence summary for the bounded January 1-7 Chrome-CDP wet test. The
generated logs and JSON artifacts remain local under `data/may_26_followup/20260503T214851Z/` and
are not part of the repository.

Exact local evidence files used:

- Discovery log:
  `data/may_26_followup/20260503T214851Z/logs/05c_backfill_discover_2026_01_01_07_brave_exa.log`
- Post-discovery diagnostic:
  `data/may_26_followup/20260503T214851Z/artifacts/diagnose_discovery_after_discover_brave_exa.json`
- Chrome CDP preflight log:
  `data/may_26_followup/20260503T214851Z/logs/07a_chrome_cdp_preflight_brave_exa_retry.log`
- Retry scrape log:
  `data/may_26_followup/20260503T214851Z/logs/07c_backfill_scrape_2026_01_01_07_brave_exa_retry.log`
- Post-scrape diagnostic:
  `data/may_26_followup/20260503T214851Z/artifacts/diagnose_discovery_after_scrape_brave_exa_chrome_cdp.json`

## Scope

- Window: `2026-01-01T00:00:00+00:00` through `2026-01-07T23:59:59+00:00`
- Discovery mode: Brave + Exa search, with Google CSE disabled for this local run
- Scrape mode: browser-backed scraping attached to a local Chrome DevTools endpoint
- Browser environment:
  - `DENBUST_BROWSER_MODE=chrome_cdp`
  - `DENBUST_CHROME_CDP_URL=http://127.0.0.1:9222`

## Why Google CSE Was Bypassed

The local Google CSE setup returned `403 PERMISSION_DENIED` / API-access failure during the wet-test
setup path. The evidence pass therefore used a Brave+Exa/no-Google search config to exercise the
backfill path without depending on that local API access.

This does not remove or deprecate Google CSE support in code. It only records a reproducible local
operator mode for wet tests where Google Programmable Search API access is unavailable or
misconfigured.

## Discovery Evidence

The post-discovery diagnostic artifact reported:

| Figure | Value | JSON path |
| --- | ---: | --- |
| Persisted candidates | 3,116 | `queue_health.total_candidates` |
| Brave candidates | 1,602 | `engine_overlap.brave` |
| Exa candidates | 1,517 | `engine_overlap.exa` |
| Brave/Exa overlap | 3 | `engine_overlap.brave_exa_shared` |
| Google CSE candidates | 0 | `engine_overlap.google_cse` |
| Search-engine-only candidates | 3,116 | `source_search_coverage.search_engine_only_candidates` |
| Initial scrape-eligible candidates after discovery | 2,701 | `queue_drain.remaining_eligible_candidate_count` |

Google CSE was disabled for this local run, which is why the Google CSE candidate count is zero.

## Scrape Evidence

The valid Chrome-CDP retry scrape and post-scrape diagnostic artifact reported:

| Figure | Value | JSON path |
| --- | ---: | --- |
| Attempted candidates | 100 | `queue_drain.persisted_attempted_candidate_count` |
| Persisted scrape attempts | 189 | `queue_drain.persisted_scrape_attempt_count` |
| Provisional operational rows retained | 28 | `candidate_conversion.operational_record_matches` |
| Partial pages | 88 | `queue_health.partial_page_candidates` |
| Scrape failures | 12 | `queue_health.scrape_failed_candidates` |
| Remaining eligible candidates | 2,689 | `queue_drain.remaining_eligible_candidate_count` |
| Inferred stop reason | `budget_cap_reached` | `queue_drain.inferred_stop_reason` |

Attempted candidate source mix:

| Source | Attempted candidates |
| --- | ---: |
| Brave | 11 |
| Haaretz | 34 |
| ICE | 2 |
| Maariv | 7 |
| Mako | 17 |
| Walla | 15 |
| Ynet | 14 |

Source mix comes from `queue_drain.attempted_source_mix`.

The scrape run actually exercised Chrome-CDP browser-backed source paths. The retry scrape log shows
Haaretz browser navigation across search keywords and Mako browser navigation across search keywords
plus the `men-news` section.

## Invalid First Scrape Attempt

The first no-CDP scrape attempt was aborted/reset before it could serve as valid scrape evidence.
Do not use that attempt to evaluate browser-backed scraping, queue-drain behavior, or scrape
conversion. The figures above come from the Chrome-CDP retry scrape and the matching post-scrape
diagnostic artifact.

## Interpretation

The bounded scrape reached the configured budget cap with a large eligible backlog still available,
so the stop reason is explainable as a budget limit rather than a queue-contract failure. The run
also exposed candidate-quality noise, including social/profile/app-store/dictionary-like surfaces
that can consume scrape budget. That follow-up belongs in `DISC-PR-NOISE-FILTERS`, not in this
config-and-evidence slice.

After `SCRAPE-PR-PARTIAL-DIAGNOSTICS`, future `denbust diagnose-discovery` artifacts include
`partial_page_diagnostics` for this exact interpretation gap. The original checked-in artifact still
records only the older `queue_health.partial_page_candidates=88` figure, but fresh diagnostics over
the same persisted state can now report how many partial candidates produced candidate-fallback
operational rows, how many stayed metadata-only when operational matching is enabled, whether
partial extraction came from generic fetch, a source adapter, or generic fallback after a
source-adapter attempt, which domains/source hints dominate partials, and which persisted
current-candidate classifier/taxonomy warning signals affect conversion interpretation.

For `SRC-PR-ISRAELHAYOM`, fresh diagnostics over the same persisted state showed
`israelhayom.co.il` as a repeated source suggestion with 25 candidate-only main-domain URLs across
two runs. Direct candidate-state inspection showed 33 Israel Hayom-family candidate-only URLs and no
Israel Hayom attempted scrape or partial-page evidence in the bounded drain. That evidence supports
bounded main-domain generic-fetch source-family recognition, while leaving source-targeted
discovery/backfill fanout, Israel Hayom subdomains, browser scraper work, queue fairness,
Mako/Haaretz Chrome-CDP behavior, and unrelated source families out of scope.

For `SRC-PR-KAN`, fresh diagnostics and candidate-state inspection over the same persisted state
showed two official `kan.org.il` candidate-only URLs and no official Kan attempted-scrape or
partial-page evidence in the bounded drain:

| Candidate ID | URL | Status | Content basis | Source hints | Discovered via | Scrape attempts |
| --- | --- | --- | --- | --- | --- | ---: |
| `candidate_c26f6055d9d8e0fb1ead6e4b` | `https://www.kan.org.il/content/kan-news/local/983197/` | `unsupported_source` | `candidate_only` | `www.facebook.com` | `brave` | 0 |
| `candidate_d784dda040a57b3c2c7022ca` | `https://www.kan.org.il/content/kan-news/local/296141/` | `new` | `candidate_only` | `brave` | `brave` | 0 |

The same keyword searches also surfaced unrelated Kan-named domains such as `kanisrael.co.il`,
`kan-ashkelon.co.il` Facebook posts, and other non-Kan hosts. This weak evidence does not prove
generic-fetch extraction quality for Kan. It only supports low-confidence diagnostic labeling for
future official Kan news article-path candidates under `kan.org.il/content/kan-news/`, while leaving
source-targeted discovery/backfill fanout, unrelated Kan-named domains, non-article Kan pages,
browser scraper work, queue fairness, Mako/Haaretz Chrome-CDP behavior, and unrelated source
families out of scope.

For `SRC-PR-NEWS1`, candidate-state inspection over the same persisted state showed three
main-domain News1 archive candidate-only URLs, all Exa-discovered, all still scrape-eligible after
the bounded drain, and no News1 attempted-scrape or partial-page evidence:

| Candidate ID | URL | Status | Content basis | Source hints | Discovered via | Scrape attempts |
| --- | --- | --- | --- | --- | --- | ---: |
| `candidate_f3635f618524800b7e7fefce` | `https://www.news1.co.il/Archive/001-D-512703-00.html` | `new` | `candidate_only` | `exa` | `exa` | 0 |
| `candidate_72ea3953c62c179a8d85a6ae` | `https://www.news1.co.il/Archive/0026-D-175044-00.html` | `new` | `candidate_only` | `exa` | `exa` | 0 |
| `candidate_824eaa3d83da7c421dbe46e2` | `https://www.news1.co.il/Archive/001-D-512714-00.html` | `new` | `candidate_only` | `exa` | `exa` | 0 |

The same post-scrape diagnostic did not emit `news1.co.il` among the top source suggestions. This
weak but concrete evidence supports low-confidence diagnostic labeling for future
main-domain News1 archive-path candidates under `news1.co.il/Archive/`, while leaving
source-targeted discovery/backfill fanout, non-archive News1 pages, generic metadata hardening,
browser scraper work, queue fairness, Mako/Haaretz Chrome-CDP behavior, and unrelated source
families out of scope.

## Fresh Globes/TheMarker Follow-Up Evidence

The 2026-05-14 fresh Phase C January 1-7 discovery/backfill evidence pass used the same
Brave+Exa/no-Google local search mode and Chrome-CDP scrape shape under the generated local root
`data/may_26_followup/20260514T135635Z/`. The generated artifacts are intentionally untracked.

The run reported:

| Figure | Value |
| --- | ---: |
| Persisted candidates | 3,745 |
| Attempted candidates | 100 |
| Persisted scrape attempts | 159 |
| Provisional operational rows retained | 30 |
| Partial pages | 97 |
| Scrape failures | 3 |
| Remaining eligible candidates | 3,143 |
| Inferred stop reason | `budget_cap_reached` |

The partial-page diagnostics showed all 97 partials came from generic fetch, with top partial
domains led by `themarker.com=32`, `haaretz.co.il=27`, `mako.co.il=13`, `news.walla.co.il=10`, and
`ynet.co.il=9`. Source suggestions still showed Globes and TheMarker pressure after the prior
source-family slice: `themarker.com` had 298 candidates, 266 candidate-only candidates, and 34
attempts; `globes.co.il` had 216 candidates, 215 candidate-only candidates, and one attempt.

This evidence does not justify a browser/CDP scraper, new source-family fanout, queue fairness
change, scrape-cap change, Mako/Haaretz behavior change, or unrelated source-family work. It does
justify a generic source-suggestion diagnostics correction that separates partial recoveries from
definite scrape failures: TheMarker can simultaneously be a top backlog source and a source with
substantial metadata-only generic-fetch progress. Counting `partial` attempts as
`scrape_failure_count` overstates failure pressure and obscures whether the next step should be
metadata/classifier hardening, scraper work, or deferral pending more evidence. The correction does
not otherwise change source-suggestion ranking.

## Fresh Sport1 Candidate-Only Follow-Up Evidence

The same 2026-05-14 fresh Phase C diagnostic showed `sport1.maariv.co.il` as a repeated
candidate-only source suggestion:

| Figure | Value |
| --- | ---: |
| Candidates | 147 |
| Candidate-only candidates | 147 |
| Runs | 2 |
| Scrape attempts | 0 |
| Partial recoveries | 0 |
| Scrape successes | 0 |
| Scrape failures | 0 |
| Score | 185.75 |

Candidate-state inspection showed Sport1 sports-vertical paths such as `israeli-soccer`,
`world-soccer`, `square`, basketball, tennis, and related sections, but the candidates were still
article-like URLs and some titles contained legal/crime-adjacent wording. That evidence supports a
diagnostic-only `sports_vertical_candidate_only` source-suggestion classification. It does not
support mapping Sport1 into Maariv-family article handling, suppressing Sport1 before
scrape/classifier evidence exists, source-targeted query fanout, a browser/CDP scraper, queue
fairness or scrape-cap changes, Mako/Haaretz behavior changes, or broader sports-domain filtering.
Sport1 candidates and regular `www.maariv.co.il` news/law article URLs remain scrape-eligible.

## 2026-05-14 Addendum: Fresh Classifier Candidate-Fallback Diagnostic Evidence

The same 2026-05-14 fresh Phase C diagnostic reported 30 retained candidate-fallback operational
rows. All 30 were partial-page fallback rows and all 30 were low-confidence at the retained-record
level, while none had missing or invalid taxonomy pairs in the persisted operational state. Direct
inspection of the local operational rows showed the low-confidence fallback rows were spread across
Haaretz, Walla, Mako, Ynet, TheMarker, Brave-only, and Globes source labels and across valid TFHT
taxonomy pairs such as `brothels/keeping_brothel`,
`human_trafficking/trafficking_slavery_conditions`, and
`pimping_prostitution/phenomenon_coverage`.

The scrape log also contained run-level classifier warnings: invalid taxonomy pairs
`pimping_prostitution / advertising_prostitution` and
`human_trafficking / phenomenon_coverage`, plus several JSON parse failures. Those warnings were
visible only in the run log, not in candidate state or the retained operational records. This
supports improving persisted diagnostic aggregation for matched candidate-fallback rows, while
leaving run-log warning capture as a separate diagnostic persistence follow-up.

`CLASSIFIER-PR-CANDIDATE-FALLBACK-DIAGNOSTICS` therefore adds source/domain/taxonomy/confidence
breakdowns for low-confidence fallback rows in `partial_page_diagnostics.classifier_warning_signals`.
It does not change classifier prompts or policy, taxonomy validity, queue fairness, scrape
candidate selection, generic fetch behavior, browser/CDP scraping, source-family support, or source
targeted query fanout.

## 2026-05-14 Addendum: Run-Level Classifier Warning Artifacts

`CLASSIFIER-PR-RUN-WARNING-ARTIFACTS` addresses the separate run-log-only warning gap from the same
fresh Phase C bounded drain. The bounded implementation path is the existing run debug summary
artifact, not discovery diagnostics: parse failures and invalid taxonomy pairs occur inside the
classifier parser before there is necessarily a durable candidate or operational-record identity to
attach to.

Run debug summaries now persist `classifier_summary.warning_counts` with counts for:

- `parse_failure_count`
- `invalid_taxonomy_pair_count`
- `invalid_legacy_pair_count`
- `relevant_without_usable_taxonomy_count`

Fallback-only `scrape_candidates` and `backfill_scrape` runs also emit a compact scrape debug
payload so retained provisional candidate-fallback rows do not leave classifier parser warnings
visible only in process logs. The compact payload includes a dedicated `fallback_classifier_summary`
for fallback classifier input counts, retained fallback operational-record counts, and warning
counts. This is diagnostic persistence only. It does not change classifier prompts, taxonomy
validity, classifier policy, queue fairness, scrape candidate selection, generic fetch behavior,
browser/CDP scraping, source-family support, or source-targeted query fanout.

## 2026-05-14 Addendum: Warning-Count Evidence Interpretation

`CLASSIFIER-PR-WARNING-EVIDENCE-INTERPRETATION` ran the next bounded Phase C January 1-7 evidence
pass after run-level classifier warning counts were persisted. The generated local evidence root is
`data/may_26_followup/20260514T182934Z/`; generated artifacts remain untracked.

The run reused the established local Brave+Exa/no-Google configuration,
`agents/news/local_search_brave_exa.yaml`, because the local Google CSE path is still treated as an
operator-dependent API-access risk. The scrape drain used a temporary Chrome-CDP endpoint at
`http://127.0.0.1:9222` and did not broaden the window, source list, search-engine set, queue
policy, scrape cap, scraper behavior, or source-family support.

Operator notes: the first discovery attempt failed before this shell loaded the repo-local
`direnv` environment, so the successful run used `direnv exec .` to load `.env.local` without
printing secret values. The initial Chrome-CDP preflight also failed because no browser was
listening on port 9222; the successful scrape used a temporary Chrome profile with remote debugging
enabled for this evidence pass.

The run reported:

| Figure | Value |
| --- | ---: |
| Persisted candidates | 3,743 |
| Attempted candidates | 100 |
| Persisted scrape attempts | 159 |
| Provisional operational rows retained | 30 |
| Partial pages | 97 |
| Scrape failures | 3 |
| Remaining eligible candidates | 3,148 |
| Inferred stop reason | `budget_cap_reached` |

The full post-scrape run payload
`data/may_26_followup/20260514T182934Z/state/news_items/backfill_scrape/logs/2026-05-14T18-38-54-795669Z.json`
persisted these warning counters:

| Counter | Count | Rate |
| --- | ---: | ---: |
| `classifier_summary.warning_counts.parse_failure_count` | 4 | 4% of 100 fallback classifier inputs |
| `classifier_summary.warning_counts.invalid_taxonomy_pair_count` | 1 | 1% of 100 fallback classifier inputs |
| `classifier_summary.warning_counts.invalid_legacy_pair_count` | 0 | 0% |
| `classifier_summary.warning_counts.relevant_without_usable_taxonomy_count` | 0 | 0% |
| `fallback_classifier_summary.warning_counts.parse_failure_count` | 4 | 4% of 100 fallback classifier inputs |
| `fallback_classifier_summary.warning_counts.invalid_taxonomy_pair_count` | 1 | 1% of 100 fallback classifier inputs |
| `fallback_classifier_summary.warning_counts.invalid_legacy_pair_count` | 0 | 0% |
| `fallback_classifier_summary.warning_counts.relevant_without_usable_taxonomy_count` | 0 | 0% |

The same payload recorded `fallback_classifier_input_count=100` and
`fallback_operational_record_count=30`. During this interpretation pass, the matching compact
`.summary.json` retained `classifier_summary.warning_counts` but did not yet include
`fallback_classifier_summary`; this PR closes that compact-summary artifact gap for future runs so
operators do not need the full debug JSON to see fallback classifier input counts and warning
counts.

The post-scrape discovery diagnostic still showed that all 30 retained fallback rows had valid
persisted taxonomy pairs and that `invalid_taxonomy_pair_record_count` was zero. That means the
observed warning pressure is real classifier-output loss before retention, not corrupted persisted
operational taxonomy.

Interpretation: 5 warning events across 100 fallback classifier inputs, including four parse
failures, is enough to justify a bounded classifier-output follow-up, but the next PR should first
check whether retained artifacts expose raw parse-failure output shapes before changing parser
behavior. The evidence does not justify changing classifier prompts, taxonomy validity policy,
legacy taxonomy policy, queue fairness, scrape candidate selection, generic fetch behavior,
browser/CDP scraper behavior, scrape caps, source-family support, or source-targeted query fanout in
this interpretation slice.

## 2026-05-14 Addendum: Classifier Output Robustness Evidence Review

`CLASSIFIER-PR-OUTPUT-ROBUSTNESS` inspected the generated evidence root
`data/may_26_followup/20260514T182934Z/`, including the backfill-scrape debug payload
`state/news_items/backfill_scrape/logs/2026-05-14T18-38-54-795669Z.json`, its compact
`.summary.json`, the post-scrape discovery diagnostic, operational rows, run metadata, and the
root-level `logs/`, `reports/`, and `summaries/` directories. The persisted artifacts confirmed the
same four parse failures, one invalid taxonomy pair, 100 fallback classifier inputs, and 30 retained
fallback operational rows, but they did not persist the raw malformed classifier responses. The
root-level log/report/summary directories were empty.

The actual malformed response shapes are therefore insufficiently observable from the retained
artifacts. Earlier local process logs from related runs showed JSON decoder messages consistent with
object-like non-JSON text, but those messages are not retained in this evidence root and are not
enough to characterize the four target parse failures as a specific recoverable output category.

No parser recovery was added because accepting guessed pseudo-JSON would change classifier output
semantics without evidence that the target raw responses were safely recoverable. Instead,
representative current-policy regression coverage now proves plausible malformed object-like
non-JSON outputs are rejected deterministically as low-confidence not-relevant results and still
increment `parse_failure_count`. Existing canonical JSON success behavior and invalid taxonomy-pair
rejection/counting behavior remain unchanged.

The next classifier-output follow-up should persist a sanitized parse-failure shape sample or
structured parse-error evidence in run debug artifacts before considering any parser recovery. That
future evidence-capture work should still avoid prompt changes, taxonomy-policy changes, queue
behavior changes, scraper behavior changes, scrape-cap changes, source-family support, and raw
generated data commits.

## 2026-05-14 Addendum: Parse-Failure Shape Evidence Capture

`CLASSIFIER-PR-PARSE-FAILURE-EVIDENCE-CAPTURE` closes the artifact gap identified above for future
runs. Run debug payloads now carry sanitized parse-failure shape evidence under
`classifier_summary.parse_failure_diagnostics`; fallback-only scrape/backfill payloads also carry
the same object under `fallback_classifier_summary.parse_failure_diagnostics`. Compact
`.summary.json` artifacts retain both summary objects.

The diagnostic object records stable category counts for `empty_response`, `json_decode_error`,
`non_object_json_array`, `non_object_json_scalar`, `object_like_non_json`,
`markdown_wrapped_malformed_json`, `truncated_response`, and `other_parse_failure`. It also keeps up
to five samples with only structural metadata: response length, normalized length, line count, JSON
error position, Markdown-code-fence flags, and a bounded character-class `shape_signature` of at
most 80 characters.

The shape samples intentionally do not store raw classifier response text, full article text,
secrets, provider headers, prompts, candidate bodies, or generated `data/` artifacts. The fields are
evidence for the next bounded robustness decision only. This slice does not change classifier
prompts, parser recovery behavior, taxonomy validity policy, legacy taxonomy policy, queue
fairness, scrape candidate selection, generic fetch behavior, browser/CDP scraping, scrape caps,
source-family support, or source-targeted query fanout.
