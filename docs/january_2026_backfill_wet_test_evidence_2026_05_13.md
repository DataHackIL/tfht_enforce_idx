# January 1-7, 2026 Backfill Wet-Test Evidence

Date summarized: 2026-05-13

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
