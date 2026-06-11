# Batch Scraping Protocol

This document defines the standing protocol for working through the candidate
backlog under a **limited scraping budget** (rate-limit / block risk, time
cost, and Claude classification spend). It is mandatory for any agent running
`scrape_candidates` against the backlog.

## Why batches

Discovery finds thousands of candidates per month. The prefilter cascade and
the candidation steps below cut that to a small, high-signal set so we spend
scrape and Claude budget only where an enforcement event might actually exist.
We process the backlog in **fixed-size batches** (default 60) rather than
draining the whole queue at once.

## The pipeline, end to end

```
discover → candidates → Stage B (NaiveBayes) → balanced batch selection
        → Stage B2 (manual LLM filter) → scrape → Stage C/D (disabled)
        → Claude classification → operational record → release
```

| Stage | What | Cost | Who |
|-------|------|------|-----|
| Stage A | lexicon / domain reputation | free | model (disabled) |
| **Stage B** | NaiveBayes thin prefilter, `enforce` mode | free / CPU | model |
| Balanced selection | month-frequency-weighted, source-balanced batch | free | code (`--balanced-batch N`) |
| **Stage B2** | **manual LLM/agent junk-and-spam filter** | agent judgment | **the operating agent** |
| Scrape | fetch + extract article body | HTTP budget | code |
| Stage C/D | embeddings / SLM thick pass | free / CPU | model (disabled) |
| Claude classification | enforcement-relevance on full text | Claude API | model |

## Stage B2 — manual LLM filtering (the new rule)

Stage B is statistical and lets through a long tail of **keyword-rich spam**:
escort-listing sites, massage-ad pages, and SEO-bait domains whose title and
snippet contain the enforcement lexicon (`זנות`, `עיסוי`, `ליווי`, …) but which
will never yield a real enforcement event.

**Stage B2 is a mandatory judgment pass performed by the operating agent (the
LLM) on the planned batch, before any scrape budget is spent.** The agent reads
each candidate's title, snippet, and domain and removes the ones that are
clearly junk by common sense.

### Protocol

1. **Plan** the batch (`--balanced-batch N`) or dry-run the planner to list the
   selected candidates with `title`, `snippet`, `domain`, `candidate_id`.
2. **Review (Stage B2).** For each candidate decide: real news outlet covering
   a plausible enforcement event → **keep**; escort/massage listing, ad page,
   SEO spam, off-topic aggregator → **remove**.
3. **Suppress** the removed candidates so they leave the pool permanently and
   never re-enter a future batch:
   - **Whole spam domain** → add it to `_IRRELEVANT_CONTENT_DOMAINS` in
     `src/denbust/discovery/candidate_filters.py` (a PR — this also blocks
     future discovery) **and** retroactively suppress existing rows:
     ```bash
     denbust candidates-b2-suppress --config <cfg> --domains spam1.co.il,spam2.com \
             --note "B2: escort-listing spam"
     ```
   - **One-off junk on an otherwise-legitimate domain** →
     ```bash
     denbust candidates-b2-suppress --config <cfg> --ids cand_a,cand_b \
             --note "B2: off-topic"
     ```
4. **Re-plan** the batch. With the junk suppressed, the planner tops up to the
   full batch size with clean candidates.
5. **Run** the scrape on the clean batch.
6. **Record** what Stage B2 removed in the batch report.

### Decision heuristics

Remove when:
- domain is an escort/massage/companionship listing or directory,
- the page is an ad, classified, or "girls near you" aggregator,
- the domain is generic SEO spam unrelated to Israeli enforcement news,
- the title is obviously a service listing, not journalism.

Keep when in doubt about a *real news outlet* — Claude classification is the
final guard on borderline journalism. Stage B2 only removes the **clearly**
junk; it is a precision tool against spam, not a second relevance judge.

## Balanced batch selection

`--balanced-batch N` selects N candidates from the full Stage-B-passing pool:

- **Frequency-weighted across months** — months with more passers get
  proportionally more slots (largest-remainder apportionment), so the busy,
  under-covered months are prioritised without starving quiet ones.
- **Source-balanced within each month** — round-robin across publication
  sources/source-families (by scrape priority) so no single site monopolises a
  month and load is spread to reduce per-host block risk.

Implementation: `src/denbust/discovery/balanced_selection.py`.

## Domain-frequency gate

Open-web discovery (broad + taxonomy queries) on prostitution/escort/massage
keywords inherently drags in a long tail of one-off spam domains — escort
listings and massage ads are SEO-optimised for exactly those terms. The tail is
unbounded: ~68% of all domains in the store are single-candidate, and a fresh
batch of new ones appears every run, so a denylist can never keep up.

The **domain-frequency gate** flips the default from "scrape unless blocklisted"
to "earn your way in by recurring":

```bash
denbust run --job scrape_candidates --balanced-batch 60 --min-domain-frequency 2
```

A candidate is held out of the batch unless its domain is a curated known outlet
(always exempt) **or** its domain has been seen at least N times across the
store. Real outlets recur and pass; one-off spam appears once and is held back
(never deleted — it becomes eligible automatically if the domain ever recurs).

This is the primary tool for the single-shot tail; the domain blocklist and
Stage B2 still handle recurring spam that clears the gate. Implementation:
`domain_frequencies` + `filter_by_domain_frequency` in `balanced_selection.py`.

## Automated per-domain LLM verdict gate

The frequency gate kills the single-shot tail, but **recurring** off-topic
domains (real-estate/finance/escort sites seen 2+ times) still need judgment —
previously a manual blocklist PR per batch. The verdict gate automates that:

```bash
# 1. Classify not-yet-judged domains once; cache the verdicts (and optionally
#    suppress candidates on block-verdict domains immediately):
denbust classify-domains --config <cfg> --suppress

# 2. Apply the cached verdicts as a gate at scrape-selection time:
denbust run --job scrape_candidates --balanced-batch 60 \
            --min-domain-frequency 2 --use-domain-verdicts
```

Each new domain is sent to the LLM **once** with a few sample titles and judged
`allow` (a plausible Israeli enforcement-news source — including niche/Russian/
English Israeli outlets) or `block` (escort/SEO/foreign/off-topic). The verdict
is cached durably in `domain_verdicts.jsonl`, so the cost is one cheap call per
*new* domain, never per batch. Known outlets are exempt; the static
`_IRRELEVANT_CONTENT_DOMAINS` blocklist is honoured as an always-block set.

This is the scalable successor to manual Stage B2 blocklist rounds: instead of a
human enumerating bad domains forever, the model judges each domain once and the
decision compounds. Implementation: `src/denbust/discovery/domain_verdicts.py`.

## Outputs of each batch

Every batch run should report:
- month × source allocation of the planned batch,
- Stage B2 removals (ids/domains and why),
- scrape outcomes (succeeded / failed / partial),
- Claude classification outcomes (relevant / not),
- any spam domains newly added to the blocklist.
