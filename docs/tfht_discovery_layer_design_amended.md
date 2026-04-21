# Design: Persistent Multi-Engine Discovery & Candidacy Layer for `tfht_enforce_idx`

## Goal

Add a **persistent discovery/candidacy layer** to `tfht_enforce_idx` that can support **N discovery engines**, while initially implementing:

- **Brave Search API**
- **Exa**
- **Google Custom Search JSON API**

At the same time, preserve the existing **source-specific candidacy** flow, so that final scraping candidates come from **both**:

1. **search-engine discovery**
2. **source-native candidacy** (RSS, source adapters, source-specific listing/search pages)

The key amendment in this version of the design is:

> **Candidacy becomes a durable, first-class layer of its own.**

That means all candidates are written to:
- a dedicated **operational database table** (Supabase)
- a dedicated **state/export file layer** (state repo)

before final scraping succeeds.

This allows:
- re-attempting failed scraping later
- upgrading old candidates with better scraping code
- systematic backfill
- AI-based self-healing and retry
- discovery of new sources from search results
- future event-level unification across multiple news items and references

---

## Why this amendment is needed

The first version of the discovery-layer design already separated:
- discovery
- candidate union
- final fetch/scrape

But for this project, that is not quite enough.

Because the next major features are likely to be:

1. **Backfill** over historical time windows
2. **Automatic AI-based self-healing** of scraping paths and parameters
3. **Automatic AI-based addition of new sources**
4. **Social-network discovery via search-engine queries**
5. **News-items-to-events unification**

the system should not treat candidates as transient in-memory objects that are discarded after one scrape attempt.

Instead, candidates should become a durable queue/history layer that the system can revisit repeatedly.

---

## Core design decision

### Candidacy is a separate durable layer

The architecture should now explicitly separate:

1. **Discovery**
2. **Candidate persistence**
3. **Scrape attempts**
4. **Operational news item creation**
5. **Event inference later**

This means:

- discovery writes candidates first
- candidates persist independently of scrape success
- scraping updates candidate status over time
- successful scraping can produce `news_items`
- failed or partial candidates remain retryable
- later systems can attach:
  - self-healing strategies
  - backfill queues
  - source-suggestion logic
  - event-level clustering/inference

---

## Revised high-level architecture

```text
                      +----------------------+
                      | Query Builder        |
                      |  - keywords          |
                      |  - time windows      |
                      |  - Hebrew/English    |
                      |  - source targeting  |
                      +----------+-----------+
                                 |
                +----------------+----------------+
                |                                 |
                v                                 v
     +----------------------+         +----------------------+
     | Search Discovery     |         | Source-native        |
     | Engines              |         | Candidacy            |
     |  - Brave             |         |  - RSS               |
     |  - Exa               |         |  - site listing      |
     |  - Google CSE        |         |  - source search     |
     +----------+-----------+         +----------+-----------+
                |                                 |
                +----------------+----------------+
                                 |
                                 v
                    +--------------------------+
                    | Candidate Normalization  |
                    |  - URL normalization     |
                    |  - title/snippet/date    |
                    |  - provenance            |
                    +------------+-------------+
                                 |
                                 v
                    +--------------------------+
                    | Candidate Persistence    |
                    |  - Supabase table        |
                    |  - state repo file       |
                    |  - retry state           |
                    |  - provenance history    |
                    +------------+-------------+
                                 |
                                 v
                    +--------------------------+
                    | Candidate Union /        |
                    | Merge / Queueing         |
                    |  - canonical_url         |
                    |  - multi-engine overlap  |
                    +------------+-------------+
                                 |
                                 v
                    +--------------------------+
                    | Scrape Attempt Layer     |
                    |  - source adapter        |
                    |  - generic fallback      |
                    |  - partial fetch state   |
                    |  - retry scheduling      |
                    +------------+-------------+
                                 |
                 +---------------+----------------+
                 |                                |
                 v                                v
   +-----------------------------+   +-----------------------------+
   | Full article-derived row    |   | Candidate retained         |
   |  - normal operational row   |   |  - fetch_failed            |
   |  - normal classifier path   |   |  - partial                 |
   |  - ready for release flow   |   |  - retryable later         |
   +-----------------------------+   +-----------------------------+
                                 |
                                 v
                    +--------------------------+
                    | Future event inference   |
                    |  - many news items       |
                    |  - social refs           |
                    |  - official refs         |
                    +--------------------------+
```

---

## Design principles

1. **Discovery is not scraping**
2. **Candidacy is not scraping**
3. **Candidacy is durable**
4. **Search-engine candidates and source-native candidates are both first-class**
5. **Canonical URL unification happens before final scrape attempts**
6. **Failed scraping should not destroy candidate value**
7. **Retryability is a core feature, not an afterthought**
8. **The design must support incremental backfill**
9. **The design must support future self-healing**
10. **The design must support future source expansion**
11. **The design must support future event unification**

---

## Core conceptual split

### A. Discovery engines
Return candidate URLs and search metadata.

### B. Source-native candidacy
Generate candidates directly from known sources.

### C. Candidate persistence layer
Store all candidates durably, whether or not scraping succeeds.

### D. Scrape attempt layer
Attempts to fetch and normalize the candidate into a real article/news-item record.

### E. Candidate retry layer
Allows later re-attempts after:
- code fixes
- self-healing
- new source adapters
- better generic extraction
- budgeted backfill

### F. News-item layer
The existing operational `news_items` dataset.

### G. Event layer (future)
Unifies multiple `news_items` and other references into actual events.

---

## Discovery engine abstraction

### Interface

Introduce a discovery-engine interface such as:

```python
class DiscoveryEngine(Protocol):
    name: str

    async def discover(
        self,
        queries: list["DiscoveryQuery"],
        context: "DiscoveryContext",
    ) -> list["DiscoveredCandidate"]:
        ...
```

### Requirements
Each engine implementation should:
- receive one or more search queries
- apply engine-specific filters if supported
- return normalized candidate objects
- expose engine-specific diagnostics and cost metadata

### Context objects
- `DiscoveryContext`: shared execution context for a discovery run, such as the `run_id`, engine-level config, rate-limit settings, and any shared telemetry hooks.
- `SourceDiscoveryContext`: the equivalent context object for source-native producers, carrying the `run_id`, source-specific config, date windows, and any producer diagnostics hooks.

---

## Source-native candidacy abstraction

Treat source-native candidacy as another candidate producer, not a separate “special case.”

```python
class SourceCandidateProducer(Protocol):
    name: str

    async def discover_candidates(
        self,
        context: "SourceDiscoveryContext",
    ) -> list["DiscoveredCandidate"]:
        ...
```

This lets the system treat:
- Brave
- Exa
- Google CSE
- RSS
- source archive listings
- source search endpoints

as different producers of the same candidate object type.

---

## Core models

### 1. Discovery query model

```python
from pydantic import Field

class DiscoveryQuery(BaseModel):
    query_text: str
    language: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    preferred_domains: list[str] = Field(default_factory=list)
    excluded_domains: list[str] = Field(default_factory=list)
    source_hint: str | None = None
    query_kind: Literal["broad", "source_targeted", "taxonomy_targeted", "social_targeted"]
    tags: list[str] = Field(default_factory=list)
```

### Notes
- `broad`: cross-web query
- `source_targeted`: targeted toward known news domains
- `taxonomy_targeted`: targeted at specific TFHT typology buckets
- `social_targeted`: e.g. `site:www.facebook.com "בית בושת"`

---

### 2. Discovered candidate model

```python
class DiscoveredCandidate(BaseModel):
    discovery_id: str
    producer_name: str
    producer_kind: Literal["search_engine", "source_native", "social_search"]
    query_text: str | None
    candidate_url: str
    canonical_url: str | None
    title: str | None
    snippet: str | None
    discovered_at: datetime
    publication_datetime_hint: datetime | None
    domain: str | None
    rank: int | None
    producer_confidence: float | None
    source_hint: str | None
    metadata: dict[str, Any] = Field(default_factory=dict)
```

This is still not a final `news_items` row.

---

### 3. Persistent candidate model

This is the new key model.

```python
class PersistentCandidate(BaseModel):
    candidate_id: str
    canonical_url: str | None
    current_url: str
    domain: str | None

    titles: list[str]
    snippets: list[str]
    discovered_via: list[str]
    discovery_queries: list[str]
    source_hints: list[str]

    first_seen_at: datetime
    last_seen_at: datetime

    candidate_status: Literal[
        "new",
        "queued",
        "scrape_pending",
        "scrape_in_progress",
        "scrape_succeeded",
        "scrape_failed",
        "partially_scraped",
        "unsupported_source",
        "suppressed",
        "closed",
    ]

    scrape_attempt_count: int
    last_scrape_attempt_at: datetime | None
    next_scrape_attempt_at: datetime | None

    last_scrape_error_code: str | None
    last_scrape_error_message: str | None

    content_basis: Literal[
        "candidate_only",
        "search_result_only",
        "partial_page",
        "full_article_page",
    ]

    retry_priority: int
    needs_review: bool
    backfill_batch_id: str | None
    self_heal_eligible: bool
    source_discovery_only: bool
    metadata: dict[str, Any] = Field(default_factory=dict)
```

### Why this model matters
It makes candidacy durable and retryable.

---

### 4. Candidate provenance model

Candidates should preserve full provenance.

```python
class CandidateProvenance(BaseModel):
    provenance_id: str
    run_id: str
    candidate_id: str
    producer_name: str
    producer_kind: str
    query_text: str | None
    raw_url: str
    normalized_url: str | None
    title: str | None
    snippet: str | None
    publication_datetime_hint: datetime | None
    rank: int | None
    domain: str | None
    discovered_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
```

This is important for:
- engine overlap analysis
- source-native vs search-engine recall
- source-addition suggestions later
- event/source attribution later

---

### 5. Scrape attempt model

```python
class ScrapeAttempt(BaseModel):
    attempt_id: str
    candidate_id: str
    started_at: datetime
    finished_at: datetime | None

    attempt_kind: Literal[
        "source_adapter",
        "generic_fetch",
        "generic_extract",
        "self_heal_retry",
        "manual_retry",
        "backfill_retry",
    ]

    fetch_status: Literal[
        "success",
        "partial",
        "failed",
        "unsupported",
        "blocked",
        "timeout",
    ]

    source_adapter_name: str | None
    extracted_title: str | None
    extracted_publication_datetime: datetime | None
    extracted_body_hash: str | None

    error_code: str | None
    error_message: str | None
    diagnostics: dict[str, Any] = Field(default_factory=dict)
```

This allows:
- repeated attempts
- auditability
- self-healing feedback loops
- prioritization of future retries

---

## Initial discovery engines

### 1. Brave Search API

### Role
- broad candidate discovery
- source-targeted queries
- fast general engine

### Why keep it
Even if it misses some Israeli sources, it still contributes overlap/diversity and may surface items source-native candidacy misses.

---

### 2. Exa

### Role
- semantic complement
- example-expansion
- “find similar pages” workflows
- future use in source discovery and event-surface discovery

### Why it matters for the amended design
Exa becomes more useful once candidates are persistent, because:
- similar-page discovery can run against previously validated or manually reviewed examples
- newly discovered candidates can remain in queue even if scraping fails today

---

### 3. Google Custom Search JSON API

### Why choose Google here
Given the Hebrew-query observation about Brave missing major sources, Google is the best initial third engine because it likely improves mainstream-source recall.

### Role
- recall-oriented discovery
- source-targeted site queries
- benchmark engine for “did we miss obvious results?”

---

## Extensibility to N engines

Later engines can include:
- Bing Web Search API
- Tavily
- legal-site search providers
- social-targeted engine wrappers
- internal source-suggestion engines

All engine-specific quirks should live in:
- engine adapters
- engine config blocks
- engine normalization helpers

not in the central candidacy store or scrape logic.

---

## Preserve source-native candidacy

This is still a hard requirement.

Source-native candidacy should continue producing candidates from:
- RSS
- archive/list pages
- source-specific search endpoints
- source-domain-specific listing logic
- source adapters with bespoke candidate extraction

### Important amendment
Source-native discovery should now also write into the same durable candidate layer.

That means source-native candidacy is no longer “just an input to the immediate scrape loop”; it is a first-class producer of persistent candidates.

---

## Candidate generation strategy

### A. Broad cross-web queries
Examples:
- `"בית בושת"`
- `"סחר בבני אדם" זנות`
- `"החזקת מקום לשם זנות"`
- `"צו סגירה" בית בושת`
- `"פשיטה" בית בושת`
- `"סרסור" זנות`

Run across:
- Brave
- Exa
- Google

### B. Source-targeted search-engine queries
Examples:
- `site:ynet.co.il "בית בושת"`
- `site:mako.co.il "סחר בבני אדם"`
- `site:maariv.co.il "צו סגירה" זנות`

These compensate for weak source-native site search.

### C. Source-native candidacy
Current source modules continue producing candidates via:
- RSS
- archive/list pages
- source-specific extraction

### D. Example-driven semantic discovery
Use Exa against:
- validated example URLs
- reviewed manual examples
- typology seed examples

### E. Social-targeted discovery (future)
Examples:
- `site:www.facebook.com "בית בושת"`
- `site:www.instagram.com "סחר בבני אדם"`
- `site:t.me "בית בושת"`

These should initially be candidate-only or reference-only items, not full article rows.

---

## Candidate normalization

Before persistence/union, normalize every candidate into a common structure.

Normalize:
- raw URL
- canonical URL if possible
- domain
- title
- snippet
- publication date hint
- provenance
- producer metadata

### Important
Canonicalization must happen before persistence dedup/merge, because candidates should map onto the same durable record where possible.

---

## Candidate persistence layer

This is the main amendment.

### Required behavior
All candidates, from both search engines and source-native discovery, are written first into durable storage.

### Storage targets
- **Supabase:** primary operational candidate tables
- **State repo:** candidate snapshots / manifests / run snapshots / optional lightweight queue files

### Why both
- Supabase is the live operational store
- state repo provides Git-traceable snapshots and light operational memory
- backfill/recovery/debugging benefit from both

---

### Recommended storage model

#### Supabase tables

### 1. `discovery_runs`
One row per discovery execution.

Fields:
- `run_id`
- `started_at`
- `finished_at`
- `dataset_name`
- `job_name`
- `status`
- `query_count`
- `candidate_count`
- `merged_candidate_count`
- `queued_for_scrape_count`
- `errors_json`

### 2. `candidate_provenance`
Append-only raw discovery events.

Fields:
- `provenance_id`
- `run_id`
- `candidate_id`
- `producer_name`
- `producer_kind`
- `query_text`
- `raw_url`
- `normalized_url`
- `title`
- `snippet`
- `publication_datetime_hint`
- `rank`
- `domain`
- `metadata_json`

### 3. `persistent_candidates`
Primary durable candidate queue.

Fields:
- `candidate_id`
- `canonical_url`
- `current_url`
- `domain`
- `source_discovery_only`
- `first_seen_at`
- `last_seen_at`
- `candidate_status`
- `scrape_attempt_count`
- `last_scrape_attempt_at`
- `next_scrape_attempt_at`
- `last_scrape_error_code`
- `last_scrape_error_message`
- `content_basis`
- `retry_priority`
- `needs_review`
- `backfill_batch_id`
- `self_heal_eligible`
- `metadata_json` — stores structured or repeated context that is modeled earlier on `PersistentCandidate` but is not broken out as first-class columns here, including `titles`, `snippets`, `discovered_via`, `discovery_queries`, and `source_hints`

### 4. `scrape_attempts`
One row per scrape attempt.

Fields:
- `attempt_id`
- `candidate_id`
- `started_at`
- `finished_at`
- `attempt_kind`
- `fetch_status`
- `source_adapter_name`
- `extracted_title`
- `extracted_publication_datetime`
- `extracted_body_hash`
- `error_code`
- `error_message`
- `diagnostics_json`

### 5. existing `news_items`
Successful scraping/normalization creates or updates downstream `news_items`.

---

## State repo files

In addition to Supabase, write candidate-layer files such as:

```text
state_repo/
  news_items/
    discover/
      runs/
        2026-04-10T12-00-01-000000Z.json
      candidates/
        latest_candidates.jsonl
        retry_queue.jsonl
        backfill_queue.jsonl
      metrics/
        engine_overlap_latest.json
```

### Role of these files
- operational debugging
- quick recovery
- Git-visible queue state
- lightweight offline analysis
- easier contributor visibility

### Important
The state repo should not be the only candidate store. Supabase remains the main operational truth.

---

## Candidate union / merge

Merge all candidate streams by canonical URL where possible.

### Required behavior
For each durable candidate, retain:
- all discovery engines that found it
- all source-native producers that found it
- all query texts
- best available title/snippet/date hints
- source-adapter hints
- candidate history

### Important amendment
Union/merge should update the persistent candidate row, not just build a transient merged object.

---

## Scrape attempt layer

After candidate persistence, the system should select candidates for scraping.

### Fetch priority
1. known source adapter if domain supported
2. generic HTML fetch/extract
3. partial capture if possible
4. retain as candidate-only if no fetch path works

### Key amendment
Scrape attempts are no longer one-shot.
They update:
- candidate status
- attempt count
- last error
- next retry time
- self-heal eligibility

---

## Candidate retry semantics

This is central to the amended design.

## A candidate can remain useful even if scraping fails
Examples:
- source blocked temporarily
- source adapter broken
- generic fetch failed
- extractors improved later
- source is later formally supported
- self-healing modifies parameters or extraction path

### Required statuses
At minimum:
- `scrape_failed`
- `partially_scraped`
- `unsupported_source`
- `queued`
- `scrape_pending`
- `scrape_succeeded`

### Required retry controls
- retry count
- retry priority
- next retry timestamp
- last error class
- self-heal eligibility
- backfill-batch association

---

## Search-result-only fallback rows

If final page fetch fails, the candidate may still yield a minimal metadata-only row.

### This should be explicit
Fields such as:
- `content_basis = "search_result_only"`
- `candidate_status = "scrape_failed"`
- `record_confidence = "low"`
- `needs_review = true`

### Recommended use
Useful for:
- daily monitoring
- candidate review queues
- recall analysis
- backfill staging
- event-reference support later

### Public behavior
Do not treat them as equivalent to fully fetched rows by default.

---

## Backfill support

This design should explicitly support historical backfill.

### Backfill should operate at the candidate layer
That means backfill jobs:
- generate discovery queries for historical date windows
- write candidates into persistent storage
- assign them to backfill batches
- schedule scrape attempts gradually over time

### Why this is the right layer
If backfill discovery finds 10,000 candidates, you do not want to block on scraping them immediately.
You want:
- a queue
- prioritization
- incremental daily processing
- resumability

## Recommended model additions
- `backfill_batches`
- `backfill_batch_id` on candidates
- queue priority rules:
  - newest first for daily freshness
  - oldest first for historical gap-closure jobs
  - source-specific priority overrides

### Implementation note
The current implementation persists `backfill_batches` into Supabase and mirrors them into the
state repo, with `news_items / backfill_discover` reading
`DENBUST_BACKFILL_DATE_FROM` / `DENBUST_BACKFILL_DATE_TO` to define one requested batch window per
invocation. Historical source-native discovery is capability-based and records warnings for sources
that do not implement explicit window fetching.

---

## Self-healing support

This design should explicitly support AI-based self-healing later.

### Why candidate persistence matters here
Self-healing needs a backlog of failures to learn from and re-attempt.

### Future self-healing loop can operate on:
- repeated scrape failures for same source
- repeated extraction errors
- repeated blocks/timeouts
- candidates that are valuable but currently unsupported

### Candidate-layer fields that support this
- `last_scrape_error_code`
- `last_scrape_error_message`
- `scrape_attempt_count`
- `self_heal_eligible`
- provenance/source adapter metadata

### Future self-heal job
Could:
- inspect recent failed candidates
- propose new CSS selectors/parsing params
- suggest source-adapter changes
- create a revised scrape attempt
- update candidate status and attempt logs

This requires the durable candidate queue/history that this amended design introduces.

---

## Automatic source addition support

This design should also support future source expansion.

### Mechanism
Candidates discovered via search may repeatedly come from domains that are:
- not in your current source list
- relevant
- scrapeable enough
- worth adding as first-class source adapters or source-native producers

### Add a future “source suggestion” process
Based on candidate provenance:
- count relevant candidates per unseen domain
- measure repeated occurrence over time
- measure scrape success/failure rate
- suggest “consider adding source X”

### Important
This is much easier when candidate provenance is durable and query-linked.

---

## Social-network discovery support

This design should support discovery from Facebook and similar sites via search-engine queries.

### Recommended initial treatment
Social results should enter the same durable candidate layer, but likely with:
- `producer_kind = "social_search"`
- `content_basis = "candidate_only"` or `search_result_only`
- limited/no full follow-up scraping
- lower confidence by default
- event-reference eligibility later

### Why keep them
They may be useful as:
- corroborating references
- signals of an event
- inputs into future event inference
- source-suggestion signals

### Why not force full scraping
Social pages often:
- block fetches
- require login
- have unstable HTML
- are less appropriate for current public row generation

So they should initially be retained mainly as candidates/references.

---

## Future event unification support

The durable candidate layer also helps the later `news_items -> events` layer.

### Why
An event may be referenced by:
- one or more scraped news items
- candidate-only rows from social results
- later official documents
- later manually added references

If candidates are discarded after failed scraping, that evidence disappears.

### Recommendation
Future event inference should be able to consume:
- successful `news_items`
- candidate-only references
- manually reviewed candidate references
- other document/reference layers later

So the persistent candidate layer is a useful precursor to the event table.

---

## Config design

### Discovery config block

```yaml
discovery:
  enabled: true
  persist_candidates: true
  mode: merged

  engines:
    brave:
      enabled: true
      api_key_env: DENBUST_BRAVE_API_KEY
      max_results_per_query: 20

    exa:
      enabled: true
      api_key_env: DENBUST_EXA_API_KEY
      max_results_per_query: 20
      allow_find_similar: true

    google_cse:
      enabled: true
      api_key_env: DENBUST_GOOGLE_CSE_API_KEY
      cse_id_env: DENBUST_GOOGLE_CSE_ID
      max_results_per_query: 10
```

### Source-native config block

```yaml
source_discovery:
  enabled: true
  persist_candidates: true
  sources:
    ynet:
      enabled: true
    mako:
      enabled: true
    maariv:
      enabled: true
```

### Candidate persistence config

```yaml
candidates:
  supabase_table: persistent_candidates
  provenance_table: candidate_provenance
  scrape_attempts_table: scrape_attempts

  keep_search_only_fallbacks: true
  require_review_for_search_only: true
  allow_retry_on_fetch_failure: true
  default_retry_backoff_hours: 24
  max_retry_attempts: 10
```

### Backfill config

```yaml
backfill:
  enabled: false
  batch_window_days: 7
  max_candidates_per_run: 500
  max_scrape_attempts_per_run: 100
```

---

## Job model

### New recommended jobs

```text
news_items / discover
news_items / scrape_candidates
news_items / ingest
news_items / backfill_discover
news_items / backfill_scrape
```

### Meaning

#### `news_items / discover`
Runs discovery engines and source-native candidacy, persists candidates.

#### `news_items / scrape_candidates`
Consumes queued candidates and attempts scraping/extraction.

#### `news_items / ingest`
Optionally combines discover + scrape + news-item creation in one convenience job.

#### `news_items / backfill_discover`
Discovers candidates over historical windows and writes them to durable storage.

#### `news_items / backfill_scrape`
Slowly drains historical candidate queues into scraped rows.

### Recommendation
Start operationally with:
- a combined path for freshness
- but explicit candidate persistence underneath

This gives simplicity now and room for queue-oriented expansion later.

---

## Validation and observability

This layer should now be measured at three levels:

### 1. Discovery quality
- candidates returned by engine
- overlap between engines
- overlap with source-native candidacy
- known-example recall

### 2. Candidate durability / queue health
- new candidates
- queued candidates
- stale candidates
- scrape-failed candidates
- retry backlog
- unsupported-source candidates

### 3. Candidate-to-news-item conversion
- scrape success rate
- partial scrape rate
- search-result-only fallback rate
- per-engine conversion rate
- per-source conversion rate

### Key reports
- engine overlap matrix
- source-native vs search recall
- queue aging report
- top scrape failures by source/domain
- top candidate-only domains
- source-suggestion report
- candidate-to-event-reference report later

---

## Recommended evaluation methodology

For a curated set of known relevant URLs:
- check if source-native candidacy found them
- check if Brave found them
- check if Exa found them
- check if Google found them
- check if merged candidacy created a durable candidate
- check if scrape succeeded
- if scrape failed, check if candidate remained retryable

That last step is the key amendment.

---

## Practical repository structure

A good repo shape would now be:

```text
src/denbust/
  discovery/
    base.py
    models.py
    query_builder.py
    merge.py
    runners.py
    persistence.py
    queue.py
    retry.py
    engines/
      brave.py
      exa.py
      google_cse.py

  sources/
    ...
```

And later potentially:

```text
src/denbust/
  self_heal/
  source_suggestions/
  events/
```

The important separation is:
- `discovery/` = find and persist candidates
- `sources/` = scrape known source pages well
- future `events/` = infer real events from rows and references

---

## Recommended implementation milestones

The detailed rollout plan uses discovery-layer planning identifiers in the form `DL-PR-XX`
to distinguish them from GitHub PR numbers. For example: `DL-PR-01`, `DL-PR-02`, `DL-PR-03`.

### Milestone 1 — persistent candidacy foundation
- add persistent candidate models
- add candidate provenance and scrape-attempt models
- add Supabase candidate tables
- add state-repo candidate files
- write source-native candidates into the durable layer too

### Milestone 2 — Brave + source-native merged discovery
- discovery engine abstraction
- Brave adapter
- candidate union/update into durable store
- scrape-attempt queue
- final source-aware scraping from durable candidates

### Milestone 3 — Exa + Google CSE
- add Exa (implemented)
- add Google CSE (implemented)
- add overlap metrics
- add queue health metrics

### Milestone 4 — retryability and fallback
- explicit retry scheduling
- search-result-only fallback rows
- partial-scrape retention
- scrape-attempt history

### Milestone 5 — backfill support
- historical query generation
- backfill batch model
- slow-drain backfill queue
- date-window scheduling

### Milestone 6 — future-facing hooks
- self-heal eligibility flags
- source suggestion reports
- social-targeted candidate support
- event-reference compatibility

---

## Bottom line

The discovery layer should now be understood as:

> **multi-engine discovery feeding a durable candidacy layer, which then feeds scraping, retry, backfill, and later event inference.**

The right amended design is:

- support **N discovery engines**
- start with **Brave + Exa + Google CSE**
- **retain source-native candidacy**
- write **all candidates** into:
  - a dedicated **Supabase candidate layer**
  - dedicated **state-repo candidate files**
- treat failed or partial scraping as **retryable candidate state**, not as terminal loss
- support later:
  - historical backfill
  - AI-based self-healing
  - AI-based source addition
  - social-network candidate discovery
  - event-level unification

That makes candidacy a proper operational substrate for the next several phases of the project.
