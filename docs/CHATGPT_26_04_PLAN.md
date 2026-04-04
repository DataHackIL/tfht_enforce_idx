# Recommended Direction for Evolving the `news_items` Pipeline

The right move is to evolve the current `news_items` pipeline into a typology-driven, validation-aware system, not to bolt on a parallel workflow.

That is because the repo already has the main primitives you need: a real `news_items` ingest/release/backup flow, metadata-only public outputs, and a validation subsystem with a permanent validation set and classifier-variant evaluation. The current project also already treats public output as metadata-only and excludes full text, which is aligned with the TFHT direction from the meeting and follow-up materials.

## Recommended direction

Build around five connected layers:

- A canonical TFHT taxonomy
- A website/report-facing data contract
- A human feedback and override loop
- A proper labeled-validation pipeline
- Reporting outputs aligned to the monthly report + minisite

## 1) Canonical TFHT taxonomy as a first-class asset

The biggest gap between the current project and the new feedback is that the system still appears to be driven mainly by the repo’s internal category scheme, while the client now has a clearer typology and a narrower “relevant to the index” subset. The product materials also already frame the world in legal/enforcement buckets such as brothels, pimping, trafficking, and prostitution-consumption enforcement.

From the attached typology spreadsheet I inspected directly, the client’s taxonomy is wider and more structured than the current production framing. It includes broad columns like:

- סחר בבני אדם
- סרסור וזנות
- בתי בושת

with multiple subcategories under each, including items that are relevant to the TFHT project and a narrower subset relevant specifically to the enforcement index.

### What to do

Create a versioned taxonomy package in the repo, for example:

```text
taxonomy/
  tfht_typology_v1.yaml
  tfht_typology_v1_examples.csv
  tfht_typology_v1_mapping.yaml
```

That taxonomy should contain:

- `domain_category`
- `domain_subcategory`
- `index_relevant` boolean
- optional `index_bucket`
- optional `legal_reference`
- example URLs / headlines
- alias terms / normalization hints

### Important design choice

Do not use the LLM to invent category names freely anymore.

Instead:

- classify into a closed label set
- keep `unknown / needs_review` as an allowed outcome
- separate:
       - broad domain relevance
       - taxonomy label
       - whether the item is relevant for the public `מדד האכיפה`

That makes the system much easier to validate and update when TFHT sends revised category tables.

## 2) Add a website/report-facing schema, not just a dataset schema

The current `news_items` dataset is public-metadata oriented, which is good. But the client now clearly wants the data to feed:

- a central table that a site can read from
- a monthly report
- later possibly controlled/manual additions and official data

The monthly report template specifically expects:

- monthly headline numbers
- detailed case blurbs with links
- a separate `פעילות המטה` section
- disclosure/project copy

From the attached manual tracking workbook I inspected directly, the current manual workflow also uses fields like:

- date
- address / city
- event
- relevant details
- status
- source info

and organizes entries in month-oriented sheets.

### What to do

Add a second normalized view on top of `news_items`:

- Raw operational/news item record: current style, source-oriented
- Index record / website row: TFHT-facing, presentation-oriented

For example:

```text
news_items_raw
  id
  canonical_url
  title
  source_name
  publication_datetime
  summary_one_sentence
  taxonomy_category
  taxonomy_subcategory
  index_relevant
  ...

index_rows
  id
  month_key
  event_date
  city
  address_text
  event_label
  category
  subcategory
  public_blurb
  status
  source_urls
  source_count
  confidence
  manually_reviewed
  manually_overridden
```

This avoids forcing the minisite/report layer to reverse-engineer presentation rows from source-level metadata every time.

## 3) Add a real feedback + overrides loop

The meeting transcript is explicit that TFHT wants:

- a way to send feedback on tagging
- a way to note things the report missed
- later a manually editable table or secure page / Google Sheet
- eventual ability to add non-news data manually

That is also consistent with the product definition’s staged data-collection vision.

### What to do

Add a manual annotations pipeline with three input types:

### A. Item-level correction table

For existing detected items:

- canonical URL / item ID
- corrected relevance
- corrected category
- corrected subcategory
- corrected summary
- notes
- reviewer
- reviewed_at

### B. Missing-item table

For things TFHT saw and the system missed:

- URL
- source
- title
- date
- expected category/subcategory
- why it matters
- reviewer notes

### C. Monthly-report feedback table

For report-level feedback:

- month
- missing cases
- bad grouping/counts
- phrasing issues
- suggested inclusion/exclusion
- notes

The simplest first implementation is:

- CSV import/export
- optional Google Sheet sync later

### Key principle

Manual overrides should be persistent and upstream of release/report generation.

That means:

- if TFHT corrects a label once, that should not be overwritten next day by the model
- manual labels should be able to feed both production outputs and validation data

## 4) Extend the existing validation subsystem instead of replacing it

This is the most important implementation point.

The repo already has:

- a permanent validation-set merge flow
- a validation CSV normalizer
- classifier-variant evaluation against labeled examples

So the correct plan is to adapt that subsystem to TFHT’s new labeled examples and future annotation tables.

### What to change

### A. Expand the validation row schema

Right now the validation rows are geared toward:

- source
- url
- title/snippet
- relevant
- category
- sub_category
- review status

Extend that schema to include:

- index_relevant
- taxonomy_version
- expected_month_bucket optional
- expected_city optional
- expected_status optional
- annotation_source (`manual table` / `typology example` / `Google alerts comparison` / `monthly report QA`)

### B. Support “occasional new manually generated examples table”

Add a dedicated import flow such as:

```text
denbust validation import-reviewed-table path/to/examples.xlsx
denbust validation finalize-draft path/to/reviewed_draft.csv
denbust validation evaluate --matrix agents/validation/classifier_variants.yaml
```

The important part is not the exact command names, but the behavior:

- ingest a manually created sheet/CSV/XLSX
- normalize columns
- validate labels against the closed taxonomy
- deduplicate by canonical URL
- merge into the permanent validation set
- preserve provenance

### C. Split evaluation into stages

Do not evaluate only “relevant vs not relevant”.

Evaluate:

- source discovery / recall against known URLs
- relevance classification
- category classification
- subcategory classification
- index relevance decision

That matters because the new feedback is not just “is this article relevant?” but “does it belong in our index, under our typology, in the way we need it?”

### Metrics to track

For each evaluated batch:

- recall on known example URLs
- relevance precision/recall/F1
- category accuracy
- subcategory accuracy
- index-relevance precision/recall/F1
- confusion matrix by top categories
- unknown / needs_review rate

## 5) Introduce a typology-aware classifier pipeline

The current classifier still looks like a broad relevance + category + subcategory prompt. That was right for earlier phases, but now you have enough labeled structure to make it stricter.

### Recommended new inference structure

### Step 1: broad candidate relevance

Binary or ternary:

- relevant
- not_relevant
- uncertain

### Step 2: closed-set taxonomy assignment

Choose only from the TFHT taxonomy.

### Step 3: index relevance

Separate decision:

- belongs in broad prostitution/trafficking world
- belongs specifically in the enforcement index output

### Step 4: optional extraction

Only after classification:

- city / address
- event type
- status hints
- counts
- actor/entity names

This will give you much better control than one large all-in-one prompt.

## 6) Add a “known examples / coverage” test suite

The meeting explicitly mentioned checking against Google Alerts and identifying misses. The repo already has `docs/articles_examples.md` and source-coverage notes.

### What to do

Add two validation collections:

### A. Permanent labeled set

Curated, reviewed, versioned

### B. Coverage watchlist

Known URLs that should be detected by source discovery, even if not all are part of the formal classifier evaluation set

This lets you test:

- scraper/source coverage
- dedup grouping
- classifier correctness

separately.

### Recommended CI / scheduled checks

- CI on fixtures and reviewed validation rows
- scheduled or on-demand “live coverage check” against known URLs/examples
- report deltas when a previously detectable source/example becomes undetectable

## 7) Add a monthly report generator aligned to the template

The current system already has ingest/release/backup and a daily review path. The new client request adds a much more concrete monthly output target.

The attached monthly report template expects:

- headline monthly stats
- 3–6 concrete case blurbs with links
- TFHT activity section
- project/disclosure text

### What to do

Create a separate monthly-report job, not just another email mode.

For example:

`news_items / monthly_report`

Inputs:

- released/public index rows for a month
- manual TFHT activity inputs
- report template config

Outputs:

- Markdown / JSON report payload
- optional DOCX/HTML later
- minisite-compatible metrics payload

### Practical first version

Generate:

- counts by index bucket
- top notable cases
- short factual blurbs
- source links
- placeholder section for manual TFHT activity if not yet integrated

## 8) Design the minisite/export contract now

The attached mockup and email make it clear the end product is not “a dataset exists,” but “a site and monthly public-facing communication can be generated from a central table.” The mockup emphasizes monthly aggregation, timeliness, and high-level metrics.

### What to do

Define a stable export contract such as:

```json
{
  "month": "2026-03",
  "headline_metrics": {...},
  "cases": [...],
  "activity": [...],
  "methodology": {...}
}
```

and separately a flat table for direct rendering/filtering.

That export should be generated from the same normalized `index_rows` and manual annotations, not from ad hoc reporting logic.

## 9) Recommended implementation milestones

### Milestone 1 — taxonomy + schema alignment

- add versioned TFHT taxonomy files
- closed-set label enums/mappings
- add index_relevant
- add website/index row model
- update classifier prompts to use closed labels

### Milestone 2 — annotations + overrides

- manual correction table import
- missing-item import
- persistent overrides
- Google Sheet-compatible CSV format

### Milestone 3 — validation upgrade

- extend permanent validation set schema
- add import for reviewed manual example tables
- add stage-wise metrics
- add typology-aware evaluation reports

### Milestone 4 — monthly reporting

- implement monthly_report job
- generate counts + case blurbs + disclosure/methodology section
- support manual TFHT activity inputs

### Milestone 5 — minisite export

- stable JSON/table contract
- export tailored for the website table
- optional secured/manual ingestion path later

## 10) Concrete recommendations for Codex scope

When you ask Codex to implement this, I would have it do Phase 1 first:

- import the typology spreadsheet into a versioned taxonomy asset
- convert classification to a closed taxonomy
- add index_relevant
- add validation import for manual reviewed examples tables
- extend the permanent validation set
- add evaluation metrics by category/subcategory/index relevance
- add a minimal override table

That is the highest-leverage work because it improves:

- correctness
- explainability
- future maintainability
- ability to absorb new examples tables

The monthly report and minisite export should come immediately after that, because the template and manual tracking workbook already show the target shape clearly.

## Bottom line

The project is already far enough along that this should be treated as a taxonomy/validation/product-alignment phase, not another scraping phase.

The best plan is:

- freeze the client taxonomy into versioned repo assets
- make the classifier choose from that taxonomy
- treat manual examples and corrections as first-class data
- extend the existing validation subsystem to absorb new reviewed tables
- generate monthly report + minisite outputs from a normalized index layer

That gets the repo aligned with the new TFHT feedback while keeping it consistent with the current architecture and avoiding a second, parallel system.
