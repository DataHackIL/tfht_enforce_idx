# Phase C Plan: Typology Alignment, Validated Examples & Monthly Report Generation

> Written: 2026-04-04
> Based on: TFHT client meeting (2026-03-22), Eden's follow-up email (2026-03-24), and the four attached artefacts (Typology.xlsx, Manual Tracking.xlsx, Monthly Report Template.docx, mini-site wireframe).

---

## Context and motivation

The project was built with a self-designed two-level taxonomy (5 categories × 8 subcategories) baked into Python enums, with no externally-reviewed typology and an empty validation set.

The TFHT meeting and Eden's follow-up delivered three things that change this:

1. **A professionally-reviewed closed-set typology** — three top-level categories, ~20 subcategories, with explicit red/pink marks indicating which subcategories are relevant to the *enforcement index* specifically (vs. relevant to the domain broadly).
2. **A manually-maintained tracking spreadsheet** — real labeled enforcement events (Jan–Mar 2026) that can seed and grow the validation set.
3. **A monthly report template** — the exact output shape TFHT wants the pipeline to auto-generate.

This plan describes how to bring the codebase in line with all three.

---

## TFHT typology (source of truth)

The typology Excel sheet defines three top-level categories. Subcategories marked `index_relevant: true` are those that count toward the enforcement index; unmarked ones are still classifiable but excluded from index scoring and reporting.

### Category 1: סחר בבני אדם (human_trafficking)

| Subcategory (HE) | Slug | index_relevant |
|---|---|---|
| סחר למטרת ניצול מיני (זנות) | trafficking_sexual_exploitation | ✓ |
| סחר למטרת נישואין בכפייה | trafficking_forced_marriage | ✓ |
| סחר למטרת עבודת כפייה | trafficking_forced_labor | — |
| סחר למטרת נטילת איברים | trafficking_organ_harvesting | — |
| הבאת אדם למדינה אחרת לשם זנות | trafficking_cross_border_prostitution | ✓ |
| החזקה בתנאי עבדות | trafficking_slavery_conditions | ✓ |
| עבדות מינית | sexual_slavery | ✓ |
| סחר בנשים | trafficking_women | ✓ |

### Category 2: סרסור וזנות (pimping_prostitution)

| Subcategory (HE) | Slug | index_relevant |
|---|---|---|
| סרסור | pimping | ✓ |
| הבאת אדם לידי זנות | bringing_into_prostitution | ✓ |
| שידול לזנות | soliciting_prostitution | ✓ |
| עדויות של נשים בזנות | women_testimonies | — |
| סיקור תופעת הזנות | phenomenon_coverage | — |
| זנות מקוונת | online_prostitution | ✓ |
| חוק איסור צריכת זנות / המודל הנורדי | nordic_model_law | — |

### Category 3: בתי בושת (brothels)

| Subcategory (HE) | Slug | index_relevant |
|---|---|---|
| החזקת מקום לשם זנות | keeping_brothel | ✓ |
| השכרת מקום לשם זנות | renting_brothel | ✓ |
| פרסום זנות | advertising_prostitution | ✓ |
| קנס בגין צריכת זנות | client_fine | ✓ |
| סגירה מנהלית / צו מנהלי | administrative_closure | ✓ |
| ערעור על צו מנהלי | closure_appeal | ✓ |
| כתב אישום על החזקת/השכרת מקום | brothel_indictment | ✓ |

A fourth implied outcome: `not_relevant` — article does not belong to any of the above categories.

---

## Scope of this phase

| # | Work area | Priority |
|---|---|---|
| C-1 | Externalise taxonomy to a versioned YAML file | P0 |
| C-2 | Update classifier to use closed-set TFHT taxonomy + `index_relevant` | P0 |
| C-3 | Update data models and Supabase schema to reflect new taxonomy | P0 |
| C-4 | Bootstrap validation set from manual tracking spreadsheet | P0 |
| C-5 | Support periodic labeled-example import from TFHT's sheets | P1 |
| C-6 | Monthly report generation | P1 |
| C-7 | Update tests and CI | P1 |
| C-8 | Update keywords / re-scan with new taxonomy | P2 |

---

## C-1: Externalise taxonomy to a versioned YAML file

**Goal:** Single source of truth for the typology, readable by code and humans, easily updated when TFHT revises the hierarchy.

### File: `taxonomy/tfht_typology_v1.yaml`

```yaml
version: "1"
language: he
categories:
  - id: human_trafficking
    label_he: סחר בבני אדם
    label_en: Human Trafficking
    subcategories:
      - id: trafficking_sexual_exploitation
        label_he: סחר למטרת ניצול מיני (זנות)
        index_relevant: true
        example_urls:
          - https://www.israelhayom.co.il/...  # from typology sheet
      - id: trafficking_forced_marriage
        label_he: סחר למטרת נישואין בכפייה
        index_relevant: true
      - id: trafficking_forced_labor
        label_he: סחר למטרת עבודת כפייה
        index_relevant: false
      - id: trafficking_organ_harvesting
        label_he: סחר למטרת נטילת איברים
        index_relevant: false
      - id: trafficking_cross_border_prostitution
        label_he: הבאת אדם למדינה אחרת לשם העיסוק בזנות
        index_relevant: true
      - id: trafficking_slavery_conditions
        label_he: החזקה בתנאי עבדות
        index_relevant: true
      - id: sexual_slavery
        label_he: עבדות מינית
        index_relevant: true
      - id: trafficking_women
        label_he: סחר בנשים
        index_relevant: true
  - id: pimping_prostitution
    label_he: סרסור וזנות
    label_en: Pimping and Prostitution
    subcategories:
      - id: pimping
        label_he: סרסור
        index_relevant: true
      - id: bringing_into_prostitution
        label_he: הבאת אדם לידי זנות
        index_relevant: true
      - id: soliciting_prostitution
        label_he: שידול לזנות
        index_relevant: true
      - id: women_testimonies
        label_he: עדויות של נשים בזנות
        index_relevant: false
      - id: phenomenon_coverage
        label_he: סיקור תופעת הזנות
        index_relevant: false
      - id: online_prostitution
        label_he: זנות מקוונת
        index_relevant: true
      - id: nordic_model_law
        label_he: חוק איסור צריכת זנות / המודל הנורדי
        index_relevant: false
  - id: brothels
    label_he: בתי בושת
    label_en: Brothels
    subcategories:
      - id: keeping_brothel
        label_he: החזקת מקום לשם זנות
        index_relevant: true
      - id: renting_brothel
        label_he: השכרת מקום לשם זנות
        index_relevant: true
      - id: advertising_prostitution
        label_he: פרסום זנות
        index_relevant: true
      - id: client_fine
        label_he: קנס בגין צריכת זנות
        index_relevant: true
      - id: administrative_closure
        label_he: סגירה מנהלית / צו מנהלי
        index_relevant: true
      - id: closure_appeal
        label_he: ערעור על צו מנהלי
        index_relevant: true
      - id: brothel_indictment
        label_he: כתב אישום על החזקת/השכרת מקום לשם זנות
        index_relevant: true
```

### `src/denbust/classifier/taxonomy.py` (new file)

A lightweight loader and in-memory model:

```python
@dataclass(frozen=True)
class SubcategoryDef:
    id: str
    label_he: str
    index_relevant: bool
    example_urls: list[str]

@dataclass(frozen=True)
class CategoryDef:
    id: str
    label_he: str
    label_en: str
    subcategories: list[SubcategoryDef]

@dataclass(frozen=True)
class Typology:
    version: str
    categories: list[CategoryDef]

    def subcategory(self, cat_id: str, sub_id: str) -> SubcategoryDef: ...
    def is_index_relevant(self, cat_id: str, sub_id: str) -> bool: ...
    def all_subcategory_ids(self) -> list[str]: ...
    def index_relevant_subcategory_ids(self) -> list[str]: ...

def load_typology(path: Path | None = None) -> Typology:
    """Load from taxonomy/tfht_typology_v1.yaml by default."""
```

The `TAXONOMY_PATH` default resolves relative to the package root so it works both locally and in CI.

---

## C-2: Update classifier to use closed-set TFHT taxonomy

**Goal:** Classifier emits a category + subcategory from the TFHT closed set, plus an `index_relevant` flag derived from the taxonomy (not from the LLM). The classifier should not invent categories.

### Classification strategy

Two-stage classification is cleaner and cheaper than one big prompt:

**Stage 1 — Broad relevance gate**
Fast, cheap. Prompt: "Does this article concern prostitution, brothels, pimping, or human trafficking in Israel? Answer yes/no."
If no → `category=not_relevant`, skip stage 2.

**Stage 2 — Closed-set subcategory**
Send the article with the full Hebrew subcategory list (IDs + labels). Ask: "Which single category and subcategory best describes this enforcement-related content?"
Validate the response against the YAML-loaded taxonomy. If validation fails, fall back to the parent category with `subcategory=None`.

`None` is the canonical Python representation for a missing subcategory; serializers/exporters may map that to their own null/empty representation, but the in-memory classifier result should use `None` consistently.

**`index_relevant` derivation:**
After classification, set `index_relevant = typology.is_index_relevant(category, subcategory)`. This is a lookup, not an LLM call.
For the fallback case above, `typology.is_index_relevant(category, None)` must return `False`.

### Changes to `classifier/relevance.py`

- Replace hardcoded `ALLOWED_SUBCATEGORIES` dict with `Typology` loaded from YAML.
- The classification prompt becomes a template that injects subcategory options from the loaded taxonomy (so updating the YAML file automatically updates the prompt, with no code change needed).
- `ClassificationResult` gains an `index_relevant: bool` field, populated from taxonomy lookup.
- The confidence field remains.

### Changes to `data_models.py`

Replace the `Category` and `SubCategory` `StrEnum`s with:

```python
# Thin wrappers — validate against loaded taxonomy at parse time.
CategoryId = str   # one of the taxonomy category IDs
SubcategoryId = str  # one of the taxonomy subcategory IDs
```

Or keep them as `StrEnum` but generate them from the YAML at import time (via a small codegen helper or by constructing the enum programmatically). The codegen approach avoids out-of-sync enums.

The simplest migration: change the type annotations to `str` with a `Literal` union generated at load time, and add a validator. This avoids regenerating Python source on every taxonomy update.

**`ClassifiedArticle` and `UnifiedItem`** gain `index_relevant: bool`.

---

## C-3: Update data models and Supabase schema

### `news_items/models.py`

Add `index_relevant: bool` to both `NewsItemPublicRecord` and `NewsItemOperationalRecord`.

Update the category/subcategory fields to use the new TFHT taxonomy IDs (the current enums have different slugs — e.g. `BROTHEL` → `brothels`, `CLOSURE` → `administrative_closure`).

Add `taxonomy_version: str` to `NewsItemOperationalRecord` so that records created under different taxonomy versions can be distinguished.

### Supabase migration: `supabase/migrations/20260404_phase_c_taxonomy.sql`

```sql
-- Add index_relevant flag
ALTER TABLE news_items ADD COLUMN index_relevant BOOLEAN;

-- Add taxonomy_version
ALTER TABLE news_items ADD COLUMN taxonomy_version TEXT DEFAULT 'v1';

-- Update category/subcategory constraints to allow new slugs
-- (drop old CHECK constraints, add new ones from YAML-generated list)
```

Backfill strategy: run a one-off script that re-classifies existing rows using the new taxonomy mapping table (old slug → new slug). Most mappings are mechanical (e.g. `brothel/closure` → `brothels/administrative_closure`).

---

## C-4: Bootstrap validation set from manual tracking spreadsheet

**Goal:** Import Eden's manually-tracked enforcement events as labeled positive examples into `validation/news_items/classifier_validation.csv`.

### What the tracking spreadsheet provides

The **מדד האכיפה sheet** (Sheet 3 in `Manual Tracking.xlsx`) contains real enforcement events with: date, address, event description, status, and source URL — one row per event. These are all positive (relevant + index_relevant) examples.

The **Typology.xlsx** contains one "negative example" per category (articles that should NOT enter the index), attached as article URLs.

### Import script: `src/denbust/validation/import_manual_tracking.py`

A CLI-invocable script (or `denbust` sub-command) that:

1. Reads `TFHT Enforcement Index - Manual Tracking.xlsx`, Sheet 3 (מדד האכיפה).
2. For each row: infers `category` + `subcategory` from the event description using the repeatable mapping in `taxonomy/event_type_mapping.yaml` (see C-5).
3. Resolves the source URL to a canonical form.
4. Writes one row per event to a draft validation CSV (`validation/news_items/validation_import_draft.csv`) in `DRAFT_COLUMNS` format with `draft_source=manual_tracking` and `review_status=reviewed` (since Eden manually compiled them).
5. Invokes the existing `dataset.py` merge logic to incorporate reviewed rows into `validation/news_items/classifier_validation.csv`.

**Important:** The import script is idempotent — it checks the canonical URL against existing rows before inserting. Re-running it with an updated spreadsheet only appends new rows.

### Negative examples

Import the negative example URLs from `Typology.xlsx` (one per category, marked as "should not enter the index") as rows with `relevant=false`, `category=pimping_prostitution`, `subcategory=phenomenon_coverage` or `subcategory=women_testimonies` (as appropriate for each example), `index_relevant=false`, `review_status=reviewed`, `draft_source=typology_sheet`.

### Initial seeded validation set (expected size)

From the tracking sheet (Jan–Mar 2026): ~15 positive events.
From the typology sheet: ~3 negative examples.
Total bootstrap: ~18 labeled rows. Small but real and professionally verified.

---

## C-5: Support periodic labeled-example import from TFHT

**Goal:** Eden can periodically send an updated tracking spreadsheet and the system can ingest new examples without manual intervention.

### Approach

The import script from C-4 is the mechanism. To make it repeatable and low-friction:

1. **Annotated import format** — define a simple convention: Eden's sheet already has `אירוע` (event type) and `פרטים` (details). Add a lightweight mapping from common event descriptions to (category, subcategory) slug pairs. The mapping lives in `taxonomy/event_type_mapping.yaml`:

```yaml
# Maps free-text event type phrases (Hebrew) to taxonomy slugs.
# Used by the import script. Extend as new patterns appear.
mappings:
  - match: "צו סגירה מנהלי"
    category: brothels
    subcategory: administrative_closure
  - match: "כתב אישום"
    category: brothels
    subcategory: brothel_indictment
  - match: "מעצר סרסור"
    category: pimping_prostitution
    subcategory: pimping
  - match: "סחר בנשים"
    category: human_trafficking
    subcategory: trafficking_women
  # ... etc.
```

2. **Import CLI command** — expose as `denbust validation import-tracking <path>`.
   Output: summary of rows added / skipped (already seen) / needs-review (no match found in mapping).

3. **Unmatched rows** — rows that don't match any mapping are written to a separate `needs_review.csv` for Eden or a team member to label manually, then re-imported.

4. **Taxonomy version pinning** — each imported row records `taxonomy_version: v1` so that if the taxonomy changes later, old rows are clearly marked with which version they were labeled under.

---

## C-6: Monthly report generation

Status: Implemented via the dedicated `news_items / monthly_report` job, the `denbust report monthly` CLI wrapper, and the scheduled `news-items-monthly-report.yml` workflow.

**Goal:** Automatically generate a monthly report matching the template Eden provided, as a Markdown/text file (for LLM-assisted drafting) and optionally as HTML/PDF.

### Report structure (from template)

```
[Header: מדד האכיפה — נלחמות בתעשיית המין]
[Title: דו"ח חודש <month> <year>]

הנתונים החודשיים:
• N צווי סגירה מנהליות כנגד בתי בושת
• N מעצרים של חשודים בגין סחר בבני אדם למטרת ניצול מיני
• [one bullet per index-relevant subcategory that had events]

פירוט המקרים:
• [per-event narrative with source link]
• ...

פעילות המטה:
• המטה שלח X מכתבי תמיכה / ליווה דיונים

[Footer / disclosure text]
```

### Implementation: `src/denbust/news_items/monthly_report.py`

```python
def generate_monthly_report(
    records: list[NewsItemOperationalRecord],
    month: date,
    hq_activity: str | None = None,
    typology: Typology | None = None,
) -> MonthlyReport:
    """
    Groups records by index-relevant subcategory, counts events,
    generates per-event narrative summaries, and returns a structured
    MonthlyReport object.
    """
```

`MonthlyReport` is a dataclass with:
- `month: date`
- `stats: dict[str, int]` — count per subcategory, index_relevant only
- `cases: list[CaseSummary]` — headline, narrative, source URL, category, subcategory
- `hq_activity: str | None`
- `rendered_markdown: str` — the formatted report body

### LLM-assisted narrative generation

For each news item going into the report, call the LLM once to produce a 1–2 sentence Hebrew narrative summary suitable for the "פירוט המקרים" section. The existing `enrich.py` `summary_one_sentence` field may already serve this purpose — reuse it if the model is the same; otherwise add a `monthly_report_summary` enrichment field.

A separate LLM call over the full month's events produces the statistical lead paragraph, using the structured stats dict as structured input (not free-form) to avoid hallucination:

```python
MONTHLY_STATS_PROMPT = """
להלן נתוני אכיפה לחודש {month_he}:
{stats_json}
כתוב פסקת פתיחה קצרה בעברית לדו"ח חודשי.
"""
```

### CLI command: `denbust report monthly`

```
denbust report monthly --month 2026-03 [--output report_2026_03.md]
```

Reads operational records from the configured store, filtered to the given month and `index_relevant=true`, and produces the report.

The shipped implementation also exposes the same behavior as a real dataset job:

```
denbust run --dataset news_items --job monthly_report --config agents/news/local.yaml
```

### GitHub Actions integration

A new monthly GitHub Actions workflow `news-items-monthly-report.yml` runs on the 1st of each month, generates the report bundle into the state repo, and leaves final human editing/publication outside the workflow.

---

## C-7: Update tests and CI

### Unit tests to add/update

- `tests/unit/test_taxonomy.py` — load taxonomy from YAML, verify all expected IDs present, verify `is_index_relevant` returns correct values for several spot-checks.
- `tests/unit/test_classifier_taxonomy.py` — mock LLM responses; verify that a response with a valid taxonomy ID is accepted, an invalid ID falls back gracefully, and `index_relevant` is derived from taxonomy (not from LLM output).
- `tests/unit/test_validation_import.py` — test the import script against a fixture Excel file with known rows; verify idempotency; verify unmatched rows go to `needs_review`.
- `tests/unit/test_monthly_report.py` — build `NewsItemOperationalRecord` fixtures with known categories; verify stat counts and rendered Markdown structure.
- Update `tests/unit/test_news_items_phase_b.py` wherever it references old category/subcategory enum values.

### Validation evaluation

With a seeded validation set (from C-4), the existing `evaluate.py` machinery can now produce meaningful precision/recall/F1 numbers. Add the updated `agents/validation/classifier_variants.yaml` with:
- `baseline` — current classifier (pre-Phase-C, for regression comparison)
- `v1_taxonomy` — Phase-C classifier with new taxonomy

Run `denbust validation evaluate` in CI and publish the results as an artifact.

---

## C-8: Update keywords and re-scan (deferred)

The ingest keywords in `agents/news/github.yaml` and `local.yaml` were assembled before the official typology. Now that we have the authoritative Hebrew subcategory labels, review and extend:

- Add: `"נישואין בכפייה"`, `"עבדות מינית"`, `"זנות מקוונת"`, `"צו הגבלת שימוש"`, `"קנס צריכת זנות"`
- The stage-1 relevance gate in the new classifier means false positives from broader keywords are cheap — the classifier filters them.

A one-time re-scan over the last 90 days with updated keywords can be run manually after the taxonomy migration to catch events that were previously missed.

Cross-plan sequencing note: the earlier choice between `C-8` and `DL-PR-09` was resolved in favor of `DL-PR-09`, which shipped in PR `#87` on 2026-04-21. That sequencing question is now fully closed: `C-8` was deferred until the entire `DL-PR-*` track, including `DL-PR-10` and `DL-PR-11`, was complete.

---

## Ordering and dependencies

```
C-1 (taxonomy YAML)
  └─► C-2 (classifier update)
        └─► C-3 (data models + migration)
              └─► C-6 (monthly report)
C-4 (bootstrap validation set)        [can run in parallel with C-2]
  └─► C-5 (periodic import workflow)
C-7 (tests)     [touches C-1, C-2, C-4, C-6 — do last]
C-8 (keywords)  [independent, safe to defer]
```

Recommended execution order for a solo contributor inside Phase C remains: C-1 → C-2 → C-3 → C-4 → C-5 → C-7 → C-6 → C-8.
Actual cross-plan repository sequencing inserted the full `DL-PR-*` discovery rollout ahead of `C-8`; once `DL-PR-11` is merged, the keyword expansion/re-scan work can move back to the front of the plan.

---

## Files created / modified summary

| Path | Action |
|---|---|
| `taxonomy/tfht_typology_v1.yaml` | **New** — TFHT closed-set typology with index_relevant flags |
| `taxonomy/event_type_mapping.yaml` | **New** — Hebrew phrase → taxonomy slug mapping for import |
| `src/denbust/classifier/taxonomy.py` | **New** — taxonomy loader and in-memory model |
| `src/denbust/classifier/relevance.py` | **Modify** — two-stage classification, closed-set validation from YAML |
| `src/denbust/data_models.py` | **Modify** — category/subcategory types from taxonomy, add `index_relevant` |
| `src/denbust/news_items/models.py` | **Modify** — add `index_relevant`, `taxonomy_version` fields |
| `src/denbust/news_items/monthly_report.py` | **New** — monthly report generation |
| `src/denbust/validation/import_manual_tracking.py` | **New** — import from TFHT tracking spreadsheet |
| `src/denbust/validation/common.py` | **Modify** — add `taxonomy_version`, `index_relevant`, `annotation_source` to validation row schema |
| `src/denbust/cli.py` | **Modify** — add `denbust report monthly` and `denbust validation import-tracking` commands |
| `supabase/migrations/20260404_phase_c_taxonomy.sql` | **New** — add `index_relevant`, `taxonomy_version` columns |
| `agents/validation/classifier_variants.yaml` | **Modify** — add `v1_taxonomy` variant |
| `.github/workflows/news-items-monthly-report.yml` | **New** — monthly report automation |
| `tests/unit/test_taxonomy.py` | **New** |
| `tests/unit/test_classifier_taxonomy.py` | **New** |
| `tests/unit/test_validation_import.py` | **New** |
| `tests/unit/test_monthly_report.py` | **New** |
| `tests/unit/test_news_items_phase_b.py` | **Modify** — update category references |
| `validation/news_items/classifier_validation.csv` | **Populate** — ~18 rows from bootstrap import |

---

## What this phase does NOT include

- **Minisite / web frontend** — Eden's team is building this separately; this plan only ensures the Supabase table and public release bundle provide the data shape they need.
- **Manual override UI** — the Google Sheet / password-protected form for manual event entry is a separate integration. The architecture supports it (any data source that writes to Supabase in the `news_items` schema is compatible), but building the form is out of scope here.
- **Court records / official sources** — Phase D, as planned.
- **Police data intake** — depends on consent/feasibility discussions (noted in meeting); architectural hooks for an additional source adapter already exist.
