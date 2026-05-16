# Labeling & Tagging Semantics

Reference for all label, tag, and status values used in the TFHT Enforcement Index pipeline.
Covers the full lifecycle from discovery through publication.

---

## Review Decisions (Human Operator)

The review workbench asks the operator to make exactly one decision per item.

| Decision | Description |
|---|---|
| **Include** | Article is on-topic; should be published in the public index. Sets `publication_status = 'approved'`. |
| **Exclude** | Article is off-topic or a false positive; should never appear publicly. Sets `publication_status = 'suppressed'` + `suppression_reason = 'false_positive'`. |
| **Internal only** | Article is real but too sensitive to publish externally (e.g. victim details, unverified allegations). Sets `publication_status = 'internal_only'`. |

> **Important – no data is deleted.**
> All three decisions keep the record in Supabase permanently. `is_publicly_releasable()` requires `publication_status IN ('approved', 'published')`. Records with `suppressed` or `internal_only` status are retained as internal evidence and are suitable training data for the classifier.

---

## Publication Status Enum

Full set of `publication_status` values a `news_items` row can hold:

| Value | Who sets it | Publicly releasable |
|---|---|---|
| `draft` | Default on ingest | No |
| `approved` | Human review → Include | Yes |
| `published` | Release pipeline post-publish | Yes |
| `suppressed` | Human review → Exclude | No (retained internally) |
| `internal_only` | Human review → Internal only | No (retained internally) |

---

## `index_relevant` Field

A boolean field on `news_items` rows.

- Automatically **pre-ticked** in the review workbench when an unreviewed item is loaded.
- Selecting **Index relevant** in the UI automatically switches the **Decision** to **Include** (not vice versa — changing Decision does not affect Index relevant).
- Semantics: `index_relevant = true` means the operator considers the article a meaningful data point for the enforcement index itself (i.e. worth counting and categorising, not just archiving).

---

## Taxonomy Labels

Taxonomy labels classify an article into the enforcement typology. Two levels:

| Level | Field | UI control |
|---|---|---|
| **Category** | `primary_category` (inferred) | Checkbox grid — select all that apply |
| **Subcategory** | `subcategory_tags` (list) | Checkbox grid nested under selected category |

Taxonomy values are defined in `src/denbust/taxonomy/` and drive both the discovery query vocabulary and the classifier prompt. Do not add taxonomy values only in the UI — they must be added to the taxonomy module first.

---

## Workflow Tags (`topic_tags`)

Workflow tags are a free-form `text[]` column on `news_items`. They augment (not replace) taxonomy. The review app and ingest app present the following named tags as toggles:

| Tag | Hebrew UI label | Usage guidance |
|---|---|---|
| `false-positive` | תוצאה שגויה | Item was returned by discovery or the classifier but is clearly off-topic. Use with **Exclude** decision to build a clean false-positive training corpus. |
| `needs-verification` | דורש אימות | Claim is plausible but unverified; do not publish until confirmed. Use with **Internal only** decision. |
| `victim-sensitive` | רגיש לנפגע | Contains identifying details about a victim. Use with **Internal only** decision. |
| `police-operation` | מבצע משטרתי | Article covers an active or recent enforcement operation. |
| `conviction` | הרשעה | Article reports a conviction or sentencing. |
| `plea-deal` | הסדר טיעון | Article covers a plea agreement. |
| `arrest` | מעצר | Article reports an arrest or detention. |
| `indictment` | כתב אישום | Article covers charges being filed. |
| `legislation` | חקיקה | Article covers a new law, amendment, or proposed legislation. |
| `ngo-report` | דו"ח ארגון | Article summarises or cites a civil-society or NGO report. |
| `media-investigation` | חקירת עיתונות | Article is an investigative journalism piece. |

Tags are stored as-is (lowercase English strings). The Hebrew labels are display-only and exist only in the UI translation layer — they do not touch the DB schema.

Multiple tags may be applied to the same item. Tags accumulate on the array; the UI never removes existing tags (to preserve audit history set by earlier sessions).

---

## Bulk False-Positive Action

The review workbench supports bulk exclusion for efficiently clearing obviously off-topic items.

**Flow:**

1. Use the checkbox on the left edge of each candidate card to select one or more items.
2. A sticky **Bulk bar** appears at the bottom of the left panel, showing the count of selected items.
3. Press **Mark as false positive** to apply, in a single operation, to all selected items:
   - `publication_status = 'suppressed'`
   - `suppression_reason = 'false_positive'`
   - `topic_tags` appended with `'false-positive'`
   - `review_status = 'reviewed'`
   - `manually_reviewed = true`

**Data retention:** bulk-excluded items are retained in Supabase exactly like single-item exclusions. Nothing is deleted.

---

## Suppression Reasons

When an item is suppressed (`publication_status = 'suppressed'`), a reason is recorded:

| Reason | Set by |
|---|---|
| `false_positive` | Review decision → Exclude (single or bulk) |
| *(future values TBD)* | |

---

## Audit Trail

Every review action writes:

| Field | Value |
|---|---|
| `manually_reviewed` | `true` |
| `manually_overridden` | `true` |
| `review_status` | `'reviewed'` |
| `annotation_source` | `'review_app'` or `'ingest_app'` |

Ingest-app items additionally set `content_basis = 'candidate_only'` on creation.

---

## Label Flow (Lifecycle Diagram)

```
Discovery run
  │
  ▼
Candidate (CandidateStatus.NEW)
  │
  ├── classify_search_noise() → UNSUPPORTED_SOURCE  (no DB write to news_items)
  │
  └── Scrape → news_items row created
        publication_status = 'draft'
        review_status = 'pending'
        index_relevant = true (default)
        │
        ▼
      Human review (review workbench)
        │
        ├── Include → approved + taxonomy + tags
        ├── Exclude → suppressed + false_positive tag
        └── Internal only → internal_only + sensitive tags
              │
              ▼
            Release pipeline
              approved → published (publicly releasable)
```

---

## Agent Action Boundaries

Agents (Claude-based automation) may:
- Set `publication_status`, `review_status`, `annotation_source`, `content_basis`
- Add taxonomy tags, workflow tags
- Write `index_relevant`

Agents must **not**:
- Delete rows from `news_items` or the candidate state
- Publish items with `publication_status != 'approved'`
- Override a human `manually_reviewed = true` decision
