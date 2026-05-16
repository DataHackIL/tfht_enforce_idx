# Review Workbench — UI/UX Reference

The review workbench is a Cloudflare Pages application that allows human operators to review, label, and approve discovery candidates before publication.

**URL (production):** `https://tfht-review-workbench.pages.dev`
**Source:** `review_app/` in this repository
**Authentication:** Google OAuth; allowed emails configured in `review_app/functions/_shared.js`

---

## Layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  TFHT Review                    [Night mode]  [EN/HE]  [User email] │
├──────────────────────────────┬───────────────────────────────────────┤
│  FILTER BAR                  │  DETAIL PANE                          │
│  [keyword] [source] [status] │  Title / URL / metadata               │
│  [sort order]                │  ─────────────────────────────────    │
├──────────────────────────────│  REVIEW FORM                          │
│  CANDIDATE LIST              │  Decision  ○ Include ○ Exclude        │
│                              │            ○ Internal only            │
│  ┌──────────────────────┐   │                                        │
│  │ ☐ │ Card body        │   │  [✓] Index relevant                    │
│  │   │ score / domain   │   │                                        │
│  │   │ snippet          │   │  Taxonomy (categories + subcategories) │
│  └──────────────────────┘   │  Workflow tags                         │
│  ┌──────────────────────┐   │  Notes (free text)                     │
│  │ ☐ │ Card body        │   │                                        │
│  └──────────────────────┘   │  [Save]                                │
│  …                           │                                        │
├──────────────────────────────┤                                        │
│  BULK BAR (when items        │                                        │
│  selected)                   │                                        │
│  3 selected [Mark FP] [✕]    │                                        │
└──────────────────────────────┴───────────────────────────────────────┘
```

---

## Filter Bar

Controls which candidates appear in the left panel.

| Control | Description |
|---|---|
| **Keyword search** | Free-text filter on title/snippet |
| **Source filter** | Drop-down to show only a specific news source |
| **Status filter** | `pending` (unreviewed), `reviewed`, or `all` |
| **Sort order** | `High score first` (default) or `Low score first` — useful for bulk-excluding clearly irrelevant low-scored items |

Filters apply immediately on change. The list reloads from the Cloudflare Function.

---

## Summary Row

A single line above the candidate list shows the total count matching current filters.

---

## Candidate Card Anatomy

Each card in the left column has two interactive regions:

```
┌─────────────────────────────────────────┐
│ ☐  │ [Title of article]                 │
│    │  source.co.il · score: 0.87        │
│    │  Publication date                  │
│    │  Snippet of text…                  │
└─────────────────────────────────────────┘
  ↑        ↑
checkbox  click to open in detail pane
(bulk     (does not tick checkbox)
 select)
```

- The **checkbox** on the left edge is for bulk selection only. Ticking it does NOT open the detail pane.
- The **card body** (title, domain, score, snippet) is a button. Clicking it loads the item in the detail pane and highlights the card.
- Cards with an existing review decision show a coloured left border:
  - Green: Include / Approved
  - Red: Exclude / Suppressed
  - Amber: Internal only

---

## Bulk Selection

Multiple cards can be selected simultaneously using their checkboxes.

When at least one card is checked, a **Bulk bar** appears at the bottom of the left panel:

```
  3 selected  [Mark as false positive]  [✕ Clear]
```

- **Mark as false positive**: applies `Exclude` + `false-positive` tag to all selected items in a single API call.
- **Clear (✕)**: deselects all items without making any changes.
- The bulk bar automatically disappears when the selection is cleared.

See [labeling-semantics.md](./labeling-semantics.md#bulk-false-positive-action) for the exact fields written.

---

## Detail Pane

Clicking a card loads its full detail on the right:

| Section | Content |
|---|---|
| **Header** | Title (linked to original URL), source domain, publication date |
| **Score** | Classifier relevance score (0.0–1.0); shown with a label like "very high" / "high" / "medium" / "low" |
| **Query info** | Discovery query text and query kind (broad, source-targeted, taxonomy-targeted) |
| **Snippet** | Summary text from the search result or scrape |
| **Metadata** | Taxonomy categories assigned by classifier, workflow tags already applied |

---

## Review Form

Below the detail metadata sits the review form.

### Decision (radio group)

- **Include** — on-topic; approve for publication
- **Exclude** — false positive; suppress
- **Internal only** — real but not for external publication

When loading an **unreviewed** item:
- All radios start unselected (no default decision is forced)
- **Index relevant** is pre-ticked

### Index Relevant (checkbox)

- Pre-ticked on load for all unreviewed items.
- Ticking it automatically selects the **Include** radio.
- The reverse is not true: changing the Decision radio does not affect the checkbox.

### Taxonomy

A two-level grid:

1. **Categories** — checkboxes for top-level taxonomy categories (e.g. Prostitution, Human Trafficking, Legislation…)
2. **Subcategories** — once a category is checked, its subcategories expand inline

Values come from the same taxonomy used by the classifier. See `src/denbust/taxonomy/`.

### Workflow Tags

A row of toggle-buttons for pre-defined operational tags. Multiple may be active at once.
See [labeling-semantics.md](./labeling-semantics.md#workflow-tags-topic_tags) for the full tag vocabulary.

### Notes

A free-text textarea. Stored in the `notes` field on the `news_items` row. Useful for flagging edge cases for team review.

### Save Button

Writes the decision, taxonomy, tags, and notes to Supabase via a PATCH request. The button shows a spinner while saving and a checkmark on success.

---

## Queue Behaviour

- Items with `review_status = 'pending'` are the default view.
- After saving a review, the workbench automatically advances to the next unreviewed item in the list.
- The left panel does not automatically reload; use the filter controls or browser refresh to get fresh data.

---

## Language Toggle (HE/EN)

A toggle in the header switches the entire UI between **Hebrew** (default, RTL) and **English** (LTR).

- All button labels, field descriptions, headers, and placeholder text switch language.
- Underlying field values, DB column names, tag strings, and schema are never affected.
- The `<html dir>` and `<html lang>` attributes update on toggle.

---

## Night Mode

A moon/sun icon in the header toggles between light and dark themes. Preference is not persisted across sessions.

---

## Authentication

Access is restricted to a Google-OAuth allowlist. Unauthenticated requests receive a `401`. The allowed email list is maintained in `review_app/functions/_shared.js` → `ALLOWED_EMAILS`.

Current allowed emails (case-insensitive):
- `shaypal5@gmail.com`
- `Eden@tfht.org`
- `moria@tfht.org`
- `Shaked@tfht.org`

---

## Deployment

The app is deployed to Cloudflare Pages as `tfht-review-workbench`.

```bash
# Deploy (must cd into review_app first so wrangler picks up functions/)
cd review_app/
npx wrangler pages deploy public --project-name tfht-review-workbench
```

> **Important:** deploying from the repo root with `npx wrangler pages deploy review_app/public` silently omits the `functions/` bundle. Always `cd review_app/` first.

Environment variables (set in Cloudflare dashboard → Pages → Settings → Variables):

| Variable | Description |
|---|---|
| `DENBUST_SUPABASE_URL` | Supabase project URL |
| `DENBUST_SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key (write access) |
| `GOOGLE_CLIENT_ID` | OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth client secret |
| `SESSION_SECRET` | Signing secret for session cookies |

---

## API Endpoints (Cloudflare Functions)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/candidates` | Paginated candidate list; supports `?filter=`, `?source=`, `?status=`, `?sort=` |
| `PATCH` | `/api/review` | Save a review decision for a single candidate |
| `GET` | `/api/taxonomy` | Taxonomy category/subcategory tree |
| `GET` | `/api/me` | Current authenticated user info |
