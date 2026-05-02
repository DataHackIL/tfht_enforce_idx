# Milestone 3 Validation Upgrade PR Breakdown

This note breaks Milestone 3 from [CHATGPT_26_04_PLAN.md](/Users/shaypalachy/clones/tfht_enforce_idx/docs/CHATGPT_26_04_PLAN.md) into four reviewable PRs.

## PR 3.1 — extend permanent validation set schema

- Expand the permanent validation CSV and typed models to carry taxonomy-aware labels plus optional expected output fields.
- Keep finalize/evaluate backward-compatible with older validation rows that do not yet have the new columns.
- Migrate the tracked `validation/news_items/classifier_validation.csv` asset to the new header shape.
- Status: merged.

## PR 3.2 — import reviewed manual example tables

- Add a generic reviewed-examples import adapter into the validation subsystem.
- Keep the existing TFHT manual-tracking workbook adapter, but add support for occasional manually generated CSV/XLSX reviewed-example tables.
- Normalize those tables into the existing reviewed/finalize flow, including taxonomy validation, canonical-URL dedupe, and provenance such as `annotation_source`.
- Status: merged.

## PR 3.3 — stage-wise validation metrics

- Extend evaluation beyond overall relevance into separate stages:
  - relevance
  - enforcement-related
  - taxonomy category / subcategory
  - index relevance
- Keep legacy rows usable, while making taxonomy-aware metrics conditional on taxonomy-labeled examples.
- Status: merged.

## PR 3.4 — typology-aware evaluation reports

- Add richer validation outputs for humans, built on top of PR 3.3 metrics.
- Include category/subcategory breakdowns and explicit handling of legacy versus taxonomy-labeled examples.
- Keep this as a reporting/output PR rather than another schema or import PR.
- Status: merged.

## Follow-up — validation integrity and live validation hardening

- Lint the tracked validation CSV before any model-backed evaluation so malformed rows, invalid
  booleans/datetimes/categories, invalid taxonomy pairs, and relevant rows missing taxonomy labels
  fail locally before API calls.
- Keep `denbust validation-lint --validation-set ...` available for credential-free checks.
- Share taxonomy/category/index-relevance row-integrity checks between validation lint and
  reviewed-row finalize/import, while leaving CSV-shape diagnostics in the lint layer.
- Treat Anthropic provider/API failures as fatal run errors instead of synthetic `not_relevant`
  predictions.
- Keep local validation and live-check outputs untracked; summarize the results in PRs or operator
  notes.
- Status: merged hardening follow-up.

## Sequencing

The intended order is:

1. PR 3.1: schema
2. PR 3.2: import
3. PR 3.3: metrics
4. PR 3.4: reporting
5. Follow-up: integrity linting and live-validation hardening

That split keeps each PR narrow:

- schema changes land before new import shapes depend on them
- import lands before metrics start relying on the richer permanent set
- reports land last, once the underlying metrics are stable
- hardening follows once local experiment runs expose validation-data and provider-failure failure
  modes that need to be enforced by the repo
