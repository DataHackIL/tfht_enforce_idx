# TFHT Review Workbench

Cloudflare Pages app for reviewing DenBust/TFHT discovery candidates and materialized `news_items`
rows from Supabase.

## Scope

- Read operational rows from Supabase through Cloudflare Pages Functions.
- Sort the fetched review queue by a local likelihood score.
- Let allowed reviewers mark rows as include, exclude, needs review, or internal only.
- Let reviewers assign the Phase C taxonomy pair, index relevance, notes, city, case/event label,
  and workflow tags.
- Show the standard workflow tag vocabulary before allowing custom tag additions.
- Provide a local day/night theme preference for long review sessions.
- Store `news_items` annotations on the existing operational row fields.
- Store discovery-only candidate annotations under `persistent_candidates.metadata.review_app_annotation`.

This app does not publish public release bundles, change scrape queue behavior, or run discovery.

## Theme

Day mode is the default. Night mode is a browser-local UI preference stored in `localStorage` under
`tfht-review-theme`; it is not a user profile setting, permission, or server-side identity claim.
The page applies the stored theme in an early `<head>` script before first paint and scopes dark
colors through `:root[data-theme="night"]`.

## Review Vocabulary

The app keeps taxonomy and workflow tagging separate:

- `Case / event label` is free text for a short human-readable case name.
- `Workflow tags` are selected from a visible standard list first.
- Reviewers can add a custom workflow tag only from the custom-tag control shown below the standard
  list.

Current standard workflow tags:

- `strong-positive`
- `weak-positive`
- `false-positive`
- `duplicate-risk`
- `needs-source-check`
- `needs-fact-check`
- `needs-privacy-check`
- `paywall`
- `partial-page`
- `policy-context`
- `court`
- `police`
- `welfare`
- `reporting-context`

## Cloudflare Pages

Use the private Cloudflare account associated with `shaypal5@gmail.com`, not the Adanim account.
The token is loaded from the documented personal env file on this laptop:

```bash
source /Users/shaypalachy/.config/noa/cloudflare_api_token.env
```

Expected Pages project name:

```text
tfht-review-workbench
```

Required Pages secrets:

```text
DENBUST_SUPABASE_URL
DENBUST_SUPABASE_SERVICE_ROLE_KEY
TFHT_REVIEW_ALLOWED_EMAILS
```

`TFHT_REVIEW_ALLOWED_EMAILS` is a comma-separated allow-list. Cloudflare Access should also protect
the Pages hostname with OTP/email authentication for the same reviewer set.

Deploy:

```bash
npx wrangler pages deploy review_app/public \
  --project-name tfht-review-workbench \
  --branch main \
  --commit-dirty=true
```

From inside `review_app/`, the equivalent command is:

```bash
npx wrangler pages deploy public --project-name tfht-review-workbench --branch main --commit-dirty=true
```
