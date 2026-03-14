# News Sources Skill

Use this guidance when working on news sources, scraper behavior, fixtures, or local scanning workflows.

## Current Source Model

- `ynet` and `walla` are RSS-based sources
- `mako` is Playwright-backed and uses headless Chromium for rendered search/section pages
- `maariv` is an HTML scraper without a browser runtime

## Mako-Specific Rules

- Treat Mako as anti-bot sensitive
- Prefer adjusting rendered-page helpers and DOM parsing over adding ad hoc HTTP fallbacks
- Search and section scraping are separate paths; validate both when changing Mako behavior
- Normalize Mako article URLs before deduplication or seen tracking so search-result links with query params collapse to canonical article URLs
- When diagnosing Mako failures, capture state such as URL/title/page shape; do not assume Chromium is missing if section scraping already works

## Testing Expectations

- Do not add live Mako calls to CI tests
- Test Mako with fixtures and mocked rendered HTML/browser helpers
- Prefer focused coverage for search state handling, URL normalization, and parser updates
- Keep browser/network behavior mocked in unit and integration tests

## Local Validation

- Install Chromium once on a new machine:

```bash
python -m playwright install chromium
```

- For personal runs, keep your config outside the repo when possible and pass it with an absolute `--config` path
- Do not rely on ignored files under `agents/local/` as durable personal state across branch/worktree changes
