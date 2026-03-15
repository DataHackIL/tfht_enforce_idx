# denbust

מדד האכיפה - Enforcement Index

Monitor Israeli news for anti-brothel law enforcement activity: raids, arrests, closures, trafficking cases.

## What it does (Phase 1)

- Scans Israeli news RSS feeds (Ynet, Mako, Walla, etc.)
- Finds reports about brothel raids, prostitution arrests, pimping, trafficking
- Deduplicates same story across multiple sources
- Outputs unified items with all source links
- Supports output via CLI or SMTP email reports

## Quick Start

```bash
pip install -e ".[dev]"
python -m playwright install chromium
denbust scan --config agents/news.yaml
```

To send reports by email, set `output.format: email` in your config and provide SMTP env vars
from `.env.example`.

Mako scraping uses a headless Chromium browser. After installing dependencies on a new machine, run
`python -m playwright install chromium` once before your first live scan.

## Persistence Modes

### Local mode

Local runs continue to use the repo-local defaults:

- seen store: `data/seen.json`
- run snapshots: `data/runs/`

Example:

```bash
denbust scan --config agents/news.yaml
```

You can override the persistence locations without changing YAML by setting:

- `DENBUST_STORE_PATH`
- `DENBUST_RUNS_DIR`

### GitHub Actions + state repo mode

Scheduled GitHub Actions runs use this repo as the code runner and a separate repo,
`tfht_enforce_idx_state`, as the canonical mutable state store.

The workflow:

- checks out this repo
- checks out the state repo into `state_repo/`
- runs `denbust scan --config agents/news-github.yaml`
- points persistence at the checked-out state repo via:
  - `DENBUST_STORE_PATH=state_repo/seen.json`
  - `DENBUST_RUNS_DIR=state_repo/runs`
- commits and pushes the updated `seen.json` and new run snapshot only if files changed

Required secrets for GitHub-run mode:

- `ANTHROPIC_API_KEY`
- `STATE_REPO_PAT`
- `DENBUST_EMAIL_SMTP_HOST`
- `DENBUST_EMAIL_SMTP_PORT`
- `DENBUST_EMAIL_SMTP_USERNAME`
- `DENBUST_EMAIL_SMTP_PASSWORD`
- `DENBUST_EMAIL_FROM`
- `DENBUST_EMAIL_TO`
- `DENBUST_EMAIL_USE_TLS`
- `DENBUST_EMAIL_SUBJECT`

Expected `tfht_enforce_idx_state` structure:

```text
tfht_enforce_idx_state/
├── seen.json
└── runs/
```

Bootstrap notes:

- `seen.json` may be absent initially; it will be created automatically
- `runs/` will be created automatically by the workflow if missing
- a small `README.md` in the state repo is fine but optional

## Example Output

```
📍 פשיטה על בית בושת ברמת גן
תאריך: 2026-02-15
קטגוריה: בית בושת

תקציר: המשטרה פשטה על דירה ברמת גן...

מקורות:
• Ynet: https://ynet.co.il/...
• Mako: https://mako.co.il/...
```

## Documentation

- [Product Definition](docs/product_def.md) - Full project background (Hebrew)
- [MVP Spec](docs/MVP_SPEC.md) - Phase 1 technical scope
- [Implementation Plan](docs/IMPLEMENTATION_PLAN.md) - Task breakdown

## Roadmap

- **Phase 1** (current): News monitoring via RSS
- **Phase 2**: Court records scraping
- **Phase 3**: Analytics dashboard
