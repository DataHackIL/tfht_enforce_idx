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
