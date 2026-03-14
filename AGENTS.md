# AGENTS.md

> **SYNC NOTE:** `CLAUDE.md` mirrors this file. Keep them in sync when updating repository guidance.

---

## Project Overview

**denbust** (מדד האכיפה) is a Python tool for monitoring enforcement of anti-brothel laws in Israel.

### Project Phases

**Phase 1 (Current):** News monitoring
- 4 sources: 2 RSS feeds (Ynet, Walla) + 2 scrapers (Mako, Maariv)
- Scan the last X days for reports about brothel raids, prostitution arrests, pimping cases, trafficking, and closure orders
- Classify relevance with an LLM, deduplicate the same story across sources, and output unified items
- Output is currently available via CLI and SMTP email, with multi-output fanout through `output.formats`

**Phase 2 (Future):** Court records
- Scrape Israeli Courts for related proceedings
- Correlate news stories with court cases
- Track closure orders and reopenings

**Phase 3 (Future):** Analytics
- Historical enforcement database
- Regional enforcement gap analysis
- Web dashboard with statistics

---

## Current Architecture

```text
┌─────────────────┐     ┌─────────────────────────┐
│   RSS Sources   │     │   Scraper Sources       │
│  (Ynet, Walla)  │     │ Mako, Maariv            │
└────────┬────────┘     └────────┬────────────────┘
         │ HTTP/RSS               │ Playwright + HTML parsing (Mako)
         │ keyword + date filter  │ HTML parsing (Maariv)
         └──────────┬─────────────┘
                    ▼
         LLM Classifier (relevance + category)
                    │
                    ▼
         Deduplicator (cross-source grouping)
                    │
                    ▼
         Output fanout (`cli`, `email`)
```

- **Primary language:** Python 3.11+
- **Runtime:** CLI (`denbust scan`), cron or GitHub Actions for automation
- **Current outputs:** `cli`, `email`
- **Telegram note:** `telegram` still exists in config as an enum but is not implemented; the pipeline currently logs a warning and falls back to CLI output
- **No secrets in repo:** all credentials stay in environment variables

---

## What We Monitor

### Primary Topics
- Brothel raids and closures
- Prostitution-related arrests
- Pimp (סרסור) arrests and sentencing
- Human trafficking cases
- Administrative closure orders

### Secondary Topics
- Massage parlor / spa busts (fronts)
- Online platform takedowns
- Police operations targeting prostitution
- Court sentences in related cases
- Trafficking victim rescues

---

## Repository Structure

```text
denbust/
├── src/denbust/
│   ├── __init__.py
│   ├── cli.py                 # Typer CLI entry point
│   ├── config.py              # Config model + env-backed properties
│   ├── data_models.py         # RawArticle, ClassifiedArticle, UnifiedItem
│   ├── pipeline.py            # Orchestration + output fanout
│   ├── sources/
│   │   ├── base.py            # Source protocol
│   │   ├── rss.py             # RSS fetchers (Ynet, Walla)
│   │   ├── mako.py            # Playwright-backed scraper
│   │   └── maariv.py          # HTML scraper
│   ├── classifier/
│   │   └── relevance.py       # LLM classification
│   ├── dedup/
│   │   └── similarity.py      # Cross-source deduplication
│   ├── output/
│   │   ├── email.py           # SMTP report delivery
│   │   └── formatter.py       # Console/report formatting
│   └── store/
│       └── seen.py            # Seen URL persistence
├── data/
│   ├── runs/                  # Per-run outputs (gitignored)
│   └── seen.json              # Default seen-URL store (gitignored)
├── agents/                    # Checked-in example configs
├── tests/
│   ├── fixtures/              # RSS XML + HTML fixtures
│   ├── integration/
│   ├── smoke/
│   └── unit/
├── .github/
│   ├── workflows/
│   └── skills/                # Repo-local guidance for agents and Copilot
└── pyproject.toml
```

---

## Development Guidelines

### 1) Code Style & Quality
- Python 3.11+ preferred
- Full type annotations
- Lint/format: Ruff
- Type check: mypy
- Tests: pytest

```bash
ruff format .
ruff check .
mypy src/
pytest
```

### 2) Legal & Ethical Constraints
- Only use public, freely accessible sources
- Prefer RSS over scraping when a stable feed exists
- Respect robots.txt and anti-abuse boundaries
- Never store PII beyond what appears in public news articles
- Comply with Israeli privacy law

### 3) Data Fetching
- **RSS sources:** fetch the feed, then filter by keyword/date
- **Mako:** browser-backed with Playwright + headless Chromium; searches and section pages are rendered before parsing
- **Maariv:** HTML scraping with fixtures and mocked HTTP in tests
- Keep request pacing conservative and identify the client clearly
- Normalize Mako article URLs before deduplication or seen tracking so `?Partner=searchResults` does not create duplicate stories
- Handle source failures per source/search path and continue the rest of the scan

### 4) Classification
- LLM classifies relevance, category, sub-category, and confidence
- Prompts should stay short, Hebrew-aware, and grounded in article text
- Log decisions for review
- Never fabricate article details

### 5) Deduplication
- Same story from multiple sources should produce one unified item
- Keep all source links on the unified item
- Prefer canonical article URLs before similarity/dedup logic runs

### 6) Output
- Preferred config uses `output.formats`, not duplicate `format` keys
- Example:

```yaml
output:
  formats:
    - cli
    - email
```

- Legacy `output.format` is still accepted for backward compatibility, but `formats` is the preferred shape for new configs

---

## Secrets & Configuration

Never commit secrets. Use env vars:

```text
ANTHROPIC_API_KEY

DENBUST_TELEGRAM_BOT_TOKEN   # Optional, Telegram mode is not implemented yet
DENBUST_TELEGRAM_CHAT_ID     # Optional

DENBUST_EMAIL_SMTP_HOST
DENBUST_EMAIL_SMTP_PORT
DENBUST_EMAIL_SMTP_USERNAME
DENBUST_EMAIL_SMTP_PASSWORD
DENBUST_EMAIL_FROM
DENBUST_EMAIL_TO             # Comma-separated recipients
DENBUST_EMAIL_USE_TLS
DENBUST_EMAIL_SUBJECT
```

Notes:
- `DENBUST_EMAIL_TO` accepts comma-separated recipients and is parsed into a list
- Keep personal/local configs outside the repo when possible, for example under `~/.config/denbust/`
- Do not rely on ignored files under `agents/local/` for durable personal setup across branch/worktree changes
- Local runs can use an absolute config path:

```bash
denbust scan --config ~/.config/denbust/news-email.yaml
```

---

## Testing Guidance

- No live network calls in tests
- No live browser scraping in CI
- Unit tests should cover config normalization, classifier parsing, dedup logic, output fanout, and store behavior
- Integration tests should use mocked HTTP or mocked rendered HTML
- Mako tests should exercise rendered-HTML helpers and fixtures, not live Mako pages

Useful commands:

```bash
pytest -q
pytest -q tests/integration -k Mako
pytest -q tests/unit
```

---

## CI & Automation Guidance

Current workflow shape:
- `pre-commit.ci` handles pre-commit checks
- `.github/workflows/ci-test.yml` is the main CI workflow
- `unit-tests` and `integration-tests` produce raw coverage artifacts used by the `coverage` job
- `coverage` combines coverage data, generates `coverage.xml`, uploads the `coverage-xml` artifact, and uploads to Codecov
- `pr-agent-context` runs both from the main PR workflow and from `.github/workflows/pr-agent-context-refresh.yml`
- `pr-agent-context` now consumes the combined `coverage-xml` artifact directly for patch coverage
- Validation workflows exist for `pyproject.toml` and `codecov.yml`

When editing CI:
- keep workflow responsibilities narrow
- do not add duplicate coverage producers if the `coverage` job can be reused
- prefer artifact reuse over reconstructing results in multiple places

---

## Quick Reference

| Task | Command |
|------|---------|
| Install (dev) | `pip install -e ".[dev]"` |
| Install Mako browser runtime | `python -m playwright install chromium` |
| Run scan | `denbust scan --config agents/news.yaml` |
| Run with external config | `denbust scan --config ~/.config/denbust/news-email.yaml` |
| Tests | `pytest -q` |
| Lint | `ruff check .` |
| Format | `ruff format .` |
| Type check | `mypy src/` |

---

## Local Agent Overrides (Optional, Untracked)

- If `LOCAL_AGENTS.md` exists at repo root, treat it as additive local instructions
- `LOCAL_AGENTS.md` must remain untracked
- On conflicts, repository policy and security constraints take precedence
