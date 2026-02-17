# CLAUDE.md

> **SYNC NOTE:** This file mirrors AGENTS.md. When updating either file, update both to keep them in sync.

---

## Project Overview

**denbust** (מדד האכיפה) is a Python tool for monitoring enforcement of anti-brothel laws in Israel.

### Project Phases

**Phase 1 (Current)**: News monitoring
- 4 sources: 2 via RSS (Ynet, Walla) + 2 via scraping (Mako, Maariv)
- Scan last X days (configurable)
- Find reports about brothel raids, prostitution arrests, pimping cases, trafficking
- Deduplicate same story across multiple sources
- Output unified items via CLI or Telegram

**Phase 2 (Future)**: Court records
- Scrape Israeli Courts website for relevant proceedings
- Correlate news stories with court cases
- Track closure orders and reopenings

**Phase 3 (Future)**: Analytics
- Historical enforcement database
- Regional enforcement gap analysis
- Web dashboard with statistics

---

## What We Monitor (Phase 1)

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

## Technical Architecture (Phase 1)

```
┌─────────────────┐     ┌─────────────────┐
│   RSS Sources   │     │    Scrapers     │
│  (Ynet, Walla)  │     │ (Mako, Maariv)  │
└────────┬────────┘     └────────┬────────┘
         │ feedparser            │ beautifulsoup
         │ + keyword filter      │ + date filter
         └──────────┬────────────┘
                    ▼
         LLM Classifier (relevance + category)
                    │
                    ▼
         Deduplicator (cross-source grouping)
                    │
                    ▼
         Output (CLI / Telegram)
```

- **Primary Language:** Python 3.11+
- **Runtime**: CLI (`denbust scan`), cron for scheduling
- **No secrets in repo:** all credentials via env vars

---

## Repository Structure

```
denbust/
├── src/denbust/
│   ├── __init__.py
│   ├── cli.py              # CLI entry point
│   ├── config.py           # Configuration + validation
│   ├── models.py           # RawArticle, UnifiedItem
│   ├── pipeline.py         # Orchestration
│   ├── sources/
│   │   ├── base.py         # Source protocol
│   │   ├── rss.py          # RSS fetcher (Ynet, Walla)
│   │   ├── mako.py         # Mako scraper
│   │   └── maariv.py       # Maariv scraper
│   ├── classifier/
│   │   └── relevance.py    # LLM classification
│   ├── dedup/
│   │   └── similarity.py   # Cross-source deduplication
│   ├── output/
│   │   ├── formatter.py    # Unified output format
│   │   └── telegram.py     # Telegram notifier
│   └── store/
│       └── seen.py         # Track seen URLs
├── tests/
│   └── fixtures/           # Sample RSS XML + HTML
├── data/                   # seen.json persistence
├── agents/                 # Config files
└── pyproject.toml
```

---

## Development Guidelines

### 1) Code Style & Quality
- Python 3.11+ preferred
- Full type annotations
- Lint/format: ruff
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
- Prefer RSS feeds over HTML scraping
- Respect robots.txt
- Never store PII beyond what's in public news articles
- Comply with Israeli privacy law

### 3) Data Fetching
- **RSS sources**: Fetch feed, filter by keywords + date
- **Scrapers**: Search/browse relevant sections, parse HTML
- Filter results by date (last X days configurable)
- Add 1-2 second delays between requests
- Clear User-Agent identification
- Handle timeouts and errors gracefully
- Save fixture files for tests (RSS XML, HTML)

### 4) Classification
- LLM classifies: relevance, category, sub_category, confidence
- Categories: brothel, prostitution, pimping, trafficking, enforcement
- Sub-categories: closure/opening, arrest/sentence, rescue, etc.
- Keep prompts short, Hebrew-aware
- Log decisions for review
- Never fabricate article details

### 5) Deduplication
- Same story from multiple sources = single unified item
- Group by title similarity
- Keep all source links in output
- Pick best snippet for summary

### 6) Notifications
- CLI: default output, print to stdout
- Telegram: optional, for alerts
- Never log secrets (bot tokens)

---

## Secrets & Configuration

Never commit secrets. Use env vars:

```
ANTHROPIC_API_KEY
DENBUST_TELEGRAM_BOT_TOKEN   # Optional
DENBUST_TELEGRAM_CHAT_ID     # Optional
```

---

## Testing

- Unit tests: keyword matching, dedup logic, LLM response parsing
- Integration tests: pipeline with mocked HTTP
- No live network calls in tests

```bash
pytest -q
pytest -q -k dedup
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Install (dev) | `pip install -e ".[dev]"` |
| Run scan | `denbust scan` |
| Run with config | `denbust scan --config agents/news.yaml` |
| Tests | `pytest` |
| Lint | `ruff check .` |
| Format | `ruff format .` |
| Type check | `mypy src/` |
