# MVP Spec (Phase 1)

Phase 1 focuses on **news monitoring only**. Court record scraping is deferred to Phase 2.

Scan Israeli news websites for enforcement-related stories, deduplicate across sources, present in unified format.

---

## What to Look For

### Primary Topics
- **Brothel raids/closures** - police operations shutting down locations
- **Prostitution arrests** - individuals caught in prostitution-related offenses
- **Pimp arrests** - סרסורים caught, charged, or sentenced
- **Human trafficking** - סחר בבני אדם cases and arrests
- **Closure orders** - צווי סגירה issued against locations

### Secondary Topics
- **Massage parlor/spa busts** - often fronts for prostitution
- **Online platform takedowns** - escort sites, ads platforms
- **Police operations** - מבצעי משטרה targeting prostitution rings
- **Court sentences** - verdicts and sentencing in related cases
- **Administrative fines** - קנסות for prostitution consumption
- **Location exposés** - reports identifying active prostitution areas
- **Trafficking victim rescues** - חילוץ נפגעות סחר

---

## News Sources

MVP uses a hybrid approach: **2 RSS sources + 2 scraped sources**.

### RSS Sources (simpler, more reliable)

| Source | RSS URL | Notes |
|--------|---------|-------|
| Ynet | `ynet.co.il/Integration/StoryRss2.xml` | General news feed |
| Walla | `rss.walla.co.il/feed/1` | News feed |

### Scraped Sources (search results pages)

| Source | Search URL Pattern | Notes |
|--------|-------------------|-------|
| Mako | `mako.co.il/AjaxPage?...&q={query}` | Men section has relevant content |
| Maariv | `maariv.co.il/news/law` + search | Law/crime section |

### Strategy

1. **RSS**: Fetch feed, filter items by date (last X days), pre-filter by keywords
2. **Scraping**: Search with Hebrew keywords, parse results, filter by date
3. Respect rate limits: 1-2 second delays between requests

### Example Articles (for validation)

See [articles_examples.md](./articles_examples.md) for real examples found in the last 2 weeks. The system should detect these types of articles:
- Mako "men" section crime reports
- Maariv law/crime section
- Cross-published stories (same story on multiple sites)

---

## Core Abstractions

```
Config           - YAML: sources list, keywords, days back, notifier settings
Source           - Protocol: fetch(days) -> list[RawArticle]
  ├─ RSSSource   - Fetch RSS feed, filter by keywords + date
  └─ Scraper     - Search with keywords, parse HTML, filter by date
RawArticle       - url, title, snippet, date, source_name
Classifier       - LLM: is_relevant(article) -> bool, category, sub_category
Deduplicator     - Group same story across sources by similarity
UnifiedItem      - Canonical representation: headline, summary, sources[], date, category, sub_category
Notifier         - Protocol: send(items) -> None
```

---

## Data Flow

```
┌──────────────┐     ┌──────────────┐
│ RSS Sources  │     │  Scrapers    │
│ (Ynet,Walla) │     │ (Mako,Maariv)│
└──────┬───────┘     └──────┬───────┘
       │ feedparser         │ beautifulsoup
       │ + keyword filter   │ + date filter
       └─────────┬──────────┘
                 ▼
         ┌──────────────┐
         │ Raw Articles │
         └──────┬───────┘
                ▼
         ┌──────────────┐
         │  Classifier  │ (LLM: relevant? category? sub_category?)
         └──────┬───────┘
                ▼
         ┌──────────────┐
         │ Deduplicator │ (group same story across sources)
         └──────┬───────┘
                ▼
         ┌──────────────┐
         │   Renderer   │ → CLI or Telegram
         └──────────────┘
```

1. **RSS sources**: Fetch feed → filter by keywords → filter by date
2. **Scrapers**: Search with keywords → parse HTML → filter by date
3. Combine all raw articles
4. Filter out already-seen URLs
5. Classify with LLM (relevance + category + sub_category)
6. Deduplicate: group articles about same story
7. Render unified items with all source links
8. Output to CLI or Telegram

---

## MVP Scope

### In Scope
- **4 news sources**: 2 via RSS (Ynet, Walla) + 2 via scraping (Mako, Maariv)
- **Configurable time window**: scan last X days (e.g., 7 or 14 days)
- **Hybrid fetching**: RSS feeds + search page scraping
- **CLI output**: `denbust scan` prints unified items
- **Telegram notifications**: optional, for new items
- **LLM classification**: relevance + category + sub_category
- **Cross-source deduplication**: same story = one item with multiple source links
- **Simple persistence**: track seen URLs to avoid re-alerting
- **Validation**: should find articles like those in articles_examples.md

### Out of Scope (Phase 1)
- Court records scraping (Phase 2)
- Full article text extraction (search snippets sufficient)
- Historical database / analytics
- Web dashboard
- Email notifications
- Scheduled daemon (use cron for now)

---

## Tech Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Language | Python 3.11+ | Type hints, async |
| HTTP | `httpx` | Async, timeouts, retries |
| RSS | `feedparser` | Standard RSS parsing |
| HTML Parsing | `beautifulsoup4` + `lxml` | Robust scraping |
| Config | `pydantic` + `pyyaml` | Validation |
| CLI | `typer` | Simple, typed |
| LLM | `anthropic` (Claude) | Good Hebrew support |
| Dedup | `difflib` | Title similarity scoring |
| Persistence | JSON file | Simple seen-URLs tracking |
| Telegram | `httpx` direct | Lightweight |
| Testing | `pytest` + `respx` | Mock HTTP |
| Lint | `ruff` | Fast |

---

## File Structure

```
denbust/
├── src/denbust/
│   ├── __init__.py
│   ├── cli.py              # typer CLI
│   ├── config.py           # Config models
│   ├── models.py           # RawArticle, UnifiedItem
│   ├── pipeline.py         # Orchestration
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py         # Source protocol
│   │   ├── rss.py          # Generic RSS fetcher (Ynet, Walla)
│   │   ├── mako.py         # Mako scraper
│   │   └── maariv.py       # Maariv scraper
│   ├── classifier/
│   │   ├── __init__.py
│   │   └── relevance.py    # LLM classification
│   ├── dedup/
│   │   ├── __init__.py
│   │   └── similarity.py   # Cross-source deduplication
│   ├── output/
│   │   ├── __init__.py
│   │   ├── formatter.py    # Unified item rendering
│   │   └── telegram.py     # Telegram notifier
│   └── store/
│       └── seen.py         # Track seen URLs (JSON)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│       ├── rss/            # Sample RSS XML
│       └── html/           # Sample search result HTML
├── data/
│   └── seen.json           # Persisted seen URLs
├── agents/
│   └── news.yaml           # Default config
└── pyproject.toml
```

---

## Example Config

```yaml
# agents/news.yaml
name: enforcement-news
days: 14  # How many days back to search

sources:
  # RSS sources - filter by keywords after fetching
  - name: ynet
    type: rss
    url: https://www.ynet.co.il/Integration/StoryRss2.xml
    enabled: true

  - name: walla
    type: rss
    url: https://rss.walla.co.il/feed/1
    enabled: true

  # Scraped sources - search with keywords
  - name: mako
    type: scraper
    enabled: true

  - name: maariv
    type: scraper
    enabled: true

keywords:  # For RSS filtering and scraper searches
  - זנות
  - בית בושת
  - סרסור
  - סחר בבני אדם
  - צו סגירה
  - ליווי
  - נערות ליווי
  - תעשיית המין
  - עיסוי חשוד
  - זירת זנות

classifier:
  provider: anthropic
  model: claude-sonnet-4-20250514
  # ANTHROPIC_API_KEY from env

dedup:
  similarity_threshold: 0.7  # Title similarity for grouping

output:
  format: cli  # or 'telegram'
  # DENBUST_TELEGRAM_BOT_TOKEN, DENBUST_TELEGRAM_CHAT_ID from env

store:
  path: data/seen.json
```

---

## Categories & Sub-categories

Classification uses a two-level system: **category** + **sub-category**.

| Category | Sub-category | Description |
|----------|--------------|-------------|
| `brothel` | `closure` | Raid, shutdown, closure order issued |
| `brothel` | `opening` | Reopening, still operating despite order, new location discovered |
| `prostitution` | `arrest` | Individual arrested for prostitution-related offense |
| `prostitution` | `fine` | Administrative fine issued |
| `pimping` | `arrest` | Pimp arrested |
| `pimping` | `sentence` | Pimp sentenced/convicted |
| `trafficking` | `arrest` | Trafficking suspect arrested |
| `trafficking` | `rescue` | Victims rescued |
| `trafficking` | `sentence` | Trafficker sentenced |
| `enforcement` | `operation` | Police operation, general enforcement activity |
| `enforcement` | `other` | Other enforcement-related news |

The `brothel.opening` sub-category is particularly important - it indicates potential enforcement gaps where places continue operating.

---

## Classification Prompt (Draft)

```
You classify Hebrew news articles for relevance to anti-prostitution enforcement.

Given a news headline and snippet, determine:
1. Is this relevant to: brothels, prostitution, pimping, human trafficking, or enforcement?
2. Category: brothel | prostitution | pimping | trafficking | enforcement | not_relevant
3. Sub-category (if relevant):
   - brothel: closure | opening
   - prostitution: arrest | fine
   - pimping: arrest | sentence
   - trafficking: arrest | rescue | sentence
   - enforcement: operation | other
4. Confidence: high | medium | low

Article:
כותרת: {title}
תקציר: {snippet}

Respond JSON only:
{"relevant": true/false, "category": "...", "sub_category": "...", "confidence": "..."}
```

---

## Unified Item Format (Output)

```
🚨 פשיטה על בית בושת ברמת גן
תאריך: 2026-02-15
קטגוריה: בית בושת » סגירה

תקציר: המשטרה פשטה על דירה ברמת גן שפעלה כבית בושת. נעצרו 3 חשודים...

מקורות:
• Ynet: https://ynet.co.il/...
• Mako: https://mako.co.il/...
• Walla: https://walla.co.il/...
```

Category icons:
- 🚨 brothel.closure (raid/shutdown)
- ⚠️ brothel.opening (still operating / reopened)
- 👮 arrest (prostitution/pimping/trafficking)
- ⚖️ sentence
- 🆘 trafficking.rescue
- 🔍 enforcement.operation

---

## Environment Variables

```bash
ANTHROPIC_API_KEY=sk-ant-...
DENBUST_TELEGRAM_BOT_TOKEN=123456:ABC...  # Optional
DENBUST_TELEGRAM_CHAT_ID=123456789        # Optional
```

---

## Done When

- [ ] `denbust scan` fetches from 4 sources (2 RSS + 2 scrapers)
- [ ] `--days` flag controls how far back to search (default from config)
- [ ] RSS sources: fetch feed, filter by keywords + date
- [ ] Scrapers: search with keywords, filter by date
- [ ] LLM classifies relevance + category + sub_category
- [ ] Same story from multiple sources = single unified item
- [ ] Unified items printed to CLI in readable Hebrew format
- [ ] Optional: Telegram notification for new items
- [ ] seen.json tracks URLs to avoid duplicate alerts on re-run
- [ ] **Validation**: system finds articles like those in articles_examples.md
- [ ] Unit tests for RSS parsing, HTML scraping, dedup logic
- [ ] Integration test with mocked responses

---

## Phase 2 Preview (Not in MVP)

- Israeli Courts website scraping
- Correlation: news story → court case
- Location extraction and mapping
- Historical enforcement database
- Web dashboard with statistics
