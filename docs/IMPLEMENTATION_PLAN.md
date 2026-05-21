# Implementation Plan (Phase 1)

Task list for implementing Phase 1 MVP as defined in [MVP_SPEC.md](./MVP_SPEC.md).

**Phase 1 Goal**: Scan Israeli news sites, classify relevance, deduplicate across sources, output unified items.

---

## Task Dependency Graph

```
#1 Project scaffolding
 └─► #2 Core models
      ├─► #3a RSS source (Ynet, Walla) ──┐
      ├─► #3b Scrapers (Mako, Maariv) ───┤
      ├─► #4 LLM classifier ─────────────┼─► #7 Pipeline + CLI
      ├─► #5 Deduplicator ───────────────┤         │
      ├─► #6 Output formatter ───────────┤         │
      └─► #6a Seen URL tracker ──────────┘         │
                                                   ▼
      #8 Unit tests ◄──────────────────────► #9 Integration tests
                              │                    │
                              └────────┬───────────┘
                                       ▼
                          #10 Validation + manual test
                                       │
                                       ▼
                          #11 Telegram notifier (later)
```

---

## Tasks

### Phase 1.1: Foundation

#### #1 Set up project scaffolding
- [ ] Create `pyproject.toml` with dependencies:
  - `httpx` (HTTP client)
  - `feedparser` (RSS parsing)
  - `beautifulsoup4` + `lxml` (HTML parsing)
  - `pydantic` + `pyyaml` (config)
  - `typer` (CLI)
  - `anthropic` (LLM)
  - Dev: `pytest`, `respx`, `pytest-mock`, `ruff`, `mypy`
- [ ] Create `src/denbust/` package structure
- [ ] Create basic CLI entry point (`denbust scan`)
- [ ] Set up `ruff.toml` and `mypy.ini`
- [ ] Create `data/` directory

#### #2 Implement core models
**Blocked by**: #1

- [ ] Create `models.py`:
  - `RawArticle`: url, title, snippet, date, source_name
  - `UnifiedItem`: headline, summary, sources (list), date, category, sub_category
- [ ] Create `config.py` with pydantic models:
  - `SourceConfig`: name, enabled
  - `ClassifierConfig`: provider, model
  - `DedupConfig`: similarity_threshold
  - `OutputConfig`: format (cli/telegram)
  - `StoreConfig`: path
  - Root `Config`: name, days, search_keywords, sources, classifier, dedup, output, store
- [ ] Support env var overrides for secrets
- [ ] CLI flag `--days` to override config

---

### Phase 1.2: Components

#### #3a Implement RSS source
**Blocked by**: #2

- [ ] Create `sources/base.py` with `Source` protocol:
  - `fetch(days: int) -> list[RawArticle]`
- [ ] Create `sources/rss.py`:
  - Generic RSS fetcher using `feedparser`
  - Filter items by keywords (title + summary)
  - Filter by date (last X days)
  - Extract: title, url, snippet, published date
  - Handle Hebrew encoding (UTF-8)
- [ ] Configure for Ynet and Walla RSS feeds
- [ ] Create `tests/fixtures/rss/` with sample XML

#### #3b Implement scrapers (Mako, Maariv)
**Blocked by**: #2

- [ ] Create `sources/mako.py`:
  - Mako "men" section has relevant crime content
  - Search or browse relevant sections
  - Parse HTML with BeautifulSoup
  - Extract: title, url, snippet, date
  - Filter by date (last X days)
- [ ] Create `sources/maariv.py`:
  - Law/crime section: `maariv.co.il/news/law`
  - Search functionality
  - Parse search results HTML
- [ ] Handle Hebrew encoding (UTF-8)
- [ ] Respect rate limits: 1-2 second delays
- [ ] Create `tests/fixtures/html/` with sample pages
- [ ] **Validate**: should find articles from articles_examples.md

#### #4 Implement LLM classifier
**Blocked by**: #2

- [ ] Create `classifier/relevance.py`:
  - `classify(article: RawArticle) -> ClassificationResult`
  - `ClassificationResult`: relevant, category, sub_category, confidence
  - Categories: brothel, prostitution, pimping, trafficking, enforcement
  - Sub-categories: closure/opening, arrest/fine/sentence, rescue, operation
  - Hebrew prompt for Claude
  - Parse JSON response
  - Handle errors gracefully (default to not_relevant)
- [ ] Keep token usage low (short prompt, snippet only)

#### #5 Implement deduplicator
**Blocked by**: #2

- [ ] Create `dedup/similarity.py`:
  - `Deduplicator` class
  - `group(articles: list[RawArticle]) -> list[ArticleGroup]`
  - Use title similarity (difflib.SequenceMatcher or similar)
  - Configurable threshold (default 0.7)
  - Group = list of articles about same story
- [ ] Pick "best" article as primary (longest snippet, earliest date)

#### #6 Implement output formatter
**Blocked by**: #2

- [ ] Create `output/formatter.py`:
  - `format_unified_item(item: UnifiedItem) -> str`
  - Hebrew-friendly format
  - Include: headline, date, category » sub_category, summary, source links
  - Emoji indicators: 🚨 closure, ⚠️ opening, 👮 arrest, ⚖️ sentence, 🆘 rescue
- [ ] CLI output: print to stdout

#### #6a Implement seen URL tracker
**Blocked by**: #2

- [ ] Create `store/seen.py`:
  - `SeenStore` class
  - `is_seen(url: str) -> bool`
  - `mark_seen(urls: list[str])`
  - `load()` / `save()` to JSON file
- [ ] Track URLs to avoid duplicate alerts on re-run

---

### Phase 1.3: Integration

#### #7 Implement pipeline and CLI
**Blocked by**: #3a, #3b, #4, #5, #6, #6a

- [ ] Create `pipeline.py` orchestrating:
  1. Load config from YAML
  2. For RSS sources: fetch feed, filter by keywords + date
  3. For scrapers: search with keywords, filter by date
  4. Combine all raw articles
  5. Filter out already-seen URLs
  6. Classify with LLM (relevance + category + sub_category)
  7. Keep only relevant articles
  8. Deduplicate across sources
  9. Format as unified items
  10. Output to CLI
  11. Mark URLs as seen
- [ ] Create `cli.py`:
  - `denbust scan [--config PATH] [--days N]`
  - Default config: `agents/news.yaml`
  - Logging: source counts, classified counts, final items

---

### Phase 1.4: Testing

#### #8 Write unit tests
**Blocked by**: #3a, #3b, #4, #5, #6

- [ ] `tests/unit/test_config.py` - config validation
- [ ] `tests/unit/test_rss.py` - RSS parsing + keyword filtering
- [ ] `tests/unit/test_scrapers.py` - HTML parsing for Mako, Maariv
- [ ] `tests/unit/test_classifier.py` - LLM response parsing
- [ ] `tests/unit/test_dedup.py` - similarity grouping
- [ ] `tests/unit/test_formatter.py` - output format
- [ ] `tests/fixtures/rss/` - sample RSS XML
- [ ] `tests/fixtures/html/` - sample HTML from Mako, Maariv

#### #9 Write integration tests
**Blocked by**: #7

- [ ] `tests/integration/test_pipeline.py`:
  - Full pipeline with mocked HTTP (respx)
  - Mocked LLM responses
  - Verify dedup + formatting
- [ ] `tests/integration/test_scrapers.py`:
  - Each scraper with fixture HTML

---

### Phase 1.5: Validation

#### #10 Validation + manual test
**Blocked by**: #9

- [ ] Create `agents/news.yaml` with working config
- [ ] Manual test: run against real sources (with delays!)
- [ ] **Validate against articles_examples.md**:
  - Run `denbust scan --days 14`
  - Verify system finds Mako articles from examples
  - Verify system finds Maariv articles from examples
  - Check cross-source deduplication works
- [ ] Verify Hebrew display is correct in CLI
- [ ] Document rate limiting (delays between requests)
- [ ] Update README with usage instructions

---

### Phase 1.6: Notifications (Later)

#### #11 Telegram notifier
**Blocked by**: #10 (add after core pipeline is validated)

- [ ] Create `output/telegram.py`:
  - `TelegramNotifier` class
  - `send(items: list[UnifiedItem])`
  - Format messages for Telegram (markdown)
  - Handle message length limits
  - Never log bot token
- [ ] Wire into pipeline based on config

---

## Estimated Effort

### Core Pipeline (Priority)

| Task | Complexity | Est. Time |
|------|------------|-----------|
| #1 Scaffolding | Low | 1h |
| #2 Models + Config | Low | 1h |
| #3a RSS source | Low | 1h |
| #3b Scrapers (Mako, Maariv) | Medium | 3h |
| #4 LLM classifier | Medium | 2h |
| #5 Deduplicator | Medium | 2h |
| #6 Formatter (CLI) | Low | 1h |
| #6a Seen tracker | Low | 1h |
| #7 Pipeline + CLI | Medium | 2h |
| #8 Unit tests | Medium | 2h |
| #9 Integration tests | Medium | 2h |
| #10 Validation | Low | 1h |
| **Subtotal** | | **~19h** |

### Later

| Task | Complexity | Est. Time |
|------|------------|-----------|
| #11 Telegram notifier | Low | 1h |

---

## Notes

- News site HTML structures change frequently; keep scrapers modular and testable
- Save fixture HTML files for each site to catch breaking changes
- Start with conservative classification (fewer false positives)
- Log all classifications for review and prompt tuning
- Respect rate limits: add 1-2 second delays between requests
- Some sites may block automated access; use proper User-Agent

---

## Phase 2 (Future)

After Phase 1 is stable:
- Court records scraping (הרשות השופטת)
- News ↔ court case correlation
- SQLite database for history
- Location extraction
- Web dashboard

---

## Phase 3 — Local Pre-Classification Filter Cascade (LPF-PR-XX)

**Detailed design:** [docs/local_prefilter_cascade_design.md](./local_prefilter_cascade_design.md)
**Implementation plan:** [docs/local_prefilter_cascade_implementation_plan.md](./local_prefilter_cascade_implementation_plan.md)

**Goal:** Insert a local, non-LLM-API-based filtering cascade between the discovery/triage layer and the Claude-Sonnet relevance classifier, to drop high-confidence true negatives before they consume paid LLM budget. Target ≥ 50% Claude-call reduction at recall ≥ 0.98.

**Architecture:** four cascade stages, each calibrated to ≥ 99% per-stage recall:

- **Stage A** — scored lexicon + domain reputation posterior + URL heuristics (< 100 µs / candidate).
- **Stage B** — trained text classifier (Naive Bayes on char n-grams; SetFit option) (~ 5 ms / candidate).
- **Stage C** — multilingual sentence-embedding similarity (centroid + FAISS kNN) over `intfloat/multilingual-e5-large` (~ 30 ms / candidate).
- **Stage D** — local SLM (DictaLM-2.0-Instruct via MLX) scored by token logprobs of `כן` / `לא` (~ 800 ms / candidate).

Both a thin (pre-scrape, A+B) and a thick (post-scrape, A–D) pass run; both write structured `PrefilterDecision` telemetry. Runtime modes: `off | shadow | enforce`. Default ships `off`; promoted to `shadow` in `LPF-PR-09`; operator action required to flip to `enforce` after a 7-day shadow window meets the recall floor.

### Task summary

| PR | Goal | Status |
|----|------|--------|
| LPF-PR-01 | Foundation: package, models, config, telemetry stubs | planned |
| LPF-PR-02 | Labeled-candidates dataset assembly | planned |
| LPF-PR-03 | Stage A: lexicon + domain reputation + URL heuristics | planned |
| LPF-PR-04 | Stage B: Naive Bayes default | planned |
| LPF-PR-05 | Stage B: SetFit alternative | planned |
| LPF-PR-06 | Stage C: embedding similarity (centroid + FAISS kNN) | planned |
| LPF-PR-07 | Stage D: local SLM judge via MLX | planned |
| LPF-PR-08 | Cascade orchestrator + pipeline integration | planned |
| LPF-PR-09 | Shadow-mode telemetry harness + Supabase migration | planned |
| LPF-PR-10 | Calibration tooling + golden-set regression CI | planned |
| LPF-PR-11 (optional) | Claude-distilled student (LoRA on DictaBERT) | later |
| LPF-PR-12 (optional) | Active learning loop + BERTopic cluster filter | later |

See [docs/local_prefilter_cascade_implementation_plan.md](./local_prefilter_cascade_implementation_plan.md) for per-PR scope, file paths, dependencies, test plans, and acceptance criteria.
