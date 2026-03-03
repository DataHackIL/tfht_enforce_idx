# AGENTS.md - tfht_enforce_idx (denbust)

This file defines contributor and coding-agent rules for this repository.

## Scope and intent

- Keep changes focused, minimal, and test-backed.
- Preserve current behavior unless the issue explicitly allows changes.
- Prefer deterministic, explicit behavior over hidden heuristics.

## Project basics

- Package: `denbust`
- Purpose: monitor Israeli enforcement reporting for anti-brothel and trafficking laws.
- Current scope: Phase 1 news monitoring pipeline.
- Runtime: CLI (`denbust scan`) and scheduled runs.
- Primary source path: `src/denbust/`.

## Repository layout

```
tfht_enforce_idx/
├── src/denbust/
│   ├── cli.py
│   ├── config.py
│   ├── pipeline.py
│   ├── data_models.py
│   ├── sources/            # RSS + scraper sources
│   ├── classifier/         # Relevance/category classification
│   ├── dedup/              # Cross-source deduplication
│   ├── output/             # CLI/Telegram formatting and output
│   └── store/              # Seen-URL persistence
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── agents/                 # Scan config files
├── docs/
└── pyproject.toml
```

## Core quality requirements

- Use Python 3.11+ and full type annotations for new/edited code.
- Run formatting/lint/type checks before commit:
  - `ruff format .`
  - `ruff check .`
  - `mypy src/`
- Run relevant tests for touched code (at minimum):
  - `pytest -q`
  - `pytest -q -k dedup` when dedup logic changes

## Domain and legal constraints

- Use only public, freely accessible sources.
- Prefer RSS over scraping where possible.
- Respect robots.txt and use polite request pacing.
- Never fabricate article details in summaries/classification.
- Never store secrets or non-public PII.

## Pipeline expectations

- Filter articles by configured date window and relevant topics.
- Classifier output must include relevance, category, and confidence.
- Dedup should unify cross-source duplicates while preserving all source links.
- CLI output is default; Telegram is optional and must never leak bot tokens.

## PR expectations

- Keep one logical change per PR.
- Add/update regression tests when fixing bugs.
- Summarize behavior changes and include test evidence.
- Keep CI green before merge.

## Local overrides (optional, untracked)

- If `LOCAL_AGENTS.md` exists at repo root, treat it as additive local instructions.
- `LOCAL_AGENTS.md` must remain untracked.
- On conflicts, repository and security policy take precedence.

## Security and secrets

- Never commit credentials or machine-local secrets.
- Use environment variables for runtime secrets:
  - `ANTHROPIC_API_KEY`
  - `DENBUST_TELEGRAM_BOT_TOKEN`
  - `DENBUST_TELEGRAM_CHAT_ID`
