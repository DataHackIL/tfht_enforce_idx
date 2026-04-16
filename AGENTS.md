# AGENTS.md

`CLAUDE.md` must remain a symlink to this file.

## Repo Rules

- Use the repo-specific GitHub MCP when available; use local `git` only if no MCP for this repo is exposed.
- Do not commit secrets, tokens, browser state, or personal config.
- Keep `LOCAL_AGENTS.md` untracked; treat it as additive only.
- Prefer editing checked-in agent context in `AGENTS.md`, `llms.txt`, and `.agent-plan.md`; do not move dynamic state back into `AGENTS.md`.

## Branch And PR Rules

- Default branch prefix for agent work: `codex/`.
- Keep branches single-purpose.
- Open PRs against `main`.
- Preserve `CLAUDE.md -> AGENTS.md` when changing repo guidance.

## Environment

- Python `>=3.11`
- Install dev dependencies:

```bash
pip install -e ".[dev]"
```

- Install browser runtime before live Mako runs:

```bash
python -m playwright install chromium
```

## Required Validation Commands

Run the narrowest relevant checks for the files you changed. For cross-cutting changes, run the full set.

```bash
ruff format .
ruff check .
mypy src/
pytest -q
```

Useful targeted commands:

```bash
pytest -q tests/unit
pytest -q tests/integration -k Mako
denbust scan --config agents/news/local.yaml
denbust run --dataset news_items --job discover --config agents/news/local.yaml
denbust run --dataset news_items --job scrape_candidates --config agents/news/local.yaml
denbust release --dataset news_items --config agents/release/news_items.yaml
denbust backup --dataset news_items --config agents/backup/news_items.yaml
```

## Code Standards

- Full type annotations are required.
- Ruff is the formatter and linter of record.
- Mypy runs in strict mode; keep new code strict-clean.
- Keep public behavior and CLI names backward compatible unless the task explicitly changes them.
- Prefer small, composable modules over large cross-cutting rewrites.

## Architecture Boundaries

- Dataset/job identity is defined through `src/denbust/models/` and `src/denbust/datasets/`; do not hardcode ad hoc dataset/job routing elsewhere.
- State-path resolution must go through:
  - `src/denbust/store/state_paths.py`
  - `src/denbust/discovery/state_paths.py`
- Discovery/candidacy models live under `src/denbust/discovery/`; ingest/release/backup logic must consume those models instead of redefining candidate state.
- `news_items` operational/public record schemas live in `src/denbust/news_items/models.py`; reuse them instead of introducing parallel row schemas.
- Release and backup integrations belong under `src/denbust/publish/` and `src/denbust/news_items/`; avoid embedding publication logic inside unrelated modules.
- Source adapters belong under `src/denbust/sources/`; source-specific scraping logic should not leak into CLI or config modules.
- Config normalization lives in `src/denbust/config.py`; prefer env/YAML plumbing there instead of scattered `os.environ` reads.

## Fetching And Data Handling Rules

- Prefer public, stable interfaces; use RSS where a stable feed exists.
- Keep source failures isolated per source/query path; do not abort the entire run on one source failure.
- Normalize Mako URLs before deduplication or seen-state writes so query params do not fork duplicate records.
- Do not fabricate article details or inferred facts beyond what exists in the source text and structured model outputs.

## Config And Secrets

- Keep durable personal config outside the repo, for example under `~/.config/denbust/`.
- Prefer `output.formats` over legacy `output.format` in new config examples.
- Supported sensitive env vars include:
  - `ANTHROPIC_API_KEY`
  - `DENBUST_*`
- Never add secrets to fixtures, examples, docs, or workflow YAML.

## Testing Constraints

- No live network calls in tests.
- No live browser scraping in CI tests.
- Use fixtures/mocked HTTP/rendered HTML for source tests.
- When changing discovery, ingestion, or persistence behavior, add or update tests in `tests/unit` or `tests/integration`.

## CI Notes

- Main CI workflow: `.github/workflows/ci-test.yml`
- Reuse the existing coverage flow instead of adding duplicate coverage producers.
- Prefer artifact reuse over recomputing the same coverage or validation outputs in multiple jobs.
