# CI and Agent Integrations Skill

Use this guidance when working on GitHub Actions, coverage, Codecov, or `pr-agent-context`.

## Coverage Model

- `unit-tests` and `integration-tests` produce raw coverage artifacts
- `coverage` is the canonical combined coverage producer
- `coverage` generates `coverage.xml`, uploads the `coverage-xml` artifact, and uploads to Codecov
- Prefer reusing the existing `coverage` job rather than creating extra coverage assembly paths

## PR Agent Context

- The main PR workflow invokes `pr-agent-context` from `ci-test.yml`
- Refresh runs live in `.github/workflows/pr-agent-context-refresh.yml`
- Refresh is intentionally separate from main CI
- `pr-agent-context` consumes the `coverage-xml` artifact directly for patch coverage
- When changing patch coverage behavior, preserve the shared `coverage` job as the source of truth

## Validation Workflows

- `codecov-yaml-validate.yml` validates `codecov.yml` / `.codecov.yml`
- `pyproject-validate.yml` validates `pyproject.toml`
- `validate-agent-plan` in `ci-test.yml` validates `.agent-plan.md` structure and wording
- Keep those workflows narrowly scoped to the files they validate

## Editing Rules

- Keep workflow responsibilities minimal and explicit
- Avoid adding duplicate status sources when an existing workflow or artifact already provides the needed data
- Preserve current prompt template usage for `pr-agent-context` unless the task explicitly changes it
- When updating agent guidance, keep `AGENTS.md` and `CLAUDE.md` aligned
- Treat `.agent-plan.md` drift as CI-visible repo hygiene, not optional documentation cleanup
