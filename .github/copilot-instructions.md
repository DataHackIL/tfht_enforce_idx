# Copilot Instructions

Read [AGENTS.md](../AGENTS.md) first for repo-wide guidance.

Treat `.agent-plan.md` according to `AGENTS.md`:
- write it as post-merge mainline truth
- keep the required section headings and status markers intact
- do not reintroduce branch-local wording such as `in progress`

Use the repo-local skill docs for task-specific guidance:
- News sources, scraping, Mako behavior, and local runs: [.github/skills/news-sources/SKILL.md](skills/news-sources/SKILL.md)
- CI, coverage, Codecov, and `pr-agent-context`: [.github/skills/ci-and-agent-integrations/SKILL.md](skills/ci-and-agent-integrations/SKILL.md)

Do not duplicate or contradict `AGENTS.md`. If repository guidance changes, update `AGENTS.md`; `CLAUDE.md` mirrors it.
