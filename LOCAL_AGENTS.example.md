# LOCAL_AGENTS.example.md

Copy this file to `LOCAL_AGENTS.md` for machine-specific agent instructions.
`LOCAL_AGENTS.md` is intentionally untracked.

## Optional local MCP routing

- For local git operations, prefer your local git MCP namespace (`mcp__git_<your_server>__*`).
- Use GitHub MCP tools for remote repository and PR operations.

## Optional local workflow preferences

- Prefer MCP tools over shell when both are available.
- Prefer your local Python environment manager (`uv`, `venv`, or equivalent).
- Keep local-only paths, credentials, and machine quirks in `LOCAL_AGENTS.md` only.
