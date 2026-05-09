# LOCAL_AGENTS.example.md

Copy this file to `LOCAL_AGENTS.md` for machine-specific agent instructions.
`LOCAL_AGENTS.md` is intentionally untracked.

## Optional local MCP routing

- Use standard `git` and `gh` CLI first for repository and GitHub work.
- Use git/GitHub MCP tools only for CLI gaps, unavailable CLI, or explicit user request.
- If an MCP is needed, use your local git MCP namespace (`mcp__git_<your_server>__*`) and the matching GitHub MCP.

## Optional local workflow preferences

- Prefer standard CLI tools over MCP tools when both are available and equivalent.
- Prefer your local Python environment manager (`uv`, `venv`, or equivalent).
- Keep local-only paths, credentials, and machine quirks in `LOCAL_AGENTS.md` only.
