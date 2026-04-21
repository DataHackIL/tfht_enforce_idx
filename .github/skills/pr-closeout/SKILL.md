# PR Closeout Skill

Use this guidance when finishing a feature branch, tracked plan item, or any task that is expected to end in a GitHub pull request.

## Completion Rule

- Work is not complete until a non-draft PR is open on GitHub.
- The PR must target `main` unless the task explicitly says otherwise.
- The PR must have a detailed description that is ready for human review.
- The PR must have appropriate labels and an assigned GitHub milestone.

## Preferred Tooling

- Use the repo-specific GitHub MCP first for repository reads and GitHub mutations when the needed capability exists.
- Use the repo-specific Git MCP first for local git actions when available.
- Fall back to `gh` or local `git` only for capabilities the MCP path does not expose cleanly.

## Required Closeout Steps

1. Confirm the branch contains only the intended scope.
2. Run the narrowest relevant validation for the touched files.
3. Stage and commit the intended changes.
4. Push the branch to `origin`.
5. Open a non-draft PR against `main`.
6. Write a detailed PR body covering:
   - what changed
   - why it changed
   - user or operator impact
   - validation performed
7. Apply appropriate labels for the workstream or tracked plan item.
8. Assign the PR to the relevant GitHub milestone.
9. If the needed label or milestone does not exist, create it before finishing.
10. Update any checked-in planning or agent-context files that should describe the post-merge state.
11. Run `python scripts/validate_agent_plan.py` before opening the PR if `.agent-plan.md` changed.

## Repo-Specific Expectations

- For tracked plan work, keep `.agent-plan.md`, `README.md`, and relevant human-facing planning docs aligned with the PR's expected post-merge state.
- Rewrite `.agent-plan.md` as post-merge mainline truth rather than branch-local progress notes.
- Keep `.agent-plan.md` in the validated format: required headings, exact mainline status fields, and exactly one `[next]` item.
- Do not treat a local branch, a pushed branch, or a draft PR as the terminal state.
- If GitHub publication is blocked, report the blocker explicitly; do not claim the task is complete.
