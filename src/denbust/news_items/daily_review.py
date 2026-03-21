"""AI-powered daily review of latest news_items ingest artifacts."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import anthropic
import httpx
from anthropic.types import TextBlock
from pydantic import BaseModel, Field

REVIEW_PROMPT = """You are reviewing the latest automated ingest run for a metadata-only public dataset
of Israeli news items about prostitution, brothels, trafficking, and enforcement.

Your job is to decide whether the latest run merits opening one or more GitHub issues.

Open issues only for actionable engineering problems or suspicious regressions, for example:
- source failures or sustained zero-result behavior
- classifier/output anomalies
- likely false-negative runs that need investigation
- state/logging anomalies

Do not open issues for normal low-signal days with no likely bug.

Return JSON only:
{{
  "issues": [
    {{
      "fingerprint": "stable-kebab-case-id",
      "title": "Concise engineering issue title",
      "body_markdown": "Markdown body describing the problem, evidence, and suggested next step."
    }}
  ]
}}

Artifacts:
run_snapshot:
{run_snapshot_json}

debug_summary:
{debug_summary_json}

debug_log:
{debug_log_json}
"""

MAX_PROMPT_LIST_ITEMS = 10
MAX_PROMPT_STRING_LENGTH = 500


class ReviewArtifacts(BaseModel):
    """Resolved latest-ingest artifacts to review."""

    run_timestamp: str
    stem: str
    run_snapshot_path: Path
    debug_summary_path: Path
    debug_log_path: Path
    run_snapshot: dict[str, Any]
    debug_summary: dict[str, Any]
    debug_log: dict[str, Any]


class IssueCandidate(BaseModel):
    """A candidate GitHub issue returned by the review model."""

    fingerprint: str
    title: str
    body_markdown: str


class ReviewResult(BaseModel):
    """Structured review output."""

    issues: list[IssueCandidate] = Field(default_factory=list)


ReviewArtifacts.model_rebuild()
IssueCandidate.model_rebuild()
ReviewResult.model_rebuild()


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON file into a dict payload."""
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def issue_marker(fingerprint: str) -> str:
    """Build a hidden marker used to deduplicate AI-opened issues."""
    return f"<!-- denbust-review:{fingerprint} -->"


def normalize_fingerprint(raw: str, *, title: str) -> str:
    """Normalize a candidate fingerprint into a stable issue key."""
    source = raw.strip().lower() or title.strip().lower()
    normalized = re.sub(r"[^a-z0-9._-]+", "-", source).strip("-")
    return normalized or "unnamed-review-issue"


def extract_json_block(text: str) -> dict[str, Any]:
    """Parse a JSON object from a model response, with markdown-fence tolerance."""
    payload = text.strip()
    if payload.startswith(("```", "~~~")):
        fenced_match = re.match(
            r"^(?P<fence>`{3}|~{3})(?P<lang>\w+)?\s*\n?(?P<body>.*?)(?:\n?(?P=fence))?\s*$",
            payload,
            re.DOTALL,
        )
        if fenced_match:
            payload = fenced_match.group("body").strip()
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("Review response must be a JSON object")
    return data


def _compact_for_prompt(value: Any) -> Any:
    """Trim large debug payloads down to a compact prompt-safe structure."""
    if isinstance(value, dict):
        return {str(key): _compact_for_prompt(subvalue) for key, subvalue in value.items()}
    if isinstance(value, list):
        compact_items = [_compact_for_prompt(item) for item in value[:MAX_PROMPT_LIST_ITEMS]]
        if len(value) > MAX_PROMPT_LIST_ITEMS:
            compact_items.append({"_truncated_count": len(value) - MAX_PROMPT_LIST_ITEMS})
        return compact_items
    if isinstance(value, str) and len(value) > MAX_PROMPT_STRING_LENGTH:
        return value[:MAX_PROMPT_STRING_LENGTH].rstrip() + "... [truncated]"
    return value


def latest_daily_review_artifacts(
    *,
    state_root: Path,
    dataset_name: str = "news_items",
    job_name: str = "ingest",
    workflow_name: str = "daily-state-run",
) -> ReviewArtifacts:
    """Find the latest daily-ingest artifacts from the state repo."""
    runs_dir = state_root / dataset_name / job_name / "runs"
    logs_dir = state_root / dataset_name / job_name / "logs"
    candidates = sorted(logs_dir.glob("*.summary.json"), reverse=True)
    for summary_path in candidates:
        debug_summary = _load_json(summary_path)
        summary_workflow = str(debug_summary.get("workflow", {}).get("workflow_name") or "")
        if workflow_name and summary_workflow != workflow_name:
            continue

        stem = summary_path.name.removesuffix(".summary.json")
        debug_log_path = logs_dir / f"{stem}.json"
        run_snapshot_path = runs_dir / f"{stem}.json"
        if not debug_log_path.exists() or not run_snapshot_path.exists():
            continue

        return ReviewArtifacts(
            run_timestamp=str(debug_summary.get("run_timestamp")),
            stem=stem,
            run_snapshot_path=run_snapshot_path,
            debug_summary_path=summary_path,
            debug_log_path=debug_log_path,
            run_snapshot=_load_json(run_snapshot_path),
            debug_summary=debug_summary,
            debug_log=_load_json(debug_log_path),
        )

    raise FileNotFoundError(
        f"No complete {dataset_name}/{job_name} artifacts found for workflow '{workflow_name}'"
    )


class AnthropicDailyReviewer:
    """Use Anthropic to decide whether daily ingest issues should be opened."""

    def __init__(self, *, api_key: str, model: str) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    def review(self, artifacts: ReviewArtifacts) -> ReviewResult:
        """Review the latest artifacts and return issue candidates."""
        prompt = REVIEW_PROMPT.format(
            run_snapshot_json=json.dumps(
                artifacts.run_snapshot, ensure_ascii=False, sort_keys=True
            ),
            debug_summary_json=json.dumps(
                artifacts.debug_summary, ensure_ascii=False, sort_keys=True
            ),
            debug_log_json=json.dumps(
                _compact_for_prompt(artifacts.debug_log),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        response = self._client.messages.create(
            model=self._model,
            max_tokens=1400,
            messages=[{"role": "user", "content": prompt}],
        )
        text_parts: list[str] = []
        if response.content:
            for block in response.content:
                if isinstance(block, TextBlock):
                    text_parts.append(block.text)
        text = "\n".join(text_parts).strip()
        if not text:
            return ReviewResult()
        try:
            payload = extract_json_block(text)
        except (json.JSONDecodeError, ValueError):
            return ReviewResult()
        issues_payload = payload.get("issues", [])
        if not isinstance(issues_payload, list):
            return ReviewResult()

        issues: list[IssueCandidate] = []
        for issue in issues_payload:
            if not isinstance(issue, dict):
                continue
            title = str(issue.get("title", "")).strip()
            body = str(issue.get("body_markdown", "")).strip()
            if not title or not body:
                continue
            issues.append(
                IssueCandidate(
                    fingerprint=normalize_fingerprint(
                        str(issue.get("fingerprint", "")),
                        title=title,
                    ),
                    title=title,
                    body_markdown=body,
                )
            )
        return ReviewResult(issues=issues)


class GitHubIssueClient:
    """Small GitHub issues client for AI-generated review findings."""

    def __init__(self, *, repository: str, token: str, labels: list[str] | None = None) -> None:
        self._repository = repository
        self._labels = labels or []
        self._client = httpx.Client(
            timeout=30.0,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def existing_open_fingerprints(self) -> set[str]:
        """Return fingerprints for already-open AI review issues."""
        fingerprints: set[str] = set()
        url: str | None = f"https://api.github.com/repos/{self._repository}/issues"
        params: dict[str, Any] | None = {"state": "open", "per_page": 100}

        while url:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            for issue in response.json():
                if not isinstance(issue, dict) or "pull_request" in issue:
                    continue
                body = str(issue.get("body") or "")
                match = re.search(r"<!-- denbust-review:([a-z0-9._-]+) -->", body)
                if match:
                    fingerprints.add(match.group(1))

            link_header = response.headers.get("Link", "")
            next_url: str | None = None
            if link_header:
                for part in link_header.split(","):
                    section = part.strip().split(";")
                    if len(section) < 2:
                        continue
                    url_part = section[0].strip()
                    rel_part = section[1].strip()
                    if (
                        rel_part == 'rel="next"'
                        and url_part.startswith("<")
                        and url_part.endswith(">")
                    ):
                        next_url = url_part[1:-1]
                        break
            url = next_url
            params = None
        return fingerprints

    def create_issue(self, candidate: IssueCandidate, artifacts: ReviewArtifacts) -> None:
        """Create an issue for a new review finding."""
        body = "\n\n".join(
            [
                issue_marker(candidate.fingerprint),
                candidate.body_markdown,
                "### Review context",
                f"- Run timestamp: `{artifacts.run_timestamp}`",
                f"- Run snapshot: `{artifacts.run_snapshot_path.as_posix()}`",
                f"- Debug summary: `{artifacts.debug_summary_path.as_posix()}`",
                f"- Debug log: `{artifacts.debug_log_path.as_posix()}`",
                (
                    f"- Workflow run: {artifacts.debug_summary.get('workflow', {}).get('run_url')}"
                    if artifacts.debug_summary.get("workflow", {}).get("run_url")
                    else "- Workflow run: unavailable"
                ),
            ]
        )
        payload: dict[str, Any] = {
            "title": f"[daily-ai-review] {candidate.title}",
            "body": body,
        }
        if self._labels:
            payload["labels"] = self._labels

        response = self._client.post(
            f"https://api.github.com/repos/{self._repository}/issues",
            json=payload,
        )
        response.raise_for_status()


def review_latest_daily_run(
    *,
    state_root: Path,
    repository: str,
    anthropic_api_key: str,
    github_token: str,
    workflow_name: str = "daily-state-run",
    model: str = "claude-sonnet-4-20250514",
    labels: list[str] | None = None,
) -> int:
    """Review the latest daily run and create any missing issues."""
    artifacts = latest_daily_review_artifacts(
        state_root=state_root,
        workflow_name=workflow_name,
    )
    reviewer = AnthropicDailyReviewer(api_key=anthropic_api_key, model=model)
    review = reviewer.review(artifacts)
    if not review.issues:
        print(f"No review issues suggested for {artifacts.run_timestamp}.")
        return 0

    client = GitHubIssueClient(repository=repository, token=github_token, labels=labels)
    try:
        existing_fingerprints = client.existing_open_fingerprints()
        created = 0
        for issue in review.issues:
            if issue.fingerprint in existing_fingerprints:
                print(f"Skipping existing issue fingerprint: {issue.fingerprint}")
                continue
            client.create_issue(issue, artifacts)
            created += 1
            print(f"Created review issue: {issue.fingerprint}")
        return created
    finally:
        client.close()


def main() -> None:
    """CLI entrypoint for the daily ingest review workflow."""
    state_root = Path(os.getenv("DENBUST_STATE_ROOT", "state_repo"))
    repository = os.getenv("GITHUB_REPOSITORY")
    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    github_token = os.getenv("GITHUB_TOKEN")
    workflow_name = os.getenv("DENBUST_REVIEW_WORKFLOW_NAME", "daily-state-run")
    raw_model = os.getenv("DENBUST_REVIEW_MODEL", "").strip()
    model = raw_model or "claude-sonnet-4-20250514"
    raw_labels = os.getenv("DENBUST_REVIEW_ISSUE_LABELS", "")
    labels = [label.strip() for label in raw_labels.split(",") if label.strip()]

    if not repository:
        raise SystemExit("GITHUB_REPOSITORY is required for daily review.")
    if not anthropic_api_key:
        raise SystemExit("ANTHROPIC_API_KEY is required for daily review.")
    if not github_token:
        raise SystemExit("GITHUB_TOKEN is required for daily review.")

    created = review_latest_daily_run(
        state_root=state_root,
        repository=repository,
        anthropic_api_key=anthropic_api_key,
        github_token=github_token,
        workflow_name=workflow_name,
        model=model,
        labels=labels,
    )
    print(f"Daily review created {created} issue(s).")


if __name__ == "__main__":
    main()
