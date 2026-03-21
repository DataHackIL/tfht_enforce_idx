"""Unit tests for the daily AI review workflow helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from denbust.news_items.daily_review import (
    AnthropicDailyReviewer,
    GitHubIssueClient,
    IssueCandidate,
    ReviewArtifacts,
    ReviewResult,
    extract_json_block,
    issue_marker,
    latest_daily_review_artifacts,
    normalize_fingerprint,
    review_latest_daily_run,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class TestDailyReviewHelpers:
    """Tests for latest-artifact resolution and parsing helpers."""

    def test_latest_daily_review_artifacts_prefers_matching_daily_summary(
        self, tmp_path: Path
    ) -> None:
        """The latest summary from the target workflow should be selected."""
        runs_dir = tmp_path / "news_items" / "ingest" / "runs"
        logs_dir = tmp_path / "news_items" / "ingest" / "logs"

        _write_json(
            logs_dir / "2026-03-21T04-00-00-000000Z.summary.json",
            {
                "run_timestamp": "2026-03-21T04:00:00Z",
                "workflow": {"workflow_name": "daily-state-run"},
            },
        )
        _write_json(logs_dir / "2026-03-21T04-00-00-000000Z.json", {"raw_articles": []})
        _write_json(runs_dir / "2026-03-21T04-00-00-000000Z.json", {"result_summary": "daily"})

        _write_json(
            logs_dir / "2026-03-21T04-30-00-000000Z.summary.json",
            {
                "run_timestamp": "2026-03-21T04:30:00Z",
                "workflow": {"workflow_name": "weekly-state-run"},
            },
        )
        _write_json(logs_dir / "2026-03-21T04-30-00-000000Z.json", {"raw_articles": []})
        _write_json(runs_dir / "2026-03-21T04-30-00-000000Z.json", {"result_summary": "weekly"})

        artifacts = latest_daily_review_artifacts(state_root=tmp_path)

        assert artifacts.stem == "2026-03-21T04-00-00-000000Z"
        assert artifacts.run_snapshot["result_summary"] == "daily"

    def test_extract_json_block_handles_markdown_fences(self) -> None:
        """Anthropic JSON responses may be wrapped in markdown fences."""
        payload = extract_json_block('```json\n{"issues":[]}\n```')
        assert payload == {"issues": []}

    def test_normalize_fingerprint_falls_back_to_title(self) -> None:
        """Blank fingerprints should be derived from the issue title."""
        assert (
            normalize_fingerprint("", title="Mako returned zero results!")
            == "mako-returned-zero-results"
        )

    def test_issue_marker_builds_hidden_marker(self) -> None:
        """Open issues should carry a stable hidden marker."""
        assert issue_marker("mako-zero-results") == "<!-- denbust-review:mako-zero-results -->"


class TestDailyReviewClients:
    """Tests for the Anthropic and GitHub review clients."""

    def test_anthropic_daily_reviewer_parses_valid_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reviewer output should normalize fingerprints and drop malformed issues."""

        class FakeTextBlock:
            def __init__(self, text: str) -> None:
                self.text = text

        class FakeResponse:
            def __init__(self, text: str) -> None:
                self.content = [FakeTextBlock(text)]

        class FakeMessages:
            def create(self, **_: object) -> FakeResponse:
                return FakeResponse(
                    '```json\n{"issues":[{"fingerprint":"","title":"Mako returned zero results","body_markdown":"Check source."}]}\n```'
                )

        class FakeClient:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessages()

        monkeypatch.setattr("denbust.news_items.daily_review.anthropic.Anthropic", FakeClient)
        monkeypatch.setattr("denbust.news_items.daily_review.TextBlock", FakeTextBlock)

        reviewer = AnthropicDailyReviewer(api_key="test", model="model")
        artifacts = ReviewArtifacts(
            run_timestamp="2026-03-21T04:00:00Z",
            stem="2026-03-21T04-00-00-000000Z",
            run_snapshot_path=Path("runs/example.json"),
            debug_summary_path=Path("logs/example.summary.json"),
            debug_log_path=Path("logs/example.json"),
            run_snapshot={"result_summary": "x"},
            debug_summary={"suspicions": []},
            debug_log={"raw_articles": []},
        )

        result = reviewer.review(artifacts)

        assert result == ReviewResult(
            issues=[
                IssueCandidate(
                    fingerprint="mako-returned-zero-results",
                    title="Mako returned zero results",
                    body_markdown="Check source.",
                )
            ]
        )

    def test_github_issue_client_extracts_open_markers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Open issue markers should be parsed from the issue body."""

        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> list[dict[str, object]]:
                return [
                    {"body": "<!-- denbust-review:mako-zero-results -->\nbody"},
                    {"pull_request": {"url": "x"}, "body": "<!-- denbust-review:pr -->"},
                ]

        class FakeClient:
            def __init__(self, **_: object) -> None:
                pass

            def get(self, *_: object, **__: object) -> FakeResponse:
                return FakeResponse()

            def close(self) -> None:
                return None

        monkeypatch.setattr("denbust.news_items.daily_review.httpx.Client", FakeClient)

        client = GitHubIssueClient(repository="DataHackIL/tfht_enforce_idx", token="token")
        try:
            assert client.existing_open_fingerprints() == {"mako-zero-results"}
        finally:
            client.close()


def test_review_latest_daily_run_skips_existing_fingerprints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The orchestrator should only create new issues for unseen fingerprints."""
    runs_dir = tmp_path / "news_items" / "ingest" / "runs"
    logs_dir = tmp_path / "news_items" / "ingest" / "logs"
    stem = "2026-03-21T04-00-00-000000Z"

    _write_json(
        logs_dir / f"{stem}.summary.json",
        {"run_timestamp": "2026-03-21T04:00:00Z", "workflow": {"workflow_name": "daily-state-run"}},
    )
    _write_json(logs_dir / f"{stem}.json", {"raw_articles": []})
    _write_json(runs_dir / f"{stem}.json", {"result_summary": "ok"})

    class FakeReviewer:
        def __init__(self, **_: object) -> None:
            pass

        def review(self, _artifacts: ReviewArtifacts) -> ReviewResult:
            return ReviewResult(
                issues=[
                    IssueCandidate(
                        fingerprint="existing-problem",
                        title="Existing problem",
                        body_markdown="Already open",
                    ),
                    IssueCandidate(
                        fingerprint="new-problem",
                        title="New problem",
                        body_markdown="Open this",
                    ),
                ]
            )

    created: list[str] = []

    class FakeIssueClient:
        def __init__(self, **_: object) -> None:
            pass

        def existing_open_fingerprints(self) -> set[str]:
            return {"existing-problem"}

        def create_issue(self, candidate: IssueCandidate, artifacts: ReviewArtifacts) -> None:
            del artifacts
            created.append(candidate.fingerprint)

        def close(self) -> None:
            return None

    monkeypatch.setattr("denbust.news_items.daily_review.AnthropicDailyReviewer", FakeReviewer)
    monkeypatch.setattr("denbust.news_items.daily_review.GitHubIssueClient", FakeIssueClient)

    created_count = review_latest_daily_run(
        state_root=tmp_path,
        repository="DataHackIL/tfht_enforce_idx",
        anthropic_api_key="test",
        github_token="token",
    )

    assert created_count == 1
    assert created == ["new-problem"]
