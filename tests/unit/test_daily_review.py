"""Unit tests for the daily AI review workflow helpers."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import pytest

from denbust.news_items.daily_review import (
    AnthropicDailyReviewer,
    GitHubIssueClient,
    IssueCandidate,
    ReviewArtifacts,
    ReviewResult,
    _compact_for_prompt,
    _load_json,
    extract_json_block,
    issue_marker,
    latest_daily_review_artifacts,
    main,
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

    def test_extract_json_block_handles_unclosed_markdown_fence(self) -> None:
        """Open markdown fences should still parse the remaining JSON."""
        payload = extract_json_block('```json\n{"issues":[]}')
        assert payload == {"issues": []}

    def test_extract_json_block_rejects_non_object_json(self) -> None:
        """Review responses must decode to an object."""
        with pytest.raises(ValueError, match="Review response must be a JSON object"):
            extract_json_block('["not-an-object"]')

    def test_extract_json_block_handles_inline_tilde_fence(self) -> None:
        """Single-line tilde fences should still parse correctly."""
        payload = extract_json_block('~~~json {"issues":[]}~~~')
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

    def test_load_json_rejects_non_object_payload(self, tmp_path: Path) -> None:
        """Artifact JSON files must decode to objects."""
        path = tmp_path / "bad.json"
        path.write_text('["not-an-object"]', encoding="utf-8")

        with pytest.raises(ValueError, match="Expected JSON object"):
            _load_json(path)

    def test_latest_daily_review_artifacts_raises_when_daily_artifacts_missing(
        self, tmp_path: Path
    ) -> None:
        """Missing or incomplete daily artifacts should fail clearly."""
        logs_dir = tmp_path / "news_items" / "ingest" / "logs"
        _write_json(
            logs_dir / "2026-03-21T04-00-00-000000Z.summary.json",
            {
                "run_timestamp": "2026-03-21T04:00:00Z",
                "workflow": {"workflow_name": "daily-state-run"},
            },
        )

        with pytest.raises(FileNotFoundError, match="No complete news_items/ingest artifacts"):
            latest_daily_review_artifacts(state_root=tmp_path)

    def test_compact_for_prompt_truncates_large_lists_and_strings(self) -> None:
        """Large debug payloads should be compacted before prompting."""
        compact = _compact_for_prompt(
            {
                "raw_articles": [{"title": f"title-{index}"} for index in range(12)],
                "notes": "x" * 600,
            }
        )

        assert len(compact["raw_articles"]) == 11
        assert compact["raw_articles"][-1] == {"_truncated_count": 2}
        assert str(compact["notes"]).endswith("... [truncated]")


class TestDailyReviewClients:
    """Tests for the Anthropic and GitHub review clients."""

    def test_anthropic_daily_reviewer_parses_valid_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
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

    def test_anthropic_daily_reviewer_handles_non_list_and_malformed_issues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Malformed issue payloads should be ignored rather than crashing review."""

        class FakeTextBlock:
            def __init__(self, text: str) -> None:
                self.text = text

        class FakeResponse:
            def __init__(self, text: str) -> None:
                self.content = [FakeTextBlock(text)]

        class FakeMessages:
            def __init__(self, text: str) -> None:
                self._text = text

            def create(self, **_: object) -> FakeResponse:
                return FakeResponse(self._text)

        class FakeClient:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessages('{"issues":[{}, "bad", {"title":"x"}]}')

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

        assert reviewer.review(artifacts) == ReviewResult()

        class FakeClientNonList:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessages('{"issues":"not-a-list"}')

        monkeypatch.setattr(
            "denbust.news_items.daily_review.anthropic.Anthropic", FakeClientNonList
        )
        reviewer = AnthropicDailyReviewer(api_key="test", model="model")
        assert reviewer.review(artifacts) == ReviewResult()

        class FakeClientNoText:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessages("")

        monkeypatch.setattr("denbust.news_items.daily_review.anthropic.Anthropic", FakeClientNoText)
        reviewer = AnthropicDailyReviewer(api_key="test", model="model")
        assert reviewer.review(artifacts) == ReviewResult()

        class FakeNonTextBlock:
            pass

        class FakeResponseNoText:
            def __init__(self) -> None:
                self.content = [FakeNonTextBlock()]

        class FakeMessagesNoText:
            def create(self, **_: object) -> FakeResponseNoText:
                return FakeResponseNoText()

        class FakeClientNoTextBlocks:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessagesNoText()

        monkeypatch.setattr(
            "denbust.news_items.daily_review.anthropic.Anthropic", FakeClientNoTextBlocks
        )
        reviewer = AnthropicDailyReviewer(api_key="test", model="model")
        assert reviewer.review(artifacts) == ReviewResult()

        class FakeClientBadJson:
            def __init__(self, **_: object) -> None:
                self.messages = FakeMessages("not-json")

        monkeypatch.setattr(
            "denbust.news_items.daily_review.anthropic.Anthropic", FakeClientBadJson
        )
        reviewer = AnthropicDailyReviewer(api_key="test", model="model")
        assert reviewer.review(artifacts) == ReviewResult()

    def test_github_issue_client_extracts_open_markers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Open issue markers should be parsed from the issue body."""

        class FakeResponse:
            def __init__(self, issues: list[dict[str, object]], link: str = "") -> None:
                self._issues = issues
                self.headers = {"Link": link} if link else {}

            def raise_for_status(self) -> None:
                return None

            def json(self) -> list[dict[str, object]]:
                return self._issues

        class FakeClient:
            def __init__(self, **_: object) -> None:
                self.calls = 0

            def get(self, *_: object, **__: object) -> FakeResponse:
                self.calls += 1
                if self.calls == 1:
                    return FakeResponse(
                        [
                            {"body": "<!-- denbust-review:mako-zero-results -->\nbody"},
                            {"pull_request": {"url": "x"}, "body": "<!-- denbust-review:pr -->"},
                        ],
                        link='<https://api.github.com/repositories/1/issues?page=2>; rel="next"',
                    )
                return FakeResponse(
                    [{"body": "<!-- denbust-review:haaretz-zero-results -->\nbody"}]
                )

            def close(self) -> None:
                return None

        monkeypatch.setattr("denbust.news_items.daily_review.httpx.Client", FakeClient)

        client = GitHubIssueClient(repository="DataHackIL/tfht_enforce_idx", token="token")
        try:
            assert client.existing_open_fingerprints() == {
                "mako-zero-results",
                "haaretz-zero-results",
            }
        finally:
            client.close()

    def test_github_issue_client_create_issue_includes_labels(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Configured labels should be passed through on issue creation."""
        captured: dict[str, Any] = {}

        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

        class FakeClient:
            def __init__(self, **_: object) -> None:
                pass

            def post(self, _url: str, json: dict[str, Any]) -> FakeResponse:
                captured.update(json)
                return FakeResponse()

            def close(self) -> None:
                captured["closed"] = True

        monkeypatch.setattr("denbust.news_items.daily_review.httpx.Client", FakeClient)

        client = GitHubIssueClient(
            repository="DataHackIL/tfht_enforce_idx",
            token="token",
            labels=["ai-review", "triage"],
        )
        artifacts = ReviewArtifacts(
            run_timestamp="2026-03-21T04:00:00Z",
            stem="2026-03-21T04-00-00-000000Z",
            run_snapshot_path=Path("runs/example.json"),
            debug_summary_path=Path("logs/example.summary.json"),
            debug_log_path=Path("logs/example.json"),
            run_snapshot={},
            debug_summary={"workflow": {"run_url": "https://example.com/run/1"}},
            debug_log={},
        )
        try:
            client.create_issue(
                IssueCandidate(
                    fingerprint="new-problem",
                    title="New problem",
                    body_markdown="Investigate.",
                ),
                artifacts,
            )
        finally:
            client.close()

        assert captured["title"] == "[daily-ai-review] New problem"
        assert captured["labels"] == ["ai-review", "triage"]
        assert "<!-- denbust-review:new-problem -->" in captured["body"]
        assert captured["closed"] is True

    def test_github_issue_client_create_issue_without_labels(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Labels should be omitted when none are configured."""
        captured: dict[str, Any] = {}

        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

        class FakeClient:
            def __init__(self, **_: object) -> None:
                pass

            def post(self, _url: str, json: dict[str, Any]) -> FakeResponse:
                captured.update(json)
                return FakeResponse()

            def close(self) -> None:
                return None

        monkeypatch.setattr("denbust.news_items.daily_review.httpx.Client", FakeClient)

        client = GitHubIssueClient(repository="DataHackIL/tfht_enforce_idx", token="token")
        try:
            client.create_issue(
                IssueCandidate(
                    fingerprint="new-problem",
                    title="New problem",
                    body_markdown="Investigate.",
                ),
                ReviewArtifacts(
                    run_timestamp="2026-03-21T04:00:00Z",
                    stem="2026-03-21T04-00-00-000000Z",
                    run_snapshot_path=Path("runs/example.json"),
                    debug_summary_path=Path("logs/example.summary.json"),
                    debug_log_path=Path("logs/example.json"),
                    run_snapshot={},
                    debug_summary={"workflow": {}},
                    debug_log={},
                ),
            )
        finally:
            client.close()

        assert "labels" not in captured


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


def test_review_latest_daily_run_returns_zero_when_no_issues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A clean review should not create issues."""
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
            return ReviewResult()

    class FakeIssueClient:
        def __init__(self, **_: object) -> None:
            raise AssertionError("Issue client should not be constructed when no issues exist")

    monkeypatch.setattr("denbust.news_items.daily_review.AnthropicDailyReviewer", FakeReviewer)
    monkeypatch.setattr("denbust.news_items.daily_review.GitHubIssueClient", FakeIssueClient)

    created_count = review_latest_daily_run(
        state_root=tmp_path,
        repository="DataHackIL/tfht_enforce_idx",
        anthropic_api_key="test",
        github_token="token",
    )

    assert created_count == 0
    assert "No review issues suggested" in capsys.readouterr().out


def test_main_requires_expected_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI entrypoint should fail fast when required env is missing."""
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(SystemExit, match="GITHUB_REPOSITORY is required"):
        main()

    monkeypatch.setenv("GITHUB_REPOSITORY", "DataHackIL/tfht_enforce_idx")
    with pytest.raises(SystemExit, match="ANTHROPIC_API_KEY is required"):
        main()

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    with pytest.raises(SystemExit, match="GITHUB_TOKEN is required"):
        main()


def test_main_reads_env_and_prints_created_count(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The CLI entrypoint should pass env-derived settings into the orchestrator."""
    monkeypatch.setenv("DENBUST_STATE_ROOT", "/tmp/state-root")
    monkeypatch.setenv("GITHUB_REPOSITORY", "DataHackIL/tfht_enforce_idx")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("DENBUST_REVIEW_WORKFLOW_NAME", "daily-state-run")
    monkeypatch.setenv("DENBUST_REVIEW_MODEL", "claude-test")
    monkeypatch.setenv("DENBUST_REVIEW_ISSUE_LABELS", "ai-review, triage ")

    captured: dict[str, Any] = {}

    def fake_review_latest_daily_run(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 2

    monkeypatch.setattr(
        "denbust.news_items.daily_review.review_latest_daily_run",
        fake_review_latest_daily_run,
    )

    main()

    assert captured["state_root"] == Path("/tmp/state-root")
    assert captured["repository"] == "DataHackIL/tfht_enforce_idx"
    assert captured["anthropic_api_key"] == "test-key"
    assert captured["github_token"] == "token"
    assert captured["workflow_name"] == "daily-state-run"
    assert captured["model"] == "claude-test"
    assert captured["labels"] == ["ai-review", "triage"]
    assert "Daily review created 2 issue(s)." in capsys.readouterr().out


def test_main_uses_default_model_when_env_blank(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Blank model env vars should fall back to the default model name."""
    monkeypatch.setenv("DENBUST_STATE_ROOT", "/tmp/state-root")
    monkeypatch.setenv("GITHUB_REPOSITORY", "DataHackIL/tfht_enforce_idx")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("DENBUST_REVIEW_MODEL", "   ")
    monkeypatch.delenv("DENBUST_REVIEW_ISSUE_LABELS", raising=False)

    captured: dict[str, Any] = {}

    def fake_review_latest_daily_run(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(
        "denbust.news_items.daily_review.review_latest_daily_run",
        fake_review_latest_daily_run,
    )

    main()

    assert captured["model"] == "claude-sonnet-4-20250514"
    assert "Daily review created 0 issue(s)." in capsys.readouterr().out


def test_module_main_entrypoint_executes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Running the module as __main__ should invoke the entrypoint successfully."""
    logs_dir = tmp_path / "news_items" / "ingest" / "logs"
    runs_dir = tmp_path / "news_items" / "ingest" / "runs"
    stem = "2026-03-21T04-00-00-000000Z"
    _write_json(
        logs_dir / f"{stem}.summary.json",
        {"run_timestamp": "2026-03-21T04:00:00Z", "workflow": {"workflow_name": "daily-state-run"}},
    )
    _write_json(logs_dir / f"{stem}.json", {"raw_articles": []})
    _write_json(runs_dir / f"{stem}.json", {"result_summary": "ok"})

    class FakeMessages:
        def create(self, **_: object) -> Any:
            class FakeResponse:
                content: list[Any] = []

            return FakeResponse()

    class FakeAnthropicClient:
        def __init__(self, **_: object) -> None:
            self.messages = FakeMessages()

    monkeypatch.setattr("anthropic.Anthropic", FakeAnthropicClient)
    monkeypatch.setenv("DENBUST_STATE_ROOT", str(tmp_path))
    monkeypatch.setenv("GITHUB_REPOSITORY", "DataHackIL/tfht_enforce_idx")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    module_path = Path(
        importlib.import_module("denbust.news_items.daily_review").__file__
    ).resolve()
    namespace = {
        "__name__": "__main__",
        "__file__": str(module_path),
        "Any": Any,
        "Path": Path,
    }
    exec(compile(module_path.read_text(encoding="utf-8"), str(module_path), "exec"), namespace)
