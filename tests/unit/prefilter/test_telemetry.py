"""Unit tests for prefilter.telemetry — JSONL decision writer."""

from __future__ import annotations

import json
from pathlib import Path

from denbust.prefilter.models import PrefilterDecision
from denbust.prefilter.telemetry import PrefilterDecisionWriter


def _decision(
    candidate_id: str = "cand-1",
    verdict: str = "pass",
    decided_at: str = "2026-05-21T10:00:00+00:00",
) -> PrefilterDecision:
    return PrefilterDecision(
        candidate_id=candidate_id,
        pass_kind="thin",  # type: ignore[arg-type]
        verdict=verdict,  # type: ignore[arg-type]
        stopped_at_stage="passed_all",  # type: ignore[arg-type]
        stage_scores=(),
        decided_at=decided_at,
        config_hash="abc123",
    )


class TestPrefilterDecisionWriter:
    def test_append_creates_file(self, tmp_path: Path) -> None:
        writer = PrefilterDecisionWriter(tmp_path / "decisions")
        writer.append(_decision())
        files = list((tmp_path / "decisions").glob("*.jsonl"))
        assert len(files) == 1

    def test_append_writes_valid_jsonl(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision("cand-a"))
        writer.append(_decision("cand-b"))
        files = list(decisions_dir.glob("*.jsonl"))
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        r1 = json.loads(lines[0])
        r2 = json.loads(lines[1])
        assert r1["candidate_id"] == "cand-a"
        assert r2["candidate_id"] == "cand-b"

    def test_same_day_goes_to_same_file(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer = PrefilterDecisionWriter(decisions_dir)
        # Both decisions share the same date prefix
        writer.append(_decision("x", decided_at="2026-05-21T08:00:00+00:00"))
        writer.append(_decision("y", decided_at="2026-05-21T23:59:00+00:00"))
        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2

    def test_different_days_go_to_different_files(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision("a", decided_at="2026-05-20T10:00:00+00:00"))
        writer.append(_decision("b", decided_at="2026-05-21T10:00:00+00:00"))
        files = sorted(decisions_dir.glob("*.jsonl"))
        assert len(files) == 2
        assert files[0].name == "2026-05-20.jsonl"
        assert files[1].name == "2026-05-21.jsonl"

    def test_second_writer_appends_not_overwrites(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer1 = PrefilterDecisionWriter(decisions_dir)
        writer1.append(_decision("first", decided_at="2026-05-21T08:00:00+00:00"))

        writer2 = PrefilterDecisionWriter(decisions_dir)
        writer2.append(_decision("second", decided_at="2026-05-21T09:00:00+00:00"))

        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2  # both lines present

    def test_flush_is_noop(self, tmp_path: Path) -> None:
        writer = PrefilterDecisionWriter(tmp_path / "decisions")
        writer.flush()  # should not raise

    def test_creates_decisions_dir_on_first_write(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "deeply" / "nested" / "decisions"
        assert not decisions_dir.exists()
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision())
        assert decisions_dir.exists()
