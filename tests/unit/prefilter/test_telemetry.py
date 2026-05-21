"""Unit tests for prefilter.telemetry — JSONL decision writer."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from denbust.prefilter.models import PrefilterDecision
from denbust.prefilter.telemetry import PrefilterDecisionWriter

_DT_BASE = datetime(2026, 5, 21, 10, 0, 0, tzinfo=UTC)


def _decision(
    candidate_id: str = "cand-1",
    verdict: str = "pass",
    decided_at: datetime | None = None,
) -> PrefilterDecision:
    return PrefilterDecision(
        candidate_id=candidate_id,
        pass_kind="thin",
        verdict=verdict,  # type: ignore[arg-type]
        stopped_at_stage="passed_all",
        stage_scores=(),
        decided_at=decided_at if decided_at is not None else _DT_BASE,
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

    def test_decided_at_serialized_as_iso_string(self, tmp_path: Path) -> None:
        writer = PrefilterDecisionWriter(tmp_path / "decisions")
        writer.append(_decision())
        files = list((tmp_path / "decisions").glob("*.jsonl"))
        record = json.loads(files[0].read_text(encoding="utf-8").strip())
        # Must be a string in the JSONL, not a raw datetime object
        assert isinstance(record["decided_at"], str)
        # Must be round-trippable as ISO-8601
        dt = datetime.fromisoformat(record["decided_at"])
        assert dt.tzinfo is not None

    def test_same_day_goes_to_same_file(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision("x", decided_at=datetime(2026, 5, 21, 8, 0, 0, tzinfo=UTC)))
        writer.append(_decision("y", decided_at=datetime(2026, 5, 21, 23, 59, 0, tzinfo=UTC)))
        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2

    def test_different_days_go_to_different_files(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision("a", decided_at=datetime(2026, 5, 20, 10, 0, 0, tzinfo=UTC)))
        writer.append(_decision("b", decided_at=datetime(2026, 5, 21, 10, 0, 0, tzinfo=UTC)))
        files = sorted(decisions_dir.glob("*.jsonl"))
        assert len(files) == 2
        assert files[0].name == "2026-05-20.jsonl"
        assert files[1].name == "2026-05-21.jsonl"

    def test_second_writer_appends_not_overwrites(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "decisions"
        writer1 = PrefilterDecisionWriter(decisions_dir)
        writer1.append(_decision("first", decided_at=datetime(2026, 5, 21, 8, 0, 0, tzinfo=UTC)))

        writer2 = PrefilterDecisionWriter(decisions_dir)
        writer2.append(_decision("second", decided_at=datetime(2026, 5, 21, 9, 0, 0, tzinfo=UTC)))

        files = list(decisions_dir.glob("*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2  # both lines present

    def test_creates_decisions_dir_on_first_write(self, tmp_path: Path) -> None:
        decisions_dir = tmp_path / "deeply" / "nested" / "decisions"
        assert not decisions_dir.exists()
        writer = PrefilterDecisionWriter(decisions_dir)
        writer.append(_decision())
        assert decisions_dir.exists()
