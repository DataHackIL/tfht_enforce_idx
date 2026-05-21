"""Unit tests for Parquet round-trip fidelity in prefilter.labels."""

from __future__ import annotations

from pathlib import Path

from denbust.prefilter.labels import LabeledCandidate, read_labels_parquet, write_labels_parquet


def _make_rows() -> list[LabeledCandidate]:
    return [
        LabeledCandidate(
            candidate_id="cand_001",
            domain="example.com",
            url="https://example.com/cand_001",
            title="A test title",
            snippet="A test snippet",
            article_body=None,
            label="negative",
            label_source="triage_manual",
            split="train",
            labeled_at="2026-01-01T00:00:00Z",
            decision_hash="abc123",
        ),
        LabeledCandidate(
            candidate_id="cand_002",
            domain="news.com",
            url="https://news.com/cand_002",
            title="Second title",
            snippet="Second snippet",
            article_body="Full article body text",
            label="positive",
            label_source="claude_classifier",
            split="val",
            labeled_at="2026-01-02T12:00:00Z",
            decision_hash="def456",
        ),
        LabeledCandidate(
            candidate_id="cand_003",
            domain="",
            url="https://other.com/cand_003",
            title="",
            snippet="",
            article_body=None,
            label="negative",
            label_source="triage_auto",
            split="test",
            labeled_at="2026-01-03T08:00:00Z",
            decision_hash="ghi789",
        ),
    ]


class TestRoundTrip:
    def test_all_rows_preserved(self, tmp_path: Path) -> None:
        rows = _make_rows()
        path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, path)
        recovered = read_labels_parquet(path)
        assert len(recovered) == len(rows)

    def test_field_values_preserved(self, tmp_path: Path) -> None:
        rows = _make_rows()
        path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, path)
        recovered = read_labels_parquet(path)
        for orig, rec in zip(rows, recovered):
            assert rec.candidate_id == orig.candidate_id
            assert rec.domain == orig.domain
            assert rec.url == orig.url
            assert rec.title == orig.title
            assert rec.snippet == orig.snippet
            assert rec.article_body == orig.article_body
            assert rec.label == orig.label
            assert rec.label_source == orig.label_source
            assert rec.split == orig.split
            assert rec.labeled_at == orig.labeled_at
            assert rec.decision_hash == orig.decision_hash

    def test_none_article_body_survives(self, tmp_path: Path) -> None:
        rows = [r for r in _make_rows() if r.article_body is None]
        path = tmp_path / "labels_none.parquet"
        write_labels_parquet(rows, path)
        recovered = read_labels_parquet(path)
        for rec in recovered:
            assert rec.article_body is None

    def test_nonempty_article_body_survives(self, tmp_path: Path) -> None:
        rows = [r for r in _make_rows() if r.article_body is not None]
        path = tmp_path / "labels_body.parquet"
        write_labels_parquet(rows, path)
        recovered = read_labels_parquet(path)
        for orig, rec in zip(rows, recovered):
            assert rec.article_body == orig.article_body

    def test_empty_list_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.parquet"
        write_labels_parquet([], path)
        recovered = read_labels_parquet(path)
        assert recovered == []

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "dir" / "labels.parquet"
        write_labels_parquet(_make_rows(), path)
        assert path.exists()

    def test_frozen_dataclass_equality(self, tmp_path: Path) -> None:
        """Recovered rows compare equal to the originals field-by-field."""
        rows = _make_rows()
        path = tmp_path / "labels.parquet"
        write_labels_parquet(rows, path)
        recovered = read_labels_parquet(path)
        assert rows == recovered

    def test_unicode_content_preserved(self, tmp_path: Path) -> None:
        row = LabeledCandidate(
            candidate_id="heb_001",
            domain="ynet.co.il",
            url="https://ynet.co.il/heb_001",
            title="עצור חשוד ברצח",
            snippet="המשטרה עצרה חשוד ברצח שאירע בתל אביב",
            article_body=None,
            label="positive",
            label_source="triage_manual",
            split="train",
            labeled_at="2026-01-01T00:00:00Z",
            decision_hash="heb123",
        )
        path = tmp_path / "hebrew.parquet"
        write_labels_parquet([row], path)
        (recovered,) = read_labels_parquet(path)
        assert recovered.title == row.title
        assert recovered.snippet == row.snippet
