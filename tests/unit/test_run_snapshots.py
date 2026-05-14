"""Unit tests for run snapshot persistence."""

import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import HttpUrl

from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.models.common import DatasetName, JobName
from denbust.store.run_snapshots import (
    RunSnapshot,
    snapshot_filename,
    write_run_debug_log,
    write_run_debug_summary,
    write_run_snapshot,
)


def build_item() -> UnifiedItem:
    """Create a sample unified item for snapshot tests."""
    return UnifiedItem(
        headline="פשיטה על בית בושת",
        summary="סיכום",
        sources=[SourceReference(source_name="ynet", url=HttpUrl("https://ynet.co.il/1"))],
        date=datetime(2026, 3, 1, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
    )


class TestRunSnapshots:
    """Tests for run snapshot helpers."""

    def test_snapshot_filename_is_git_safe(self) -> None:
        """Snapshot filenames should avoid colon characters."""
        filename = snapshot_filename(datetime(2026, 3, 15, 4, 0, 0, 123456, tzinfo=UTC))

        assert filename == "2026-03-15T04-00-00-123456Z.json"

    def test_write_run_snapshot_creates_directory_and_json(self, tmp_path: Path) -> None:
        """Snapshots should be written under the configured runs directory."""
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            started_at=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            finished_at=datetime(2026, 3, 15, 4, 0, 3, tzinfo=UTC),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            config_name="enforcement-news",
            config_path="agents/news/local.yaml",
            days_searched=7,
            source_count=6,
            output_formats=["cli", "email"],
            raw_article_count=3,
            unseen_article_count=2,
            relevant_article_count=1,
            unified_item_count=1,
            seen_count_before=10,
            seen_count_after=11,
            items=[build_item()],
            warnings=["partial source failure"],
            errors=["mako: timeout"],
            result_summary="ingest completed with 1 unified item(s)",
            release_manifest={"status": "placeholder"},
        )

        path = write_run_snapshot(tmp_path / "runs", snapshot)

        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert '"dataset_name": "news_items"' in content
        assert '"job_name": "ingest"' in content
        assert '"config_name": "enforcement-news"' in content
        assert '"output_formats": [' in content
        assert '"warnings": [' in content
        assert '"errors": [' in content

    def test_write_run_debug_log_creates_directory_and_json(self, tmp_path: Path) -> None:
        """Detailed run debug logs should be written under the configured logs directory."""
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            config_name="enforcement-news",
        )

        path = write_run_debug_log(
            tmp_path / "logs",
            snapshot,
            {"rejected_articles": [{"title": "כתבה", "relevant": False}]},
        )

        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert '"rejected_articles": [' in content
        assert '"relevant": false' in content

    def test_write_run_debug_summary_creates_compact_machine_json(self, tmp_path: Path) -> None:
        """Compact run summaries should retain only automation-oriented fields."""
        snapshot = RunSnapshot(
            run_timestamp=datetime(2026, 3, 15, 4, 0, 0, tzinfo=UTC),
            dataset_name=DatasetName.NEWS_ITEMS,
            job_name=JobName.INGEST,
            config_name="enforcement-news",
        )

        path = write_run_debug_summary(
            tmp_path / "logs",
            snapshot,
            {
                "schema_version": "news_items.ingest.debug.v1",
                "run_timestamp": "2026-03-15T04:00:00Z",
                "dataset_name": "news_items",
                "job_name": "ingest",
                "config_name": "enforcement-news",
                "result_summary": "no relevant articles found",
                "counts": {"unseen_article_count": 2},
                "workflow": {"run_id": "123"},
                "source_summaries": [{"source_name": "mako", "raw_article_count": 0}],
                "classifier_summary": {
                    "rejected_article_count": 2,
                    "parse_failure_diagnostics": {
                        "category_counts": {"object_like_non_json": 1},
                        "samples": [{"category": "object_like_non_json"}],
                        "sample_count": 1,
                        "sample_max_count": 8,
                        "sample_shape_max_length": 80,
                    },
                },
                "fallback_classifier_summary": {
                    "fallback_classifier_input_count": 3,
                    "fallback_operational_record_count": 1,
                    "warning_counts": {
                        "parse_failure_count": 1,
                        "invalid_taxonomy_pair_count": 0,
                        "invalid_legacy_pair_count": 0,
                        "relevant_without_usable_taxonomy_count": 0,
                    },
                    "parse_failure_diagnostics": {
                        "category_counts": {"object_like_non_json": 1},
                        "samples": [{"category": "object_like_non_json"}],
                        "sample_count": 1,
                        "sample_max_count": 8,
                        "sample_shape_max_length": 80,
                    },
                },
                "problems": {"all_unseen_rejected": True},
                "suspicions": ["all_unseen_rejected"],
                "warnings": [],
                "errors": [],
                "rejected_articles": [{"title": "כתבה"}],
            },
        )

        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert '"schema_version": "news_items.ingest.debug.v1"' in content
        assert '"suspicions": [' in content
        payload = json.loads(content)
        assert payload["classifier_summary"]["parse_failure_diagnostics"] == {
            "category_counts": {"object_like_non_json": 1},
            "samples": [{"category": "object_like_non_json"}],
            "sample_count": 1,
            "sample_max_count": 8,
            "sample_shape_max_length": 80,
        }
        assert payload["fallback_classifier_summary"] == {
            "fallback_classifier_input_count": 3,
            "fallback_operational_record_count": 1,
            "warning_counts": {
                "parse_failure_count": 1,
                "invalid_taxonomy_pair_count": 0,
                "invalid_legacy_pair_count": 0,
                "relevant_without_usable_taxonomy_count": 0,
            },
            "parse_failure_diagnostics": {
                "category_counts": {"object_like_non_json": 1},
                "samples": [{"category": "object_like_non_json"}],
                "sample_count": 1,
                "sample_max_count": 8,
                "sample_shape_max_length": 80,
            },
        }
        assert '"rejected_articles"' not in content
