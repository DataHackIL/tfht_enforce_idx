"""Unit tests for scaffolded release and backup publishing models."""

from pathlib import Path

import pytest

from denbust.models.runs import RunSnapshot
from denbust.ops.storage import LocalJsonOperationalStore, NullOperationalStore
from denbust.publish.backup import BackupExecutor, NullBackupExecutor
from denbust.publish.release import NullReleaseBuilder, ReleaseBuilder, ReleaseFormat


class IncompleteReleaseBuilder(ReleaseBuilder):
    """Concrete test subclass for abstract release builder behavior."""

    def describe(self) -> str:
        return "test release builder"


class IncompleteBackupExecutor(BackupExecutor):
    """Concrete test subclass for abstract backup executor behavior."""

    def describe(self) -> str:
        return "test backup executor"


def test_null_release_builder_returns_consistent_placeholder_artifact(tmp_path: Path) -> None:
    """The placeholder artifact should match its declared release format."""
    manifest = NullReleaseBuilder().build_manifest("news_items", tmp_path / "publication")

    assert len(manifest.primary_files) == 1
    artifact = manifest.primary_files[0]
    assert artifact.format == ReleaseFormat.PARQUET
    assert artifact.path.name == "placeholder.parquet"


def test_null_release_builder_describe_reports_scaffold_status() -> None:
    """The release scaffold should describe itself clearly."""
    assert NullReleaseBuilder().describe() == (
        "Release publication is scaffolded but not implemented in Phase A."
    )


def test_release_builder_is_abstract() -> None:
    """The abstract release builder contract should remain explicit."""
    with pytest.raises(TypeError, match="implementation for abstract method 'build_manifest'"):
        IncompleteReleaseBuilder()


def test_null_backup_executor_returns_placeholder_target(tmp_path: Path) -> None:
    """The backup scaffold should return a placeholder local target."""
    state_root = tmp_path / "state-root"
    manifest = NullBackupExecutor().build_manifest("news_items", state_root)

    assert manifest.dataset_name == "news_items"
    assert len(manifest.targets) == 1
    assert manifest.targets[0].location == str(state_root)


def test_null_backup_executor_describe_reports_scaffold_status() -> None:
    """The backup scaffold should describe itself clearly."""
    assert NullBackupExecutor().describe() == (
        "Backup uploads are scaffolded but not implemented in Phase A."
    )


def test_backup_executor_is_abstract() -> None:
    """The abstract backup executor contract should remain explicit."""
    with pytest.raises(TypeError, match="implementation for abstract method 'build_manifest'"):
        IncompleteBackupExecutor()


def test_null_operational_store_methods_are_noops() -> None:
    """The null store should safely ignore all operational operations."""
    store = NullOperationalStore()

    store.write_run_metadata(RunSnapshot(config_name="test-config"))
    store.upsert_records("news_items", [{"id": "1"}])
    assert store.fetch_records("news_items", limit=5) == []
    store.mark_publication_state("news_items", ["1"], "published")


def test_local_json_operational_store_noop_record_methods(tmp_path: Path) -> None:
    """The local JSON store should expose the same no-op record API for Phase A."""
    store = LocalJsonOperationalStore(tmp_path / "ops")

    assert store.run_metadata_path == tmp_path / "ops" / "run_metadata.jsonl"
    store.upsert_records("news_items", [{"id": "1"}])
    assert store.fetch_records("news_items", limit=2) == []
    store.mark_publication_state("news_items", ["1"], "published")
