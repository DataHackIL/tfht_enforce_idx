"""Unit tests for scaffolded release and backup publishing models."""

from pathlib import Path

from denbust.publish.release import NullReleaseBuilder, ReleaseFormat


def test_null_release_builder_returns_consistent_placeholder_artifact() -> None:
    """The placeholder artifact should match its declared release format."""
    manifest = NullReleaseBuilder().build_manifest("news_items", Path("/tmp/publication"))

    assert len(manifest.primary_files) == 1
    artifact = manifest.primary_files[0]
    assert artifact.format == ReleaseFormat.PARQUET
    assert artifact.path.name == "placeholder.parquet"
