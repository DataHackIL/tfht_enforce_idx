"""Fast smoke tests used as a CI gate before heavier suites."""

from pathlib import Path

from typer.testing import CliRunner

from denbust.cli import app
from denbust.config import Config
from denbust.store.seen import SeenStore


def test_cli_version_command_smoke() -> None:
    """CLI entrypoint should load and return version output."""
    runner = CliRunner()
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert "denbust version" in result.output


def test_config_defaults_smoke() -> None:
    """Core config model should instantiate with defaults."""
    config = Config()

    assert config.days > 0
    assert config.max_articles > 0
    assert len(config.keywords) > 0


def test_seen_store_round_trip_smoke(tmp_path: Path) -> None:
    """Seen store should persist and reload URLs."""
    path = tmp_path / "seen.json"

    first = SeenStore(path)
    first.mark_seen(["https://example.com/article"])
    first.save()

    second = SeenStore(path)
    assert second.is_seen("https://example.com/article")
