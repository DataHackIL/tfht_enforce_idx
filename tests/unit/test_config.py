"""Unit tests for config module."""

from pathlib import Path

import pytest

from denbust.config import (
    Config,
    DedupConfig,
    OutputConfig,
    OutputFormat,
    SourceConfig,
    SourceType,
    load_config,
)


class TestConfig:
    """Tests for Config class."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = Config()

        assert config.name == "enforcement-news"
        assert config.days == 3
        assert config.max_articles == 30
        assert len(config.keywords) > 0
        assert config.dedup.similarity_threshold == 0.7
        assert config.output.format == OutputFormat.CLI

    def test_custom_days(self) -> None:
        """Test custom days configuration."""
        config = Config(days=7)
        assert config.days == 7

    def test_days_validation(self) -> None:
        """Test days must be positive."""
        with pytest.raises(ValueError):
            Config(days=0)

        with pytest.raises(ValueError):
            Config(days=-1)

    def test_source_config(self) -> None:
        """Test source configuration."""
        source = SourceConfig(
            name="test",
            type=SourceType.RSS,
            url="https://example.com/feed.xml",
        )

        assert source.name == "test"
        assert source.type == SourceType.RSS
        assert source.url == "https://example.com/feed.xml"
        assert source.enabled is True

    def test_source_config_disabled(self) -> None:
        """Test disabled source configuration."""
        source = SourceConfig(
            name="test",
            type=SourceType.SCRAPER,
            enabled=False,
        )

        assert source.enabled is False

    def test_dedup_config_validation(self) -> None:
        """Test dedup threshold validation."""
        config = DedupConfig(similarity_threshold=0.5)
        assert config.similarity_threshold == 0.5

        with pytest.raises(ValueError):
            DedupConfig(similarity_threshold=1.5)

        with pytest.raises(ValueError):
            DedupConfig(similarity_threshold=-0.1)

    def test_output_config(self) -> None:
        """Test output configuration."""
        config = OutputConfig(format=OutputFormat.CLI)
        assert config.format == OutputFormat.CLI

        config = OutputConfig(format=OutputFormat.TELEGRAM)
        assert config.format == OutputFormat.TELEGRAM

    def test_env_properties(self) -> None:
        """Test environment variable properties."""
        config = Config()

        # These should return None if env vars not set
        # (We don't set them in tests)
        assert config.anthropic_api_key is None or isinstance(config.anthropic_api_key, str)


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_file_not_found(self, tmp_path: Path) -> None:
        """Test loading non-existent config file."""
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_load_config_valid(self, tmp_path: Path) -> None:
        """Test loading valid config file."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
name: test-config
days: 7
keywords:
  - test
  - keyword
sources:
  - name: ynet
    type: rss
    url: https://ynet.co.il/feed.xml
"""
        )

        config = load_config(config_path)

        assert config.name == "test-config"
        assert config.days == 7
        assert config.keywords == ["test", "keyword"]
        assert len(config.sources) == 1
        assert config.sources[0].name == "ynet"

    def test_load_config_empty(self, tmp_path: Path) -> None:
        """Test loading empty config file uses defaults."""
        config_path = tmp_path / "empty.yaml"
        config_path.write_text("")

        config = load_config(config_path)

        assert config.name == "enforcement-news"
        assert config.days == 3
