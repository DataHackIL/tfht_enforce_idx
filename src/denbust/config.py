"""Configuration management for denbust."""

import os
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class SourceType(StrEnum):
    """Type of news source."""

    RSS = "rss"
    SCRAPER = "scraper"


class SourceConfig(BaseModel):
    """Configuration for a news source."""

    name: str
    type: SourceType
    url: str | None = None  # Required for RSS sources
    enabled: bool = True


class ClassifierConfig(BaseModel):
    """Configuration for LLM classifier."""

    provider: str = "anthropic"
    model: str = "claude-sonnet-4-20250514"


class DedupConfig(BaseModel):
    """Configuration for deduplication."""

    similarity_threshold: float = Field(default=0.7, ge=0.0, le=1.0)


class OutputFormat(StrEnum):
    """Output format."""

    CLI = "cli"
    TELEGRAM = "telegram"


class OutputConfig(BaseModel):
    """Configuration for output."""

    format: OutputFormat = OutputFormat.CLI


class StoreConfig(BaseModel):
    """Configuration for persistence."""

    path: Path = Path("data/seen.json")


# Default keywords for searching news articles (Hebrew)
DEFAULT_KEYWORDS: list[str] = [
    "זנות",  # prostitution
    "בית בושת",  # brothel
    "סרסור",  # pimping
    "סחר בבני אדם",  # human trafficking
    "צו סגירה",  # closure order
    "ליווי",  # escort
    "נערות ליווי",  # escort girls
    "תעשיית המין",  # sex industry
    "עיסוי חשוד",  # suspicious massage
    "זירת זנות",  # prostitution site
]


class Config(BaseModel):
    """Root configuration for denbust."""

    name: str = "enforcement-news"
    days: int = Field(default=14, ge=1)
    keywords: list[str] = Field(default_factory=lambda: DEFAULT_KEYWORDS.copy())
    sources: list[SourceConfig] = Field(default_factory=list)
    classifier: ClassifierConfig = Field(default_factory=ClassifierConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)

    @property
    def anthropic_api_key(self) -> str | None:
        """Get Anthropic API key from environment."""
        return os.environ.get("ANTHROPIC_API_KEY")

    @property
    def telegram_bot_token(self) -> str | None:
        """Get Telegram bot token from environment."""
        return os.environ.get("DENBUST_TELEGRAM_BOT_TOKEN")

    @property
    def telegram_chat_id(self) -> str | None:
        """Get Telegram chat ID from environment."""
        return os.environ.get("DENBUST_TELEGRAM_CHAT_ID")


def load_config(path: Path) -> Config:
    """Load configuration from YAML file.

    Args:
        path: Path to YAML config file.

    Returns:
        Parsed Config object.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config is invalid.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    if data is None:
        data = {}

    return Config.model_validate(data)
