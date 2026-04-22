"""Configuration management for denbust."""

import os
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

from denbust.discovery.models import DiscoveryQueryKind
from denbust.discovery.state_paths import DiscoveryStatePaths, resolve_discovery_state_paths
from denbust.models.common import DatasetName, JobName, normalize_job_name
from denbust.store.state_paths import DatasetStatePaths, resolve_dataset_state_paths


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
    system_prompt: str | None = None
    user_prompt_template: str | None = None


class DedupConfig(BaseModel):
    """Configuration for deduplication."""

    similarity_threshold: float = Field(default=0.7, ge=0.0, le=1.0)


class OutputFormat(StrEnum):
    """Output format."""

    CLI = "cli"
    TELEGRAM = "telegram"
    EMAIL = "email"


class OutputConfig(BaseModel):
    """Configuration for output."""

    format: OutputFormat = OutputFormat.CLI
    formats: list[OutputFormat] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize_formats(self) -> "OutputConfig":
        """Normalize legacy single-format config into a de-duplicated formats list."""
        normalized: list[OutputFormat] = [self.format]

        for output_format in self.formats:
            if output_format not in normalized:
                normalized.append(output_format)

        self.formats = normalized
        self.format = normalized[0]
        return self


class StoreConfig(BaseModel):
    """Configuration for persistence."""

    state_root: Path = Path("data")
    seen_path: Path | None = None
    runs_dir: Path | None = None
    publication_dir: Path | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_paths(cls, data: object) -> object:
        """Support legacy config keys and env-based path overrides."""
        if data is None:
            normalized: dict[str, object] = {}
        elif isinstance(data, dict):
            normalized = dict(data)
        else:
            return data

        legacy_path = normalized.pop("path", None)
        if "seen_path" not in normalized and legacy_path is not None:
            normalized["seen_path"] = legacy_path

        env_state_root = os.environ.get("DENBUST_STATE_ROOT")
        if env_state_root:
            normalized["state_root"] = env_state_root

        env_seen_path = os.environ.get("DENBUST_STORE_PATH")
        if env_seen_path:
            normalized["seen_path"] = env_seen_path

        env_runs_dir = os.environ.get("DENBUST_RUNS_DIR")
        if env_runs_dir:
            normalized["runs_dir"] = env_runs_dir

        return normalized


class OperationalProvider(StrEnum):
    """Operational persistence backend."""

    NONE = "none"
    LOCAL_JSON = "local_json"
    SUPABASE = "supabase"


class OperationalConfig(BaseModel):
    """Configuration for operational persistence."""

    provider: OperationalProvider = OperationalProvider.NONE
    root_dir: Path | None = None
    supabase_schema: str = "public"
    news_items_table: str = "news_items"
    ingestion_runs_table: str = "ingestion_runs"
    release_runs_table: str = "release_runs"
    backup_runs_table: str = "backup_runs"
    suppression_rules_table: str = "suppression_rules"
    news_items_corrections_table: str = "news_items_corrections"
    news_items_missing_items_table: str = "news_items_missing_items"


class ReleaseConfig(BaseModel):
    """Configuration for release/export generation."""

    schema_version: str = "news_items-v1"
    include_csv: bool = True
    kaggle_dataset: str | None = None
    huggingface_repo_id: str | None = None
    rights_policy_version: str = "news_items-v1"
    privacy_policy_version: str = "news_items-v1"

    @model_validator(mode="before")
    @classmethod
    def _apply_env_overrides(cls, data: object) -> object:
        if data is None:
            normalized: dict[str, object] = {}
        elif isinstance(data, dict):
            normalized = dict(data)
        else:
            return data

        if os.environ.get("DENBUST_KAGGLE_DATASET"):
            normalized["kaggle_dataset"] = os.environ["DENBUST_KAGGLE_DATASET"]
        if os.environ.get("DENBUST_HUGGINGFACE_REPO_ID"):
            normalized["huggingface_repo_id"] = os.environ["DENBUST_HUGGINGFACE_REPO_ID"]
        return normalized


class GoogleDriveBackupConfig(BaseModel):
    """Google Drive latest-backup target configuration.

    `enabled` may be turned on explicitly in YAML or implicitly when
    `DENBUST_DRIVE_FOLDER_ID` is present in the environment.
    """

    enabled: bool = False
    folder_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _apply_env_overrides(cls, data: object) -> object:
        if data is None:
            normalized: dict[str, object] = {}
        elif isinstance(data, dict):
            normalized = dict(data)
        else:
            return data
        if os.environ.get("DENBUST_DRIVE_FOLDER_ID"):
            normalized["folder_id"] = os.environ["DENBUST_DRIVE_FOLDER_ID"]
            normalized["enabled"] = True
        return normalized


class ObjectStorageBackupConfig(BaseModel):
    """Object-storage latest-backup target configuration.

    `enabled` may be turned on explicitly in YAML or implicitly when
    `DENBUST_OBJECT_STORE_BUCKET` is present in the environment.
    """

    enabled: bool = False
    bucket: str | None = None
    prefix: str = "news_items/latest"

    @model_validator(mode="before")
    @classmethod
    def _apply_env_overrides(cls, data: object) -> object:
        if data is None:
            normalized: dict[str, object] = {}
        elif isinstance(data, dict):
            normalized = dict(data)
        else:
            return data
        if os.environ.get("DENBUST_OBJECT_STORE_BUCKET"):
            normalized["bucket"] = os.environ["DENBUST_OBJECT_STORE_BUCKET"]
            normalized["enabled"] = True
        if os.environ.get("DENBUST_OBJECT_STORE_PREFIX"):
            normalized["prefix"] = os.environ["DENBUST_OBJECT_STORE_PREFIX"]
        return normalized


class BackupConfig(BaseModel):
    """Configuration for backup upload targets."""

    google_drive: GoogleDriveBackupConfig = Field(default_factory=GoogleDriveBackupConfig)
    object_storage: ObjectStorageBackupConfig = Field(default_factory=ObjectStorageBackupConfig)


class DiscoveryEngineConfig(BaseModel):
    """Base configuration for a discovery engine."""

    enabled: bool = False
    api_key_env: str | None = None
    max_results_per_query: int = Field(default=20, ge=1)


class ExaDiscoveryEngineConfig(DiscoveryEngineConfig):
    """Exa-specific discovery configuration."""

    allow_find_similar: bool = True


class GoogleCseDiscoveryEngineConfig(DiscoveryEngineConfig):
    """Google Custom Search Engine configuration."""

    cse_id_env: str | None = None
    max_results_per_query: int = Field(default=10, ge=1)


class DiscoveryEnginesConfig(BaseModel):
    """Search-engine configuration for durable discovery."""

    brave: DiscoveryEngineConfig = Field(
        default_factory=lambda: DiscoveryEngineConfig(
            enabled=False,
            api_key_env="DENBUST_BRAVE_SEARCH_API_KEY",
            max_results_per_query=20,
        )
    )
    exa: ExaDiscoveryEngineConfig = Field(
        default_factory=lambda: ExaDiscoveryEngineConfig(
            enabled=False,
            api_key_env="DENBUST_EXA_API_KEY",
            max_results_per_query=20,
            allow_find_similar=True,
        )
    )
    google_cse: GoogleCseDiscoveryEngineConfig = Field(
        default_factory=lambda: GoogleCseDiscoveryEngineConfig(
            enabled=False,
            api_key_env="DENBUST_GOOGLE_CSE_API_KEY",
            cse_id_env="DENBUST_GOOGLE_CSE_ID",
            max_results_per_query=10,
        )
    )


class DiscoveryConfig(BaseModel):
    """Top-level durable discovery configuration."""

    enabled: bool = False
    persist_candidates: bool = True
    engines: DiscoveryEnginesConfig = Field(default_factory=DiscoveryEnginesConfig)
    default_query_kinds: list[DiscoveryQueryKind] = Field(
        default_factory=lambda: [
            DiscoveryQueryKind.BROAD,
            DiscoveryQueryKind.SOURCE_TARGETED,
            DiscoveryQueryKind.TAXONOMY_TARGETED,
            DiscoveryQueryKind.SOCIAL_TARGETED,
        ]
    )


class SourceDiscoveryProducerConfig(BaseModel):
    """Per-source source-native candidacy configuration."""

    enabled: bool = True


class SourceDiscoveryConfig(BaseModel):
    """Configuration for source-native candidate persistence."""

    enabled: bool = True
    persist_candidates: bool = True
    sources: dict[str, SourceDiscoveryProducerConfig] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _normalize_sources(cls, data: object) -> object:
        if data is None or not isinstance(data, Mapping):
            return data

        normalized = dict(data)
        sources = normalized.get("sources")
        if not isinstance(sources, Mapping):
            return normalized

        normalized_sources: dict[str, Mapping[str, object] | dict[str, bool]] = {}
        for name, value in sources.items():
            if isinstance(value, Mapping):
                normalized_sources[str(name)] = value
            elif isinstance(value, bool):
                normalized_sources[str(name)] = {"enabled": value}
            else:
                raise ValueError(
                    "sources entries must be mappings or booleans for shorthand "
                    f"(got {type(value).__name__} for source {name!r})"
                )

        normalized["sources"] = normalized_sources
        return normalized


class CandidatesConfig(BaseModel):
    """Configuration for durable candidate persistence and retry semantics."""

    discovery_runs_table: str = "discovery_runs"
    supabase_table: str = "persistent_candidates"
    backfill_batches_table: str = "backfill_batches"
    provenance_table: str = "candidate_provenance"
    scrape_attempts_table: str = "scrape_attempts"
    keep_search_only_fallbacks: bool = True
    require_review_for_search_only: bool = True
    allow_retry_on_fetch_failure: bool = True
    default_retry_backoff_hours: int = Field(default=24, ge=1)
    max_retry_attempts: int = Field(default=10, ge=1)


class BackfillConfig(BaseModel):
    """Optional backfill orchestration configuration."""

    enabled: bool = False
    batch_window_days: int = Field(default=7, ge=1)
    max_candidates_per_run: int = Field(default=500, ge=1)
    max_scrape_attempts_per_run: int = Field(default=100, ge=1)


# Default keywords for searching news articles (Hebrew)
DEFAULT_KEYWORDS: list[str] = [
    "זנות",  # prostitution
    "בית בושת",  # brothel
    "סרסור",  # pimping
    "סחר בבני אדם",  # human trafficking
    "צו סגירה",  # closure order
    "צו הגבלת שימוש",  # use restriction order
    "ליווי",  # escort
    "נערות ליווי",  # escort girls
    "תעשיית המין",  # sex industry
    "עיסוי חשוד",  # suspicious massage
    "זירת זנות",  # prostitution site
    "נישואין בכפייה",  # forced marriage
    "עבדות מינית",  # sexual slavery
    "זנות מקוונת",  # online prostitution
    "קנס צריכת זנות",  # prostitution-consumption fine
    "החזקת מקום לשם זנות",  # keeping a place for prostitution
    "השכרת מקום לשם זנות",  # renting a place for prostitution
    "פרסום זנות",  # advertising prostitution
]


class Config(BaseModel):
    """Root configuration for denbust."""

    name: str = "enforcement-news"
    dataset_name: DatasetName = DatasetName.NEWS_ITEMS
    job_name: JobName = JobName.INGEST
    days: int = Field(default=3, ge=1)
    max_articles: int = Field(default=30, ge=1)
    keywords: list[str] = Field(default_factory=lambda: DEFAULT_KEYWORDS.copy())
    sources: list[SourceConfig] = Field(default_factory=list)
    classifier: ClassifierConfig = Field(default_factory=ClassifierConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    operational: OperationalConfig = Field(default_factory=OperationalConfig)
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    source_discovery: SourceDiscoveryConfig = Field(default_factory=SourceDiscoveryConfig)
    candidates: CandidatesConfig = Field(default_factory=CandidatesConfig)
    backfill: BackfillConfig = Field(default_factory=BackfillConfig)
    release: ReleaseConfig = Field(default_factory=ReleaseConfig)
    backup: BackupConfig = Field(default_factory=BackupConfig)

    @model_validator(mode="before")
    @classmethod
    def _normalize_identity(cls, data: object) -> object:
        """Normalize dataset/job identity for backward compatibility."""
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        if "dataset_name" not in normalized:
            normalized["dataset_name"] = DatasetName.NEWS_ITEMS
        normalized["job_name"] = normalize_job_name(normalized.get("job_name"))
        return normalized

    @property
    def state_paths(self) -> DatasetStatePaths:
        """Resolve dataset/job-scoped state paths for this config."""
        return resolve_dataset_state_paths(
            state_root=self.store.state_root,
            dataset_name=self.dataset_name,
            job_name=self.job_name,
            seen_path=self.store.seen_path,
            runs_dir=self.store.runs_dir,
            publication_dir=self.store.publication_dir,
        )

    @property
    def discovery_state_paths(self) -> DiscoveryStatePaths:
        """Resolve candidate-layer state paths for this dataset."""
        return resolve_discovery_state_paths(
            state_root=self.store.state_root,
            dataset_name=self.dataset_name,
        )

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

    @property
    def email_smtp_host(self) -> str | None:
        """Get SMTP host from environment."""
        return os.environ.get("DENBUST_EMAIL_SMTP_HOST")

    @property
    def email_smtp_port(self) -> int:
        """Get SMTP port from environment."""
        raw_port = os.environ.get("DENBUST_EMAIL_SMTP_PORT", "587")
        try:
            return int(raw_port)
        except ValueError as exc:
            raise ValueError("DENBUST_EMAIL_SMTP_PORT must be an integer") from exc

    @property
    def email_smtp_username(self) -> str | None:
        """Get SMTP username from environment."""
        return os.environ.get("DENBUST_EMAIL_SMTP_USERNAME")

    @property
    def email_smtp_password(self) -> str | None:
        """Get SMTP password from environment."""
        return os.environ.get("DENBUST_EMAIL_SMTP_PASSWORD")

    @property
    def email_from(self) -> str | None:
        """Get sender email address from environment."""
        return os.environ.get("DENBUST_EMAIL_FROM")

    @property
    def email_to(self) -> list[str]:
        """Get recipient email addresses from environment."""
        raw_recipients = os.environ.get("DENBUST_EMAIL_TO", "")
        return [email.strip() for email in raw_recipients.split(",") if email.strip()]

    @property
    def email_use_tls(self) -> bool:
        """Get SMTP STARTTLS flag from environment."""
        raw_value = os.environ.get("DENBUST_EMAIL_USE_TLS", "true").strip().lower()
        return raw_value not in {"0", "false", "no", "off"}

    @property
    def email_subject(self) -> str | None:
        """Get optional email subject from environment."""
        return os.environ.get("DENBUST_EMAIL_SUBJECT")

    @property
    def supabase_url(self) -> str | None:
        """Get Supabase project URL from the environment."""
        return os.environ.get("DENBUST_SUPABASE_URL")

    @property
    def supabase_service_role_key(self) -> str | None:
        """Get the Supabase service role key from the environment."""
        return os.environ.get("DENBUST_SUPABASE_SERVICE_ROLE_KEY")

    @property
    def brave_search_api_key(self) -> str | None:
        """Get the Brave Search API key from the configured environment variable."""
        env_name = self.discovery.engines.brave.api_key_env
        return os.environ.get(env_name) if env_name else None

    @property
    def exa_api_key(self) -> str | None:
        """Get the Exa API key from the configured environment variable."""
        env_name = self.discovery.engines.exa.api_key_env
        return os.environ.get(env_name) if env_name else None

    @property
    def google_cse_api_key(self) -> str | None:
        """Get the Google CSE API key from the configured environment variable."""
        env_name = self.discovery.engines.google_cse.api_key_env
        return os.environ.get(env_name) if env_name else None

    @property
    def google_cse_id(self) -> str | None:
        """Get the Google CSE identifier from the configured environment variable."""
        env_name = self.discovery.engines.google_cse.cse_id_env
        return os.environ.get(env_name) if env_name else None

    @property
    def huggingface_token(self) -> str | None:
        """Get the Hugging Face token from the environment."""
        return os.environ.get("HF_TOKEN")

    @property
    def kaggle_username(self) -> str | None:
        """Get the Kaggle username from the environment."""
        return os.environ.get("KAGGLE_USERNAME")

    @property
    def kaggle_key(self) -> str | None:
        """Get the Kaggle API key from the environment."""
        return os.environ.get("KAGGLE_KEY")

    @property
    def drive_service_account_json(self) -> str | None:
        """Get the Google Drive service-account JSON from the environment."""
        return os.environ.get("DENBUST_DRIVE_SERVICE_ACCOUNT_JSON")

    @property
    def object_store_endpoint_url(self) -> str | None:
        """Get the object-storage endpoint URL from the environment."""
        return os.environ.get("DENBUST_OBJECT_STORE_ENDPOINT_URL")

    @property
    def object_store_access_key_id(self) -> str | None:
        """Get the object-storage access key ID from the environment."""
        return os.environ.get("DENBUST_OBJECT_STORE_ACCESS_KEY_ID")

    @property
    def object_store_secret_access_key(self) -> str | None:
        """Get the object-storage secret access key from the environment."""
        return os.environ.get("DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY")


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
