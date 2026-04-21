"""Unit tests for config module."""

from pathlib import Path

import pytest

from denbust.config import (
    BackfillConfig,
    CandidatesConfig,
    Config,
    DedupConfig,
    DiscoveryConfig,
    DiscoveryQueryKind,
    GoogleDriveBackupConfig,
    ObjectStorageBackupConfig,
    OutputConfig,
    OutputFormat,
    ReleaseConfig,
    SourceConfig,
    SourceDiscoveryConfig,
    SourceType,
    StoreConfig,
    load_config,
)
from denbust.models.common import DatasetName, JobName


class TestConfig:
    """Tests for Config class."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = Config()

        assert config.name == "enforcement-news"
        assert config.dataset_name == DatasetName.NEWS_ITEMS
        assert config.job_name == JobName.INGEST
        assert config.days == 3
        assert config.max_articles == 30
        assert len(config.keywords) > 0
        assert config.dedup.similarity_threshold == 0.7
        assert config.output.format == OutputFormat.CLI
        assert config.output.formats == [OutputFormat.CLI]
        assert config.store.state_root == Path("data")
        assert config.store.seen_path is None
        assert config.store.runs_dir is None
        assert config.store.publication_dir is None
        assert config.discovery.enabled is False
        assert config.discovery.persist_candidates is True
        assert config.discovery.default_query_kinds == [
            DiscoveryQueryKind.BROAD,
            DiscoveryQueryKind.SOURCE_TARGETED,
            DiscoveryQueryKind.SOCIAL_TARGETED,
        ]
        assert config.source_discovery.enabled is True
        assert config.candidates.discovery_runs_table == "discovery_runs"
        assert config.backfill.enabled is False
        assert config.state_paths.seen_path == Path("data/news_items/ingest/seen.json")
        assert config.state_paths.runs_dir == Path("data/news_items/ingest/runs")
        assert config.state_paths.publication_dir == Path("data/news_items/ingest/publication")
        assert config.discovery_state_paths.namespace_dir == Path("data/news_items/discover")
        assert config.discovery_state_paths.latest_candidates_path == Path(
            "data/news_items/discover/candidates/latest_candidates.jsonl"
        )

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
        assert config.formats == [OutputFormat.CLI]

        config = OutputConfig(format=OutputFormat.TELEGRAM)
        assert config.format == OutputFormat.TELEGRAM
        assert config.formats == [OutputFormat.TELEGRAM]

        config = OutputConfig(format=OutputFormat.EMAIL)
        assert config.format == OutputFormat.EMAIL
        assert config.formats == [OutputFormat.EMAIL]

    def test_output_config_multiple_formats(self) -> None:
        """Test multiple output formats are supported and de-duplicated."""
        config = OutputConfig(formats=[OutputFormat.CLI, OutputFormat.EMAIL, OutputFormat.CLI])

        assert config.format == OutputFormat.CLI
        assert config.formats == [OutputFormat.CLI, OutputFormat.EMAIL]

    def test_env_properties(self) -> None:
        """Test environment variable properties."""
        config = Config()

        # These should return None if env vars not set
        # (We don't set them in tests)
        assert config.anthropic_api_key is None or isinstance(config.anthropic_api_key, str)

    def test_env_properties_read_telegram_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Telegram settings should be read from the environment."""
        monkeypatch.setenv("DENBUST_TELEGRAM_BOT_TOKEN", "bot-token")
        monkeypatch.setenv("DENBUST_TELEGRAM_CHAT_ID", "chat-id")

        config = Config()

        assert config.telegram_bot_token == "bot-token"
        assert config.telegram_chat_id == "chat-id"

    def test_email_port_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid SMTP ports should raise a clear error."""
        monkeypatch.setenv("DENBUST_EMAIL_SMTP_PORT", "not-an-int")

        config = Config()

        with pytest.raises(ValueError, match="DENBUST_EMAIL_SMTP_PORT must be an integer"):
            _ = config.email_smtp_port

    def test_store_config_accepts_legacy_path(self) -> None:
        """Legacy store.path config should map to seen_path."""
        store = StoreConfig.model_validate({"path": "custom/seen.json"})

        assert store.seen_path == Path("custom/seen.json")
        assert store.runs_dir is None

    def test_store_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Store paths should be overrideable through environment variables."""
        monkeypatch.setenv("DENBUST_STATE_ROOT", "/tmp/state-root")
        monkeypatch.setenv("DENBUST_STORE_PATH", "/tmp/state/seen.json")
        monkeypatch.setenv("DENBUST_RUNS_DIR", "/tmp/state/runs")

        config = Config(
            store=StoreConfig(
                state_root=Path("ignored-root"),
                seen_path=Path("ignored.json"),
                runs_dir=Path("ignored"),
            )
        )

        assert config.store.seen_path == Path("/tmp/state/seen.json")
        assert config.store.runs_dir == Path("/tmp/state/runs")
        assert config.store.state_root == Path("/tmp/state-root")
        assert config.state_paths.seen_path == Path("/tmp/state/seen.json")
        assert config.state_paths.runs_dir == Path("/tmp/state/runs")

    def test_store_config_defaults_when_validating_none(self) -> None:
        """Validating a missing store config should use defaults."""
        store = StoreConfig.model_validate(None)

        assert store.state_root == Path("data")
        assert store.seen_path is None
        assert store.runs_dir is None

    def test_config_normalizes_scan_job_to_ingest(self) -> None:
        """Legacy scan naming should map to the canonical ingest job."""
        config = Config.model_validate({"job_name": "scan"})

        assert config.job_name == JobName.INGEST

    def test_config_identity_normalizer_passthrough_for_non_mapping(self) -> None:
        """The config identity normalizer should pass through non-mapping values unchanged."""
        sentinel = object()

        assert Config._normalize_identity(sentinel) is sentinel

    def test_config_derives_namespaced_paths_from_state_root(self) -> None:
        """State paths should be namespaced by dataset and job."""
        config = Config(
            dataset_name=DatasetName.DOCS_METADATA,
            job_name=JobName.BACKUP,
            store={"state_root": "state-root"},
        )

        assert config.state_paths.namespace_dir == Path("state-root/docs_metadata/backup")
        assert config.state_paths.seen_path == Path("state-root/docs_metadata/backup/seen.json")
        assert config.state_paths.runs_dir == Path("state-root/docs_metadata/backup/runs")
        assert config.state_paths.publication_dir == Path(
            "state-root/docs_metadata/backup/publication"
        )

    def test_config_explicit_paths_override_derived_root(self) -> None:
        """Explicit YAML paths should bypass state_root derivation."""
        config = Config(
            store={
                "state_root": "state-root",
                "seen_path": "custom/seen.json",
                "runs_dir": "custom/runs",
                "publication_dir": "custom/publication",
            }
        )

        assert config.state_paths.seen_path == Path("custom/seen.json")
        assert config.state_paths.runs_dir == Path("custom/runs")
        assert config.state_paths.publication_dir == Path("custom/publication")

    def test_source_discovery_config_normalizes_boolean_source_toggles(self) -> None:
        """Per-source discovery config should accept shorthand booleans."""
        config = Config.model_validate(
            {
                "source_discovery": {
                    "sources": {
                        "ynet": True,
                        "mako": {"enabled": False},
                    }
                }
            }
        )

        assert config.source_discovery.sources["ynet"].enabled is True
        assert config.source_discovery.sources["mako"].enabled is False

    def test_source_discovery_normalizer_passthrough_for_non_mapping(self) -> None:
        """SourceDiscoveryConfig's pre-validator should leave non-mapping values untouched."""
        sentinel = object()

        assert SourceDiscoveryConfig._normalize_sources(sentinel) is sentinel
        assert SourceDiscoveryConfig._normalize_sources(None) is None

    def test_source_discovery_rejects_non_boolean_shorthand(self) -> None:
        """Source shorthand should only accept actual booleans, not truthy strings."""
        with pytest.raises(ValueError, match="sources entries must be mappings or booleans"):
            SourceDiscoveryConfig.model_validate(
                {
                    "sources": {
                        "ynet": "false",
                    }
                }
            )

    def test_discovery_config_parses_engine_blocks(self) -> None:
        """Discovery engine settings should parse from YAML-like mappings."""
        config = DiscoveryConfig.model_validate(
            {
                "enabled": True,
                "engines": {
                    "brave": {"enabled": True, "max_results_per_query": 25},
                    "exa": {"enabled": True, "allow_find_similar": False},
                    "google_cse": {"enabled": True, "cse_id_env": "CUSTOM_GOOGLE_CSE_ID"},
                },
            }
        )

        assert config.enabled is True
        assert config.engines.brave.enabled is True
        assert config.engines.brave.max_results_per_query == 25
        assert config.engines.exa.allow_find_similar is False
        assert config.engines.google_cse.cse_id_env == "CUSTOM_GOOGLE_CSE_ID"

    def test_candidates_config_defaults(self) -> None:
        """Candidate persistence defaults should match the design doc scaffolding."""
        config = CandidatesConfig()

        assert config.discovery_runs_table == "discovery_runs"
        assert config.supabase_table == "persistent_candidates"
        assert config.backfill_batches_table == "backfill_batches"
        assert config.provenance_table == "candidate_provenance"
        assert config.scrape_attempts_table == "scrape_attempts"
        assert config.default_retry_backoff_hours == 24
        assert config.max_retry_attempts == 10

    def test_backfill_config_validation(self) -> None:
        """Backfill settings should enforce positive queue limits."""
        config = BackfillConfig()

        assert config.batch_window_days == 7
        assert config.max_candidates_per_run == 500
        assert config.max_scrape_attempts_per_run == 100

        with pytest.raises(ValueError):
            BackfillConfig(batch_window_days=0)

    def test_store_normalizer_passthrough_for_non_mapping(self) -> None:
        """The pre-validator should pass through non-mapping values unchanged."""
        sentinel = object()

        assert StoreConfig._normalize_paths(sentinel) is sentinel

    def test_release_config_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Release config should pick up configured publication targets from env."""
        monkeypatch.setenv("DENBUST_KAGGLE_DATASET", "owner/news-items")
        monkeypatch.setenv("DENBUST_HUGGINGFACE_REPO_ID", "org/news-items")

        config = ReleaseConfig.model_validate(None)

        assert config.kaggle_dataset == "owner/news-items"
        assert config.huggingface_repo_id == "org/news-items"

    def test_release_config_validator_passthrough_for_non_mapping(self) -> None:
        """ReleaseConfig's pre-validator should leave non-mapping values untouched."""
        sentinel = object()

        assert ReleaseConfig._apply_env_overrides(sentinel) is sentinel

    def test_backup_target_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Backup target configs should enable themselves when env vars are present."""
        monkeypatch.setenv("DENBUST_DRIVE_FOLDER_ID", "drive-folder")
        monkeypatch.setenv("DENBUST_OBJECT_STORE_BUCKET", "bucket")
        monkeypatch.setenv("DENBUST_OBJECT_STORE_PREFIX", "latest/news-items")

        drive = GoogleDriveBackupConfig.model_validate(None)
        object_storage = ObjectStorageBackupConfig.model_validate(None)

        assert drive.enabled is True
        assert drive.folder_id == "drive-folder"
        assert object_storage.enabled is True
        assert object_storage.bucket == "bucket"
        assert object_storage.prefix == "latest/news-items"

    def test_backup_target_validators_passthrough_for_non_mapping(self) -> None:
        """Backup config pre-validators should leave non-mapping values untouched."""
        sentinel = object()

        assert GoogleDriveBackupConfig._apply_env_overrides(sentinel) is sentinel
        assert ObjectStorageBackupConfig._apply_env_overrides(sentinel) is sentinel

    def test_discovery_env_properties(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Discovery API key helpers should honor configured env names."""
        monkeypatch.setenv("DENBUST_BRAVE_SEARCH_API_KEY", "brave-key")
        monkeypatch.setenv("DENBUST_EXA_API_KEY", "exa-key")
        monkeypatch.setenv("DENBUST_GOOGLE_CSE_API_KEY", "google-key")
        monkeypatch.setenv("DENBUST_GOOGLE_CSE_ID", "search-engine-id")

        config = Config()

        assert config.brave_search_api_key == "brave-key"
        assert config.exa_api_key == "exa-key"
        assert config.google_cse_api_key == "google-key"
        assert config.google_cse_id == "search-engine-id"

    def test_phase_b_env_properties(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Phase B service credentials should be exposed through config properties."""
        monkeypatch.setenv("DENBUST_SUPABASE_URL", "https://supabase.example")
        monkeypatch.setenv("DENBUST_SUPABASE_SERVICE_ROLE_KEY", "service-role")
        monkeypatch.setenv("HF_TOKEN", "hf-token")
        monkeypatch.setenv("KAGGLE_USERNAME", "kaggle-user")
        monkeypatch.setenv("KAGGLE_KEY", "kaggle-key")
        monkeypatch.setenv("DENBUST_DRIVE_SERVICE_ACCOUNT_JSON", "/tmp/sa.json")
        monkeypatch.setenv("DENBUST_OBJECT_STORE_ENDPOINT_URL", "https://r2.example")
        monkeypatch.setenv("DENBUST_OBJECT_STORE_ACCESS_KEY_ID", "access")
        monkeypatch.setenv("DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY", "secret")

        config = Config()

        assert config.supabase_url == "https://supabase.example"
        assert config.supabase_service_role_key == "service-role"
        assert config.huggingface_token == "hf-token"
        assert config.kaggle_username == "kaggle-user"
        assert config.kaggle_key == "kaggle-key"
        assert config.drive_service_account_json == "/tmp/sa.json"
        assert config.object_store_endpoint_url == "https://r2.example"
        assert config.object_store_access_key_id == "access"
        assert config.object_store_secret_access_key == "secret"


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

    def test_load_config_multiple_output_formats(self, tmp_path: Path) -> None:
        """Test loading multiple output formats from YAML."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
output:
  formats:
    - cli
    - email
"""
        )

        config = load_config(config_path)

        assert config.output.format == OutputFormat.CLI
        assert config.output.formats == [OutputFormat.CLI, OutputFormat.EMAIL]

    def test_load_config_legacy_store_path(self, tmp_path: Path) -> None:
        """Legacy YAML store.path should still load correctly."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
store:
  path: state/seen.json
"""
        )

        config = load_config(config_path)

        assert config.store.seen_path == Path("state/seen.json")
        assert config.store.runs_dir is None
        assert config.state_paths.seen_path == Path("state/seen.json")
        assert config.state_paths.runs_dir == Path("data/news_items/ingest/runs")
