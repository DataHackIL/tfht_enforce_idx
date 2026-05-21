"""Unit tests for prefilter.config — parsing, validation, YAML round-trip."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from denbust.prefilter.config import (
    PrefilterConfig,
    PrefilterMode,
    StageAConfig,
    StageBConfig,
    StageDConfig,
)


class TestPrefilterMode:
    def test_values(self) -> None:
        assert PrefilterMode.OFF == "off"
        assert PrefilterMode.SHADOW == "shadow"
        assert PrefilterMode.ENFORCE == "enforce"

    def test_unknown_mode_raises(self) -> None:
        with pytest.raises(ValidationError):
            PrefilterConfig.model_validate({"mode": "turbo"})


class TestDefaultConfig:
    def test_defaults(self) -> None:
        cfg = PrefilterConfig()
        assert cfg.enabled is False
        assert cfg.mode == PrefilterMode.OFF
        assert cfg.recall_floor_per_stage == 0.99
        assert cfg.shadow_min_days_before_enforce == 7
        assert cfg.refresh.domain_reputation_min_observations == 20
        assert cfg.refresh.domain_reputation_recompute_every_days == 7

    def test_model_cache_dir_tilde_expanded(self) -> None:
        cfg = PrefilterConfig()
        assert "~" not in str(cfg.model_cache_dir)
        assert cfg.model_cache_dir.is_absolute()

    def test_stage_defaults(self) -> None:
        cfg = PrefilterConfig()
        assert cfg.stages.a.enabled is True
        assert cfg.stages.a.threshold == 0.95
        assert cfg.stages.b.model == "naive_bayes"
        assert cfg.stages.c.enable_for_thin_pass is False
        assert cfg.stages.d.timeout_seconds == 5.0
        assert cfg.stages.d.batch_size == 4


class TestValidation:
    def test_threshold_above_one_raises(self) -> None:
        with pytest.raises(ValidationError):
            StageAConfig(threshold=1.5)

    def test_threshold_below_zero_raises(self) -> None:
        with pytest.raises(ValidationError):
            StageBConfig(threshold=-0.1)

    def test_threshold_at_bounds_ok(self) -> None:
        assert StageAConfig(threshold=0.0).threshold == 0.0
        assert StageAConfig(threshold=1.0).threshold == 1.0

    def test_recall_floor_above_one_raises(self) -> None:
        with pytest.raises(ValidationError):
            PrefilterConfig(recall_floor_per_stage=1.1)

    def test_recall_floor_zero_raises(self) -> None:
        with pytest.raises(ValidationError):
            PrefilterConfig(recall_floor_per_stage=0.0)

    def test_timeout_nonpositive_raises(self) -> None:
        with pytest.raises(ValidationError):
            StageDConfig(timeout_seconds=0.0)

    def test_batch_size_zero_raises(self) -> None:
        with pytest.raises(ValidationError):
            StageDConfig(batch_size=0)

    def test_shadow_min_days_zero_raises(self) -> None:
        with pytest.raises(ValidationError):
            PrefilterConfig(shadow_min_days_before_enforce=0)


class TestYamlRoundTrip:
    def test_round_trip_defaults(self) -> None:
        original = PrefilterConfig()
        dumped = yaml.safe_dump(original.model_dump(mode="json"))
        loaded_data = yaml.safe_load(dumped)
        restored = PrefilterConfig.model_validate(loaded_data)
        assert restored.mode == original.mode
        assert restored.enabled == original.enabled
        assert restored.stages.a.threshold == original.stages.a.threshold
        assert restored.stages.d.model == original.stages.d.model

    def test_round_trip_custom(self) -> None:
        original = PrefilterConfig.model_validate(
            {
                "enabled": True,
                "mode": "shadow",
                "stages": {
                    "a": {"threshold": 0.90},
                    "d": {"model": "qwen2.5-7b-instruct", "batch_size": 8},
                },
            }
        )
        dumped = yaml.safe_dump(original.model_dump(mode="json"))
        loaded_data = yaml.safe_load(dumped)
        restored = PrefilterConfig.model_validate(loaded_data)
        assert restored.enabled is True
        assert restored.mode == PrefilterMode.SHADOW
        assert restored.stages.a.threshold == 0.90
        assert restored.stages.d.batch_size == 8
        assert restored.stages.d.model == "qwen2.5-7b-instruct"


class TestConfigIntegrationWithRootConfig:
    """Verify that PrefilterConfig parses when embedded in the root Config."""

    def test_root_config_has_prefilter_field(self) -> None:
        from denbust.config import Config

        cfg = Config()
        assert isinstance(cfg.prefilter, PrefilterConfig)
        assert cfg.prefilter.enabled is False
        assert cfg.prefilter.mode == PrefilterMode.OFF

    def test_root_config_parses_prefilter_yaml_block(self, tmp_path: Path) -> None:
        from denbust.config import load_config

        yaml_content = """
name: test
prefilter:
  enabled: true
  mode: shadow
  stages:
    a:
      threshold: 0.90
"""
        cfg_path = tmp_path / "test.yaml"
        cfg_path.write_text(yaml_content, encoding="utf-8")
        cfg = load_config(cfg_path)
        assert cfg.prefilter.enabled is True
        assert cfg.prefilter.mode == PrefilterMode.SHADOW
        assert cfg.prefilter.stages.a.threshold == 0.90
