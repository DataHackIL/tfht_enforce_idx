"""Pydantic configuration models for the pre-classification filter cascade."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# PrefilterMode
# ---------------------------------------------------------------------------


class PrefilterMode(StrEnum):
    """Operational mode for the cascade.

    OFF
        Cascade is not invoked; pipeline is completely unchanged.
    SHADOW
        Cascade runs and records ``PrefilterDecision`` rows, but does **not**
        drop any candidate from the Claude queue.  Use this for the first
        validation period after wiring the cascade into the pipeline.
    ENFORCE
        Cascade drops candidates whose verdict is ``"drop"`` before they reach
        the Claude classifier.  Only safe to enable after a shadow period with
        confirmed recall ≥ the configured floor.
    """

    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


# ---------------------------------------------------------------------------
# Per-stage config
# ---------------------------------------------------------------------------


class StageAConfig(BaseModel):
    """Configuration for Stage A (lexicon + domain reputation + URL heuristics)."""

    enabled: bool = True
    threshold: float = Field(default=0.95, ge=0.0, le=1.0)


class StageBConfig(BaseModel):
    """Configuration for Stage B (trained text classifier)."""

    enabled: bool = True
    model: str = "naive_bayes"
    threshold: float = Field(default=0.95, ge=0.0, le=1.0)


class StageCConfig(BaseModel):
    """Configuration for Stage C (embedding centroid / kNN similarity)."""

    enabled: bool = True
    model: str = "multilingual-e5-large"
    threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    enable_for_thin_pass: bool = False


class StageDConfig(BaseModel):
    """Configuration for Stage D (local SLM logprob judge)."""

    enabled: bool = True
    model: str = "dictalm2.0-instruct"
    backend: str = "mlx"
    batch_size: int = Field(default=4, ge=1)
    threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    timeout_seconds: float = Field(default=5.0, gt=0.0)


class PrefilterStagesConfig(BaseModel):
    """Container for all per-stage configurations."""

    a: StageAConfig = Field(default_factory=StageAConfig)
    b: StageBConfig = Field(default_factory=StageBConfig)
    c: StageCConfig = Field(default_factory=StageCConfig)
    d: StageDConfig = Field(default_factory=StageDConfig)


# ---------------------------------------------------------------------------
# Refresh schedule config
# ---------------------------------------------------------------------------


class PrefilterRefreshConfig(BaseModel):
    """Schedule configuration for periodic model-artifact refreshes."""

    domain_reputation_min_observations: int = Field(default=20, ge=1)
    domain_reputation_recompute_every_days: int = Field(default=7, ge=1)


# ---------------------------------------------------------------------------
# Root PrefilterConfig
# ---------------------------------------------------------------------------


class PrefilterConfig(BaseModel):
    """Top-level configuration for the local pre-classification filter cascade.

    Placed under the ``prefilter:`` key in dataset YAML configs.  When
    ``enabled`` is ``False`` (the default), the cascade is a silent no-op and
    adds zero overhead to the pipeline.
    """

    enabled: bool = False
    mode: PrefilterMode = PrefilterMode.OFF
    model_cache_dir: Path = Path("~/.cache/denbust/prefilter")
    stages: PrefilterStagesConfig = Field(default_factory=PrefilterStagesConfig)
    recall_floor_per_stage: float = Field(default=0.99, gt=0.0, le=1.0)
    shadow_min_days_before_enforce: int = Field(default=7, ge=1)
    refresh: PrefilterRefreshConfig = Field(default_factory=PrefilterRefreshConfig)

    @model_validator(mode="after")
    def _expand_model_cache_dir(self) -> PrefilterConfig:
        """Expand ``~`` in ``model_cache_dir`` to the real home directory."""
        self.model_cache_dir = self.model_cache_dir.expanduser()
        return self
