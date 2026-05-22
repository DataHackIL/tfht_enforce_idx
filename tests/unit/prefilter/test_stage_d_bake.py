"""Unit tests for bake_stage_d artifact writing in prefilter.stage_d.

These tests do NOT require mlx_lm to be installed — baking only writes files
and does not load any model.  All tests run on every platform.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from denbust.prefilter.stage_d import (
    _DEFAULT_BASE_MODEL_D,
    _DEFAULT_CB_THRESHOLD,
    _DEFAULT_PROMPT_TEMPLATE,
    _DEFAULT_TIMEOUT_SECONDS,
    _PROMPT_FILE,
    _STAGE_D_SUBDIR,
    StageDModelMeta,
    bake_stage_d,
)

# ---------------------------------------------------------------------------
# Artifact existence and structure
# ---------------------------------------------------------------------------


class TestBakeArtifacts:
    """bake_stage_d writes the expected files with correct structure."""

    def test_returns_meta_and_path(self, tmp_path: Path) -> None:
        meta, stage_dir = bake_stage_d(tmp_path)
        assert isinstance(meta, StageDModelMeta)
        assert stage_dir == tmp_path / _STAGE_D_SUBDIR

    def test_stage_dir_created(self, tmp_path: Path) -> None:
        _, stage_dir = bake_stage_d(tmp_path)
        assert stage_dir.is_dir()

    def test_prompt_txt_exists(self, tmp_path: Path) -> None:
        _, stage_dir = bake_stage_d(tmp_path)
        assert (stage_dir / _PROMPT_FILE).exists()

    def test_meta_json_exists(self, tmp_path: Path) -> None:
        _, stage_dir = bake_stage_d(tmp_path)
        assert (stage_dir / "meta.json").exists()

    def test_prompt_content_matches(self, tmp_path: Path) -> None:
        _, stage_dir = bake_stage_d(tmp_path)
        written = (stage_dir / _PROMPT_FILE).read_text(encoding="utf-8")
        assert written == _DEFAULT_PROMPT_TEMPLATE

    def test_meta_base_model_id(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path)
        assert meta.base_model_id == _DEFAULT_BASE_MODEL_D

    def test_meta_timeout_seconds(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path)
        assert meta.timeout_seconds == _DEFAULT_TIMEOUT_SECONDS

    def test_meta_circuit_breaker_threshold(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path)
        assert meta.circuit_breaker_threshold == _DEFAULT_CB_THRESHOLD

    def test_meta_baked_at_is_iso8601(self, tmp_path: Path) -> None:
        from datetime import datetime

        meta, _ = bake_stage_d(tmp_path)
        dt = datetime.fromisoformat(meta.baked_at)
        assert dt.tzinfo is not None  # must be timezone-aware

    def test_meta_json_round_trips(self, tmp_path: Path) -> None:
        meta, stage_dir = bake_stage_d(tmp_path)
        raw = json.loads((stage_dir / "meta.json").read_text(encoding="utf-8"))
        assert raw["base_model_id"] == meta.base_model_id
        assert raw["timeout_seconds"] == meta.timeout_seconds
        assert raw["circuit_breaker_threshold"] == meta.circuit_breaker_threshold
        assert raw["prompt_version"] == meta.prompt_version

    def test_prompt_version_is_12_hex_chars(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path)
        assert len(meta.prompt_version) == 12
        assert all(c in "0123456789abcdef" for c in meta.prompt_version)


# ---------------------------------------------------------------------------
# Custom parameters
# ---------------------------------------------------------------------------


class TestBakeCustomParameters:
    """bake_stage_d respects caller-supplied overrides."""

    def test_custom_base_model_id(self, tmp_path: Path) -> None:
        custom = "Qwen/Qwen2.5-7B-Instruct"
        meta, _ = bake_stage_d(tmp_path, base_model_id=custom)
        assert meta.base_model_id == custom

    def test_custom_timeout_seconds(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path, timeout_seconds=5.0)
        assert meta.timeout_seconds == 5.0

    def test_custom_circuit_breaker_threshold(self, tmp_path: Path) -> None:
        meta, _ = bake_stage_d(tmp_path, circuit_breaker_threshold=1)
        assert meta.circuit_breaker_threshold == 1

    def test_custom_prompt_template(self, tmp_path: Path) -> None:
        custom_prompt = "כותרת: {title}\nתוכן: {body}\nכן/לא?"
        _, stage_dir = bake_stage_d(tmp_path, prompt_template=custom_prompt)
        assert (stage_dir / _PROMPT_FILE).read_text(encoding="utf-8") == custom_prompt

    def test_custom_prompt_changes_prompt_version(self, tmp_path: Path) -> None:
        """Different prompt templates must produce different prompt_version hashes."""
        meta_a, _ = bake_stage_d(tmp_path / "a", prompt_template="prompt A: {title} {body}")
        meta_b, _ = bake_stage_d(tmp_path / "b", prompt_template="prompt B: {title} {body}")
        assert meta_a.prompt_version != meta_b.prompt_version

    def test_same_prompt_idempotent_version(self, tmp_path: Path) -> None:
        """Same prompt template → same prompt_version on every call."""
        meta_1, _ = bake_stage_d(tmp_path / "run1")
        meta_2, _ = bake_stage_d(tmp_path / "run2")
        assert meta_1.prompt_version == meta_2.prompt_version


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestBakeValidation:
    """bake_stage_d validates its inputs."""

    def test_missing_title_placeholder_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="title"):
            bake_stage_d(tmp_path, prompt_template="only {body} here")

    def test_missing_body_placeholder_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="body"):
            bake_stage_d(tmp_path, prompt_template="only {title} here")

    def test_empty_prompt_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            bake_stage_d(tmp_path, prompt_template="no placeholders at all")


# ---------------------------------------------------------------------------
# Atomic overwrite (rename-aside)
# ---------------------------------------------------------------------------


class TestBakeAtomicOverwrite:
    """Calling bake_stage_d twice atomically replaces the artifacts."""

    def test_second_bake_overwrites(self, tmp_path: Path) -> None:
        bake_stage_d(tmp_path, prompt_template="v1: {title} {body}")
        _, stage_dir = bake_stage_d(tmp_path, prompt_template="v2: {title} {body}")
        written = (stage_dir / _PROMPT_FILE).read_text(encoding="utf-8")
        assert written == "v2: {title} {body}"

    def test_no_tmp_dir_left_behind(self, tmp_path: Path) -> None:
        bake_stage_d(tmp_path)
        leftover = list(tmp_path.glob(f"{_STAGE_D_SUBDIR}.tmp.*"))
        assert leftover == [], f"tmp dirs left: {leftover}"

    def test_no_old_dir_left_behind(self, tmp_path: Path) -> None:
        bake_stage_d(tmp_path)
        bake_stage_d(tmp_path, prompt_template="v2: {title} {body}")
        leftover = list(tmp_path.glob(f"{_STAGE_D_SUBDIR}.old.*"))
        assert leftover == [], f"old dirs left: {leftover}"

    def test_out_dir_created_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "deep" / "nested"
        bake_stage_d(nested)
        assert (nested / _STAGE_D_SUBDIR).is_dir()
