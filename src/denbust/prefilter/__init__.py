"""Local pre-classification filter cascade for denbust.

This package inserts a local, non-LLM-API-based filtering cascade between
the discovery/triage layer and the Claude-Sonnet relevance classifier to
drop high-confidence true negatives before they consume paid LLM budget.

Public surface
--------------
CascadeOrchestrator       — evaluate a candidate through the active stages.
PrefilterConfig           — pydantic config model (lives under ``prefilter:`` in YAML).
PrefilterMode             — off | shadow | enforce operational modes.
PrefilterDecision         — structured per-candidate decision returned by the orchestrator.
StageScore                — per-stage probability + threshold + reason.
CandidateView             — read-only Protocol that candidate objects must satisfy.
StageEvaluator            — Protocol that every cascade stage must implement.
PrefilterStatePaths       — resolved artifact-directory layout for a dataset/job.
resolve_prefilter_state_paths — factory for :class:`PrefilterStatePaths`.
PassKind                  — ``Literal["thin", "thick"]``.
Verdict                   — ``Literal["pass", "drop"]``.
"""

from __future__ import annotations

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.models import (
    CandidateView,
    PassKind,
    PrefilterDecision,
    StageEvaluator,
    StageScore,
    Verdict,
)
from denbust.prefilter.state_paths import PrefilterStatePaths, resolve_prefilter_state_paths

__all__ = [
    "CandidateView",
    "CascadeOrchestrator",
    "PassKind",
    "PrefilterConfig",
    "PrefilterDecision",
    "PrefilterMode",
    "PrefilterStatePaths",
    "StageEvaluator",
    "StageScore",
    "Verdict",
    "resolve_prefilter_state_paths",
]
