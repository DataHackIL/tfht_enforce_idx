"""Local pre-classification filter cascade for denbust.

This package inserts a local, non-LLM-API-based filtering cascade between
the discovery/triage layer and the Claude-Sonnet relevance classifier to
drop high-confidence true negatives before they consume paid LLM budget.

Public surface
--------------
CascadeOrchestrator  — evaluate a candidate through the active stages.
PrefilterConfig      — pydantic config model (lives under ``prefilter:`` in YAML).
PrefilterMode        — off | shadow | enforce operational modes.
PrefilterDecision    — structured per-candidate decision returned by the orchestrator.
StageScore           — per-stage probability + threshold + reason.
"""

from __future__ import annotations

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.models import PrefilterDecision, StageScore

__all__ = [
    "CascadeOrchestrator",
    "PrefilterConfig",
    "PrefilterDecision",
    "PrefilterMode",
    "StageScore",
]
