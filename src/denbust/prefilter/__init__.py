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
LabeledCandidate          — one labeled row in the training dataset.
Label                     — ``Literal["positive", "negative"]``.
LabelSourceName           — ``Literal["triage_manual", "triage_auto", "claude_classifier"]``.
Split                     — ``Literal["train", "val", "test"]``.
assemble_labels           — assemble the labeled dataset from state-repo signals.
write_labels_parquet      — serialise rows to Parquet.
read_labels_parquet       — deserialise rows from Parquet.
"""

from __future__ import annotations

from denbust.prefilter.cascade import CascadeOrchestrator
from denbust.prefilter.config import PrefilterConfig, PrefilterMode
from denbust.prefilter.labels import (
    Label,
    LabeledCandidate,
    LabelSourceName,
    Split,
    assemble_labels,
    read_labels_parquet,
    write_labels_parquet,
)
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
    "Label",
    "LabeledCandidate",
    "LabelSourceName",
    "PassKind",
    "PrefilterConfig",
    "PrefilterDecision",
    "PrefilterMode",
    "PrefilterStatePaths",
    "Split",
    "StageEvaluator",
    "StageScore",
    "Verdict",
    "assemble_labels",
    "read_labels_parquet",
    "resolve_prefilter_state_paths",
    "write_labels_parquet",
]
