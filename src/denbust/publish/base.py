"""Shared publication and backup interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod


class PhaseAScaffold(ABC):
    """Marker interface for Phase A scaffold-only operations."""

    @abstractmethod
    def describe(self) -> str:
        """Describe the current scaffolded behavior."""
