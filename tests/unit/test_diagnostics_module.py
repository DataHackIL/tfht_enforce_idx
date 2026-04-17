"""Tests for lazy diagnostics package exports."""

from __future__ import annotations

import pytest

import denbust.diagnostics as diagnostics


def test_diagnostics_module_rejects_unknown_exports() -> None:
    """Unknown lazy exports should raise AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute 'does_not_exist'"):
        diagnostics.__getattr__("does_not_exist")


def test_diagnostics_module_dir_lists_lazy_exports() -> None:
    """dir() should expose the lazily available diagnostics names."""
    names = diagnostics.__dir__()

    assert "run_discovery_diagnostics" in names
    assert "source_health" in names
