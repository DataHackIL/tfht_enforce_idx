"""Diagnostic helpers for source-health and discovery investigations."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

_EXPORTS: dict[str, tuple[str, str | None]] = {
    "DiscoveryDiagnosticReport": ("denbust.diagnostics.discovery", "DiscoveryDiagnosticReport"),
    "build_discovery_diagnostic_report": (
        "denbust.diagnostics.discovery",
        "build_discovery_diagnostic_report",
    ),
    "persist_discovery_diagnostic_artifacts": (
        "denbust.diagnostics.discovery",
        "persist_discovery_diagnostic_artifacts",
    ),
    "render_discovery_diagnostic_report": (
        "denbust.diagnostics.discovery",
        "render_discovery_diagnostic_report",
    ),
    "run_discovery_diagnostics": ("denbust.diagnostics.discovery", "run_discovery_diagnostics"),
    "SourceDiagnosticReport": ("denbust.diagnostics.source_health", "SourceDiagnosticReport"),
    "SourceDiagnosticResult": ("denbust.diagnostics.source_health", "SourceDiagnosticResult"),
    "render_source_diagnostic_report": (
        "denbust.diagnostics.source_health",
        "render_source_diagnostic_report",
    ),
    "run_source_diagnostics": ("denbust.diagnostics.source_health", "run_source_diagnostics"),
    "run_source_diagnostics_async": (
        "denbust.diagnostics.source_health",
        "run_source_diagnostics_async",
    ),
    "source_health": ("denbust.diagnostics.source_health", None),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Lazily resolve diagnostics exports to avoid package import cycles."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name)
    value: ModuleType | Any = module if attr_name is None else getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Report the lazily exported attribute names for interactive use."""
    return sorted(set(globals()) | set(__all__))
