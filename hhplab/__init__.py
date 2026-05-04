"""HHP-Lab - Continuum of Care boundary data infrastructure.

The package root intentionally avoids eager submodule imports so source-owned
packages can depend on shared utilities without pulling the entire package into
the import graph. Public subpackages remain available through lazy attribute
access for compatibility.
"""

from __future__ import annotations

from importlib import import_module

from hhplab._version import __version__ as __version__

_LAZY_EXPORTS = frozenset(
    {
        "acs",
        "analysis_geo",
        "audit_panels",
        "bls",
        "builds",
        "census",
        "cli",
        "config",
        "curated_migrate",
        "curated_policy",
        "geo",
        "hud",
        "ingest",
        "measures",
        "metro",
        "naming",
        "nhgis",
        "panel",
        "paths",
        "pep",
        "pit",
        "provenance",
        "raw_snapshot",
        "recipe",
        "registry",
        "rents",
        "source_registry",
        "sources",
        "viz",
        "xwalks",
        "year_spec",
    }
)

__all__ = sorted(_LAZY_EXPORTS | {"__version__"})


def __getattr__(name: str) -> object:
    """Lazily import common subpackages and shared modules."""
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted(set(globals()) | _LAZY_EXPORTS)
