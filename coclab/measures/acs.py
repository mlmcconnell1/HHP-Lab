"""Compatibility facade for ACS-owned aggregation helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

__all__ = [
    "aggregate_to_coc",
    "aggregate_to_geo",
    "_maybe_remap_ct_planning_regions",
]


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("coclab.acs.aggregate")


def __getattr__(name: str) -> object:
    return getattr(_impl(), name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_impl())))
