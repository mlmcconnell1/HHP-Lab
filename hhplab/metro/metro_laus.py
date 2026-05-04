"""Compatibility wrappers for BLS-owned LAUS metro helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

_FORWARDED_ATTRS = {
    "BLS_API_V2_URL",
    "BLS_ANNUAL_AVERAGE_PERIOD",
    "LAUS_MEASURE_CODES",
    "LAUS_METRO_OUTPUT_COLUMNS",
}


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("hhplab.bls.laus_series")


def build_laus_series_id(cbsa_code: str, measure: str, state_fips: str) -> str:
    """Delegate to ``hhplab.bls.laus_series.build_laus_series_id``."""
    return _impl().build_laus_series_id(cbsa_code, measure, state_fips)


def build_all_series_ids(cbsa_code: str, state_fips: str) -> dict[str, str]:
    """Delegate to ``hhplab.bls.laus_series.build_all_series_ids``."""
    return _impl().build_all_series_ids(cbsa_code, state_fips)


def __getattr__(name: str) -> object:
    if name in _FORWARDED_ATTRS:
        return getattr(_impl(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(_FORWARDED_ATTRS | {"build_all_series_ids", "build_laus_series_id"})
