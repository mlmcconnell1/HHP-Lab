"""BLS source helpers and metro-native LAUS workflows."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from pathlib import Path
from types import ModuleType

from coclab.bls.laus import (
    BLS_ANNUAL_AVERAGE_PERIOD,
    BLS_API_V2_URL,
    LAUS_MEASURE_CODES,
    LAUS_METRO_OUTPUT_COLUMNS,
    build_all_series_ids,
    build_laus_series_id,
)


@lru_cache(maxsize=1)
def _ingest_module() -> ModuleType:
    return import_module("coclab.ingest.bls_laus")


def fetch_laus_annual_averages(
    series_ids: list[str],
    year: int,
    api_key: str | None = None,
) -> dict[str, float | int]:
    """Fetch BLS LAUS annual-average values for a list of series IDs."""
    return _ingest_module().fetch_laus_annual_averages(
        series_ids,
        year,
        api_key=api_key,
    )


def ingest_laus_metro(
    year: int,
    definition_version: str = "glynn_fox_v1",
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Ingest metro-native BLS LAUS annual-average data."""
    return _ingest_module().ingest_laus_metro(
        year=year,
        definition_version=definition_version,
        project_root=project_root,
        api_key=api_key,
    )


def __getattr__(name: str) -> object:
    if name == "BlsQuotaExhausted":
        return getattr(_ingest_module(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BLS_ANNUAL_AVERAGE_PERIOD",
    "BLS_API_V2_URL",
    "BlsQuotaExhausted",
    "LAUS_MEASURE_CODES",
    "LAUS_METRO_OUTPUT_COLUMNS",
    "build_all_series_ids",
    "build_laus_series_id",
    "fetch_laus_annual_averages",
    "ingest_laus_metro",
]
