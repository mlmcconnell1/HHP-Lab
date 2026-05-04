"""Compatibility wrappers for rents-owned metro aggregation helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

import pandas as pd


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("hhplab.rents.zori_metro")


def aggregate_zori_to_metro(
    zori_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    *,
    definition_version: str = "glynn_fox_v1",
    min_coverage: float = 0.90,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``hhplab.rents.zori_metro.aggregate_zori_to_metro``."""
    return _impl().aggregate_zori_to_metro(
        zori_df,
        weights_df,
        definition_version=definition_version,
        min_coverage=min_coverage,
        county_membership_df=county_membership_df,
    )


def collapse_zori_to_yearly(
    monthly_df: pd.DataFrame,
    method: str = "pit_january",
) -> pd.DataFrame:
    """Delegate to ``hhplab.rents.zori_metro.collapse_zori_to_yearly``."""
    return _impl().collapse_zori_to_yearly(monthly_df, method)


def aggregate_yearly_zori_to_metro(
    zori_yearly: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``hhplab.rents.zori_metro.aggregate_yearly_zori_to_metro``."""
    return _impl().aggregate_yearly_zori_to_metro(
        zori_yearly,
        county_population,
        county_membership_df=county_membership_df,
    )


__all__ = [
    "aggregate_yearly_zori_to_metro",
    "aggregate_zori_to_metro",
    "collapse_zori_to_yearly",
]
