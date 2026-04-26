"""Compatibility wrappers for ACS-owned metro aggregation helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

import pandas as pd


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("coclab.acs.metro")


def build_metro_tract_crosswalk(
    acs_data: pd.DataFrame,
    *,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``coclab.acs.metro.build_metro_tract_crosswalk``."""
    return _impl().build_metro_tract_crosswalk(
        acs_data,
        county_membership_df=county_membership_df,
    )


def aggregate_acs_to_metro(
    acs_data: pd.DataFrame,
    *,
    weighting: str = "area",
    definition_version: str = "glynn_fox_v1",
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``coclab.acs.metro.aggregate_acs_to_metro``."""
    return _impl().aggregate_acs_to_metro(
        acs_data,
        weighting=weighting,
        definition_version=definition_version,
        county_membership_df=county_membership_df,
    )


__all__ = ["aggregate_acs_to_metro", "build_metro_tract_crosswalk"]
