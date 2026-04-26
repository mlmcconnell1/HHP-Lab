"""Compatibility wrappers for PEP-owned metro aggregation helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

import pandas as pd


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("coclab.pep.metro")


def aggregate_pep_to_metro(
    pep_df: pd.DataFrame,
    *,
    definition_version: str = "glynn_fox_v1",
    weighting: str = "area_share",
    min_coverage: float = 0.0,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``coclab.pep.metro.aggregate_pep_to_metro``."""
    return _impl().aggregate_pep_to_metro(
        pep_df,
        definition_version=definition_version,
        weighting=weighting,
        min_coverage=min_coverage,
        county_membership_df=county_membership_df,
    )


__all__ = ["aggregate_pep_to_metro"]
