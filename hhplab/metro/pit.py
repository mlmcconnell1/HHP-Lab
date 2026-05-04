"""Compatibility wrappers for PIT-owned metro aggregation helpers."""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from types import ModuleType

import pandas as pd


@lru_cache(maxsize=1)
def _impl() -> ModuleType:
    return import_module("hhplab.pit.pit_metro")


def aggregate_pit_to_metro(
    pit_df: pd.DataFrame,
    *,
    definition_version: str = "glynn_fox_v1",
    coc_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Delegate to ``hhplab.pit.pit_metro.aggregate_pit_to_metro``."""
    return _impl().aggregate_pit_to_metro(
        pit_df,
        definition_version=definition_version,
        coc_membership_df=coc_membership_df,
    )


__all__ = ["aggregate_pit_to_metro"]
