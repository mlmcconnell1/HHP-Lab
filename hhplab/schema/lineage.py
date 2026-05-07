"""Controlled lineage tokens for canonical analysis measures."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import pandas as pd

from hhplab.schema.columns import TOTAL_POPULATION


class PopulationSource(StrEnum):
    """Controlled source tokens for ``total_population``."""

    ACS5 = "acs5"
    PEP = "pep"
    DECENNIAL = "decennial"
    BLOCK = "block"


class PopulationMethod(StrEnum):
    """Controlled derivation method tokens for ``total_population``."""

    NATIVE = "native"
    AREA_CROSSWALK = "area_crosswalk"
    POPULATION_CROSSWALK = "population_crosswalk"
    TRACT_MEDIATED_CROSSWALK = "tract_mediated_crosswalk"
    BLOCK_MEDIATED_CROSSWALK = "block_mediated_crosswalk"


@dataclass(frozen=True)
class PopulationLineage:
    """Lineage values attached to a canonical ``total_population`` column."""

    source: PopulationSource
    source_year: int | str
    method: PopulationMethod
    crosswalk_id: str | None = None
    crosswalk_geometry: str | None = None
    crosswalk_vintage: int | str | None = None


def population_lineage_columns(measure: str = TOTAL_POPULATION) -> tuple[str, ...]:
    """Return the controlled lineage columns for *measure*."""
    return (
        f"{measure}_source",
        f"{measure}_source_year",
        f"{measure}_method",
        f"{measure}_crosswalk_id",
        f"{measure}_crosswalk_geometry",
        f"{measure}_crosswalk_vintage",
    )


def normalize_population_measure(
    df: pd.DataFrame,
    *,
    source_column: str,
    lineage: PopulationLineage,
    drop_source_column: bool = True,
) -> pd.DataFrame:
    """Rename a source-native population column to ``total_population``.

    The input must not already contain a distinct ``total_population`` column.
    Mixed-source panels that intentionally carry multiple population estimates
    should alias one side before calling this helper.
    """
    if source_column not in df.columns:
        return df.copy()
    if source_column != TOTAL_POPULATION and TOTAL_POPULATION in df.columns:
        raise ValueError(
            f"Cannot normalize '{source_column}' to '{TOTAL_POPULATION}' because "
            f"'{TOTAL_POPULATION}' is already present. Alias one source-specific "
            "measure before normalizing."
        )

    result = df.copy()
    if source_column != TOTAL_POPULATION:
        result[TOTAL_POPULATION] = result[source_column]
        if drop_source_column:
            result = result.drop(columns=[source_column])

    result[f"{TOTAL_POPULATION}_source"] = lineage.source.value
    result[f"{TOTAL_POPULATION}_source_year"] = str(lineage.source_year)
    result[f"{TOTAL_POPULATION}_method"] = lineage.method.value
    result[f"{TOTAL_POPULATION}_crosswalk_id"] = lineage.crosswalk_id
    result[f"{TOTAL_POPULATION}_crosswalk_geometry"] = lineage.crosswalk_geometry
    result[f"{TOTAL_POPULATION}_crosswalk_vintage"] = (
        None if lineage.crosswalk_vintage is None else str(lineage.crosswalk_vintage)
    )
    return result
