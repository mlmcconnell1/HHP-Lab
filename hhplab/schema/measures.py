"""Canonical measure definitions for analysis-ready artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from hhplab.schema.columns import (
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    LAUS_MEASURE_COLUMNS,
    SAE_MEASURE_COLUMNS,
    TOTAL_POPULATION,
)


@dataclass(frozen=True)
class MeasureDefinition:
    """Stable definition for a canonical analysis measure."""

    name: str
    family: Literal["population", "pit", "rent", "labor", "demographic"]
    unit: str
    default_aggregation: Literal["sum", "mean", "weighted_mean"]


TOTAL_POPULATION_MEASURE = MeasureDefinition(
    name=TOTAL_POPULATION,
    family="population",
    unit="persons",
    default_aggregation="sum",
)

PIT_MEASURES: tuple[MeasureDefinition, ...] = (
    MeasureDefinition("pit_total", "pit", "persons", "sum"),
    MeasureDefinition("pit_sheltered", "pit", "persons", "sum"),
    MeasureDefinition("pit_unsheltered", "pit", "persons", "sum"),
)

ACS5_MEASURES: tuple[str, ...] = tuple(ACS_MEASURE_COLUMNS)
ACS1_MEASURES: tuple[str, ...] = tuple(ACS1_MEASURE_COLUMNS)
LAUS_MEASURES: tuple[str, ...] = tuple(LAUS_MEASURE_COLUMNS)
SAE_MEASURES: tuple[str, ...] = tuple(SAE_MEASURE_COLUMNS)
