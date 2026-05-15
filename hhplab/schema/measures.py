"""Canonical measure definitions for analysis-ready artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from hhplab.schema.columns import (
    ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
    ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
    ACS1_IMPUTATION_LINEAGE_COLUMNS,
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


@dataclass(frozen=True)
class ACS1ImputationMeasureSpec:
    """Declarative contract for an ACS1-controlled / ACS5-share measure."""

    name: str
    family: Literal["population", "poverty", "labor", "housing", "income"]
    target_geo_type: str
    value_kind: Literal["count", "rate"]
    acs1_source_columns: tuple[str, ...]
    acs5_support_columns: tuple[str, ...]
    output_column: str
    numerator_source_columns: tuple[str, ...] = ()
    denominator_source_column: str | None = None
    numerator_output_column: str | None = None
    denominator_output_column: str | None = None
    allocation_method: str = "acs1_controlled_acs5_tract_share"
    denominator_source: str = "acs5_tract_support"
    zero_denominator_policy: Literal["null_rate", "zero_count"] = "null_rate"
    validation_abs_tolerance: float = 1e-6
    validation_rel_tolerance: float = 1e-9

    @property
    def output_columns(self) -> tuple[str, ...]:
        columns = [
            self.numerator_output_column,
            self.denominator_output_column,
            self.output_column,
        ]
        return tuple(column for column in columns if column is not None)

    @property
    def modeled_flag_column(self) -> str:
        return "is_modeled"

    @property
    def synthetic_flag_column(self) -> str:
        return "is_synthetic"

    @property
    def provenance_columns(self) -> tuple[str, ...]:
        return tuple(ACS1_IMPUTATION_LINEAGE_COLUMNS)

    def validate(self) -> None:
        """Raise a clear error if the measure declaration is internally invalid."""
        if not self.acs1_source_columns:
            raise ValueError(f"{self.name} must declare ACS1 source columns.")
        if not self.acs5_support_columns:
            raise ValueError(f"{self.name} must declare ACS5 tract support columns.")
        if self.value_kind == "rate":
            missing = [
                label
                for label, value in (
                    ("numerator_source_columns", self.numerator_source_columns),
                    ("denominator_source_column", self.denominator_source_column),
                    ("numerator_output_column", self.numerator_output_column),
                    ("denominator_output_column", self.denominator_output_column),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"{self.name} rate spec is missing required fields: {missing}.")
            if len(self.numerator_source_columns) != 1:
                raise ValueError(
                    f"{self.name} rate spec must declare exactly one numerator_source_columns "
                    "entry because ACS1 rate imputation allocates one numerator component."
                )
            invalid_numerators = [
                column
                for column in self.numerator_source_columns
                if column not in self.acs1_source_columns
            ]
            if invalid_numerators:
                raise ValueError(
                    f"{self.name} numerator_source_columns must be present in "
                    f"acs1_source_columns: {invalid_numerators}."
                )
            if self.denominator_source_column not in self.acs5_support_columns:
                raise ValueError(
                    f"{self.name} denominator_source_column must be present in "
                    "acs5_support_columns."
                )
        if self.value_kind == "count" and (
            self.numerator_source_columns
            or self.denominator_source_column is not None
            or self.numerator_output_column is not None
            or self.denominator_output_column is not None
        ):
            raise ValueError(
                f"{self.name} count spec must not declare numerator/denominator fields."
            )


ACS1_IMPUTED_POVERTY_SPEC = ACS1ImputationMeasureSpec(
    name="poverty_rate",
    family="poverty",
    target_geo_type="tract",
    value_kind="rate",
    acs1_source_columns=("population_below_poverty", "poverty_universe"),
    acs5_support_columns=("population_below_poverty", "poverty_universe"),
    numerator_source_columns=("population_below_poverty",),
    denominator_source_column="poverty_universe",
    numerator_output_column="acs1_imputed_population_below_poverty",
    denominator_output_column="acs1_imputed_poverty_universe",
    output_column="acs1_imputed_poverty_rate",
)

ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC = ACS1ImputationMeasureSpec(
    name="total_households",
    family="housing",
    target_geo_type="tract",
    value_kind="count",
    acs1_source_columns=("total_households",),
    acs5_support_columns=("total_households",),
    output_column="acs1_imputed_total_households",
    zero_denominator_policy="zero_count",
)

ACS1_IMPUTATION_MEASURE_SPECS: tuple[ACS1ImputationMeasureSpec, ...] = (
    ACS1_IMPUTED_POVERTY_SPEC,
    ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
)

ACS1_IMPUTATION_MEASURE_COLUMNS: list[str] = list(
    dict.fromkeys(column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.output_columns)
)

ACS1_IMPUTATION_OUTPUT_COLUMNS: list[str] = [
    *ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
    *ACS1_IMPUTATION_MEASURE_COLUMNS,
    *ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
]

ACS1_IMPUTATION_MEASURES: tuple[str, ...] = tuple(ACS1_IMPUTATION_MEASURE_COLUMNS)

ACS1_IMPUTATION_REQUIRED_ACS1_SOURCE_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.acs1_source_columns
    )
)

ACS1_IMPUTATION_REQUIRED_ACS5_SUPPORT_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.acs5_support_columns
    )
)


def acs1_imputation_output_columns(
    specs: tuple[ACS1ImputationMeasureSpec, ...] = ACS1_IMPUTATION_MEASURE_SPECS,
) -> list[str]:
    """Return canonical output columns for a set of ACS1 imputation specs."""
    measure_columns = [column for spec in specs for column in spec.output_columns]
    return [
        *ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
        *dict.fromkeys(measure_columns),
        *ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
    ]


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
