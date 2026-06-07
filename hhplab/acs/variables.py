"""Centralized ACS variable definitions and column classification.

Single source of truth for all ACS variables fetched during ingestion,
column types (count vs median vs MOE), and derived column specifications.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from hhplab.schema.columns import (
    ACS5_COUNT_COLUMNS,
    ACS5_DERIVED_COLUMNS,
    ACS5_MEDIAN_COLUMNS,
    ACS5_MOE_COLUMNS,
    ACS5_SAE_COUNT_COLUMNS,
    ACS_MEASURE_COLUMNS,
    ACS_TRACT_OUTPUT_COLUMNS,
)

# ---------------------------------------------------------------------------
# Census API variables → friendly column names
# ---------------------------------------------------------------------------

# Base ACS variables (fetched directly from Census API)
ACS_VARIABLES: dict[str, str] = {
    # B01003 — Total Population
    "B01003_001E": "total_population",
    "B01003_001M": "moe_total_population",
    # B19013 — Median Household Income
    "B19013_001E": "median_household_income",
    # B19301 — Per Capita Income
    "B19301_001E": "per_capita_income",
    # B19083 — Gini Index of Income Inequality
    "B19083_001E": "gini_index",
    # B25064 — Median Gross Rent
    "B25064_001E": "median_gross_rent",
    # B25077 — Median Value of Owner-Occupied Housing Units
    "B25077_001E": "median_owner_occupied_home_value",
    # B25002 — Occupancy Status
    "B25002_001E": "total_housing_units",
    "B25002_002E": "occupied_housing_units",
    "B25002_003E": "vacant_housing_units",
    # B25003 — Tenure
    "B25003_001E": "total_households",
    "B25003_002E": "owner_households",
    "B25003_003E": "renter_households",
    # C17002 — Ratio of Income to Poverty Level
    "C17002_001E": "poverty_universe",
    "C17002_002E": "below_50pct_poverty",
    "C17002_003E": "50_to_99pct_poverty",
    # B23025 — Employment Status for Population 16+
    "B23025_003E": "civilian_labor_force",
    "B23025_005E": "unemployed_count",
    # B19001 — Household Income
    **{
        f"B19001_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "household_income_total",
                "household_income_lt_10000",
                "household_income_10000_to_14999",
                "household_income_15000_to_19999",
                "household_income_20000_to_24999",
                "household_income_25000_to_29999",
                "household_income_30000_to_34999",
                "household_income_35000_to_39999",
                "household_income_40000_to_44999",
                "household_income_45000_to_49999",
                "household_income_50000_to_59999",
                "household_income_60000_to_74999",
                "household_income_75000_to_99999",
                "household_income_100000_to_124999",
                "household_income_125000_to_149999",
                "household_income_150000_to_199999",
                "household_income_200000_plus",
            ],
            start=1,
        )
    },
    # B25063 — Gross Rent
    **{
        f"B25063_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "gross_rent_distribution_total",
                "gross_rent_distribution_with_cash_rent",
                "gross_rent_distribution_cash_rent_lt_100",
                "gross_rent_distribution_cash_rent_100_to_149",
                "gross_rent_distribution_cash_rent_150_to_199",
                "gross_rent_distribution_cash_rent_200_to_249",
                "gross_rent_distribution_cash_rent_250_to_299",
                "gross_rent_distribution_cash_rent_300_to_349",
                "gross_rent_distribution_cash_rent_350_to_399",
                "gross_rent_distribution_cash_rent_400_to_449",
                "gross_rent_distribution_cash_rent_450_to_499",
                "gross_rent_distribution_cash_rent_500_to_549",
                "gross_rent_distribution_cash_rent_550_to_599",
                "gross_rent_distribution_cash_rent_600_to_649",
                "gross_rent_distribution_cash_rent_650_to_699",
                "gross_rent_distribution_cash_rent_700_to_749",
                "gross_rent_distribution_cash_rent_750_to_799",
                "gross_rent_distribution_cash_rent_800_to_899",
                "gross_rent_distribution_cash_rent_900_to_999",
                "gross_rent_distribution_cash_rent_1000_to_1249",
                "gross_rent_distribution_cash_rent_1250_to_1499",
                "gross_rent_distribution_cash_rent_1500_to_1999",
                "gross_rent_distribution_cash_rent_2000_to_2499",
                "gross_rent_distribution_cash_rent_2500_to_2999",
                "gross_rent_distribution_cash_rent_3000_to_3499",
                "gross_rent_distribution_cash_rent_3500_plus",
                "gross_rent_distribution_no_cash_rent",
            ],
            start=1,
        )
    },
    # B25075 — Value
    **{
        f"B25075_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "owner_occupied_value_total",
                "owner_occupied_value_lt_10000",
                "owner_occupied_value_10000_to_14999",
                "owner_occupied_value_15000_to_19999",
                "owner_occupied_value_20000_to_24999",
                "owner_occupied_value_25000_to_29999",
                "owner_occupied_value_30000_to_34999",
                "owner_occupied_value_35000_to_39999",
                "owner_occupied_value_40000_to_49999",
                "owner_occupied_value_50000_to_59999",
                "owner_occupied_value_60000_to_69999",
                "owner_occupied_value_70000_to_79999",
                "owner_occupied_value_80000_to_89999",
                "owner_occupied_value_90000_to_99999",
                "owner_occupied_value_100000_to_124999",
                "owner_occupied_value_125000_to_149999",
                "owner_occupied_value_150000_to_174999",
                "owner_occupied_value_175000_to_199999",
                "owner_occupied_value_200000_to_249999",
                "owner_occupied_value_250000_to_299999",
                "owner_occupied_value_300000_to_399999",
                "owner_occupied_value_400000_to_499999",
                "owner_occupied_value_500000_to_749999",
                "owner_occupied_value_750000_to_999999",
                "owner_occupied_value_1000000_to_1499999",
                "owner_occupied_value_1500000_to_1999999",
                "owner_occupied_value_2000000_plus",
            ],
            start=1,
        )
    },
    # B25070 — Gross Rent as a Percentage of Household Income
    **{
        f"B25070_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "gross_rent_pct_income_total",
                "gross_rent_pct_income_lt_10",
                "gross_rent_pct_income_10_to_14_9",
                "gross_rent_pct_income_15_to_19_9",
                "gross_rent_pct_income_20_to_24_9",
                "gross_rent_pct_income_25_to_29_9",
                "gross_rent_pct_income_30_to_34_9",
                "gross_rent_pct_income_35_to_39_9",
                "gross_rent_pct_income_40_to_49_9",
                "gross_rent_pct_income_50_plus",
                "gross_rent_pct_income_not_computed",
            ],
            start=1,
        )
    },
    # B25091 — Mortgage Status by Selected Monthly Owner Costs as a Percentage of Household Income
    **{
        f"B25091_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "owner_costs_pct_income_total",
                "owner_costs_pct_income_with_mortgage_total",
                "owner_costs_pct_income_with_mortgage_lt_10",
                "owner_costs_pct_income_with_mortgage_10_to_14_9",
                "owner_costs_pct_income_with_mortgage_15_to_19_9",
                "owner_costs_pct_income_with_mortgage_20_to_24_9",
                "owner_costs_pct_income_with_mortgage_25_to_29_9",
                "owner_costs_pct_income_with_mortgage_30_to_34_9",
                "owner_costs_pct_income_with_mortgage_35_to_39_9",
                "owner_costs_pct_income_with_mortgage_40_to_49_9",
                "owner_costs_pct_income_with_mortgage_50_plus",
                "owner_costs_pct_income_with_mortgage_not_computed",
                "owner_costs_pct_income_without_mortgage_total",
                "owner_costs_pct_income_without_mortgage_lt_10",
                "owner_costs_pct_income_without_mortgage_10_to_14_9",
                "owner_costs_pct_income_without_mortgage_15_to_19_9",
                "owner_costs_pct_income_without_mortgage_20_to_24_9",
                "owner_costs_pct_income_without_mortgage_25_to_29_9",
                "owner_costs_pct_income_without_mortgage_30_to_34_9",
                "owner_costs_pct_income_without_mortgage_35_to_39_9",
                "owner_costs_pct_income_without_mortgage_40_to_49_9",
                "owner_costs_pct_income_without_mortgage_50_plus",
                "owner_costs_pct_income_without_mortgage_not_computed",
            ],
            start=1,
        )
    },
    # B25118 — Tenure by Household Income
    **{
        f"B25118_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "tenure_income_total",
                "tenure_income_owner_occupied_total",
                "tenure_income_owner_occupied_lt_5000",
                "tenure_income_owner_occupied_5000_to_9999",
                "tenure_income_owner_occupied_10000_to_14999",
                "tenure_income_owner_occupied_15000_to_19999",
                "tenure_income_owner_occupied_20000_to_24999",
                "tenure_income_owner_occupied_25000_to_34999",
                "tenure_income_owner_occupied_35000_to_49999",
                "tenure_income_owner_occupied_50000_to_74999",
                "tenure_income_owner_occupied_75000_to_99999",
                "tenure_income_owner_occupied_100000_to_149999",
                "tenure_income_owner_occupied_150000_plus",
                "tenure_income_renter_occupied_total",
                "tenure_income_renter_occupied_lt_5000",
                "tenure_income_renter_occupied_5000_to_9999",
                "tenure_income_renter_occupied_10000_to_14999",
                "tenure_income_renter_occupied_15000_to_19999",
                "tenure_income_renter_occupied_20000_to_24999",
                "tenure_income_renter_occupied_25000_to_34999",
                "tenure_income_renter_occupied_35000_to_49999",
                "tenure_income_renter_occupied_50000_to_74999",
                "tenure_income_renter_occupied_75000_to_99999",
                "tenure_income_renter_occupied_100000_to_149999",
                "tenure_income_renter_occupied_150000_plus",
            ],
            start=1,
        )
    },
    # B15003 — Educational Attainment for the Population 25 Years and Over
    **{
        f"B15003_{idx:03d}E": column
        for idx, column in enumerate(
            [
                "educational_attainment_25plus_total",
                "educational_attainment_25plus_no_schooling_completed",
                "educational_attainment_25plus_nursery_school",
                "educational_attainment_25plus_kindergarten",
                "educational_attainment_25plus_1st_grade",
                "educational_attainment_25plus_2nd_grade",
                "educational_attainment_25plus_3rd_grade",
                "educational_attainment_25plus_4th_grade",
                "educational_attainment_25plus_5th_grade",
                "educational_attainment_25plus_6th_grade",
                "educational_attainment_25plus_7th_grade",
                "educational_attainment_25plus_8th_grade",
                "educational_attainment_25plus_9th_grade",
                "educational_attainment_25plus_10th_grade",
                "educational_attainment_25plus_11th_grade",
                "educational_attainment_25plus_12th_grade_no_diploma",
                "educational_attainment_25plus_high_school_diploma",
                "educational_attainment_25plus_ged_or_alternative_credential",
                "educational_attainment_25plus_some_college_lt_1_year",
                "educational_attainment_25plus_some_college_1plus_year_no_degree",
                "educational_attainment_25plus_associates_degree",
                "educational_attainment_25plus_bachelors_degree",
                "educational_attainment_25plus_masters_degree",
                "educational_attainment_25plus_professional_school_degree",
                "educational_attainment_25plus_doctorate_degree",
            ],
            start=1,
        )
    },
}

# B01001 — Sex by Age (for deriving adult population 18+)
# Male 18+: variables 007 through 025
# Female 18+: variables 031 through 049
ADULT_MALE_VARS: list[str] = [f"B01001_{i:03d}E" for i in range(7, 26)]
ADULT_FEMALE_VARS: list[str] = [f"B01001_{i:03d}E" for i in range(31, 50)]
ADULT_VARS: list[str] = ADULT_MALE_VARS + ADULT_FEMALE_VARS

# Legacy alias for backwards compatibility (consumers import measures.ACS_VARS)
ACS_VARS = ACS_VARIABLES

# All Census API variable codes to request (base + adult age groups)
ALL_API_VARS: list[str] = list(ACS_VARIABLES.keys()) + ADULT_VARS

# Variables unavailable in older ACS API vintages. Requesting any of these
# variables makes the whole state request fail with HTTP 400, so tract ingest
# filters them by API year and leaves their output columns nullable.
UNAVAILABLE_API_VARS_BY_YEAR: dict[int, set[str]] = {
    year: {
        "B01003_001M",
        "B25063_025E",
        "B25063_026E",
        "B25063_027E",
    }
    for year in range(2009, 2015)
}
UNAVAILABLE_API_VARS_BY_YEAR[2009].update({"B23025_003E", "B23025_005E"})
UNAVAILABLE_API_VARS_BY_YEAR[2010].update({"B23025_003E", "B23025_005E"})
for year in range(2009, 2015):
    UNAVAILABLE_API_VARS_BY_YEAR[year].update({"B25075_026E", "B25075_027E"})
UNAVAILABLE_API_VARS_BY_YEAR[2009].add("B19083_001E")
for year in range(2009, 2012):
    UNAVAILABLE_API_VARS_BY_YEAR[year].update({f"B15003_{idx:03d}E" for idx in range(1, 26)})


def api_vars_for_year(year: int) -> list[str]:
    """Return ACS API variables supported by a specific ACS5 vintage year."""
    unavailable = UNAVAILABLE_API_VARS_BY_YEAR.get(year, set())
    return [var for var in ALL_API_VARS if var not in unavailable]


def tables_for_api_vars(api_vars: list[str]) -> list[str]:
    """Return table identifiers represented by an API variable request."""
    table_order = {table: index for index, table in enumerate(ACS_TABLES)}
    tables = {var.split("_", maxsplit=1)[0] for var in api_vars}
    return sorted(tables, key=lambda table: table_order.get(table, len(table_order)))


# Tables included (for provenance tracking)
ACS_TABLES: list[str] = [
    "B01003",
    "B01001",
    "B19013",
    "B19301",
    "B19083",
    "B25064",
    "B25077",
    "B25002",
    "B25003",
    "C17002",
    "B23025",
    "B19001",
    "B25075",
    "B25063",
    "B25070",
    "B25091",
    "B25118",
    "B15003",
]

# ---------------------------------------------------------------------------
# Aggregation registry
# ---------------------------------------------------------------------------

ACS5MeasureKind = Literal["count", "median", "rate", "distribution", "moe"]
ACS5RollupMethod = Literal[
    "area_weighted_sum",
    "population_weighted_mean",
    "ratio_of_area_weighted_sums",
    "root_sum_squared_moe",
]


@dataclass(frozen=True)
class ACS5CovariateSpec:
    """Declarative aggregation contract for a supported ACS5 covariate family."""

    name: str
    table: str
    source_variables: tuple[str, ...]
    output_columns: tuple[str, ...]
    measure_kind: ACS5MeasureKind
    denominator_column: str | None
    weight_column: str
    rollup_method: ACS5RollupMethod
    canonical_measure: bool = False
    recipe_selectable: bool = True
    caveats: str = ""


def _columns_with_prefix(prefix: str) -> tuple[str, ...]:
    return tuple(column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith(prefix))


def _source_variables_for_outputs(output_columns: tuple[str, ...]) -> tuple[str, ...]:
    output_set = set(output_columns)
    return tuple(variable for variable, column in ACS_VARIABLES.items() if column in output_set)


ACS5_COVARIATE_REGISTRY: tuple[ACS5CovariateSpec, ...] = (
    ACS5CovariateSpec(
        name="total_population",
        table="B01003",
        source_variables=("B01003_001E",),
        output_columns=("total_population",),
        measure_kind="count",
        denominator_column=None,
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        canonical_measure=True,
        caveats=(
            "Counts are apportioned by tract overlap; population is not uniformly "
            "distributed within every tract."
        ),
    ),
    ACS5CovariateSpec(
        name="total_population_moe",
        table="B01003",
        source_variables=("B01003_001M",),
        output_columns=("moe_total_population",),
        measure_kind="moe",
        denominator_column=None,
        weight_column="area_share",
        rollup_method="root_sum_squared_moe",
        caveats=(
            "Only count-style total population MOE propagation is declared here; "
            "median MOEs are not propagated."
        ),
    ),
    ACS5CovariateSpec(
        name="adult_population",
        table="B01001",
        source_variables=tuple(ADULT_VARS),
        output_columns=("adult_population",),
        measure_kind="count",
        denominator_column=None,
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        canonical_measure=True,
        caveats="Derived from age-by-sex bins before geographic aggregation.",
    ),
    ACS5CovariateSpec(
        name="median_household_income",
        table="B19013",
        source_variables=("B19013_001E",),
        output_columns=("median_household_income",),
        measure_kind="median",
        denominator_column="total_population",
        weight_column="total_population * selected_overlap_weight",
        rollup_method="population_weighted_mean",
        canonical_measure=True,
        caveats=(
            "Population-weighted mean of tract medians; it is not a true pooled household median."
        ),
    ),
    ACS5CovariateSpec(
        name="per_capita_income",
        table="B19301",
        source_variables=("B19301_001E",),
        output_columns=("per_capita_income",),
        measure_kind="median",
        denominator_column="total_population",
        weight_column="total_population * selected_overlap_weight",
        rollup_method="population_weighted_mean",
        caveats="Per-capita income is declared for weighted-average rollup by population.",
    ),
    ACS5CovariateSpec(
        name="gini_index",
        table="B19083",
        source_variables=("B19083_001E",),
        output_columns=("gini_index",),
        measure_kind="median",
        denominator_column="total_population",
        weight_column="total_population * selected_overlap_weight",
        rollup_method="population_weighted_mean",
        caveats=(
            "Weighted tract Gini is an approximation and is not a true pooled geography-level Gini."
        ),
    ),
    ACS5CovariateSpec(
        name="median_gross_rent",
        table="B25064",
        source_variables=("B25064_001E",),
        output_columns=("median_gross_rent",),
        measure_kind="median",
        denominator_column="total_population",
        weight_column="total_population * selected_overlap_weight",
        rollup_method="population_weighted_mean",
        canonical_measure=True,
        caveats="Population-weighted mean of tract medians; it is not a true pooled rent median.",
    ),
    ACS5CovariateSpec(
        name="median_owner_occupied_home_value",
        table="B25077",
        source_variables=("B25077_001E",),
        output_columns=("median_owner_occupied_home_value",),
        measure_kind="median",
        denominator_column="owner_households",
        weight_column="owner_households * selected_overlap_weight",
        rollup_method="population_weighted_mean",
        caveats=(
            "Temporary broad median rollup uses the existing weighted-average path; "
            "the aggregation-semantics task will apply owner-household weights."
        ),
    ),
    ACS5CovariateSpec(
        name="housing_occupancy",
        table="B25002",
        source_variables=_source_variables_for_outputs(
            ("total_housing_units", "occupied_housing_units", "vacant_housing_units")
        ),
        output_columns=(
            "total_housing_units",
            "occupied_housing_units",
            "vacant_housing_units",
            "vacancy_rate",
        ),
        measure_kind="rate",
        denominator_column="total_housing_units",
        weight_column="area_share",
        rollup_method="ratio_of_area_weighted_sums",
        canonical_measure=True,
        caveats=(
            "Vacancy rate is derived after count rollup as vacant_housing_units / "
            "total_housing_units."
        ),
    ),
    ACS5CovariateSpec(
        name="tenure_counts",
        table="B25003",
        source_variables=_source_variables_for_outputs(
            ("total_households", "owner_households", "renter_households")
        ),
        output_columns=("total_households", "owner_households", "renter_households"),
        measure_kind="count",
        denominator_column=None,
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Household counts are support covariates and denominators for selected "
            "expanded measures."
        ),
    ),
    ACS5CovariateSpec(
        name="poverty",
        table="C17002",
        source_variables=_source_variables_for_outputs(
            ("poverty_universe", "below_50pct_poverty", "50_to_99pct_poverty")
        ),
        output_columns=(
            "poverty_universe",
            "below_50pct_poverty",
            "50_to_99pct_poverty",
            "population_below_poverty",
            "poverty_rate",
        ),
        measure_kind="rate",
        denominator_column="poverty_universe",
        weight_column="area_share",
        rollup_method="ratio_of_area_weighted_sums",
        canonical_measure=True,
        caveats=(
            "population_below_poverty is derived from the under-50% and 50-99% "
            "poverty bins before rollup."
        ),
    ),
    ACS5CovariateSpec(
        name="labor_force_status",
        table="B23025",
        source_variables=_source_variables_for_outputs(
            ("civilian_labor_force", "unemployed_count")
        ),
        output_columns=("civilian_labor_force", "unemployed_count", "unemployment_rate"),
        measure_kind="rate",
        denominator_column="civilian_labor_force",
        weight_column="area_share",
        rollup_method="ratio_of_area_weighted_sums",
        canonical_measure=True,
        caveats=(
            "unemployment_rate is derived after count rollup as unemployed_count / "
            "civilian_labor_force."
        ),
    ),
    ACS5CovariateSpec(
        name="household_income_distribution",
        table="B19001",
        source_variables=_source_variables_for_outputs(_columns_with_prefix("household_income_")),
        output_columns=_columns_with_prefix("household_income_"),
        measure_kind="distribution",
        denominator_column="household_income_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Distribution bins support recipe-selected derived measures such as "
            "quintiles and Gini approximations; bins are aggregated as counts."
        ),
    ),
    ACS5CovariateSpec(
        name="gross_rent_distribution",
        table="B25063",
        source_variables=_source_variables_for_outputs(
            _columns_with_prefix("gross_rent_distribution_")
        ),
        output_columns=_columns_with_prefix("gross_rent_distribution_"),
        measure_kind="distribution",
        denominator_column="gross_rent_distribution_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Distribution bins support recipe-selected rent distribution summaries "
            "and are not canonical panel measures by default."
        ),
    ),
    ACS5CovariateSpec(
        name="owner_occupied_value_distribution",
        table="B25075",
        source_variables=_source_variables_for_outputs(
            _columns_with_prefix("owner_occupied_value_")
        ),
        output_columns=_columns_with_prefix("owner_occupied_value_"),
        measure_kind="distribution",
        denominator_column="owner_occupied_value_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Top value bins B25075_026E and B25075_027E are unavailable before ACS5 vintage 2015."
        ),
    ),
    ACS5CovariateSpec(
        name="rent_burden",
        table="B25070",
        source_variables=_source_variables_for_outputs(
            _columns_with_prefix("gross_rent_pct_income_")
        ),
        output_columns=(*_columns_with_prefix("gross_rent_pct_income_"), "rent_burden_30_plus"),
        measure_kind="rate",
        denominator_column="gross_rent_pct_income_total",
        weight_column="area_share",
        rollup_method="ratio_of_area_weighted_sums",
        canonical_measure=True,
        caveats=(
            "rent_burden_30_plus excludes households where gross rent as a "
            "percentage of income is not computed."
        ),
    ),
    ACS5CovariateSpec(
        name="owner_cost_burden",
        table="B25091",
        source_variables=_source_variables_for_outputs(
            _columns_with_prefix("owner_costs_pct_income_")
        ),
        output_columns=_columns_with_prefix("owner_costs_pct_income_"),
        measure_kind="distribution",
        denominator_column="owner_costs_pct_income_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Owner cost burden bins are expanded covariates and remain "
            "recipe-selectable rather than canonical by default."
        ),
    ),
    ACS5CovariateSpec(
        name="tenure_by_household_income",
        table="B25118",
        source_variables=_source_variables_for_outputs(_columns_with_prefix("tenure_income_")),
        output_columns=_columns_with_prefix("tenure_income_"),
        measure_kind="distribution",
        denominator_column="tenure_income_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats=(
            "Tenure-by-income bins support recipe-selected owner/renter income "
            "distribution measures and Gini approximations."
        ),
    ),
    ACS5CovariateSpec(
        name="educational_attainment_25plus",
        table="B15003",
        source_variables=_source_variables_for_outputs(
            _columns_with_prefix("educational_attainment_25plus_")
        ),
        output_columns=_columns_with_prefix("educational_attainment_25plus_"),
        measure_kind="distribution",
        denominator_column="educational_attainment_25plus_total",
        weight_column="area_share",
        rollup_method="area_weighted_sum",
        caveats="B15003 is unavailable before ACS5 vintage 2012.",
    ),
)

ACS5_COVARIATE_REGISTRY_BY_OUTPUT: dict[str, ACS5CovariateSpec] = {
    column: spec for spec in ACS5_COVARIATE_REGISTRY for column in spec.output_columns
}

ACS5_EXPANDED_COVARIATE_COLUMNS: tuple[str, ...] = tuple(
    column
    for spec in ACS5_COVARIATE_REGISTRY
    for column in spec.output_columns
    if spec.recipe_selectable and column not in ACS_MEASURE_COLUMNS
)


def acs5_covariate_spec_for_output(column: str) -> ACS5CovariateSpec:
    """Return the aggregation registry entry that owns an ACS5 output column."""
    try:
        return ACS5_COVARIATE_REGISTRY_BY_OUTPUT[column]
    except KeyError as exc:
        raise KeyError(f"Unsupported ACS5 covariate output column: {column}") from exc


# ---------------------------------------------------------------------------
# Column classification (for translation and aggregation)
# ---------------------------------------------------------------------------

# Count columns: area-weighted during translation and aggregation
COUNT_COLUMNS: list[str] = ACS5_COUNT_COLUMNS

# Median columns: population-weighted average during translation/aggregation
MEDIAN_COLUMNS: list[str] = ACS5_MEDIAN_COLUMNS

# Margin of error columns: propagated via sqrt(sum(w² × m²))
MOE_COLUMNS: list[str] = ACS5_MOE_COLUMNS

# Derived columns (computed from raw variables, not fetched directly)
DERIVED_COLUMNS: list[str] = ACS5_DERIVED_COLUMNS

# ---------------------------------------------------------------------------
# Output column order for canonical tract-level file
# ---------------------------------------------------------------------------

TRACT_OUTPUT_COLUMNS: list[str] = ACS_TRACT_OUTPUT_COLUMNS

# ---------------------------------------------------------------------------
# SAE tract support contract
# ---------------------------------------------------------------------------

ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE: dict[str, list[str]] = {
    "B01003": ["total_population"],
    "B01001": ["adult_population"],
    "B25003": ["total_households", "owner_households", "renter_households"],
    "B23025": ["civilian_labor_force", "unemployed_count"],
    "B19001": [
        column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("household_income_")
    ],
    "B25075": [
        column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("owner_occupied_value_")
    ],
    "B25063": [
        column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("gross_rent_distribution_")
    ],
    "B25070": [
        column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("gross_rent_pct_income_")
    ],
    "B25091": [
        column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("owner_costs_pct_income_")
    ],
    "B25118": [column for column in ACS5_SAE_COUNT_COLUMNS if column.startswith("tenure_income_")],
    "B15003": [
        column
        for column in ACS5_SAE_COUNT_COLUMNS
        if column.startswith("educational_attainment_25plus_")
    ],
}

ACS5_SAE_SUPPORT_TABLES: list[str] = list(ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE)

ACS5_SAE_DENOMINATOR_COLUMNS: list[str] = [
    "total_population",
    "adult_population",
    "total_households",
    "owner_households",
    "renter_households",
    "civilian_labor_force",
    "household_income_total",
    "gross_rent_distribution_total",
    "gross_rent_pct_income_total",
    "owner_costs_pct_income_with_mortgage_total",
    "owner_costs_pct_income_without_mortgage_total",
    "tenure_income_owner_occupied_total",
    "tenure_income_renter_occupied_total",
    "owner_occupied_value_total",
    "educational_attainment_25plus_total",
]

ACS5_SAE_SUPPORT_COLUMNS: list[str] = [
    column
    for table in ACS5_SAE_SUPPORT_TABLES
    for column in ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE[table]
]

ACS5_SAE_SUPPORT_METADATA_COLUMNS: list[str] = [
    "sae_support_tables",
    "sae_missing_support_tables",
    "sae_zero_denominator_columns",
    "sae_support_column_tables",
]

ACS5_SAE_SUPPORT_OUTPUT_COLUMNS: list[str] = [
    "tract_geoid",
    "county_fips",
    "acs_vintage",
    "tract_vintage",
    *ACS5_SAE_SUPPORT_COLUMNS,
    *ACS5_SAE_SUPPORT_METADATA_COLUMNS,
]
