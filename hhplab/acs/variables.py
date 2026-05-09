"""Centralized ACS variable definitions and column classification.

Single source of truth for all ACS variables fetched during ingestion,
column types (count vs median vs MOE), and derived column specifications.
"""

from __future__ import annotations

from hhplab.schema.columns import (
    ACS5_COUNT_COLUMNS,
    ACS5_DERIVED_COLUMNS,
    ACS5_MEDIAN_COLUMNS,
    ACS5_MOE_COLUMNS,
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
    # B25064 — Median Gross Rent
    "B25064_001E": "median_gross_rent",
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
    2010: {"B01003_001M", "B23025_003E", "B23025_005E"},
    2011: {"B01003_001M"},
    2012: {"B01003_001M"},
    2013: {"B01003_001M"},
    2014: {"B01003_001M"},
}


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
    "B25064",
    "B25003",
    "C17002",
    "B23025",
]

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
