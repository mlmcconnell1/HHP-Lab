"""Centralized ACS variable definitions and column classification.

Single source of truth for all ACS variables fetched during ingestion,
column types (count vs median vs MOE), and derived column specifications.
"""

from __future__ import annotations

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
COUNT_COLUMNS: list[str] = [
    "total_population",
    "adult_population",
    "total_households",
    "owner_households",
    "renter_households",
    "poverty_universe",
    "below_50pct_poverty",
    "50_to_99pct_poverty",
    "population_below_poverty",
    "civilian_labor_force",
    "unemployed_count",
]

# Median columns: population-weighted average during translation/aggregation
MEDIAN_COLUMNS: list[str] = [
    "median_household_income",
    "median_gross_rent",
]

# Margin of error columns: propagated via sqrt(sum(w² × m²))
MOE_COLUMNS: list[str] = [
    "moe_total_population",
]

# Derived columns (computed from raw variables, not fetched directly)
DERIVED_COLUMNS: list[str] = [
    "adult_population",
    "population_below_poverty",
]

# ---------------------------------------------------------------------------
# Output column order for canonical tract-level file
# ---------------------------------------------------------------------------

TRACT_OUTPUT_COLUMNS: list[str] = [
    "tract_geoid",
    "acs_vintage",
    "tract_vintage",
    "total_population",
    "moe_total_population",
    "adult_population",
    "total_households",
    "owner_households",
    "renter_households",
    "median_household_income",
    "median_gross_rent",
    "poverty_universe",
    "below_50pct_poverty",
    "50_to_99pct_poverty",
    "population_below_poverty",
    "civilian_labor_force",
    "unemployed_count",
    "data_source",
    "source_ref",
    "ingested_at",
]
