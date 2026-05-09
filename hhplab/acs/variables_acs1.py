"""ACS 1-year variable definitions for native ACS ingest.

Single source of truth for ACS 1-year variables fetched at native geography.
These are separate from the ACS 5-year tract-level variables in
``variables.py`` because ACS 1-year is a different Census product with
different geographic availability and temporal resolution.

The metro ingest now covers unemployment plus the additional income,
housing-cost, utility-cost, tenure, structure, and unit-mix tables
requested for ACS 1-year metro artifacts.
"""

from __future__ import annotations


def _dense_estimate_map(table: str, column_names: list[str]) -> dict[str, str]:
    """Return ``{Bxxxx_001E: name1, ...}`` for contiguous estimate tables."""
    return {
        f"{table}_{idx:03d}E": column_name
        for idx, column_name in enumerate(column_names, start=1)
    }


# ---------------------------------------------------------------------------
# Requested ACS1 detailed tables at metro geography
# ---------------------------------------------------------------------------

ACS1_TABLE_COLUMN_NAMES: dict[str, dict[str, str]] = {
    "B23025": {
        "B23025_001E": "pop_16_plus",
        "B23025_003E": "civilian_labor_force",
        "B23025_005E": "unemployed_count",
    },
    "B19001": _dense_estimate_map(
        "B19001",
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
    ),
    "B19080": _dense_estimate_map(
        "B19080",
        [
            "household_income_quintile_cutoff_lowest",
            "household_income_quintile_cutoff_second",
            "household_income_quintile_cutoff_third",
            "household_income_quintile_cutoff_fourth",
            "household_income_top_5pct_lower_limit",
        ],
    ),
    "B19081": _dense_estimate_map(
        "B19081",
        [
            "mean_household_income_lowest_quintile",
            "mean_household_income_second_quintile",
            "mean_household_income_third_quintile",
            "mean_household_income_fourth_quintile",
            "mean_household_income_highest_quintile",
            "mean_household_income_top_5pct",
        ],
    ),
    "B19082": _dense_estimate_map(
        "B19082",
        [
            "aggregate_income_share_lowest_quintile",
            "aggregate_income_share_second_quintile",
            "aggregate_income_share_third_quintile",
            "aggregate_income_share_fourth_quintile",
            "aggregate_income_share_highest_quintile",
            "aggregate_income_share_top_5pct",
        ],
    ),
    "B25064": _dense_estimate_map(
        "B25064",
        ["median_gross_rent"],
    ),
    "B25063": _dense_estimate_map(
        "B25063",
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
    ),
    "B25088": _dense_estimate_map(
        "B25088",
        [
            "median_owner_costs_total",
            "median_owner_costs_with_mortgage",
            "median_owner_costs_without_mortgage",
        ],
    ),
    "B25089": _dense_estimate_map(
        "B25089",
        [
            "aggregate_owner_costs_total",
            "aggregate_owner_costs_with_mortgage",
            "aggregate_owner_costs_without_mortgage",
        ],
    ),
    "B25070": _dense_estimate_map(
        "B25070",
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
    ),
    "B25091": _dense_estimate_map(
        "B25091",
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
    ),
    "B25119": _dense_estimate_map(
        "B25119",
        [
            "median_household_income_by_tenure_total",
            "median_household_income_owner_occupied",
            "median_household_income_renter_occupied",
        ],
    ),
    "B25118": _dense_estimate_map(
        "B25118",
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
    ),
    "B25132": _dense_estimate_map(
        "B25132",
        [
            "electricity_cost_total",
            "electricity_cost_not_charged_or_included",
            "electricity_cost_charged",
            "electricity_cost_lt_50",
            "electricity_cost_50_to_99",
            "electricity_cost_100_to_149",
            "electricity_cost_150_to_199",
            "electricity_cost_200_to_249",
            "electricity_cost_250_plus",
        ],
    ),
    "B25133": _dense_estimate_map(
        "B25133",
        [
            "gas_cost_total",
            "gas_cost_not_charged_or_included",
            "gas_cost_charged",
            "gas_cost_lt_25",
            "gas_cost_25_to_49",
            "gas_cost_50_to_74",
            "gas_cost_75_to_99",
            "gas_cost_100_to_149",
            "gas_cost_150_plus",
        ],
    ),
    "B25134": _dense_estimate_map(
        "B25134",
        [
            "water_sewer_cost_total",
            "water_sewer_cost_not_charged_or_included",
            "water_sewer_cost_charged",
            "water_sewer_cost_lt_125",
            "water_sewer_cost_125_to_249",
            "water_sewer_cost_250_to_499",
            "water_sewer_cost_500_to_749",
            "water_sewer_cost_750_to_999",
            "water_sewer_cost_1000_plus",
        ],
    ),
    "B25040": _dense_estimate_map(
        "B25040",
        [
            "heating_fuel_total",
            "heating_fuel_utility_gas",
            "heating_fuel_bottled_tank_lp_gas",
            "heating_fuel_electricity",
            "heating_fuel_fuel_oil_kerosene",
            "heating_fuel_coal_or_coke",
            "heating_fuel_wood",
            "heating_fuel_solar_energy",
            "heating_fuel_other_fuel",
            "heating_fuel_no_fuel_used",
        ],
    ),
    "B25068": _dense_estimate_map(
        "B25068",
        [
            "gross_rent_bedrooms_total",
            "gross_rent_no_bedroom_total",
            "gross_rent_no_bedroom_cash_rent_total",
            "gross_rent_no_bedroom_cash_rent_lt_300",
            "gross_rent_no_bedroom_cash_rent_300_to_499",
            "gross_rent_no_bedroom_cash_rent_500_to_749",
            "gross_rent_no_bedroom_cash_rent_750_to_999",
            "gross_rent_no_bedroom_cash_rent_1000_to_1499",
            "gross_rent_no_bedroom_cash_rent_1500_plus",
            "gross_rent_no_bedroom_no_cash_rent",
            "gross_rent_1_bedroom_total",
            "gross_rent_1_bedroom_cash_rent_total",
            "gross_rent_1_bedroom_cash_rent_lt_300",
            "gross_rent_1_bedroom_cash_rent_300_to_499",
            "gross_rent_1_bedroom_cash_rent_500_to_749",
            "gross_rent_1_bedroom_cash_rent_750_to_999",
            "gross_rent_1_bedroom_cash_rent_1000_to_1499",
            "gross_rent_1_bedroom_cash_rent_1500_plus",
            "gross_rent_1_bedroom_no_cash_rent",
            "gross_rent_2_bedrooms_total",
            "gross_rent_2_bedrooms_cash_rent_total",
            "gross_rent_2_bedrooms_cash_rent_lt_300",
            "gross_rent_2_bedrooms_cash_rent_300_to_499",
            "gross_rent_2_bedrooms_cash_rent_500_to_749",
            "gross_rent_2_bedrooms_cash_rent_750_to_999",
            "gross_rent_2_bedrooms_cash_rent_1000_to_1499",
            "gross_rent_2_bedrooms_cash_rent_1500_plus",
            "gross_rent_2_bedrooms_no_cash_rent",
            "gross_rent_3plus_bedrooms_total",
            "gross_rent_3plus_bedrooms_cash_rent_total",
            "gross_rent_3plus_bedrooms_cash_rent_lt_300",
            "gross_rent_3plus_bedrooms_cash_rent_300_to_499",
            "gross_rent_3plus_bedrooms_cash_rent_500_to_749",
            "gross_rent_3plus_bedrooms_cash_rent_750_to_999",
            "gross_rent_3plus_bedrooms_cash_rent_1000_to_1499",
            "gross_rent_3plus_bedrooms_cash_rent_1500_plus",
            "gross_rent_3plus_bedrooms_no_cash_rent",
        ],
    ),
    "B25035": _dense_estimate_map(
        "B25035",
        ["median_year_structure_built"],
    ),
    "B25024": _dense_estimate_map(
        "B25024",
        [
            "units_in_structure_total",
            "units_in_structure_1_detached",
            "units_in_structure_1_attached",
            "units_in_structure_2",
            "units_in_structure_3_to_4",
            "units_in_structure_5_to_9",
            "units_in_structure_10_to_19",
            "units_in_structure_20_to_49",
            "units_in_structure_50_plus",
            "units_in_structure_mobile_home",
            "units_in_structure_boat_rv_van_other",
        ],
    ),
    "B25010": _dense_estimate_map(
        "B25010",
        [
            "average_household_size_total",
            "average_household_size_owner_occupied",
            "average_household_size_renter_occupied",
        ],
    ),
}

ACS1_TABLES: list[str] = list(ACS1_TABLE_COLUMN_NAMES)

ACS1_VARIABLES_BY_TABLE: dict[str, list[str]] = {
    table: list(column_names)
    for table, column_names in ACS1_TABLE_COLUMN_NAMES.items()
}

ACS1_VARIABLE_NAMES: dict[str, str] = {
    variable_code: column_name
    for column_names in ACS1_TABLE_COLUMN_NAMES.values()
    for variable_code, column_name in column_names.items()
}

ACS1_VARIABLES: list[str] = [
    variable_code
    for table in ACS1_TABLES
    for variable_code in ACS1_VARIABLES_BY_TABLE[table]
]

# Backward-compatible aliases used by older tests and call sites.
ACS1_UNEMPLOYMENT_TABLE: str = "B23025"
ACS1_UNEMPLOYMENT_VARIABLES: list[str] = ACS1_VARIABLES_BY_TABLE["B23025"]

# ---------------------------------------------------------------------------
# Derived measures
# ---------------------------------------------------------------------------

DERIVED_ACS1_MEASURES: dict[str, str] = {
    "unemployment_rate_acs1": (
        "Unemployment rate from ACS 1-year (B23025_005E / B23025_003E)"
    ),
}

ACS1_FLOAT_COLUMNS: list[str] = [
    "aggregate_income_share_lowest_quintile",
    "aggregate_income_share_second_quintile",
    "aggregate_income_share_third_quintile",
    "aggregate_income_share_fourth_quintile",
    "aggregate_income_share_highest_quintile",
    "aggregate_income_share_top_5pct",
    "average_household_size_total",
    "average_household_size_owner_occupied",
    "average_household_size_renter_occupied",
    "unemployment_rate_acs1",
]

ACS1_INTEGER_COLUMNS: list[str] = [
    column_name
    for column_name in ACS1_VARIABLE_NAMES.values()
    if column_name not in ACS1_FLOAT_COLUMNS
]

ACS1_METRO_MEASURE_COLUMNS: list[str] = (
    ACS1_INTEGER_COLUMNS
    + [column for column in ACS1_FLOAT_COLUMNS if column != "unemployment_rate_acs1"]
    + ["unemployment_rate_acs1"]
)

# ---------------------------------------------------------------------------
# Output schema for metro-level ACS1 data
# ---------------------------------------------------------------------------

ACS1_METRO_OUTPUT_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "definition_version",
    "acs1_vintage",
    "cbsa_code",
    *ACS1_METRO_MEASURE_COLUMNS,
    "data_source",
    "source_ref",
    "ingested_at",
]

ACS1_COUNTY_MEASURE_COLUMNS: list[str] = ACS1_METRO_MEASURE_COLUMNS

ACS1_COUNTY_OUTPUT_COLUMNS: list[str] = [
    "state",
    "county",
    "county_fips",
    "geo_id",
    "county_name",
    "NAME",
    "acs1_vintage",
    *ACS1_COUNTY_MEASURE_COLUMNS,
    "data_source",
    "source_ref",
    "ingested_at",
]

# ---------------------------------------------------------------------------
# SAE source aggregate contract
# ---------------------------------------------------------------------------

ACS1_SAE_SOURCE_TABLES: list[str] = [
    "B23025",
    "B19001",
    "B25063",
    "B25070",
    "B25091",
    "B25118",
]

ACS1_SAE_SOURCE_COLUMNS_BY_TABLE: dict[str, list[str]] = {
    "B23025": [
        "pop_16_plus",
        "civilian_labor_force",
        "unemployed_count",
    ],
    **{
        table: list(ACS1_TABLE_COLUMN_NAMES[table].values())
        for table in ACS1_SAE_SOURCE_TABLES
        if table != "B23025"
    },
}

ACS1_SAE_SOURCE_COLUMNS: list[str] = [
    column_name
    for table in ACS1_SAE_SOURCE_TABLES
    for column_name in ACS1_SAE_SOURCE_COLUMNS_BY_TABLE[table]
]

ACS1_SAE_SOURCE_METADATA_COLUMNS: list[str] = [
    "sae_source_tables",
    "sae_unavailable_tables",
    "sae_source_column_tables",
]

ACS1_SAE_SOURCE_OUTPUT_COLUMNS: list[str] = [
    "county_fips",
    "acs1_vintage",
    *ACS1_SAE_SOURCE_COLUMNS,
    *ACS1_SAE_SOURCE_METADATA_COLUMNS,
]

# ---------------------------------------------------------------------------
# ACS 1-year availability
# ---------------------------------------------------------------------------

ACS1_FIRST_RELIABLE_YEAR: int = 2012

ACS1_UNAVAILABLE_VINTAGES: set[int] = {2020}

# Utility-cost detailed tables were added to ACS1 in 2021. Keep the output
# schema stable across vintages by backfilling these columns as NA earlier.
ACS1_TABLE_FIRST_YEAR: dict[str, int] = {
    table: ACS1_FIRST_RELIABLE_YEAR
    for table in ACS1_TABLES
}
ACS1_TABLE_FIRST_YEAR.update(
    {
        "B25132": 2021,
        "B25133": 2021,
        "B25134": 2021,
    }
)


def acs1_tables_for_vintage(vintage: int) -> list[str]:
    """Return the ACS1 detailed tables available for a vintage."""
    return [
        table
        for table in ACS1_TABLES
        if vintage >= ACS1_TABLE_FIRST_YEAR.get(table, ACS1_FIRST_RELIABLE_YEAR)
    ]


def acs1_unavailable_tables_for_vintage(vintage: int) -> list[str]:
    """Return requested ACS1 tables absent for a given vintage."""
    available = set(acs1_tables_for_vintage(vintage))
    return [table for table in ACS1_TABLES if table not in available]
