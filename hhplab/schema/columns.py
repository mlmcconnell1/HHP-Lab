"""Canonical column names and reusable column groups."""

from __future__ import annotations

TOTAL_POPULATION = "total_population"
POPULATION_DENSITY_COLUMN = "population_density_per_sq_km"

GEO_ID_COLUMNS: tuple[str, ...] = (
    "geo_type",
    "geo_id",
    "coc_id",
    "metro_id",
    "msa_id",
    "county_fips",
    "tract_geoid",
)

ACS_MEASURE_COLUMNS: list[str] = [
    TOTAL_POPULATION,
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "unemployment_rate",
]

ACS1_MEASURE_COLUMNS: list[str] = [
    "unemployment_rate_acs1",
]

LAUS_MEASURE_COLUMNS: list[str] = [
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
]

LAUS_MEASURE_CODES: dict[str, str] = {
    "unemployment_rate": "03",
    "unemployed": "04",
    "employed": "05",
    "labor_force": "06",
}

ACS5_SAE_COUNT_COLUMNS: list[str] = [
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
]

SAE_COMPONENT_COLUMNS: list[str] = [f"sae_{column}" for column in ACS5_SAE_COUNT_COLUMNS]

SAE_DERIVED_MEASURE_COLUMNS: list[str] = [
    "sae_household_income_quintile_cutoff_20",
    "sae_household_income_quintile_cutoff_40",
    "sae_household_income_median",
    "sae_household_income_quintile_cutoff_60",
    "sae_household_income_quintile_cutoff_80",
    "sae_gross_rent_median",
    "sae_rent_burden_30_plus",
    "sae_rent_burden_50_plus",
    "sae_owner_cost_burden_30_plus",
    "sae_owner_cost_burden_50_plus",
    "sae_civilian_labor_force",
    "sae_unemployed_count",
    "sae_unemployment_rate",
]

SAE_DIAGNOSTIC_COLUMNS: list[str] = [
    "sae_source_county_count",
    "sae_source_counties",
    "sae_crosswalk_tract_count",
    "sae_allocated_tract_count",
    "sae_missing_allocation_tract_count",
    "sae_crosswalk_coverage_ratio",
    "sae_crosswalk_share_sum",
    "sae_nan_share_tract_count",
    "sae_missing_support_count",
    "sae_zero_denominator_count",
    "sae_partial_coverage_count",
    "sae_direct_county_comparable",
    "sae_direct_county_comparability_reason",
    "sae_direct_county_absolute_difference",
    "sae_direct_county_relative_difference",
]

SAE_LINEAGE_COLUMNS: list[str] = [
    "acs1_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "sae_allocation_method",
    "sae_denominator_source",
    "sae_crosswalk_id",
]

SAE_MEASURE_COLUMNS: list[str] = [
    *SAE_DERIVED_MEASURE_COLUMNS,
]

SAE_OUTPUT_COLUMNS: list[str] = [
    "geo_type",
    "geo_id",
    "year",
    *SAE_LINEAGE_COLUMNS,
    *SAE_MEASURE_COLUMNS,
    *SAE_DIAGNOSTIC_COLUMNS,
]

ACS1_IMPUTATION_LINEAGE_COLUMNS: list[str] = [
    "acs1_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "acs1_imputation_method",
    "acs1_imputation_denominator_source",
    "acs1_imputation_crosswalk_id",
]

ACS1_IMPUTATION_FLAG_COLUMNS: list[str] = [
    "is_modeled",
    "is_synthetic",
]

ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS: list[str] = [
    "acs1_imputation_source_county_count",
    "acs1_imputation_tract_count",
    "acs1_imputation_zero_denominator_count",
    "acs1_imputation_missing_support_count",
    "acs1_imputation_validation_abs_diff",
    "acs1_imputation_validation_rel_diff",
]

ACS1_IMPUTATION_MEASURE_COLUMNS: list[str] = [
    "acs1_imputed_population_below_poverty",
    "acs1_imputed_poverty_universe",
    "acs1_imputed_poverty_rate",
    "acs1_imputed_total_households",
]

ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS: list[str] = [
    "geo_type",
    "geo_id",
    "year",
    *ACS1_IMPUTATION_LINEAGE_COLUMNS,
    *ACS1_IMPUTATION_FLAG_COLUMNS,
]

ACS1_IMPUTATION_OUTPUT_COLUMNS: list[str] = [
    *ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
    *ACS1_IMPUTATION_MEASURE_COLUMNS,
    *ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
]

ACS5_COUNT_COLUMNS: list[str] = [
    TOTAL_POPULATION,
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
    *ACS5_SAE_COUNT_COLUMNS,
]

ACS5_MEDIAN_COLUMNS: list[str] = [
    "median_household_income",
    "median_gross_rent",
]

ACS5_MOE_COLUMNS: list[str] = [
    "moe_total_population",
]

ACS5_DERIVED_COLUMNS: list[str] = [
    "adult_population",
    "population_below_poverty",
]

ACS_TRACT_OUTPUT_COLUMNS: list[str] = [
    "tract_geoid",
    "acs_vintage",
    "tract_vintage",
    TOTAL_POPULATION,
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
    *ACS5_SAE_COUNT_COLUMNS,
    "data_source",
    "source_ref",
    "ingested_at",
]

PEP_COUNTY_OUTPUT_COLUMNS: list[str] = [
    "county_fips",
    "state_fips",
    "county_name",
    "state_name",
    "year",
    "reference_date",
    "population",
    "estimate_type",
    "vintage",
    "data_source",
    "source_url",
    "raw_sha256",
    "ingested_at",
]

PIT_CANONICAL_COLUMNS: list[str] = [
    "pit_year",
    "coc_id",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "data_source",
    "source_ref",
    "ingested_at",
    "notes",
]

ZORI_INGEST_OUTPUT_COLUMNS: list[str] = [
    "geo_type",
    "geo_id",
    "date",
    "year",
    "month",
    "zori",
    "region_name",
    "state",
    "data_source",
    "metric",
    "ingested_at",
    "source_ref",
    "raw_sha256",
]

LAUS_METRO_OUTPUT_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "definition_version",
    "year",
    "cbsa_code",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
    "data_source",
    "series_ids",
    "source_ref",
    "ingested_at",
]

TRACT_MEDIATED_DENOMINATOR_COLUMNS: dict[str, str] = {
    "area": "tract_area",
    "population": TOTAL_POPULATION,
    "household": "total_households",
    "renter_household": "renter_households",
}

TRACT_MEDIATED_WEIGHT_COLUMNS: tuple[str, ...] = (
    "area_weight",
    "population_weight",
    "household_weight",
    "renter_household_weight",
)

TRACT_MEDIATED_COUNTY_XWALK_COLUMNS: tuple[str, ...] = (
    "geo_id",
    "boundary_vintage",
    "county_fips",
    "county_vintage",
    "tract_vintage",
    "acs_vintage",
    "denominator_source",
    "denominator_vintage",
    "county_vintage_semantics",
    "weighting_method",
    "area_weight",
    "population_weight",
    "household_weight",
    "renter_household_weight",
    "area_denominator",
    "population_denominator",
    "household_denominator",
    "renter_household_denominator",
    "county_area_total",
    "county_population_total",
    "county_household_total",
    "county_renter_household_total",
    "geo_area_total",
    "geo_population_total",
    "geo_household_total",
    "geo_renter_household_total",
    "county_area_coverage_ratio",
    "county_population_coverage_ratio",
    "county_household_coverage_ratio",
    "county_renter_household_coverage_ratio",
    "tract_count",
    "denominator_tract_count",
    "missing_denominator_tract_count",
    "denominator_tract_coverage_ratio",
    "county_tract_count",
    "county_denominator_tract_count",
    "county_missing_denominator_tract_count",
    "county_denominator_tract_coverage_ratio",
    "missing_population_tract_count",
    "missing_household_tract_count",
    "missing_renter_household_tract_count",
)

COC_PANEL_COLUMNS: list[str] = [
    "coc_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    TOTAL_POPULATION,
    POPULATION_DENSITY_COLUMN,
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "unemployment_rate",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

METRO_PANEL_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    TOTAL_POPULATION,
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "unemployment_rate_acs1",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
    "coverage_ratio",
    "boundary_changed",
    "acs1_vintage_used",
    "laus_vintage_used",
    "source",
]

MSA_PANEL_COLUMNS: list[str] = [
    "msa_id",
    "msa_name",
    "cbsa_code",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    TOTAL_POPULATION,
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

ZORI_COLUMNS: list[str] = [
    "zori_coc",
    "zori_coverage_ratio",
    "zori_is_eligible",
    "zori_excluded_reason",
    "rent_to_income",
]

ZORI_PROVENANCE_COLUMNS: list[str] = [
    "rent_metric",
    "rent_alignment",
    "zori_min_coverage",
]

DRIFT_PRONE_SOURCE_COLUMNS: tuple[str, ...] = (
    "population",
    "coverage_ratio",
    "source_year",
)
