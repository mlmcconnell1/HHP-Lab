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
