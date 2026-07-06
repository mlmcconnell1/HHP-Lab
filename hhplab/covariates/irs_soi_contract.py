"""IRS SOI county migration covariate contract."""

from __future__ import annotations

from typing import Final

IRS_SOI_SOURCE_ID: Final = "irs_soi_migration"
IRS_SOI_PROVIDER: Final = "irs"
IRS_SOI_PRODUCT: Final = "statistics_of_income_county_migration"
IRS_SOI_SOURCE_PAGE: Final = "https://www.irs.gov/statistics/soi-tax-stats-migration-data"
IRS_SOI_SOURCE_URL: Final = IRS_SOI_SOURCE_PAGE
IRS_SOI_FIRST_YEAR: Final = 2011

IRS_SOI_COUNTY_MEASURE_COLUMNS: Final = (
    "inflow_returns",
    "inflow_exemptions",
    "inflow_agi_thousands",
    "outflow_returns",
    "outflow_exemptions",
    "outflow_agi_thousands",
    "other_flows_inflow_returns",
    "other_flows_inflow_exemptions",
    "other_flows_inflow_agi_thousands",
    "other_flows_outflow_returns",
    "other_flows_outflow_exemptions",
    "other_flows_outflow_agi_thousands",
)

IRS_SOI_PAIR_MEASURE_COLUMNS: Final = (
    "migration_returns",
    "migration_exemptions",
    "migration_agi_thousands",
)

IRS_SOI_REQUIRED_RAW_COLUMNS: Final = (
    "y2_statefips",
    "y2_countyfips",
    "y1_statefips",
    "y1_countyfips",
    "n1",
    "n2",
    "agi",
)

IRS_SOI_COUNTY_CURATED_COLUMNS: Final = (
    "geo_type",
    "geo_id",
    "county_fips",
    "year",
    *IRS_SOI_COUNTY_MEASURE_COLUMNS,
    "source_id",
    "provider",
    "product",
    "topic",
    "data_source",
    "source_url",
    "raw_sha256",
    "ingested_at",
)

IRS_SOI_PAIR_CURATED_COLUMNS: Final = (
    "year",
    "origin_county_fips",
    "destination_county_fips",
    *IRS_SOI_PAIR_MEASURE_COLUMNS,
    "source_id",
    "provider",
    "product",
    "topic",
    "data_source",
    "source_url",
    "raw_sha256",
    "ingested_at",
)

IRS_SOI_OTHER_FLOW_STATE_FIPS: Final = frozenset({"57", "58", "59"})
IRS_SOI_SUMMARY_STATE_FIPS: Final = frozenset({"57", "58", "59", "96", "97", "98"})
IRS_SOI_STATE_TOTAL_COUNTY_FIPS: Final = "000"
