"""BLS QCEW county high-level annual covariate contract.

The intended raw input is the official BLS Quarterly Census of Employment and
Wages county high-level annual workbook/CSV/TXT layout. Those files expose
county rows with ownership, supersector-industry, annual employment, annual
wages, and annual establishment counts. HHP-Lab restricts ingest to county rows
with Total Covered ownership (Own=0) and total all-industries NAICS code 10.
"""

from __future__ import annotations

from typing import Final

QCEW_SOURCE_ID: Final = "qcew"
QCEW_PROVIDER: Final = "bls"
QCEW_PRODUCT: Final = "quarterly_census_of_employment_and_wages"
QCEW_SOURCE_PAGE: Final = "https://www.bls.gov/cew/downloadable-data-files.htm"
QCEW_SOURCE_URL: Final = QCEW_SOURCE_PAGE
QCEW_FIRST_YEAR: Final = 1975

QCEW_MEASURE_COLUMNS: Final[tuple[str, ...]] = (
    "annual_avg_emplvl",
    "total_annual_wages",
    "annual_avg_estabs",
)

QCEW_DERIVED_MEASURE_COLUMNS: Final[tuple[str, ...]] = (
    "annual_avg_weekly_wage",
    "avg_annual_pay",
)

QCEW_REQUIRED_CURATED_COLUMNS: Final[tuple[str, ...]] = ("county_fips", "year")
QCEW_REQUIRED_RAW_COLUMNS: Final[tuple[str, ...]] = (
    "year",
    "qtr",
    "own_code",
    "industry_code",
    "annual_avg_estabs",
    "annual_avg_emplvl",
    "total_annual_wages",
)

QCEW_COUNTY_AREA_TYPE: Final = "county"
QCEW_TOTAL_COVERED_OWN_CODE: Final = "0"
QCEW_ALL_INDUSTRY_CODE: Final = "10"
QCEW_COUNTY_TOTAL_COVERED_AGGLVL: Final = "70"
