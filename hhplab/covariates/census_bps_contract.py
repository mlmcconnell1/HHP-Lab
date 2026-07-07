"""Census Building Permits Survey (BPS) county annual raw-file contract.

Raw files are the positional CSV annual county files published at
https://www2.census.gov/econ/bps/County/ named ``co{YYYY}a.txt``. Each file has
two header rows plus a blank separator row, then one row per county. Columns
6-17 are the estimates-with-imputation series for the four structure classes;
columns 18-29 repeat the same classes for reporting-places-only ("rep") and are
intentionally excluded so measures include Census imputation for
non-reporting permit offices.
"""

from __future__ import annotations

from typing import Final

CENSUS_BPS_SOURCE_ID: Final = "census_bps"
CENSUS_BPS_PROVIDER: Final = "census"
CENSUS_BPS_PRODUCT: Final = "building_permits_survey"
CENSUS_BPS_SOURCE_PAGE: Final = "https://www.census.gov/permits"
CENSUS_BPS_RAW_URL_TEMPLATE: Final = "https://www2.census.gov/econ/bps/County/co{year}a.txt"
CENSUS_BPS_ANNUAL_FILE_GLOB: Final = "co*a.txt"

# Number of leading non-data rows in each annual file: two header rows plus a
# blank separator row (pandas drops the blank line, so only the headers are
# skipped explicitly).
CENSUS_BPS_HEADER_ROWS: Final = 2

# Positional column indices in the raw annual county file.
CENSUS_BPS_SURVEY_YEAR_COLUMN: Final = 0
CENSUS_BPS_STATE_FIPS_COLUMN: Final = 1
CENSUS_BPS_COUNTY_FIPS3_COLUMN: Final = 2

# Estimates-with-imputation (Bldgs, Units) column indices by structure class.
CENSUS_BPS_BUILDING_COLUMNS: Final[tuple[int, ...]] = (6, 9, 12, 15)
CENSUS_BPS_UNIT_COLUMNS: Final[tuple[int, ...]] = (7, 10, 13, 16)

CENSUS_BPS_MEASURE_COLUMNS: Final[tuple[str, ...]] = (
    "permitted_units",
    "permitted_buildings",
)
