"""Source contract for CDC VSRR county overdose covariates."""

from __future__ import annotations

from typing import Final

CDC_OVERDOSE_SOURCE_ID: Final = "cdc_vsrr_county_overdose"
CDC_OVERDOSE_PROVIDER: Final = "cdc"
CDC_OVERDOSE_PRODUCT: Final = "vsrr_provisional_county_overdose"
CDC_OVERDOSE_FIRST_YEAR: Final = 2020
CDC_OVERDOSE_SOURCE_PAGE: Final = (
    "https://www.cdc.gov/nchs/nvss/vsrr/prov-county-drug-overdose.htm"
)
CDC_OVERDOSE_SOURCE_URL: Final = (
    "https://data.cdc.gov/National-Center-for-Health-Statistics/"
    "VSRR-Provisional-County-Level-Drug-Overdose-Death-/gb4e-yj24"
)
CDC_OVERDOSE_REQUIRED_COLUMNS: Final = ("county_fips", "year", "reference_month")
CDC_OVERDOSE_MEASURE_COLUMNS: Final = ("overdose_deaths_12mo",)
