"""Saiz (2010) MSA supply elasticity covariate contract."""

from __future__ import annotations

from typing import Final

SAIZ_SOURCE_ID: Final = "saiz_supply_elasticity"
SAIZ_PROVIDER: Final = "saiz"
SAIZ_PRODUCT: Final = "supply_elasticity_2010"
SAIZ_SOURCE_PAGE: Final = "https://real-faculty.wharton.upenn.edu/saiz/"
SAIZ_SOURCE_URL: Final = "https://real-faculty.wharton.upenn.edu/saiz/"
SAIZ_ESTIMATE_YEAR: Final = 2010
SAIZ_MEASURE_COLUMNS: Final = (
    "saiz_elasticity",
    "saiz_inverse_elasticity",
    "saiz_undevelopable_share",
    "saiz_wrluri",
)
SAIZ_REQUIRED_CURATED_COLUMNS: Final = ("msa_id", "year")
