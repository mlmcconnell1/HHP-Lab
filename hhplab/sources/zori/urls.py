"""Zillow ZORI download URLs."""

from typing import Final

ZILLOW_ZORI_COUNTY: Final = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "County_zori_uc_sfrcondomfr_sm_month.csv"
)
ZILLOW_ZORI_ZIP: Final = (
    "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
)

__all__ = ["ZILLOW_ZORI_COUNTY", "ZILLOW_ZORI_ZIP"]
