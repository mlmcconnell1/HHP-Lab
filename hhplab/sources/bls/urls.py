"""BLS endpoint and source-reference URLs."""

from typing import Final

BLS_API_V2: Final = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_LAUS_SOURCE_REF: Final = "https://www.bls.gov/lau/home.htm"
BLS_CPI_SOURCE_REF: Final = "https://www.bls.gov/cpi/"
BLS_API_REGISTRATION_URL: Final = "https://data.bls.gov/registrationEngine/"

__all__ = [
    "BLS_API_REGISTRATION_URL",
    "BLS_API_V2",
    "BLS_CPI_SOURCE_REF",
    "BLS_LAUS_SOURCE_REF",
]
