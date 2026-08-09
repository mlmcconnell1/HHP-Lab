"""MEDSL election-data source URLs."""

from typing import Final

MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI: Final = "https://doi.org/10.7910/DVN/VOQCHQ"
MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API: Final = (
    "https://dataverse.harvard.edu/api/access/datafile/13573089"
)

__all__ = [
    "MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API",
    "MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI",
]
