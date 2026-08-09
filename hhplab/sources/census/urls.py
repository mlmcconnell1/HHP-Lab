"""Census data-product URLs owned by the Census source package."""

from typing import Final

CENSUS_API_ACS5: Final = "https://api.census.gov/data/{year}/acs/acs5"
CENSUS_API_ACS1: Final = "https://api.census.gov/data/{year}/acs/acs1"
CENSUS_PEP_DATASETS_BASE: Final = "https://www2.census.gov/programs-surveys/popest/datasets"

__all__ = ["CENSUS_API_ACS1", "CENSUS_API_ACS5", "CENSUS_PEP_DATASETS_BASE"]
