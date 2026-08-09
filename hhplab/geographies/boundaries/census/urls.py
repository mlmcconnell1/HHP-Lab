"""Census geometry-provider URLs owned by the geography boundary layer."""

from typing import Final

CENSUS_TIGER_BASE: Final = "https://www2.census.gov/geo/tiger/TIGER{year}/{layer}/"
CENSUS_TIGER_CBSA_TEMPLATE: Final = (
    "https://www2.census.gov/geo/tiger/TIGER{year}/CBSA/tl_{year}_us_cbsa.zip"
)
CENSUS_TRACT_RELATIONSHIP_URL: Final = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt"
)
CENSUS_MSA_DELINEATION_FILE_2023: Final = (
    "https://www2.census.gov/programs-surveys/metro-micro/"
    "geographies/reference-files/2023/delineation-files/list1_2023.xlsx"
)

__all__ = [
    "CENSUS_MSA_DELINEATION_FILE_2023",
    "CENSUS_TIGER_BASE",
    "CENSUS_TIGER_CBSA_TEMPLATE",
    "CENSUS_TRACT_RELATIONSHIP_URL",
]
