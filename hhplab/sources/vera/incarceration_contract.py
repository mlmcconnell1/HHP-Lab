"""Source contract for Vera county incarceration covariates."""

from __future__ import annotations

from typing import Final

from hhplab.sources.vera.urls import (
    VERA_INCARCERATION_TRENDS_COUNTY_CSV,
    VERA_INCARCERATION_TRENDS_REPO,
)

VERA_JAIL_SOURCE_ID: Final = "vera_jail_incarceration"
VERA_PRISON_SOURCE_ID: Final = "vera_prison_incarceration"
VERA_PROVIDER: Final = "vera"
VERA_JAIL_PRODUCT: Final = "incarceration_trends_jail"
VERA_PRISON_PRODUCT: Final = "incarceration_trends_prison"
VERA_SOURCE_PAGE: Final = VERA_INCARCERATION_TRENDS_REPO
VERA_SOURCE_URL: Final = VERA_INCARCERATION_TRENDS_COUNTY_CSV
VERA_REQUIRED_CURATED_COLUMNS: Final = ("county_fips", "year")

VERA_JAIL_RELIABLE_FIRST_YEAR: Final = 1999
VERA_JAIL_RELIABLE_LAST_YEAR: Final = 2023
VERA_PRISON_RELIABLE_FIRST_YEAR: Final = 1984
VERA_PRISON_LAST_YEAR: Final = 2019

VERA_JAIL_MEASURE_COLUMNS: Final = (
    "total_jail_pop",
    "male_jail_pop",
    "female_jail_pop",
    "black_jail_pop",
    "latinx_jail_pop",
    "white_jail_pop",
    "native_jail_pop",
    "aapi_jail_pop",
    "other_race_jail_pop",
    "total_pretrial_custody",
    "total_sentenced_custody",
    "total_jail_admits",
    "male_jail_admits",
    "female_jail_admits",
    "total_jail_discharges",
    "male_jail_discharges",
    "female_jail_discharges",
    "jail_rated_capacity",
)

VERA_PRISON_MEASURE_COLUMNS: Final = (
    "total_prison_pop",
    "male_prison_pop",
    "female_prison_pop",
    "black_prison_pop",
    "latinx_prison_pop",
    "white_prison_pop",
    "native_prison_pop",
    "aapi_prison_pop",
    "other_race_prison_pop",
    "total_prison_admits",
    "male_prison_admits",
    "female_prison_admits",
)
