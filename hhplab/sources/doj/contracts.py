"""Declarative DOJ sanctuary jurisdiction contracts."""

from __future__ import annotations

from dataclasses import dataclass

SANCTUARY_MSA_MATCH_COLUMNS: tuple[str, ...] = (
    "cbsa_code",
    "msa_name",
    "state_match",
    "county_match",
    "city_match",
    "matched_states",
    "matched_counties",
    "matched_cities",
    "match_basis",
)

SANCTUARY_MSA_PANEL_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "cbsa_code",
    "msa_name",
    "doj_sanctuary_msa",
    "doj_sanctuary_population_share",
    "doj_sanctuary_population",
    "doj_sanctuary_population_denominator",
    "doj_sanctuary_population_year",
    "match_basis",
    "state_match",
    "county_match",
    "city_match",
    "matched_states",
    "matched_counties",
    "matched_cities",
    "doj_sanctuary_source_date",
)

DOJ_LISTED_STATES: tuple[str, ...] = (
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Illinois",
    "Minnesota",
    "Nevada",
    "New York",
    "Oregon",
    "Rhode Island",
    "Vermont",
    "Washington",
)


@dataclass(frozen=True)
class CountyDesignation:
    """A DOJ-listed county designation with Census county FIPS."""

    label: str
    county_fips: str


@dataclass(frozen=True)
class CityDesignation:
    """A DOJ-listed city mapped conservatively to containing county/counties."""

    label: str
    county_fips: tuple[str, ...]
    match_note: str


DOJ_LISTED_COUNTIES: tuple[CountyDesignation, ...] = (
    CountyDesignation("Baltimore County, MD", "24005"),
    CountyDesignation("Cook County, IL", "17031"),
    CountyDesignation("San Diego County, CA", "06073"),
    CountyDesignation("San Francisco County, CA", "06075"),
)

DOJ_LISTED_CITIES: tuple[CityDesignation, ...] = (
    CityDesignation("Albuquerque, NM", ("35001",), "City lies in Bernalillo County, NM."),
    CityDesignation("Berkeley, CA", ("06001",), "City lies in Alameda County, CA."),
    CityDesignation("Boston, MA", ("25025",), "City lies in Suffolk County, MA."),
    CityDesignation(
        "Chicago, IL",
        ("17031", "17043"),
        "City lies primarily in Cook County, IL, with a small area in DuPage County, IL.",
    ),
    CityDesignation("Denver, CO", ("08031",), "City and county are consolidated."),
    CityDesignation(
        "East Lansing, MI",
        ("26037", "26065"),
        "City spans Clinton and Ingham counties, MI.",
    ),
    CityDesignation("Hoboken, NJ", ("34017",), "City lies in Hudson County, NJ."),
    CityDesignation("Jersey City, NJ", ("34017",), "City lies in Hudson County, NJ."),
    CityDesignation("Los Angeles, CA", ("06037",), "City lies in Los Angeles County, CA."),
    CityDesignation("New Orleans, LA", ("22071",), "City lies in Orleans Parish, LA."),
    CityDesignation(
        "New York City, NY",
        ("36005", "36047", "36061", "36081", "36085"),
        "City spans Bronx, Kings, New York, Queens, and Richmond counties, NY.",
    ),
    CityDesignation("Newark, NJ", ("34013",), "City lies in Essex County, NJ."),
    CityDesignation("Paterson, NJ", ("34031",), "City lies in Passaic County, NJ."),
    CityDesignation(
        "Philadelphia, PA",
        ("42101",),
        "City and county are coterminous.",
    ),
    CityDesignation(
        "Portland, OR",
        ("41005", "41051", "41067"),
        "City spans Clackamas, Multnomah, and Washington counties, OR.",
    ),
    CityDesignation("Rochester, NY", ("36055",), "City lies in Monroe County, NY."),
    CityDesignation("Seattle, WA", ("53033",), "City lies in King County, WA."),
    CityDesignation(
        "San Francisco City, CA",
        ("06075",),
        "City and county are consolidated.",
    ),
)
