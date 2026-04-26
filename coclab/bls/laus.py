"""BLS Local Area Unemployment Statistics (LAUS) series mapping for Glynn/Fox metros.

Provides the authoritative mapping from Glynn/Fox metro IDs to BLS LAUS
metropolitan statistical area series identifiers for annual labor-market
measures.

BLS LAUS Series ID format (20 characters)
------------------------------------------
Positions 1-2:   "LA" — LAUS prefix
Position 3:      "U" — not seasonally adjusted (annual averages are unadjusted)
Positions 4-5:   "MT" — metropolitan statistical area area type
Positions 6-7:   State FIPS code (2 digits, principal state of the MSA)
Positions 8-12:  CBSA code (5 digits, same codes used for Census ACS)
Positions 13-18: "000000" — 6-digit padding (area code filler)
Positions 19-20: Measure code (2 digits)

Example — New York (state FIPS 36, CBSA 35620), unemployment rate:
    LAUMT363562000000003

Measure codes available at MSA level
--------------------------------------
03 — Unemployment rate (%)
04 — Unemployed persons (count)
05 — Employed persons (count)
06 — Civilian labor force (count)

Note: codes 07 (employment-population ratio) and 08 (labor force participation
rate) are NOT available as BLS metro-area LAUS series. These measures are
published only at the national and state level; live BLS API queries for metro
series with codes 07/08 return no data.

Data retrieval
--------------
Annual average values are retrieved via the BLS Public API v2 with
``annualaverage: true``.  BLS returns the annual average as the special
period ``"M13"``.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# BLS LAUS series constants
# ---------------------------------------------------------------------------

#: BLS Public API v2 endpoint for time-series data.
BLS_API_V2_URL: str = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

#: Series ID prefix for metropolitan statistical area LAUS data.
#: "LA" (LAUS) + "U" (not seasonally adjusted) + "MT" (metro area type).
_LAUS_METRO_PREFIX: str = "LAUMT"

#: 6-digit padding between the CBSA code and the 2-digit measure code in
#: a LAUS metro series ID. The full area segment is: state_fips(2) + cbsa(5) +
#: padding(6) = 13 digits total.
_LAUS_AREA_PADDING: str = "000000"

#: BLS period code for annual average values (returned when annualaverage=true).
BLS_ANNUAL_AVERAGE_PERIOD: str = "M13"

#: Measure codes for the four core LAUS labor-market measures at MSA geography.
LAUS_MEASURE_CODES: dict[str, str] = {
    "unemployment_rate": "03",
    "unemployed": "04",
    "employed": "05",
    "labor_force": "06",
}

#: Canonical column order for curated LAUS metro output.
LAUS_METRO_OUTPUT_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "definition_version",
    "year",
    "cbsa_code",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
    "data_source",
    "series_ids",
    "source_ref",
    "ingested_at",
]

# ---------------------------------------------------------------------------
# Series ID builder
# ---------------------------------------------------------------------------


def build_laus_series_id(cbsa_code: str, measure: str, state_fips: str) -> str:
    """Build a BLS LAUS series ID for a given CBSA code and measure.

    Parameters
    ----------
    cbsa_code : str
        5-digit CBSA code (e.g., "35620" for New York).
    measure : str
        Measure name — one of: "unemployment_rate", "unemployed",
        "employed", "labor_force".
    state_fips : str
        2-digit FIPS code of the MSA's principal state (e.g., "36" for NY).
        Required by BLS: the area segment of a metro LAUS series ID encodes
        both the state FIPS and the CBSA code.

    Returns
    -------
    str
        20-character BLS LAUS series ID (e.g., "LAUMT363562000000003").

    Raises
    ------
    ValueError
        If ``measure`` is not a recognised LAUS measure name.
    """
    if measure not in LAUS_MEASURE_CODES:
        raise ValueError(
            f"Unknown LAUS measure {measure!r}. "
            f"Valid measures: {sorted(LAUS_MEASURE_CODES)}"
        )
    code = LAUS_MEASURE_CODES[measure]
    return f"{_LAUS_METRO_PREFIX}{state_fips}{cbsa_code}{_LAUS_AREA_PADDING}{code}"


def build_all_series_ids(cbsa_code: str, state_fips: str) -> dict[str, str]:
    """Build all four core LAUS series IDs for a given CBSA code.

    Parameters
    ----------
    cbsa_code : str
        5-digit CBSA code.
    state_fips : str
        2-digit FIPS code of the MSA's principal state.

    Returns
    -------
    dict[str, str]
        Mapping from measure name to BLS series ID.
    """
    return {
        measure: build_laus_series_id(cbsa_code, measure, state_fips)
        for measure in LAUS_MEASURE_CODES
    }
