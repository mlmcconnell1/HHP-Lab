"""Authoritative metro-universe and Glynn/Fox subset-definition data.

This module encodes both:

- the canonical Census CBSA/MSA metro universe, derived from curated MSA
  definitions; and
- the 25 Glynn/Fox metropolitan analysis units from Table 1 of:

    Glynn, C. and Fox, E.B. (2019). "Dynamics of Homelessness in Urban
    America." *Annals of Applied Statistics*, 13(1), 573-605.

For the historical subset profile, the ``profile_metro_id`` scheme uses
zero-padded indices matching the paper's ordering (``GF01`` through ``GF25``).

Membership types
----------------
- ``single``: Metro corresponds to one CoC and one county (simple case).
- ``multi_coc``: County contains multiple CoCs; PIT counts are summed
  across the member CoCs.
- ``multi_county``: CoC spans multiple counties; population and ZRI are
  aggregated across member counties (population-weighted).
- ``multi_coc_multi_county``: Multiple CoCs span multiple counties;
  both aggregation rules apply.

County FIPS codes follow the 5-digit Census standard (state + county).
"""

from __future__ import annotations

import pandas as pd

from hhplab.msa.msa_definitions import DEFINITION_VERSION as MSA_DEFINITION_VERSION

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Version identifier for this definition set.
DEFINITION_VERSION: str = "glynn_fox_v1"

#: Canonical metro-universe definition version used for full CBSA/MSA coverage.
CANONICAL_UNIVERSE_DEFINITION_VERSION: str = MSA_DEFINITION_VERSION

#: Stable profile identifier for the Glynn/Fox subset over the canonical universe.
PROFILE_NAME: str = "glynn_fox"

#: Total number of metros in the definition.
METRO_COUNT: int = 25

#: Source reference for provenance.
SOURCE_REF: str = "Glynn and Fox (2019), Table 1, p. 577"

#: USPS state/territory abbreviations mapped to 2-digit Census/BLS FIPS codes.
#: Used to derive principal-state FIPS for canonical CBSA/MSA universe rows.
STATE_ABBREV_TO_FIPS: dict[str, str] = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "PR": "72",
}

#: Historical CBSA identity aliases that must resolve to the canonical metro
#: universe IDs for year-specific source ingests.
#:
#: Each entry is:
#: ``(canonical_cbsa_code, alias_cbsa_code, start_year, end_year, alias_name, note)``.
CBSA_ALIAS_RULES: list[tuple[str, str, int, int, str, str]] = [
    (
        "31080",
        "31100",
        2012,
        2012,
        "Los Angeles-Long Beach-Santa Ana, CA Metro Area",
        "Census ACS1 2012 returns Los Angeles under legacy CBSA 31100; "
        "normalize to canonical CBSA 31080.",
    ),
]

# ---------------------------------------------------------------------------
# Metro definitions truth table
# ---------------------------------------------------------------------------

#: Each entry is (metro_id, metro_name, membership_type).
METRO_DEFINITIONS: list[tuple[str, str, str]] = [
    ("GF01", "New York, NY", "multi_county"),
    ("GF02", "Los Angeles-Long Beach-Anaheim, CA", "multi_coc"),
    ("GF03", "Chicago, IL", "multi_coc"),
    ("GF04", "Dallas-Fort Worth, TX", "single"),
    ("GF05", "Philadelphia, PA", "single"),
    ("GF06", "Houston, TX", "multi_county"),
    ("GF07", "Washington, DC", "single"),
    ("GF08", "Miami-Fort Lauderdale, FL", "single"),
    ("GF09", "Atlanta, GA", "multi_coc"),
    ("GF10", "Boston, MA", "single"),
    ("GF11", "San Francisco, CA", "single"),
    ("GF12", "Detroit, MI", "multi_coc"),
    ("GF13", "Riverside, CA", "single"),
    ("GF14", "Phoenix, AZ", "single"),
    ("GF15", "Seattle, WA", "single"),
    ("GF16", "Minneapolis-St Paul, MN", "single"),
    ("GF17", "San Diego, CA", "single"),
    ("GF18", "St. Louis, MO", "multi_coc"),
    ("GF19", "Tampa, FL", "single"),
    ("GF20", "Baltimore, MD", "multi_coc"),
    ("GF21", "Denver, CO", "multi_county"),
    ("GF22", "Pittsburgh, PA", "single"),
    ("GF23", "Portland, OR", "single"),
    ("GF24", "Charlotte, NC", "single"),
    ("GF25", "Sacramento, CA", "single"),
]

# ---------------------------------------------------------------------------
# Metro-to-CoC membership truth table
# ---------------------------------------------------------------------------

#: Each entry is (metro_id, coc_id).
#: Multiple entries per metro_id indicate PIT must be summed across CoCs.
METRO_COC_MEMBERSHIP: list[tuple[str, str]] = [
    ("GF01", "NY-600"),
    ("GF02", "CA-600"),
    ("GF02", "CA-606"),
    ("GF02", "CA-607"),
    ("GF02", "CA-612"),
    ("GF03", "IL-510"),
    ("GF03", "IL-511"),
    ("GF04", "TX-600"),
    ("GF05", "PA-500"),
    ("GF06", "TX-700"),
    ("GF07", "DC-500"),
    ("GF08", "FL-600"),
    ("GF09", "GA-500"),
    ("GF09", "GA-502"),
    ("GF10", "MA-500"),
    ("GF11", "CA-501"),
    ("GF12", "MI-501"),
    ("GF12", "MI-502"),
    ("GF13", "CA-608"),
    ("GF14", "AZ-502"),
    ("GF15", "WA-500"),
    ("GF16", "MN-500"),
    ("GF17", "CA-601"),
    ("GF18", "MO-500"),
    ("GF18", "MO-501"),
    ("GF19", "FL-501"),
    ("GF20", "MD-501"),
    ("GF20", "MD-505"),
    ("GF21", "CO-503"),
    ("GF22", "PA-600"),
    ("GF23", "OR-501"),
    ("GF24", "NC-505"),
    ("GF25", "CA-503"),
]

# ---------------------------------------------------------------------------
# Metro-to-County membership truth table
# ---------------------------------------------------------------------------

#: Each entry is (metro_id, county_fips).
#: Multiple entries per metro_id indicate population/ZRI must be aggregated
#: across counties (population-weighted).
METRO_COUNTY_MEMBERSHIP: list[tuple[str, str]] = [
    # GF01: New York - 5 NYC boroughs
    ("GF01", "36061"),  # New York (Manhattan)
    ("GF01", "36005"),  # Bronx
    ("GF01", "36081"),  # Queens
    ("GF01", "36047"),  # Kings (Brooklyn)
    ("GF01", "36085"),  # Richmond (Staten Island)
    # GF02: Los Angeles
    ("GF02", "06037"),  # Los Angeles
    # GF03: Chicago
    ("GF03", "17031"),  # Cook
    # GF04: Dallas
    ("GF04", "48113"),  # Dallas
    # GF05: Philadelphia
    ("GF05", "42101"),  # Philadelphia
    # GF06: Houston
    ("GF06", "48201"),  # Harris
    ("GF06", "48157"),  # Fort Bend
    # GF07: Washington DC
    ("GF07", "11001"),  # District of Columbia
    # GF08: Miami-Fort Lauderdale
    ("GF08", "12086"),  # Miami-Dade
    # GF09: Atlanta
    ("GF09", "13121"),  # Fulton
    # GF10: Boston
    ("GF10", "25025"),  # Suffolk
    # GF11: San Francisco
    ("GF11", "06075"),  # San Francisco
    # GF12: Detroit
    ("GF12", "26163"),  # Wayne
    # GF13: Riverside
    ("GF13", "06065"),  # Riverside
    # GF14: Phoenix
    ("GF14", "04013"),  # Maricopa
    # GF15: Seattle
    ("GF15", "53033"),  # King
    # GF16: Minneapolis-St Paul
    ("GF16", "27053"),  # Hennepin
    # GF17: San Diego
    ("GF17", "06073"),  # San Diego
    # GF18: St. Louis
    ("GF18", "29189"),  # St. Louis County
    ("GF18", "29510"),  # St. Louis City (independent city)
    # GF19: Tampa
    ("GF19", "12057"),  # Hillsborough
    # GF20: Baltimore
    ("GF20", "24005"),  # Baltimore County
    ("GF20", "24510"),  # Baltimore City (independent city)
    # GF21: Denver - 7 counties
    ("GF21", "08001"),  # Adams
    ("GF21", "08005"),  # Arapahoe
    ("GF21", "08013"),  # Boulder
    ("GF21", "08014"),  # Broomfield
    ("GF21", "08031"),  # Denver
    ("GF21", "08035"),  # Douglas
    ("GF21", "08059"),  # Jefferson
    # GF22: Pittsburgh
    ("GF22", "42003"),  # Allegheny
    # GF23: Portland
    ("GF23", "41051"),  # Multnomah
    # GF24: Charlotte
    ("GF24", "37119"),  # Mecklenburg
    # GF25: Sacramento
    ("GF25", "06067"),  # Sacramento
]

# ---------------------------------------------------------------------------
# Metro-to-CBSA (Core Based Statistical Area) mapping
# ---------------------------------------------------------------------------

#: Maps metro_id to Census CBSA code for ACS 1-year metro-level fetches.
#: CBSA codes are 5-digit strings matching the Census Bureau's metropolitan
#: statistical area identifiers.
METRO_CBSA_MAPPING: dict[str, str] = {
    "GF01": "35620",  # New York
    "GF02": "31080",  # Los Angeles
    "GF03": "16980",  # Chicago
    "GF04": "19100",  # Dallas-Fort Worth
    "GF05": "37980",  # Philadelphia
    "GF06": "26420",  # Houston
    "GF07": "47900",  # Washington DC
    "GF08": "33100",  # Miami-Fort Lauderdale
    "GF09": "12060",  # Atlanta
    "GF10": "14460",  # Boston
    "GF11": "41860",  # San Francisco
    "GF12": "19820",  # Detroit
    "GF13": "40140",  # Riverside
    "GF14": "38060",  # Phoenix
    "GF15": "42660",  # Seattle
    "GF16": "33460",  # Minneapolis-St Paul
    "GF17": "41740",  # San Diego
    "GF18": "41180",  # St. Louis
    "GF19": "45300",  # Tampa
    "GF20": "12580",  # Baltimore
    "GF21": "19740",  # Denver
    "GF22": "38300",  # Pittsburgh
    "GF23": "38900",  # Portland
    "GF24": "16740",  # Charlotte
    "GF25": "40900",  # Sacramento
}

#: Maps metro_id to the 2-digit FIPS code of the MSA's principal state.
#: For multi-state MSAs BLS uses the state where the principal city is located.
#: These codes are required to construct valid BLS LAUS metro series IDs.
#:
#: Series ID format: LA + U + MT + state_fips(2) + cbsa(5) + 000000 + measure(2)
#: Example: New York (state_fips=36, cbsa=35620) → LAUMT363562000000003
METRO_STATE_FIPS: dict[str, str] = {
    "GF01": "36",  # New York, NY
    "GF02": "06",  # Los Angeles, CA
    "GF03": "17",  # Chicago, IL
    "GF04": "48",  # Dallas-Fort Worth, TX
    "GF05": "42",  # Philadelphia, PA (principal state; also spans NJ/DE/MD)
    "GF06": "48",  # Houston, TX
    "GF07": "11",  # Washington, DC (principal state; also spans VA/MD/WV)
    "GF08": "12",  # Miami-Fort Lauderdale, FL
    "GF09": "13",  # Atlanta, GA
    "GF10": "25",  # Boston, MA (principal state; also spans NH)
    "GF11": "06",  # San Francisco, CA
    "GF12": "26",  # Detroit, MI
    "GF13": "06",  # Riverside, CA
    "GF14": "04",  # Phoenix, AZ
    "GF15": "53",  # Seattle, WA
    "GF16": "27",  # Minneapolis-St Paul, MN (principal state; also spans WI)
    "GF17": "06",  # San Diego, CA
    "GF18": "29",  # St. Louis, MO (principal state; also spans IL)
    "GF19": "12",  # Tampa, FL
    "GF20": "24",  # Baltimore, MD
    "GF21": "08",  # Denver, CO
    "GF22": "42",  # Pittsburgh, PA
    "GF23": "41",  # Portland, OR (principal state; also spans WA)
    "GF24": "37",  # Charlotte, NC (principal state; also spans SC)
    "GF25": "06",  # Sacramento, CA
}

#: Short metro names derived from METRO_DEFINITIONS (for display/DataFrame building).
_CBSA_METRO_NAMES: dict[str, str] = {
    mid: name.split(",")[0] for mid, name, _mtype in METRO_DEFINITIONS
}

#: Reverse lookup: CBSA code → metro_id.
_CBSA_TO_METRO: dict[str, str] = {v: k for k, v in METRO_CBSA_MAPPING.items()}


# ---------------------------------------------------------------------------
# DataFrame builders
# ---------------------------------------------------------------------------


def build_definitions_df() -> pd.DataFrame:
    """Build the metro definitions DataFrame from constants.

    Returns a DataFrame with columns:
    ``metro_id``, ``metro_name``, ``membership_type``,
    ``definition_version``, ``source``, ``source_ref``.
    """
    rows = [
        {
            "metro_id": mid,
            "metro_name": name,
            "membership_type": mtype,
            "definition_version": DEFINITION_VERSION,
            "source": "glynn_fox_2019",
            "source_ref": SOURCE_REF,
        }
        for mid, name, mtype in METRO_DEFINITIONS
    ]
    df = pd.DataFrame(rows)
    df["metro_id"] = df["metro_id"].astype(str)
    return df


def build_coc_membership_df() -> pd.DataFrame:
    """Build the metro-to-CoC membership DataFrame from constants.

    Returns a DataFrame with columns:
    ``metro_id``, ``coc_id``, ``definition_version``.
    """
    rows = [
        {
            "metro_id": mid,
            "coc_id": coc,
            "definition_version": DEFINITION_VERSION,
        }
        for mid, coc in METRO_COC_MEMBERSHIP
    ]
    df = pd.DataFrame(rows)
    df["metro_id"] = df["metro_id"].astype(str)
    df["coc_id"] = df["coc_id"].astype(str)
    return df


def build_county_membership_df() -> pd.DataFrame:
    """Build the metro-to-county membership DataFrame from constants.

    Returns a DataFrame with columns:
    ``metro_id``, ``county_fips``, ``definition_version``.
    """
    rows = [
        {
            "metro_id": mid,
            "county_fips": fips,
            "definition_version": DEFINITION_VERSION,
        }
        for mid, fips in METRO_COUNTY_MEMBERSHIP
    ]
    df = pd.DataFrame(rows)
    df["metro_id"] = df["metro_id"].astype(str)
    df["county_fips"] = df["county_fips"].astype(str)
    return df


def build_cbsa_mapping_df() -> pd.DataFrame:
    """Build the metro-to-CBSA mapping DataFrame from constants.

    Returns a DataFrame with columns:
    ``metro_id``, ``metro_name``, ``cbsa_code``.
    """
    rows = [
        {
            "metro_id": mid,
            "metro_name": _CBSA_METRO_NAMES[mid],
            "cbsa_code": cbsa,
        }
        for mid, cbsa in METRO_CBSA_MAPPING.items()
    ]
    df = pd.DataFrame(rows)
    df["metro_id"] = df["metro_id"].astype(str)
    df["cbsa_code"] = df["cbsa_code"].astype(str)
    return df


def build_cbsa_alias_df() -> pd.DataFrame:
    """Build the historical CBSA alias truth table as a DataFrame."""
    rows = [
        {
            "canonical_cbsa_code": canonical_cbsa_code,
            "alias_cbsa_code": alias_cbsa_code,
            "start_year": start_year,
            "end_year": end_year,
            "canonical_metro_id": _CBSA_TO_METRO.get(canonical_cbsa_code),
            "canonical_metro_name": _CBSA_METRO_NAMES.get(
                _CBSA_TO_METRO.get(canonical_cbsa_code, "")
            ),
            "alias_name": alias_name,
            "note": note,
        }
        for (
            canonical_cbsa_code,
            alias_cbsa_code,
            start_year,
            end_year,
            alias_name,
            note,
        ) in CBSA_ALIAS_RULES
    ]
    return pd.DataFrame(rows)


def build_metro_universe_df(msa_definitions_df: pd.DataFrame) -> pd.DataFrame:
    """Build the canonical metro-universe table from curated MSA definitions."""
    required = {
        "msa_id",
        "cbsa_code",
        "msa_name",
        "area_type",
        "definition_version",
        "source",
        "source_ref",
    }
    missing = sorted(required - set(msa_definitions_df.columns))
    if missing:
        raise ValueError(
            "MSA definitions are missing required columns for metro-universe "
            f"construction: {missing}"
        )

    universe = (
        msa_definitions_df[
            [
                "msa_id",
                "cbsa_code",
                "msa_name",
                "area_type",
                "definition_version",
                "source",
                "source_ref",
            ]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "msa_id": "metro_id",
                "msa_name": "metro_name",
                "definition_version": "source_definition_version",
            }
        )
        .sort_values("metro_id")
        .reset_index(drop=True)
    )
    universe["metro_id"] = universe["metro_id"].astype(str).str.zfill(5)
    universe["cbsa_code"] = universe["cbsa_code"].astype(str).str.zfill(5)
    universe["definition_version"] = CANONICAL_UNIVERSE_DEFINITION_VERSION
    return universe[
        [
            "metro_id",
            "cbsa_code",
            "metro_name",
            "area_type",
            "definition_version",
            "source_definition_version",
            "source",
            "source_ref",
        ]
    ]


def build_glynn_fox_subset_profile_df(msa_definitions_df: pd.DataFrame) -> pd.DataFrame:
    """Build the Glynn/Fox subset-profile table over the canonical universe."""
    universe = build_metro_universe_df(msa_definitions_df)
    universe_lookup = (
        universe[
            [
                "metro_id",
                "cbsa_code",
                "metro_name",
                "definition_version",
            ]
        ]
        .drop_duplicates(subset=["metro_id"])
        .set_index("metro_id")
    )

    rows: list[dict[str, object]] = []
    for rank, (profile_metro_id, profile_metro_name, _membership_type) in enumerate(
        METRO_DEFINITIONS,
        start=1,
    ):
        cbsa_code = METRO_CBSA_MAPPING[profile_metro_id]
        if cbsa_code not in universe_lookup.index:
            raise ValueError(
                "Canonical metro universe does not contain required CBSA code "
                f"{cbsa_code} for profile metro {profile_metro_id}."
            )
        canonical = universe_lookup.loc[cbsa_code]
        rows.append(
            {
                "profile": PROFILE_NAME,
                "profile_definition_version": DEFINITION_VERSION,
                "metro_definition_version": canonical["definition_version"],
                "metro_id": cbsa_code,
                "cbsa_code": canonical["cbsa_code"],
                "metro_name": canonical["metro_name"],
                "profile_metro_id": profile_metro_id,
                "profile_metro_name": profile_metro_name,
                "profile_rank": rank,
                "source": "glynn_fox_2019",
                "source_ref": SOURCE_REF,
            }
        )

    profile_df = pd.DataFrame(rows).sort_values("profile_rank").reset_index(drop=True)
    profile_df["metro_id"] = profile_df["metro_id"].astype(str).str.zfill(5)
    profile_df["cbsa_code"] = profile_df["cbsa_code"].astype(str).str.zfill(5)
    profile_df["profile_metro_id"] = profile_df["profile_metro_id"].astype(str)
    return profile_df


def canonicalize_cbsa_code(cbsa_code: str, *, year: int | None = None) -> str:
    """Resolve a CBSA code to its canonical identity for the requested year."""
    cbsa_code = str(cbsa_code).zfill(5)
    if year is None:
        return cbsa_code
    for (
        canonical_cbsa_code,
        alias_cbsa_code,
        start_year,
        end_year,
        _alias_name,
        _note,
    ) in CBSA_ALIAS_RULES:
        if alias_cbsa_code == cbsa_code and start_year <= year <= end_year:
            return canonical_cbsa_code
    return cbsa_code


def cbsa_to_metro_id(cbsa_code: str, *, year: int | None = None) -> str | None:
    """Look up the metro_id for a given CBSA code.

    Parameters
    ----------
    cbsa_code : str
        5-digit Census CBSA code (e.g., "35620" for New York).

    Returns
    -------
    str or None
        The metro_id (e.g., "GF01") if found, otherwise None.
    """
    return _CBSA_TO_METRO.get(canonicalize_cbsa_code(cbsa_code, year=year))


def metro_name_for_id(metro_id: str) -> str | None:
    """Look up the short metro name for a Glynn/Fox metro_id.

    Parameters
    ----------
    metro_id : str
        Metro identifier (e.g., "GF01").

    Returns
    -------
    str or None
        The metro name (e.g., "New York") if found, otherwise None.
    """
    return _CBSA_METRO_NAMES.get(metro_id)


def metro_state_fips_for_id(metro_id: str) -> str | None:
    """Look up the principal-state 2-digit FIPS code for a Glynn/Fox metro_id.

    Used to build BLS LAUS metro series IDs, which encode both the state FIPS
    and the CBSA code in the area segment of the series ID.

    Parameters
    ----------
    metro_id : str
        Metro identifier (e.g., "GF01").

    Returns
    -------
    str or None
        2-digit state FIPS (e.g., "36" for New York) if found, otherwise None.
    """
    return METRO_STATE_FIPS.get(metro_id)


def principal_state_fips_for_metro_name(metro_name: str) -> str:
    """Derive the principal-state FIPS code from a canonical MSA/CBSA name."""
    if "," not in metro_name:
        raise ValueError(f"Metro name {metro_name!r} does not contain the expected ', ST' suffix.")
    state_block = metro_name.split(",", 1)[1].strip()
    state_abbrev = state_block.split("-", 1)[0].strip()
    try:
        return STATE_ABBREV_TO_FIPS[state_abbrev]
    except KeyError:
        raise ValueError(
            f"Cannot derive principal-state FIPS from metro name {metro_name!r}. "
            f"Unknown state abbreviation {state_abbrev!r}."
        ) from None
