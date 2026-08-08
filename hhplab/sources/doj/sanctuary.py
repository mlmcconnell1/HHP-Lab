"""DOJ sanctuary jurisdiction to Census MSA matching utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import httpx
import pandas as pd

import hhplab.artifacts.naming.naming as naming
from hhplab.geographies.msa import DEFINITION_VERSION as MSA_DEFINITION_VERSION
from hhplab.geographies.msa import read_msa_county_membership, read_msa_definitions
from hhplab.sources.census.pep.pep_aggregate import load_pep_county
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.storage.raw_snapshot import persist_file_snapshot

DOJ_SANCTUARY_URL = (
    "https://www.justice.gov/opa/pr/"
    "justice-department-publishes-list-sanctuary-jurisdictions"
)
DOJ_SANCTUARY_SOURCE_DATE = "2025-08-05"
RAW_SANCTUARY_HTML_FILENAME = "doj_sanctuary_jurisdictions_2025-08-05.html"

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


def download_doj_sanctuary_page(raw_root: Path | None = None) -> tuple[Path, str, int]:
    """Download and persist the DOJ sanctuary jurisdiction press release HTML."""
    with httpx.Client(timeout=120.0) as client:
        response = client.get(DOJ_SANCTUARY_URL, follow_redirects=True)
        response.raise_for_status()
        raw_content = response.content

    return persist_file_snapshot(
        raw_content,
        "sanctuary",
        RAW_SANCTUARY_HTML_FILENAME,
        raw_root=raw_root,
    )


def _join_labels(values: list[str] | pd.Series) -> str:
    labels = sorted({str(value) for value in values if pd.notna(value) and str(value)})
    return "; ".join(labels)


def _build_county_designation_lookup() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "county_fips": [item.county_fips for item in DOJ_LISTED_COUNTIES],
            "county_label": [item.label for item in DOJ_LISTED_COUNTIES],
        }
    )


def _build_city_designation_lookup() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for item in DOJ_LISTED_CITIES:
        for county_fips in item.county_fips:
            rows.append(
                {
                    "county_fips": county_fips,
                    "city_label": item.label,
                    "city_match_note": item.match_note,
                }
            )
    return pd.DataFrame(rows)


def _build_county_exposure(
    msa_county_membership: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    population_year: int,
) -> pd.DataFrame:
    """Return one row per MSA with population-weighted sanctuary exposure."""
    required_membership = {"msa_id", "county_fips", "state_name"}
    missing_membership = sorted(required_membership - set(msa_county_membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership missing required column(s): "
            f"{', '.join(missing_membership)}."
        )

    required_population = {"county_fips", "year", "population"}
    missing_population = sorted(required_population - set(county_population.columns))
    if missing_population:
        raise ValueError(
            "County population data missing required column(s): "
            f"{', '.join(missing_population)}."
        )

    membership = msa_county_membership.loc[:, ["msa_id", "county_fips", "state_name"]].copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    membership["state_name"] = membership["state_name"].astype(str)

    population = county_population.loc[
        county_population["year"].astype("int64") == population_year,
        ["county_fips", "population"],
    ].copy()
    population["county_fips"] = population["county_fips"].astype(str).str.zfill(5)
    population["population"] = pd.to_numeric(population["population"], errors="coerce")
    population = population.dropna(subset=["population"])

    county_designations = set(_build_county_designation_lookup()["county_fips"].tolist())
    city_designations = set(_build_city_designation_lookup()["county_fips"].tolist())
    membership["state_sanctuary_county"] = membership["state_name"].isin(DOJ_LISTED_STATES)
    membership["county_sanctuary_county"] = membership["county_fips"].isin(county_designations)
    membership["city_sanctuary_county"] = membership["county_fips"].isin(city_designations)
    membership["sanctuary_county"] = membership[
        ["state_sanctuary_county", "county_sanctuary_county", "city_sanctuary_county"]
    ].any(axis=1)

    weighted = membership.merge(population, on="county_fips", how="left")
    weighted["population"] = weighted["population"].fillna(0.0)
    weighted["sanctuary_population"] = weighted["population"].where(
        weighted["sanctuary_county"],
        0.0,
    )
    exposure = (
        weighted.groupby("msa_id", as_index=False)
        .agg(
            doj_sanctuary_population=("sanctuary_population", "sum"),
            doj_sanctuary_population_denominator=("population", "sum"),
        )
        .sort_values("msa_id")
    )
    denominator = exposure["doj_sanctuary_population_denominator"]
    exposure["doj_sanctuary_population_share"] = (
        exposure["doj_sanctuary_population"].where(denominator > 0, 0.0)
        / denominator.where(denominator > 0, 1.0)
    )
    exposure["doj_sanctuary_population_year"] = population_year
    return exposure


def build_sanctuary_msa_matches(
    msa_definitions: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
) -> pd.DataFrame:
    """Build conservative DOJ sanctuary jurisdiction matches at MSA grain."""
    required_definitions = {"msa_id", "cbsa_code", "msa_name"}
    missing_definitions = sorted(required_definitions - set(msa_definitions.columns))
    if missing_definitions:
        raise ValueError(
            "MSA definitions missing required column(s): "
            f"{', '.join(missing_definitions)}."
        )

    required_membership = {"msa_id", "county_fips", "state_name"}
    missing_membership = sorted(required_membership - set(msa_county_membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership missing required column(s): "
            f"{', '.join(missing_membership)}."
        )

    definitions = msa_definitions[["msa_id", "cbsa_code", "msa_name"]].drop_duplicates().copy()
    definitions["msa_id"] = definitions["msa_id"].astype(str).str.zfill(5)
    definitions["cbsa_code"] = definitions["cbsa_code"].astype(str).str.zfill(5)

    membership = msa_county_membership.copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    membership["state_name"] = membership["state_name"].astype(str)

    state_rows = membership.loc[membership["state_name"].isin(DOJ_LISTED_STATES)].copy()
    state_matches = (
        state_rows.groupby("msa_id", as_index=False)["state_name"]
        .agg(_join_labels)
        .rename(columns={"state_name": "matched_states"})
    )

    county_lookup = _build_county_designation_lookup()
    county_rows = membership.merge(county_lookup, on="county_fips", how="inner")
    county_matches = (
        county_rows.groupby("msa_id", as_index=False)["county_label"]
        .agg(_join_labels)
        .rename(columns={"county_label": "matched_counties"})
    )

    city_lookup = _build_city_designation_lookup()
    city_rows = membership.merge(city_lookup, on="county_fips", how="inner")
    city_matches = (
        city_rows.groupby("msa_id", as_index=False)["city_label"]
        .agg(_join_labels)
        .rename(columns={"city_label": "matched_cities"})
    )

    result = (
        definitions.merge(state_matches, on="msa_id", how="left")
        .merge(county_matches, on="msa_id", how="left")
        .merge(city_matches, on="msa_id", how="left")
    )
    for column in ("matched_states", "matched_counties", "matched_cities"):
        result[column] = result[column].fillna("")

    result["state_match"] = result["matched_states"] != ""
    result["county_match"] = result["matched_counties"] != ""
    result["city_match"] = result["matched_cities"] != ""
    result = result[result[["state_match", "county_match", "city_match"]].any(axis=1)].copy()

    def match_basis(row: pd.Series) -> str:
        basis = []
        if bool(row["state_match"]):
            basis.append("state")
        if bool(row["county_match"]):
            basis.append("county")
        if bool(row["city_match"]):
            basis.append("city")
        return "+".join(basis)

    result["match_basis"] = result.apply(match_basis, axis=1)
    return result.loc[:, SANCTUARY_MSA_MATCH_COLUMNS].sort_values("cbsa_code").reset_index(
        drop=True
    )


def build_sanctuary_msa_panel_covariate(
    msa_definitions: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
    county_population: pd.DataFrame,
    sanctuary_msa_matches: pd.DataFrame,
    *,
    source_date: str = DOJ_SANCTUARY_SOURCE_DATE,
    population_year: int = 2020,
) -> pd.DataFrame:
    """Build a one-row-per-MSA panel covariate from DOJ sanctuary matches."""
    required_definitions = {"msa_id", "cbsa_code", "msa_name"}
    missing_definitions = sorted(required_definitions - set(msa_definitions.columns))
    if missing_definitions:
        raise ValueError(
            "MSA definitions missing required column(s): "
            f"{', '.join(missing_definitions)}."
        )
    missing_matches = sorted(set(SANCTUARY_MSA_MATCH_COLUMNS) - set(sanctuary_msa_matches.columns))
    if missing_matches:
        raise ValueError(
            "Sanctuary MSA matches missing required column(s): "
            f"{', '.join(missing_matches)}."
        )

    definitions = msa_definitions[["msa_id", "cbsa_code", "msa_name"]].drop_duplicates().copy()
    definitions["msa_id"] = definitions["msa_id"].astype(str).str.zfill(5)
    definitions["cbsa_code"] = definitions["cbsa_code"].astype(str).str.zfill(5)
    matches = sanctuary_msa_matches.loc[:, SANCTUARY_MSA_MATCH_COLUMNS].copy()
    matches["cbsa_code"] = matches["cbsa_code"].astype(str).str.zfill(5)
    exposure = _build_county_exposure(
        msa_county_membership,
        county_population,
        population_year=population_year,
    )

    result = definitions.merge(
        matches.drop(columns=["msa_name"]),
        on="cbsa_code",
        how="left",
    ).merge(exposure, on="msa_id", how="left")
    for column in ("state_match", "county_match", "city_match"):
        result[column] = result[column].map(
            lambda value: bool(value) if pd.notna(value) else False
        )
    for column in ("matched_states", "matched_counties", "matched_cities", "match_basis"):
        result[column] = result[column].fillna("")
    result["doj_sanctuary_msa"] = (
        result[["state_match", "county_match", "city_match"]].any(axis=1).astype("int64")
    )
    for column in ("doj_sanctuary_population", "doj_sanctuary_population_denominator"):
        result[column] = result[column].fillna(0.0)
    result["doj_sanctuary_population_share"] = (
        result["doj_sanctuary_population_share"].fillna(0.0)
    )
    result["doj_sanctuary_population_year"] = population_year
    result["doj_sanctuary_source_date"] = source_date
    return result.loc[:, SANCTUARY_MSA_PANEL_COLUMNS].sort_values("msa_id").reset_index(drop=True)


def write_sanctuary_msa_matches(
    *,
    msa_definition_version: str = MSA_DEFINITION_VERSION,
    source_date: str = DOJ_SANCTUARY_SOURCE_DATE,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
    download_raw: bool = True,
) -> tuple[pd.DataFrame, Path, Path | None]:
    """Build and persist the DOJ sanctuary jurisdiction MSA regression file."""
    raw_path: Path | None = None
    if download_raw:
        raw_path, _sha256, _size = download_doj_sanctuary_page(raw_root=raw_root)

    definitions = read_msa_definitions(msa_definition_version, base_dir=base_dir)
    membership = read_msa_county_membership(msa_definition_version, base_dir=base_dir)
    matches = build_sanctuary_msa_matches(definitions, membership)

    output_path = naming.sanctuary_msa_matches_path(
        source_date,
        msa_definition_version,
        base_dir=base_dir,
    )
    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=msa_definition_version,
        extra={
            "dataset_type": "sanctuary_msa_matches",
            "source": "doj_sanctuary_jurisdictions_press_release",
            "source_ref": DOJ_SANCTUARY_URL,
            "source_date": source_date,
            "raw_path": str(raw_path) if raw_path is not None else None,
            "methodology": (
                "State designations match every MSA with at least one component county "
                "in the listed state; county designations match MSAs containing that "
                "county; city designations use explicit city-to-county mappings before "
                "joining to MSA county membership."
            ),
            "city_county_mapping": [
                {
                    "city": item.label,
                    "county_fips": list(item.county_fips),
                    "note": item.match_note,
                }
                for item in DOJ_LISTED_CITIES
            ],
        },
    )
    write_parquet_with_provenance(matches, output_path, provenance)
    return matches, output_path, raw_path


def write_sanctuary_msa_panel_covariate(
    *,
    msa_definition_version: str = MSA_DEFINITION_VERSION,
    source_date: str = DOJ_SANCTUARY_SOURCE_DATE,
    base_dir: Path | str | None = None,
) -> tuple[pd.DataFrame, Path]:
    """Build and persist a panel-ready DOJ sanctuary MSA covariate artifact."""
    definitions = read_msa_definitions(msa_definition_version, base_dir=base_dir)
    membership = read_msa_county_membership(msa_definition_version, base_dir=base_dir)
    resolved_base_dir = Path("data") if base_dir is None else Path(base_dir)
    county_population_path = resolved_base_dir / "curated" / "pep"
    county_population = load_pep_county(pep_dir=county_population_path)
    matches = build_sanctuary_msa_matches(definitions, membership)
    covariate = build_sanctuary_msa_panel_covariate(
        definitions,
        membership,
        county_population,
        matches,
        source_date=source_date,
    )
    output_path = naming.sanctuary_msa_panel_covariate_path(
        source_date,
        msa_definition_version,
        base_dir=base_dir,
    )
    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=msa_definition_version,
        extra={
            "dataset_type": "sanctuary_msa_panel_covariate",
            "source": "doj_sanctuary_jurisdictions_press_release",
            "source_ref": DOJ_SANCTUARY_URL,
            "source_date": source_date,
            "row_grain": "msa_id",
            "indicator_column": "doj_sanctuary_msa",
            "intensity_column": "doj_sanctuary_population_share",
            "intensity_population_year": 2020,
            "county_population_path": str(county_population_path),
            "match_basis_column": "match_basis",
            "methodology": (
                "Panel covariate expands the conservative match-only sanctuary MSA "
                "artifact to every MSA in the definition file; matched MSAs receive "
                "doj_sanctuary_msa=1 and all other MSAs receive 0. Continuous "
                "intensity is the share of reference-year MSA county population in "
                "counties covered by a DOJ-listed state, county, or conservatively "
                "mapped city designation."
            ),
        },
    )
    write_parquet_with_provenance(covariate, output_path, provenance)
    return covariate, output_path
