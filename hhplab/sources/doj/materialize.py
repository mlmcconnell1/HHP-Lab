"""DOJ sanctuary MSA panel-covariate materialization and persistence."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import hhplab.artifacts.naming.naming as naming
from hhplab.geographies.msa import DEFINITION_VERSION as MSA_DEFINITION_VERSION
from hhplab.geographies.msa import read_msa_county_membership, read_msa_definitions
from hhplab.sources.census.pep.pep_aggregate import load_pep_county
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

from .acquisition import DOJ_SANCTUARY_SOURCE_DATE, DOJ_SANCTUARY_URL, download_doj_sanctuary_page
from .contracts import (
    DOJ_LISTED_CITIES,
    SANCTUARY_MSA_MATCH_COLUMNS,
    SANCTUARY_MSA_PANEL_COLUMNS,
)
from .matching import _build_county_exposure, build_sanctuary_msa_matches


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
