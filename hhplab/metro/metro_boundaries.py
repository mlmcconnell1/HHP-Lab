"""Materialized boundary polygons for Glynn/Fox metros."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

import hhplab.naming as naming
from hhplab.geo.geo_io import write_geoparquet
from hhplab.metro.metro_definitions import (
    DEFINITION_VERSION,
    SOURCE_REF,
    build_county_membership_df,
    build_definitions_df,
)
from hhplab.metro.metro_io import read_metro_county_membership, read_metro_definitions
from hhplab.metro.metro_validate import validate_metro_boundaries
from hhplab.provenance import ProvenanceBlock

DERIVATION_SOURCE = "derived_metro_county_union"


def _standardize_county_geometry_columns(county_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "GEOID" in county_gdf.columns:
        result = county_gdf.copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    if "geoid" in county_gdf.columns:
        result = county_gdf.rename(columns={"geoid": "GEOID"}).copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    if "county_fips" in county_gdf.columns:
        result = county_gdf.rename(columns={"county_fips": "GEOID"}).copy()
        result["county_fips"] = result["GEOID"].astype(str)
        return result
    raise ValueError("County geometry artifact must include GEOID, geoid, or county_fips.")


def _read_counties(
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> gpd.GeoDataFrame:
    path = naming.county_path(county_vintage, base_dir)
    try:
        county_gdf = gpd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"County geometry artifact not found at {path}. "
            f"Run: hhplab ingest tiger --year {county_vintage} --type counties"
        ) from None
    return _standardize_county_geometry_columns(county_gdf)


def _load_expected_artifacts(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        definitions = read_metro_definitions(definition_version, base_dir)
    except FileNotFoundError:
        definitions = build_definitions_df()
    try:
        membership = read_metro_county_membership(definition_version, base_dir)
    except FileNotFoundError:
        membership = build_county_membership_df()
    return definitions, membership


def _validate_membership_counties(
    membership: pd.DataFrame,
    counties: gpd.GeoDataFrame,
    *,
    county_vintage: str | int,
) -> None:
    available = set(counties["county_fips"].astype(str))
    required = set(membership["county_fips"].astype(str))
    missing = sorted(required - available)
    if not missing:
        return
    preview = ", ".join(missing[:5])
    if len(missing) > 5:
        preview += ", ..."
    raise ValueError(
        f"Metro membership references counties missing from county geometry vintage "
        f"{county_vintage}: {preview}. "
        f"Run: hhplab ingest tiger --year {county_vintage} --type counties"
    )


def build_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> gpd.GeoDataFrame:
    """Build metro polygons by dissolving member county geometries."""
    definitions, membership = _load_expected_artifacts(definition_version, base_dir)
    counties = _read_counties(county_vintage, base_dir)
    _validate_membership_counties(
        membership,
        counties,
        county_vintage=county_vintage,
    )

    joined = counties.merge(membership, on="county_fips", how="inner")
    if joined.empty:
        raise ValueError(
            "Metro county membership did not match any county geometries. "
            f"Check county vintage {county_vintage} and metro definition version "
            f"{definition_version}."
        )

    dissolved = joined.dissolve(by="metro_id", as_index=False, aggfunc="first")
    dedup_defs = definitions[["metro_id", "metro_name", "definition_version"]].drop_duplicates(
        subset=["metro_id"]
    )
    dissolved = dissolved.drop(
        columns=[col for col in ["metro_name", "definition_version"] if col in dissolved.columns]
    )
    result = dissolved.merge(dedup_defs, on="metro_id", how="left")
    result["geometry_vintage"] = str(county_vintage)
    result["source"] = DERIVATION_SOURCE
    result["source_ref"] = SOURCE_REF
    result["ingested_at"] = datetime.now(UTC)
    return gpd.GeoDataFrame(
        result[
            [
                "metro_id",
                "metro_name",
                "definition_version",
                "geometry_vintage",
                "source",
                "source_ref",
                "ingested_at",
                "geometry",
            ]
        ]
        .sort_values("metro_id")
        .reset_index(drop=True),
        geometry="geometry",
        crs=counties.crs,
    )


def write_metro_boundaries(
    boundaries: gpd.GeoDataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Write materialized metro boundary polygons with provenance."""
    output_path = naming.metro_boundaries_path(definition_version, county_vintage, base_dir)
    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=definition_version,
        county_vintage=str(county_vintage),
        extra={
            "dataset_type": "metro_boundaries",
            "source": DERIVATION_SOURCE,
            "source_ref": SOURCE_REF,
            "feature_count": len(boundaries),
        },
    )
    write_geoparquet(boundaries, output_path, provenance=provenance)
    return output_path


def generate_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Build, validate, and write materialized metro boundary polygons."""
    definitions, _ = _load_expected_artifacts(definition_version, base_dir)
    boundaries = build_metro_boundaries(
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )
    validation = validate_metro_boundaries(
        boundaries,
        definitions,
        county_vintage=county_vintage,
    )
    if not validation.passed:
        raise ValueError(f"Metro boundary validation failed:\n{validation.summary()}")
    return write_metro_boundaries(
        boundaries,
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )


def read_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    county_vintage: str | int = 2020,
    base_dir: Path | str | None = None,
) -> gpd.GeoDataFrame:
    """Read materialized metro boundary polygons."""
    path = naming.metro_boundaries_path(definition_version, county_vintage, base_dir)
    try:
        return gpd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Metro boundaries artifact not found at {path}. "
            "Run: hhplab generate metro-boundaries "
            f"--definition-version {definition_version} --counties {county_vintage}"
        ) from None


def validate_curated_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
):
    """Load curated metro boundaries and validate them against definitions."""
    definitions, _ = _load_expected_artifacts(definition_version, base_dir)
    boundaries = read_metro_boundaries(
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )
    return validate_metro_boundaries(
        boundaries,
        definitions,
        county_vintage=county_vintage,
    )
