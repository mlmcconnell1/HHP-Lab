"""Official Census MSA boundary polygon ingest."""

from __future__ import annotations

import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx

import hhplab.artifacts.naming.naming as naming
from hhplab.geographies.coc.coc_io import write_geoparquet
from hhplab.geographies.msa.msa_definitions import (
    DEFINITION_VERSION,
    HISTORICAL_DEFINITION_VERSION,
    build_county_membership_df,
    get_msa_source_spec,
)
from hhplab.geographies.msa.msa_io import (
    download_delineation_rows,
    read_msa_county_membership,
    read_msa_definitions,
)
from hhplab.geographies.msa.msa_validate import validate_msa_boundaries
from hhplab.registry.source_registry import check_source_changed, register_source
from hhplab.storage.provenance import ProvenanceBlock
from hhplab.storage.raw_snapshot import persist_file_snapshot

logger = logging.getLogger(__name__)

CBSA_SOURCE_NAME = "Census TIGER/Line CBSA Boundaries"


def _resolve_name_column(gdf: gpd.GeoDataFrame) -> str:
    for column in ("NAME", "NAMELSAD", "CBSA_NAME"):
        if column in gdf.columns:
            return column
    raise ValueError(
        f"CBSA shapefile is missing a usable name column. Available columns: {list(gdf.columns)}"
    )


def _load_expected_definitions(
    definition_version: str,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
):
    try:
        return read_msa_definitions(definition_version, base_dir)
    except FileNotFoundError:
        delineation_df, _, _, _ = download_delineation_rows(
            definition_version=definition_version,
            raw_root=raw_root,
        )
        from hhplab.geographies.msa.msa_definitions import build_definitions_df

        return build_definitions_df(delineation_df, definition_version)


def _load_expected_membership(
    definition_version: str,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
):
    try:
        return read_msa_county_membership(definition_version, base_dir)
    except FileNotFoundError:
        delineation_df, _, _, _ = download_delineation_rows(
            definition_version=definition_version,
            raw_root=raw_root,
        )
        return build_county_membership_df(delineation_df, definition_version)


def download_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    tiger_year: int | None = None,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
    source_file: Path | str | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, Path]:
    """Download geometry aligned to the selected MSA definition vintage."""
    spec = get_msa_source_spec(definition_version)
    tiger_year = tiger_year if tiger_year is not None else spec.geometry_vintage
    if tiger_year != spec.geometry_vintage:
        raise ValueError(
            f"Definition {definition_version} uses county geometry vintage "
            f"{spec.geometry_vintage}; got --year {tiger_year}."
        )
    historical_counties = definition_version == HISTORICAL_DEFINITION_VERSION
    url = spec.geometry_url
    zip_name = (
        f"tl_{tiger_year}_us_county10.zip"
        if historical_counties
        else f"tl_{tiger_year}_us_cbsa.zip"
    )
    expected = _load_expected_definitions(definition_version, base_dir, raw_root)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        zip_path = tmp_path / zip_name

        if source_file is not None:
            raw_content = Path(source_file).read_bytes()
        else:
            with httpx.Client(timeout=300.0) as client:
                response = client.get(url, follow_redirects=True)
                response.raise_for_status()
                raw_content = response.content
        zip_path.write_bytes(raw_content)

        raw_path, content_sha256, content_size = persist_file_snapshot(
            raw_content,
            "census_cbsa_boundary",
            zip_name,
            subdirs=(str(tiger_year),),
            raw_root=raw_root,
        )

        changed, details = check_source_changed(
            source_type="census_cbsa_boundary",
            source_url=url,
            current_sha256=content_sha256,
        )
        if changed:
            logger.warning(
                "UPSTREAM DATA CHANGED: CBSA boundary polygons for %s changed since last ingest. "
                "Previous hash: %s... Current hash: %s... Last ingested: %s",
                tiger_year,
                details["previous_sha256"][:16],
                content_sha256[:16],
                details["previous_ingested_at"],
            )

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_path)

        shp_files = list(tmp_path.glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No shapefile found in {url}")
        gdf = gpd.read_file(shp_files[0])

    if gdf.crs is None:
        raise ValueError("CBSA boundary source has no CRS; cannot safely ingest.")
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    if historical_counties:
        if not {"GEOID10", "CBSAFP10"}.issubset(gdf.columns):
            raise ValueError(
                "2010 TIGER county source is missing required columns 'GEOID10' and "
                f"'CBSAFP10'. Available columns: {list(gdf.columns)}"
            )
        working = gdf.copy()
        working["county_fips"] = working["GEOID10"].astype(str).str.zfill(5)
        working["cbsa_code"] = working["CBSAFP10"].astype(str).str.zfill(5)
        membership = _load_expected_membership(definition_version, base_dir, raw_root)
        expected_pairs = set(
            zip(
                membership["county_fips"].astype(str),
                membership["cbsa_code"].astype(str),
                strict=True,
            )
        )
        observed_pairs = set(
            zip(working["county_fips"], working["cbsa_code"], strict=True)
        )
        expected_fips = set(membership["county_fips"].astype(str))
        observed_msa_pairs = {
            pair for pair in observed_pairs if pair[0] in expected_fips
        }
        if observed_msa_pairs != expected_pairs:
            missing_pairs = sorted(expected_pairs - observed_msa_pairs)
            extra_pairs = sorted(observed_msa_pairs - expected_pairs)
            raise ValueError(
                "2010 TIGER county assignments do not exactly match historical MSA "
                f"membership; missing pairs: {missing_pairs[:10]}, "
                f"extra pairs: {extra_pairs[:10]}"
            )
        working = working[working["county_fips"].isin(membership["county_fips"])].copy()
        working = working[working["cbsa_code"].isin(expected["msa_id"])].copy()
        grouped = working.dissolve(by="cbsa_code", as_index=False)
        working = grouped
        name_column = "cbsa_code"
    elif "CBSAFP" not in gdf.columns:
        raise ValueError(
            "CBSA boundary source is missing required column 'CBSAFP'. "
            f"Available columns: {list(gdf.columns)}"
        )

    else:
        name_column = _resolve_name_column(gdf)
        working = gdf.copy()
        working["cbsa_code"] = working["CBSAFP"].astype(str).str.zfill(5)
        working = working[working["cbsa_code"].isin(expected["msa_id"])].copy()
    if working.empty:
        raise ValueError(
            "No metropolitan polygons matched the expected MSA definitions. "
            "Run 'hhplab generate msa --definition-version "
            f"{definition_version}' to inspect the current MSA ids."
        )

    merged = working.merge(
        expected[["msa_id", "cbsa_code", "msa_name", "area_type", "definition_version"]],
        on="cbsa_code",
        how="inner",
        validate="one_to_one",
    )
    ingested_at = datetime.now(UTC)
    boundaries = (
        gpd.GeoDataFrame(
            {
                "msa_id": merged["msa_id"].astype(str),
                "cbsa_code": merged["cbsa_code"].astype(str),
                "msa_name": merged["msa_name"].astype(str),
                "cbsa_name_source": (
                    "Dissolved 2010 TIGER county components"
                    if historical_counties
                    else merged[name_column].astype(str)
                ),
                "area_type": merged["area_type"].astype(str),
                "definition_version": merged["definition_version"].astype(str),
                "geometry_vintage": str(tiger_year),
                "source": spec.geometry_source,
                "source_ref": url,
                "ingested_at": ingested_at,
                "geometry": merged.geometry,
            },
            crs="EPSG:4326",
        )
        .sort_values("msa_id")
        .reset_index(drop=True)
    )
    return boundaries, content_sha256, content_size, raw_path


def write_msa_boundaries(
    boundaries: gpd.GeoDataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    tiger_year: int | None = None,
    base_dir: Path | str | None = None,
    raw_sha256: str | None = None,
    raw_file_size: int | None = None,
    raw_snapshot_path: Path | str | None = None,
) -> Path:
    """Write curated MSA boundary polygons with provenance metadata."""
    spec = get_msa_source_spec(definition_version)
    tiger_year = tiger_year if tiger_year is not None else spec.geometry_vintage
    output_path = naming.msa_boundaries_path(definition_version, base_dir)
    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=definition_version,
        extra={
            "dataset_type": "msa_boundaries",
            "source": spec.geometry_source,
            "source_ref": spec.geometry_url,
            "definition_source_ref": spec.source_ref,
            "definition_source_release_date": spec.source_release_date,
            "source_url": spec.geometry_url,
            "raw_sha256": raw_sha256,
            "raw_file_size": raw_file_size,
            "raw_snapshot_path": str(raw_snapshot_path) if raw_snapshot_path else None,
            "geometry_vintage": tiger_year,
            "feature_count": len(boundaries),
        },
    )
    write_geoparquet(boundaries, output_path, provenance=provenance)
    return output_path


def ingest_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    tiger_year: int | None = None,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
    source_file: Path | str | None = None,
) -> Path:
    """Download, validate, write, and register official MSA boundary polygons."""
    spec = get_msa_source_spec(definition_version)
    tiger_year = tiger_year if tiger_year is not None else spec.geometry_vintage
    expected = _load_expected_definitions(definition_version, base_dir, raw_root)
    boundaries, content_sha256, content_size, raw_path = download_msa_boundaries(
        definition_version,
        tiger_year=tiger_year,
        base_dir=base_dir,
        raw_root=raw_root,
        source_file=source_file,
    )
    validation = validate_msa_boundaries(boundaries, expected, definition_version)
    if not validation.passed:
        raise ValueError(f"MSA boundary validation failed:\n{validation.summary()}")

    output_path = write_msa_boundaries(
        boundaries,
        definition_version=definition_version,
        tiger_year=tiger_year,
        base_dir=base_dir,
        raw_sha256=content_sha256,
        raw_file_size=content_size,
        raw_snapshot_path=raw_path,
    )
    register_source(
        source_type="census_cbsa_boundary",
        source_url=spec.geometry_url,
        source_name=f"{CBSA_SOURCE_NAME} {tiger_year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "definition_version": definition_version,
            "definition_source_release_date": spec.source_release_date,
            "geometry_vintage": tiger_year,
            "feature_count": len(boundaries),
            "curated_path": str(output_path),
        },
    )
    return output_path


def read_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> gpd.GeoDataFrame:
    """Read curated MSA boundary polygons."""
    path = naming.msa_boundaries_path(definition_version, base_dir)
    try:
        return gpd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"MSA boundaries artifact not found at {path}. "
            f"Run: hhplab ingest msa-boundaries --definition-version {definition_version}"
        ) from None


def validate_curated_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
):
    """Load curated MSA boundaries and validate them against definitions."""
    definitions = read_msa_definitions(definition_version, base_dir)
    boundaries = read_msa_boundaries(definition_version, base_dir)
    return validate_msa_boundaries(boundaries, definitions, definition_version)
