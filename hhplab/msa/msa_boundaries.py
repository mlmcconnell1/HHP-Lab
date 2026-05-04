"""Official Census MSA boundary polygon ingest."""

from __future__ import annotations

import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx

import hhplab.naming as naming
from hhplab.geo.geo_io import write_geoparquet
from hhplab.msa.msa_definitions import DEFINITION_VERSION, SOURCE_REF
from hhplab.msa.msa_io import download_delineation_rows, read_msa_definitions
from hhplab.msa.msa_validate import validate_msa_boundaries
from hhplab.provenance import ProvenanceBlock
from hhplab.raw_snapshot import persist_file_snapshot
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import CENSUS_TIGER_CBSA_TEMPLATE

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
        delineation_df, _, _, _ = download_delineation_rows(raw_root=raw_root)
        from hhplab.msa.msa_definitions import build_definitions_df

        return build_definitions_df(delineation_df)


def download_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    tiger_year: int = 2023,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, Path]:
    """Download official CBSA polygons and filter them to metropolitan areas."""
    url = CENSUS_TIGER_CBSA_TEMPLATE.format(year=tiger_year)
    zip_name = f"tl_{tiger_year}_us_cbsa.zip"
    expected = _load_expected_definitions(definition_version, base_dir, raw_root)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        zip_path = tmp_path / zip_name

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

    if "CBSAFP" not in gdf.columns:
        raise ValueError(
            "CBSA boundary source is missing required column 'CBSAFP'. "
            f"Available columns: {list(gdf.columns)}"
        )

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
                "cbsa_name_source": merged[name_column].astype(str),
                "area_type": merged["area_type"].astype(str),
                "definition_version": merged["definition_version"].astype(str),
                "geometry_vintage": str(tiger_year),
                "source": "census_tiger_cbsa",
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
    tiger_year: int = 2023,
    base_dir: Path | str | None = None,
) -> Path:
    """Write curated MSA boundary polygons with provenance metadata."""
    output_path = naming.msa_boundaries_path(definition_version, base_dir)
    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=definition_version,
        extra={
            "dataset_type": "msa_boundaries",
            "source": "census_tiger_cbsa",
            "source_ref": SOURCE_REF,
            "geometry_vintage": tiger_year,
            "feature_count": len(boundaries),
        },
    )
    write_geoparquet(boundaries, output_path, provenance=provenance)
    return output_path


def ingest_msa_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    tiger_year: int = 2023,
    base_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> Path:
    """Download, validate, write, and register official MSA boundary polygons."""
    expected = _load_expected_definitions(definition_version, base_dir, raw_root)
    boundaries, content_sha256, content_size, raw_path = download_msa_boundaries(
        definition_version,
        tiger_year=tiger_year,
        base_dir=base_dir,
        raw_root=raw_root,
    )
    validation = validate_msa_boundaries(boundaries, expected)
    if not validation.passed:
        raise ValueError(f"MSA boundary validation failed:\n{validation.summary()}")

    output_path = write_msa_boundaries(
        boundaries,
        definition_version=definition_version,
        tiger_year=tiger_year,
        base_dir=base_dir,
    )
    register_source(
        source_type="census_cbsa_boundary",
        source_url=CENSUS_TIGER_CBSA_TEMPLATE.format(year=tiger_year),
        source_name=f"{CBSA_SOURCE_NAME} {tiger_year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "definition_version": definition_version,
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
    return validate_msa_boundaries(boundaries, definitions)
