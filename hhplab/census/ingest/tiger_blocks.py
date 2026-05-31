"""TIGER/Line tabulation block geometry ingestion."""

from __future__ import annotations

import hashlib
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
import pyarrow.parquet as pq

import hhplab.naming as naming
from hhplab.census.ingest.decennial_tract_population import STATE_FIPS_CODES
from hhplab.paths import curated_dir
from hhplab.provenance import PROVENANCE_KEY, ProvenanceBlock
from hhplab.raw_snapshot import persist_file_snapshot
from hhplab.schema.columns import BLOCK_GEOMETRY_COLUMNS
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import CENSUS_TIGER_BASE

logger = logging.getLogger(__name__)

SUPPORTED_BLOCK_VINTAGES: tuple[int, ...] = (2020,)


def _block_zip_name(year: int, state_fips: str) -> str:
    """Return the Census ZIP filename for one state's tabulation blocks."""
    if year == 2020:
        return f"tl_{year}_{state_fips}_tabblock20.zip"
    supported = ", ".join(str(vintage) for vintage in SUPPORTED_BLOCK_VINTAGES)
    raise ValueError(
        f"Unsupported block geometry vintage {year!r}. Supported vintages: {supported}."
    )


def _block_url(year: int, state_fips: str) -> str:
    """Return the Census download URL for one state's tabulation blocks."""
    zip_name = _block_zip_name(year, state_fips)
    return f"{CENSUS_TIGER_BASE.format(year=year, layer='TABBLOCK20')}{zip_name}"


def _resolve_block_column(
    gdf: gpd.GeoDataFrame,
    candidates: tuple[str, ...],
    label: str,
) -> str:
    for column in candidates:
        if column in gdf.columns:
            return column
    raise ValueError(f"Could not find {label} column. Available columns: {list(gdf.columns)}")


def normalize_block_geometry(gdf: gpd.GeoDataFrame, year: int) -> gpd.GeoDataFrame:
    """Normalize Census tabulation block geometry to the canonical schema."""
    _block_zip_name(year, "01")
    if gdf.crs is None:
        raise ValueError(
            "Source block GeoDataFrame has no CRS; cannot safely assume EPSG:4326."
        )
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    suffix = str(year)[-2:]
    geoid_column = _resolve_block_column(gdf, (f"GEOID{suffix}", "GEOID"), "block GEOID")
    state_column = _resolve_block_column(gdf, (f"STATEFP{suffix}", "STATEFP"), "state FIPS")
    county_column = _resolve_block_column(gdf, (f"COUNTYFP{suffix}", "COUNTYFP"), "county FIPS")
    tract_column = _resolve_block_column(gdf, (f"TRACTCE{suffix}", "TRACTCE"), "tract code")
    ingested_at = datetime.now(UTC).isoformat()

    state = gdf[state_column].astype(str).str.zfill(2)
    county = gdf[county_column].astype(str).str.zfill(3)
    tract = gdf[tract_column].astype(str).str.zfill(6)
    normalized = gpd.GeoDataFrame(
        {
            "block_geoid": gdf[geoid_column].astype(str).str.zfill(15),
            "state_fips": state,
            "county_fips": state + county,
            "tract_geoid": state + county + tract,
            "block_vintage": year,
            "data_source": "census_tiger_tabblock",
            "source_ref": CENSUS_TIGER_BASE.format(year=year, layer="TABBLOCK20"),
            "ingested_at": ingested_at,
            "geometry": gdf.geometry,
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    return normalized[list(BLOCK_GEOMETRY_COLUMNS)]


def _download_state_blocks(
    client: httpx.Client,
    year: int,
    state_fips: str,
    tmpdir: Path,
) -> tuple[gpd.GeoDataFrame | None, bytes | None, Path | None]:
    zip_name = _block_zip_name(year, state_fips)
    url = _block_url(year, state_fips)
    zip_path = tmpdir / zip_name
    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None, None, None
        raise

    raw_content = response.content
    zip_path.write_bytes(raw_content)
    extract_dir = tmpdir / state_fips
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        return None, None, None
    return gpd.read_file(shp_files[0]), raw_content, zip_path


def download_block_geometry(
    year: int = 2020,
    *,
    state_fips_codes: tuple[str, ...] = STATE_FIPS_CODES,
    raw_root: Path | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, list[Path], list[str]]:
    """Download and normalize Census tabulation block geometry."""
    gdfs: list[gpd.GeoDataFrame] = []
    all_content: list[bytes] = []
    raw_paths: list[Path] = []
    missing_state_fips: list[str] = []
    total_size = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        with httpx.Client(timeout=300.0) as client:
            for state_fips in state_fips_codes:
                gdf, raw_content, _zip_path = _download_state_blocks(
                    client,
                    year,
                    state_fips,
                    tmppath,
                )
                if gdf is None or raw_content is None:
                    missing_state_fips.append(state_fips)
                    continue

                gdfs.append(gdf)
                all_content.append(raw_content)
                total_size += len(raw_content)
                raw_path, _, _ = persist_file_snapshot(
                    raw_content,
                    "tiger",
                    _block_zip_name(year, state_fips),
                    subdirs=(str(year), "blocks"),
                    raw_root=raw_root,
                )
                raw_paths.append(raw_path)

    if not gdfs:
        raise ValueError(
            f"No Census tabulation block geometry rows fetched for {year}. "
            "Verify TIGER availability and requested state FIPS codes."
        )

    combined = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        geometry="geometry",
        crs=gdfs[0].crs,
    )
    content_sha256 = hashlib.sha256(b"\n".join(all_content)).hexdigest()
    return (
        normalize_block_geometry(combined, year),
        content_sha256,
        total_size,
        raw_paths,
        missing_state_fips,
    )


def get_block_geometry_output_path(
    year: int,
    base_dir: Path | str | None = None,
) -> Path:
    """Return the canonical block geometry GeoParquet output path."""
    if base_dir is None:
        base_dir = curated_dir("tiger")
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.block_geometry_filename(year)


def _write_geoparquet_with_provenance(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
    provenance: ProvenanceBlock,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path, index=False)
    table = pq.read_table(output_path)
    metadata = {
        **(table.schema.metadata or {}),
        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
    }
    pq.write_table(table.replace_schema_metadata(metadata), output_path)
    return output_path


def save_block_geometry(
    gdf: gpd.GeoDataFrame,
    year: int,
    *,
    output_dir: Path | str | None = None,
    content_sha256: str | None = None,
    content_size: int | None = None,
    raw_paths: list[Path] | None = None,
    missing_state_fips: list[str] | None = None,
) -> Path:
    """Save normalized tabulation block geometry as canonical GeoParquet."""
    output_path = get_block_geometry_output_path(year, output_dir)
    provenance = ProvenanceBlock(
        weighting="geometry",
        geo_type="block",
        extra={
            "dataset_type": "block_geometry",
            "block_vintage": year,
            "source": "census_tiger_tabblock",
            "source_url_template": _block_url(year, "{state_fips}"),
            "content_sha256": content_sha256,
            "content_size": content_size,
            "raw_paths": [str(path) for path in raw_paths or []],
            "missing_state_fips": missing_state_fips or [],
        },
    )
    return _write_geoparquet_with_provenance(gdf, output_path, provenance)


def ingest_block_geometry(
    year: int = 2020,
    *,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> Path:
    """Download and cache Census tabulation block geometry."""
    output_path = get_block_geometry_output_path(year, output_dir)
    if output_path.exists() and not force:
        return output_path

    gdf, content_sha256, content_size, raw_paths, missing_state_fips = download_block_geometry(
        year,
        raw_root=raw_root,
    )
    output_path = save_block_geometry(
        gdf,
        year,
        output_dir=output_dir,
        content_sha256=content_sha256,
        content_size=content_size,
        raw_paths=raw_paths,
        missing_state_fips=missing_state_fips,
    )

    changed, details = check_source_changed(
        source_type="census_block_geometry",
        source_url=_block_url(year, "*"),
        current_sha256=content_sha256,
    )
    if changed:
        logger.warning(
            "UPSTREAM DATA CHANGED: Census block geometry %s changed. "
            "Previous hash: %s... Current hash: %s... Last ingested: %s",
            year,
            details["previous_sha256"][:16],
            content_sha256[:16],
            details["previous_ingested_at"],
        )
    elif details.get("is_new"):
        logger.info("First time tracking Census block geometry %s in source registry", year)

    register_source(
        source_type="census_block_geometry",
        source_url=_block_url(year, "*"),
        source_name=f"Census tabulation blocks {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_paths[0]) if raw_paths else "",
        metadata={
            "year": year,
            "block_vintage": year,
            "block_count": len(gdf),
            "curated_path": str(output_path),
            "raw_path_count": len(raw_paths),
            "missing_state_fips": missing_state_fips,
        },
    )
    return output_path
