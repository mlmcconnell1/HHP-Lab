"""TIGER/Line tabulation block geometry ingestion."""

from __future__ import annotations

import hashlib
import logging
import shutil
import tempfile
import time
import warnings
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
import pyarrow.parquet as pq

import hhplab.naming as naming
from hhplab.paths import curated_dir
from hhplab.provenance import PROVENANCE_KEY, ProvenanceBlock
from hhplab.raw_snapshot import persist_file_snapshot, raw_dir, raw_path
from hhplab.schema.columns import BLOCK_GEOMETRY_COLUMNS
from hhplab.source_registry import check_source_changed, register_source
from hhplab.source_urls import CENSUS_TIGER_BASE
from hhplab.sources.census.census.ingest.decennial_tract_population import STATE_FIPS_CODES

logger = logging.getLogger(__name__)

SUPPORTED_BLOCK_VINTAGES: tuple[int, ...] = (2020,)
STATE_DOWNLOAD_ATTEMPTS = 3


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
    for attempt in range(1, STATE_DOWNLOAD_ATTEMPTS + 1):
        try:
            response = client.get(url, follow_redirects=True)
            response.raise_for_status()
            break
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None, None, None
            if attempt == STATE_DOWNLOAD_ATTEMPTS:
                raise
        except httpx.HTTPError:
            if attempt == STATE_DOWNLOAD_ATTEMPTS:
                raise
        time.sleep(float(attempt))

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


def _read_state_blocks_from_zip(
    zip_path: Path,
    state_fips: str,
    tmpdir: Path,
) -> gpd.GeoDataFrame | None:
    extract_dir = tmpdir / state_fips
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        return None
    return gpd.read_file(shp_files[0])


def _raw_block_zip_path(year: int, state_fips: str, raw_root: Path | None = None) -> Path:
    return raw_path(
        "tiger",
        year,
        _block_zip_name(year, state_fips),
        variant="blocks",
        raw_root=raw_root,
    )


def _block_geometry_parts_dir(
    year: int,
    output_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> Path:
    if output_dir is None:
        return raw_dir("tiger", year, "block_geometry_parts", raw_root=raw_root)
    output_path = get_block_geometry_output_path(year, output_dir)
    return output_path.with_name(f"{output_path.stem}_parts")


def _state_part_path(parts_dir: Path, state_fips: str) -> Path:
    return parts_dir / f"state_fips={state_fips}.parquet"


def _write_state_part(gdf: gpd.GeoDataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    gdf.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)
    return path


def _fetch_or_load_state_blocks(
    client: httpx.Client,
    *,
    year: int,
    state_fips: str,
    tmpdir: Path,
    raw_root: Path | None,
    force: bool,
) -> tuple[gpd.GeoDataFrame | None, bytes | None, Path | None]:
    cached_raw_path = _raw_block_zip_path(year, state_fips, raw_root)
    if cached_raw_path.exists() and not force:
        raw_content = cached_raw_path.read_bytes()
        gdf = _read_state_blocks_from_zip(cached_raw_path, state_fips, tmpdir)
        return gdf, raw_content, cached_raw_path

    gdf, raw_content, _zip_path = _download_state_blocks(client, year, state_fips, tmpdir)
    if gdf is None or raw_content is None:
        return None, None, None
    persisted_path, _, _ = persist_file_snapshot(
        raw_content,
        "tiger",
        _block_zip_name(year, state_fips),
        subdirs=(str(year), "blocks"),
        raw_root=raw_root,
    )
    return gdf, raw_content, persisted_path


def _stream_state_block_parts(
    year: int,
    *,
    parts_dir: Path,
    state_fips_codes: tuple[str, ...] = STATE_FIPS_CODES,
    raw_root: Path | None = None,
    force: bool = False,
) -> tuple[str, int, list[Path], list[str], dict[str, int]]:
    """Write one normalized GeoParquet part per state and return provenance inputs."""
    if force and parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)

    content_hasher = hashlib.sha256()
    total_size = 0
    raw_paths: list[Path] = []
    missing_state_fips: list[str] = []
    row_counts: dict[str, int] = {}
    has_hash_part = False

    with httpx.Client(timeout=300.0) as client:
        for state_fips in state_fips_codes:
            part_path = _state_part_path(parts_dir, state_fips)
            raw_zip_path = _raw_block_zip_path(year, state_fips, raw_root)
            raw_content: bytes | None = None

            if part_path.exists() and raw_zip_path.exists() and not force:
                row_counts[state_fips] = pq.read_metadata(part_path).num_rows
                raw_content = raw_zip_path.read_bytes()
                raw_paths.append(raw_zip_path)
            else:
                with tempfile.TemporaryDirectory(dir=parts_dir) as tmpdir:
                    gdf, raw_content, raw_path_value = _fetch_or_load_state_blocks(
                        client,
                        year=year,
                        state_fips=state_fips,
                        tmpdir=Path(tmpdir),
                        raw_root=raw_root,
                        force=force,
                    )
                if gdf is None or raw_content is None:
                    missing_state_fips.append(state_fips)
                    continue

                normalized = normalize_block_geometry(gdf, year)
                _write_state_part(normalized, part_path)
                row_counts[state_fips] = len(normalized)
                raw_paths.append(raw_path_value or raw_zip_path)

                del normalized
                del gdf

            if has_hash_part:
                content_hasher.update(b"\n")
            content_hasher.update(raw_content)
            has_hash_part = True
            total_size += len(raw_content)

    if not row_counts:
        raise ValueError(
            f"No Census tabulation block geometry rows fetched for {year}. "
            "Verify TIGER availability and requested state FIPS codes."
        )

    return content_hasher.hexdigest(), total_size, raw_paths, missing_state_fips, row_counts


def _write_parts_to_geoparquet(
    part_paths: list[Path],
    output_path: Path,
    provenance: ProvenanceBlock,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    writer: pq.ParquetWriter | None = None
    schema = None
    try:
        for part_path in part_paths:
            table = pq.read_table(part_path)
            if writer is None:
                metadata = {
                    **(table.schema.metadata or {}),
                    PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
                }
                schema = table.schema.with_metadata(metadata)
                writer = pq.ParquetWriter(tmp_path, schema)
            if schema is None:
                raise ValueError("No block geometry part files were available to assemble.")
            table = table.cast(schema)
            writer.write_table(table)
        if writer is None:
            raise ValueError("No block geometry part files were available to assemble.")
    finally:
        if writer is not None:
            writer.close()

    tmp_path.replace(output_path)
    return output_path


def download_block_geometry(
    year: int = 2020,
    *,
    state_fips_codes: tuple[str, ...] = STATE_FIPS_CODES,
    raw_root: Path | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, list[Path], list[str]]:
    """Download and normalize Census tabulation block geometry.

    Deprecated: this in-memory API accumulates every state before concatenating.
    Use ingest_block_geometry, which streams state parts to disk.
    """
    warnings.warn(
        "download_block_geometry is deprecated because it loads all state block "
        "geometry into memory. Use ingest_block_geometry instead.",
        DeprecationWarning,
        stacklevel=2,
    )
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
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    gdf.to_parquet(tmp_path, index=False)
    table = pq.read_table(tmp_path)
    metadata = {
        **(table.schema.metadata or {}),
        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
    }
    pq.write_table(table.replace_schema_metadata(metadata), tmp_path)
    tmp_path.replace(output_path)
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

    parts_dir = _block_geometry_parts_dir(year, output_dir, raw_root=raw_root)
    content_sha256, content_size, raw_paths, missing_state_fips, row_counts = (
        _stream_state_block_parts(
            year,
            parts_dir=parts_dir,
            raw_root=raw_root,
            force=force,
        )
    )
    output_path = save_block_geometry_from_parts(
        sorted(_state_part_path(parts_dir, state_fips) for state_fips in row_counts),
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
            "block_count": sum(row_counts.values()),
            "curated_path": str(output_path),
            "raw_path_count": len(raw_paths),
            "missing_state_fips": missing_state_fips,
            "parts_dir": str(parts_dir),
            "state_row_counts": row_counts,
        },
    )
    return output_path


def save_block_geometry_from_parts(
    part_paths: list[Path],
    year: int,
    *,
    output_dir: Path | str | None = None,
    content_sha256: str | None = None,
    content_size: int | None = None,
    raw_paths: list[Path] | None = None,
    missing_state_fips: list[str] | None = None,
) -> Path:
    """Save canonical tabulation block geometry by streaming per-state parts."""
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
            "part_paths": [str(path) for path in part_paths],
        },
    )
    return _write_parts_to_geoparquet(part_paths, output_path, provenance)
