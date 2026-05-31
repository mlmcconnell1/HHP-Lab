"""Census Urban Area geometry ingestion."""

from __future__ import annotations

import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
import pyarrow.parquet as pq

import hhplab.naming as naming
from hhplab.paths import curated_dir
from hhplab.provenance import PROVENANCE_KEY, ProvenanceBlock
from hhplab.raw_snapshot import hash_zip_contents, persist_file_snapshot
from hhplab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

URBAN_AREA_SOURCES: dict[int, tuple[str, str]] = {
    2010: (
        "https://www2.census.gov/geo/pvs/tiger2010st/tl_2010_us_uac10.zip",
        "tl_2010_us_uac10.zip",
    ),
    2020: (
        "https://www2.census.gov/geo/tiger/TIGER2020/UAC/tl_2020_us_uac20.zip",
        "tl_2020_us_uac20.zip",
    ),
}

URBAN_AREA_TYPE_LABELS: dict[str, str] = {
    "U": "urbanized_area",
    "C": "urban_cluster",
}


def urban_area_source(year: int) -> tuple[str, str]:
    """Return the Census download URL and ZIP name for an Urban Area vintage."""
    try:
        return URBAN_AREA_SOURCES[year]
    except KeyError as exc:
        supported = ", ".join(str(vintage) for vintage in sorted(URBAN_AREA_SOURCES))
        raise ValueError(
            f"Unsupported Urban Area vintage {year!r}. Supported vintages: {supported}."
        ) from exc


def _year_suffix(year: int) -> str:
    if year == 2010:
        return "10"
    if year == 2020:
        return "20"
    urban_area_source(year)
    raise AssertionError("unreachable")


def _resolve_column(gdf: gpd.GeoDataFrame, candidates: tuple[str, ...], label: str) -> str:
    for column in candidates:
        if column in gdf.columns:
            return column
    raise ValueError(f"Could not find {label} column. Available columns: {list(gdf.columns)}")


def normalize_urban_areas(gdf: gpd.GeoDataFrame, year: int) -> gpd.GeoDataFrame:
    """Normalize Census Urban Area source geometry to the canonical schema."""
    urban_area_source(year)
    if gdf.crs is None:
        raise ValueError(
            "Source Urban Area GeoDataFrame has no CRS; cannot safely assume EPSG:4326."
        )
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    suffix = _year_suffix(year)
    geoid_column = _resolve_column(gdf, (f"GEOID{suffix}", f"UACE{suffix}"), "Urban Area GEOID")
    name_column = _resolve_column(gdf, (f"NAME{suffix}", f"NAMELSAD{suffix}"), "Urban Area name")
    type_column = _resolve_column(gdf, (f"UATYP{suffix}", f"UATYPE{suffix}"), "Urban Area type")
    ingested_at = datetime.now(UTC).isoformat()
    type_codes = gdf[type_column].astype("string")
    type_labels = type_codes.map(URBAN_AREA_TYPE_LABELS).fillna("urban_area")

    return gpd.GeoDataFrame(
        {
            "urban_area_geoid": gdf[geoid_column].astype(str).str.zfill(5),
            "urban_area_name": gdf[name_column].astype(str),
            "urban_area_type": type_labels,
            "urban_area_vintage": year,
            "data_source": "census_tiger_urban_area",
            "source_ref": urban_area_source(year)[0],
            "ingested_at": ingested_at,
            "geometry": gdf.geometry,
        },
        geometry="geometry",
        crs="EPSG:4326",
    )


def download_urban_areas(
    year: int,
    *,
    raw_root: Path | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, Path]:
    """Download and normalize Census Urban Area geometry for 2010 or 2020."""
    url, zip_name = urban_area_source(year)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        zip_path = tmppath / zip_name
        with httpx.Client(timeout=300.0) as client:
            response = client.get(url, follow_redirects=True)
            response.raise_for_status()
            raw_content = response.content
            zip_path.write_bytes(raw_content)

        raw_path, _raw_sha256, content_size = persist_file_snapshot(
            raw_content,
            "tiger",
            zip_name,
            subdirs=(str(year), "urban_areas"),
            raw_root=raw_root,
        )
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmppath)
        shp_files = list(tmppath.glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No shapefile found in {url}")
        source_gdf = gpd.read_file(shp_files[0])

    content_sha256 = hash_zip_contents(raw_content)
    return normalize_urban_areas(source_gdf, year), content_sha256, content_size, raw_path


def get_urban_area_output_path(
    year: int,
    base_dir: Path | str | None = None,
) -> Path:
    """Return the canonical Urban Area GeoParquet output path."""
    if base_dir is None:
        base_dir = curated_dir("tiger")
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.urban_area_filename(year)


def _write_geoparquet_with_provenance(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
    provenance: ProvenanceBlock,
) -> Path:
    """Write GeoParquet while preserving GeoPandas metadata and adding HHP provenance."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path, index=False)
    table = pq.read_table(output_path)
    metadata = {
        **(table.schema.metadata or {}),
        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
    }
    pq.write_table(table.replace_schema_metadata(metadata), output_path)
    return output_path


def save_urban_areas(
    gdf: gpd.GeoDataFrame,
    year: int,
    *,
    output_dir: Path | str | None = None,
    content_sha256: str | None = None,
    content_size: int | None = None,
    source_url: str | None = None,
) -> Path:
    """Save normalized Urban Area geometry as canonical GeoParquet."""
    output_path = get_urban_area_output_path(year, output_dir)
    provenance = ProvenanceBlock(
        weighting="geometry",
        geo_type="urban_area",
        extra={
            "dataset_type": "urban_area_geometry",
            "urban_area_vintage": year,
            "source_url": source_url or urban_area_source(year)[0],
            "content_sha256": content_sha256,
            "content_size": content_size,
        },
    )
    return _write_geoparquet_with_provenance(gdf, output_path, provenance)


def ingest_urban_areas(
    year: int,
    *,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> Path:
    """Download and cache Census Urban Area geometry for 2010 or 2020."""
    output_path = get_urban_area_output_path(year, output_dir)
    if output_path.exists() and not force:
        return output_path

    url, _zip_name = urban_area_source(year)
    gdf, content_sha256, content_size, raw_path = download_urban_areas(year, raw_root=raw_root)
    output_path = save_urban_areas(
        gdf,
        year,
        output_dir=output_dir,
        content_sha256=content_sha256,
        content_size=content_size,
        source_url=url,
    )

    changed, details = check_source_changed(
        source_type="census_urban_area",
        source_url=url,
        current_sha256=content_sha256,
    )
    if changed:
        logger.warning(
            "UPSTREAM DATA CHANGED: Census Urban Area data for %s changed. "
            "Previous hash: %s... Current hash: %s... Last ingested: %s",
            year,
            details["previous_sha256"][:16],
            content_sha256[:16],
            details["previous_ingested_at"],
        )
    elif details.get("is_new"):
        logger.info("First time tracking Census Urban Areas %s in source registry", year)

    register_source(
        source_type="census_urban_area",
        source_url=url,
        source_name=f"Census Urban Areas {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "year": year,
            "urban_area_vintage": year,
            "urban_area_count": len(gdf),
            "curated_path": str(output_path),
        },
    )
    return output_path
