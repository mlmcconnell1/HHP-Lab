"""TIGER/Line county geometry ingestion."""

import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx

from hhplab.registry.source_registry import check_source_changed, register_source
from hhplab.geographies.boundaries.census.urls import CENSUS_TIGER_BASE
from hhplab.storage.paths import curated_dir
from hhplab.storage.raw_snapshot import persist_file_snapshot

logger = logging.getLogger(__name__)

TIGER_BASE = CENSUS_TIGER_BASE


def download_tiger_counties(
    year: int = 2023,
    raw_root: Path | None = None,
) -> tuple[gpd.GeoDataFrame, str, int, Path]:
    """Download all US counties for a given year.

    Raw ZIP is persisted under ``data/raw/tiger/<year>/counties/``.

    Args:
        year: TIGER vintage year (default 2023)
        raw_root: Override the default raw data root (for testing)

    Returns:
        Tuple of (GeoDataFrame, SHA-256 hash, file size, raw_path):
        GeoDataFrame with standardized schema:
        - geo_vintage: str (e.g. "2023")
        - geoid: str (county FIPS code)
        - geometry: EPSG:4326
        - source: "tiger_line"
        - ingested_at: datetime
    """
    # 2010 has a different URL structure: extra /2010/ subdirectory and county10 suffix
    if year == 2010:
        url = f"{TIGER_BASE.format(year=year, layer='COUNTY')}2010/tl_2010_us_county10.zip"
        zip_name = "tl_2010_us_county10.zip"
    else:
        url = f"{TIGER_BASE.format(year=year, layer='COUNTY')}tl_{year}_us_county.zip"
        zip_name = f"tl_{year}_us_county.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        zip_path = tmppath / zip_name

        # Download the zip file
        with httpx.Client(timeout=300.0) as client:
            response = client.get(url, follow_redirects=True)
            response.raise_for_status()
            raw_content = response.content
            zip_path.write_bytes(raw_content)

        # Persist raw ZIP to data/raw/tiger/<year>/counties/
        raw_path, content_sha256, content_size = persist_file_snapshot(
            raw_content,
            "tiger",
            zip_name,
            subdirs=(str(year), "counties"),
            raw_root=raw_root,
        )

        # Check for upstream changes
        changed, details = check_source_changed(
            source_type="census_county",
            source_url=url,
            current_sha256=content_sha256,
        )

        if changed:
            logger.warning(
                "UPSTREAM DATA CHANGED: TIGER county data for %s has changed since last download! "
                "Previous hash: %s... Current hash: %s... Last ingested: %s",
                year,
                details["previous_sha256"][:16],
                content_sha256[:16],
                details["previous_ingested_at"],
            )
        elif details.get("is_new"):
            logger.info(f"First time tracking TIGER counties {year} in source registry")

        # Extract the zip file
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmppath)

        # Find the shapefile
        shp_files = list(tmppath.glob("*.shp"))
        if not shp_files:
            raise FileNotFoundError(f"No shapefile found in {url}")

        # Read the shapefile
        gdf = gpd.read_file(shp_files[0])

    # Reproject to EPSG:4326 if needed; reject missing CRS
    if gdf.crs is None:
        msg = "Source GeoDataFrame has no CRS; cannot safely assume EPSG:4326."
        raise ValueError(msg)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Standardize schema
    ingested_at = datetime.now(UTC)
    result = gpd.GeoDataFrame(
        {
            "geo_vintage": str(year),
            "geoid": gdf["GEOID"].astype(str).str.zfill(5),
            "geometry": gdf["geometry"],
            "source": "tiger_line",
            "ingested_at": ingested_at,
        },
        crs="EPSG:4326",
    )

    return result, content_sha256, content_size, raw_path


def save_counties(gdf: gpd.GeoDataFrame, year: int = 2023) -> Path:
    """Save counties GeoDataFrame to parquet.

    Args:
        gdf: GeoDataFrame with county geometries
        year: Vintage year for filename

    Returns:
        Path to saved parquet file (e.g., counties__C2023.parquet)
    """
    from hhplab.artifacts.naming.naming import county_filename

    curated_dir("tiger").mkdir(parents=True, exist_ok=True)
    output_path = curated_dir("tiger") / county_filename(year)
    gdf.to_parquet(output_path, index=False)
    return output_path


def ingest_tiger_counties(year: int = 2023, raw_root: Path | None = None) -> Path:
    """Download and save TIGER counties in one step.

    Raw ZIP is persisted under ``data/raw/tiger/<year>/counties/``.

    Args:
        year: TIGER vintage year (default 2023)
        raw_root: Override the default raw data root (for testing)

    Returns:
        Path to saved parquet file
    """
    gdf, content_sha256, content_size, raw_path = download_tiger_counties(year, raw_root=raw_root)
    output_path = save_counties(gdf, year)

    # Register this download in source registry (local_path → raw snapshot)
    if year == 2010:
        url = f"{TIGER_BASE.format(year=year, layer='COUNTY')}2010/tl_2010_us_county10.zip"
    else:
        url = f"{TIGER_BASE.format(year=year, layer='COUNTY')}tl_{year}_us_county.zip"
    register_source(
        source_type="census_county",
        source_url=url,
        source_name=f"TIGER/Line Counties {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "year": year,
            "county_count": len(gdf),
            "curated_path": str(output_path),
        },
    )

    return output_path


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    output = ingest_tiger_counties(year)
    print(f"Saved counties to {output}")
