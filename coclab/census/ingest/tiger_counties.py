"""TIGER/Line county geometry ingestion."""

import hashlib
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx

from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/"
OUTPUT_DIR = Path("data/curated/census")


def download_tiger_counties(year: int = 2023) -> tuple[gpd.GeoDataFrame, str, int]:
    """Download all US counties for a given year.

    Args:
        year: TIGER vintage year (default 2023)

    Returns:
        Tuple of (GeoDataFrame, SHA-256 hash, file size):
        GeoDataFrame with standardized schema:
        - geo_vintage: str (e.g. "2023")
        - geoid: str (county FIPS code)
        - geometry: EPSG:4326
        - source: "tiger_line"
        - ingested_at: datetime
    """
    url = f"{TIGER_BASE.format(year=year)}tl_{year}_us_county.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        zip_path = tmppath / f"tl_{year}_us_county.zip"

        # Download the zip file
        with httpx.Client(timeout=300.0) as client:
            response = client.get(url, follow_redirects=True)
            response.raise_for_status()
            raw_content = response.content
            zip_path.write_bytes(raw_content)

        # Compute SHA-256 hash of raw zip file
        content_sha256 = hashlib.sha256(raw_content).hexdigest()
        content_size = len(raw_content)

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

    # Reproject to EPSG:4326 if needed
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Standardize schema
    ingested_at = datetime.now(UTC)
    result = gpd.GeoDataFrame(
        {
            "geo_vintage": str(year),
            "geoid": gdf["GEOID"],
            "geometry": gdf["geometry"],
            "source": "tiger_line",
            "ingested_at": ingested_at,
        },
        crs="EPSG:4326",
    )

    return result, content_sha256, content_size


def save_counties(gdf: gpd.GeoDataFrame, year: int = 2023) -> Path:
    """Save counties GeoDataFrame to parquet.

    Args:
        gdf: GeoDataFrame with county geometries
        year: Vintage year for filename

    Returns:
        Path to saved parquet file (e.g., counties__C2023.parquet)
    """
    from coclab.naming import county_filename

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / county_filename(year)
    gdf.to_parquet(output_path, index=False)
    return output_path


def ingest_tiger_counties(year: int = 2023) -> Path:
    """Download and save TIGER counties in one step.

    Args:
        year: TIGER vintage year (default 2023)

    Returns:
        Path to saved parquet file
    """
    gdf, content_sha256, content_size = download_tiger_counties(year)
    output_path = save_counties(gdf, year)

    # Register this download in source registry
    url = f"{TIGER_BASE.format(year=year)}tl_{year}_us_county.zip"
    register_source(
        source_type="census_county",
        source_url=url,
        source_name=f"TIGER/Line Counties {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(output_path),
        metadata={
            "year": year,
            "county_count": len(gdf),
        },
    )

    return output_path


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    output = ingest_tiger_counties(year)
    print(f"Saved counties to {output}")
