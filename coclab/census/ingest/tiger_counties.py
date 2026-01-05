"""TIGER/Line county geometry ingestion."""
from pathlib import Path
from datetime import datetime, timezone
import tempfile
import zipfile

import httpx
import geopandas as gpd

TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/"
OUTPUT_DIR = Path("data/curated/census")


def download_tiger_counties(year: int = 2023) -> gpd.GeoDataFrame:
    """Download all US counties for a given year.

    Args:
        year: TIGER vintage year (default 2023)

    Returns:
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
            zip_path.write_bytes(response.content)

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
    ingested_at = datetime.now(timezone.utc)
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

    return result


def save_counties(gdf: gpd.GeoDataFrame, year: int = 2023) -> Path:
    """Save counties GeoDataFrame to parquet.

    Args:
        gdf: GeoDataFrame with county geometries
        year: Vintage year for filename

    Returns:
        Path to saved parquet file
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"counties__{year}.parquet"
    gdf.to_parquet(output_path, index=False)
    return output_path


def ingest_tiger_counties(year: int = 2023) -> Path:
    """Download and save TIGER counties in one step.

    Args:
        year: TIGER vintage year (default 2023)

    Returns:
        Path to saved parquet file
    """
    gdf = download_tiger_counties(year)
    return save_counties(gdf, year)


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    output = ingest_tiger_counties(year)
    print(f"Saved counties to {output}")
