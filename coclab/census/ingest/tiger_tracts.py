"""TIGER/Line tract geometry ingestion."""

import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import click
import geopandas as gpd
import httpx
import pandas as pd

TIGER_BASE = "https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/"
OUTPUT_DIR = Path("data/curated/census")

# State and territory FIPS codes for downloading per-state tract files
STATE_FIPS_CODES = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56",  # 50 states + DC
    "60",  # American Samoa
    "66",  # Guam
    "69",  # Northern Mariana Islands
    "72",  # Puerto Rico
    "78",  # U.S. Virgin Islands
]


def _download_state_tracts(
    client: httpx.Client,
    year: int,
    state_fips: str,
    tmpdir: Path,
) -> gpd.GeoDataFrame | None:
    """Download tract data for a single state.

    Returns None if the state file doesn't exist (some territories may not have data).
    """
    url = f"{TIGER_BASE.format(year=year)}tl_{year}_{state_fips}_tract.zip"
    zip_path = tmpdir / f"tl_{year}_{state_fips}_tract.zip"

    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None  # State file doesn't exist
        raise

    zip_path.write_bytes(response.content)

    # Extract and read
    extract_dir = tmpdir / state_fips
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        return None

    return gpd.read_file(shp_files[0])


def download_tiger_tracts(
    year: int = 2023,
    show_progress: bool = False,
) -> gpd.GeoDataFrame:
    """Download all US census tracts for a given year.

    Downloads per-state tract files and combines them into a single GeoDataFrame.

    Args:
        year: TIGER vintage year (default 2023)
        show_progress: If True, display a progress bar

    Returns:
        GeoDataFrame with standardized schema:
        - geo_vintage: str (e.g. "2023")
        - geoid: str (tract FIPS code)
        - geometry: EPSG:4326
        - source: "tiger_line"
        - ingested_at: datetime
    """
    gdfs = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        with httpx.Client(timeout=300.0) as client:
            if show_progress:
                states = click.progressbar(
                    STATE_FIPS_CODES,
                    label="Downloading state tracts",
                    show_pos=True,
                )
            else:
                states = STATE_FIPS_CODES

            with states if show_progress else nullcontext(states) as state_iter:
                for state_fips in state_iter:
                    gdf = _download_state_tracts(client, year, state_fips, tmppath)
                    if gdf is not None:
                        gdfs.append(gdf)

    if not gdfs:
        raise ValueError(f"No tract data found for year {year}")

    # Combine all states
    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, crs=gdfs[0].crs)

    # Reproject to EPSG:4326 if needed
    if combined.crs and combined.crs.to_epsg() != 4326:
        combined = combined.to_crs(epsg=4326)

    # Standardize schema
    ingested_at = datetime.now(UTC)
    result = gpd.GeoDataFrame(
        {
            "geo_vintage": str(year),
            "geoid": combined["GEOID"],
            "geometry": combined["geometry"],
            "source": "tiger_line",
            "ingested_at": ingested_at,
        },
        crs="EPSG:4326",
    )

    return result


def nullcontext(value):
    """Simple context manager that returns the value unchanged."""
    class NullContext:
        def __enter__(self):
            return value
        def __exit__(self, *args):
            pass
    return NullContext()


def save_tracts(gdf: gpd.GeoDataFrame, year: int = 2023) -> Path:
    """Save tracts GeoDataFrame to parquet.

    Args:
        gdf: GeoDataFrame with tract geometries
        year: Vintage year for filename

    Returns:
        Path to saved parquet file
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"tracts__{year}.parquet"
    gdf.to_parquet(output_path, index=False)
    return output_path


def ingest_tiger_tracts(year: int = 2023, show_progress: bool = False) -> Path:
    """Download and save TIGER tracts in one step.

    Args:
        year: TIGER vintage year (default 2023)
        show_progress: If True, display a progress bar

    Returns:
        Path to saved parquet file
    """
    gdf = download_tiger_tracts(year, show_progress=show_progress)
    return save_tracts(gdf, year)


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    output = ingest_tiger_tracts(year)
    print(f"Saved tracts to {output}")
