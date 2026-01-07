"""TIGER/Line tract geometry ingestion."""

import hashlib
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import click
import geopandas as gpd
import httpx
import pandas as pd

from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

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
) -> tuple[gpd.GeoDataFrame | None, bytes | None]:
    """Download tract data for a single state.

    Returns tuple of (GeoDataFrame, raw_content) or (None, None) if the state
    file doesn't exist (some territories may not have data).
    """
    url = f"{TIGER_BASE.format(year=year)}tl_{year}_{state_fips}_tract.zip"
    zip_path = tmpdir / f"tl_{year}_{state_fips}_tract.zip"

    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None, None  # State file doesn't exist
        raise

    raw_content = response.content
    zip_path.write_bytes(raw_content)

    # Extract and read
    extract_dir = tmpdir / state_fips
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        return None, None

    return gpd.read_file(shp_files[0]), raw_content


def download_tiger_tracts(
    year: int = 2023,
    show_progress: bool = False,
) -> tuple[gpd.GeoDataFrame, str, int]:
    """Download all US census tracts for a given year.

    Downloads per-state tract files and combines them into a single GeoDataFrame.

    Args:
        year: TIGER vintage year (default 2023)
        show_progress: If True, display a progress bar

    Returns:
        Tuple of (GeoDataFrame, combined_sha256, total_size) where:
        - GeoDataFrame with standardized schema:
          - geo_vintage: str (e.g. "2023")
          - geoid: str (tract FIPS code)
          - geometry: EPSG:4326
          - source: "tiger_line"
          - ingested_at: datetime
        - combined_sha256: SHA-256 hash of all downloaded content
        - total_size: Total size in bytes of all downloaded files
    """
    gdfs = []
    all_content = []  # Collect all raw content for combined hash
    total_size = 0

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
                    gdf, raw_content = _download_state_tracts(client, year, state_fips, tmppath)
                    if gdf is not None and raw_content is not None:
                        gdfs.append(gdf)
                        all_content.append(raw_content)
                        total_size += len(raw_content)

    if not gdfs:
        raise ValueError(f"No tract data found for year {year}")

    # Compute combined SHA-256 hash of all downloaded content
    # Hash the concatenation of all individual file hashes (sorted by state FIPS)
    hasher = hashlib.sha256()
    for content in all_content:
        hasher.update(content)
    combined_sha256 = hasher.hexdigest()

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

    return result, combined_sha256, total_size


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
    # Build source URL (base URL for this year's tract data)
    source_url = TIGER_BASE.format(year=year)

    gdf, combined_sha256, total_size = download_tiger_tracts(year, show_progress=show_progress)
    output_path = save_tracts(gdf, year)

    # Check for upstream changes
    changed, details = check_source_changed(
        source_type="census_tract",
        source_url=source_url,
        current_sha256=combined_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: TIGER tract data for {year} has changed since last download! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {combined_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking TIGER tracts {year} source in registry")

    # Register this download in source registry
    register_source(
        source_type="census_tract",
        source_url=source_url,
        source_name=f"TIGER/Line Census Tracts {year}",
        raw_sha256=combined_sha256,
        file_size=total_size,
        local_path=str(output_path),
        metadata={
            "year": year,
            "vintage": str(year),
            "data_source": "US Census Bureau",
            "tract_count": len(gdf),
            "states_downloaded": len(STATE_FIPS_CODES),
        },
    )

    logger.info(f"Ingested {len(gdf)} census tracts for {year} to {output_path}")

    return output_path


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    output = ingest_tiger_tracts(year)
    print(f"Saved tracts to {output}")
