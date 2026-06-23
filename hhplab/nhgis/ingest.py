"""NHGIS tract shapefile ingestion via ipumspy.

Downloads census tract boundaries from NHGIS, which provides pre-assembled
national files (avoiding the county-by-county download required for 2010 TIGER).
"""

from __future__ import annotations

import logging
import tempfile
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd

from hhplab.paths import curated_dir
from hhplab.raw_snapshot import persist_file_snapshot
from hhplab.source_registry import register_source

if TYPE_CHECKING:
    from ipumspy import IpumsApiClient

logger = logging.getLogger(__name__)


# Known NHGIS shapefile names for census tracts
# Format: us_tract_{census_year}_tl{tiger_year}
# See: https://www.nhgis.org/gis-files
NHGIS_TRACT_SHAPEFILES = {
    2010: "us_tract_2010_tl2010",
    2020: "us_tract_2020_tl2020",
}

# Known NHGIS shapefile names for counties
# Format: us_county_{census_year}_tl{tiger_year}
NHGIS_COUNTY_SHAPEFILES = {
    2010: "us_county_2010_tl2010",
    2020: "us_county_2020_tl2020",
}

# Supported years
SUPPORTED_YEARS = set(NHGIS_TRACT_SHAPEFILES.keys())


class NhgisExtractError(Exception):
    """Error during NHGIS extract creation or download."""

    pass


def _get_shapefile_name(year: int, geo_type: str = "tracts") -> str:
    """Get the NHGIS shapefile name for a given census year and geography type.

    Args:
        year: Census year (2010 or 2020)
        geo_type: Geography type ("tracts" or "counties")

    Returns:
        NHGIS shapefile identifier

    Raises:
        ValueError: If year or geo_type is not supported
    """
    if geo_type == "tracts":
        shapefile_map = NHGIS_TRACT_SHAPEFILES
    elif geo_type == "counties":
        shapefile_map = NHGIS_COUNTY_SHAPEFILES
    else:
        raise ValueError(f"Unsupported geo_type: {geo_type}. Use 'tracts' or 'counties'.")

    if year not in shapefile_map:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_YEARS))
        raise ValueError(f"Year {year} not supported. Supported years: {supported}")
    return shapefile_map[year]


def _create_extract(year: int, geo_type: str = "tracts"):
    """Create an NHGIS extract definition for shapefiles.

    Args:
        year: Census year
        geo_type: Geography type ("tracts" or "counties")

    Returns:
        AggregateDataExtract object ready for submission
    """
    from ipumspy import AggregateDataExtract

    shapefile_name = _get_shapefile_name(year, geo_type)

    extract = AggregateDataExtract(
        collection="nhgis",
        description=f"Census {geo_type} {year} for HHP-Lab",
        shapefiles=[shapefile_name],
    )

    return extract


def _wait_for_extract(
    client: IpumsApiClient,
    extract: object,
    poll_interval_minutes: int = 2,
    max_wait_minutes: int = 60,
    progress_callback=None,
) -> None:
    """Wait for an NHGIS extract to complete.

    Args:
        client: IPUMS API client
        extract: Submitted extract object
        poll_interval_minutes: Minutes between status checks
        max_wait_minutes: Maximum time to wait before giving up
        progress_callback: Optional callback(status_message) for progress updates

    Raises:
        NhgisExtractError: If extract fails or times out
    """
    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60
    poll_interval_seconds = poll_interval_minutes * 60

    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            raise NhgisExtractError(
                f"Extract timed out after {max_wait_minutes} minutes"
            )

        # Check extract status - returns a string like "queued", "running", "completed", "failed"
        status = client.extract_status(extract)
        status_lower = status.lower()

        if progress_callback:
            progress_callback(f"Status: {status} (elapsed: {elapsed/60:.1f} min)")

        if status_lower == "completed":
            return
        elif status_lower == "failed":
            raise NhgisExtractError(f"Extract failed with status: {status}")
        elif status_lower in ("queued", "running", "started"):
            time.sleep(poll_interval_seconds)
        else:
            # Unknown status, keep waiting
            logger.warning(f"Unknown extract status: {status}, continuing to wait...")
            time.sleep(poll_interval_seconds)


def _download_and_extract(
    client: IpumsApiClient,
    extract: object,
    download_dir: Path,
) -> tuple[Path, bytes]:
    """Download extract and return path to shapefile and raw content.

    Args:
        client: IPUMS API client
        extract: Completed extract object
        download_dir: Directory to download to

    Returns:
        Tuple of (path to .shp file, raw zip content for hashing)

    Raises:
        NhgisExtractError: If download or extraction fails
    """
    # Download the extract
    client.download_extract(extract, download_dir=download_dir)

    # Find the downloaded zip file
    zip_files = list(download_dir.glob("nhgis*.zip"))
    if not zip_files:
        raise NhgisExtractError(f"No zip file found in {download_dir}")

    zip_path = zip_files[0]

    # Read raw content for hashing
    raw_content = zip_path.read_bytes()

    # Extract the zip and log contents
    extract_dir = download_dir / "extracted"
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zip_contents = zf.namelist()
        logger.info(f"Zip contains {len(zip_contents)} entries: {zip_contents[:10]}")
        zf.extractall(extract_dir)

    # Check for nested zips and extract them
    nested_zips = list(extract_dir.rglob("*.zip"))
    for nested_zip in nested_zips:
        nested_extract_dir = nested_zip.parent / nested_zip.stem
        nested_extract_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(nested_zip, "r") as nzf:
            logger.info(
                f"Extracting nested zip {nested_zip.name} "
                f"with {len(nzf.namelist())} entries"
            )
            nzf.extractall(nested_extract_dir)

    # Find the shapefile - NHGIS nests files in subdirectories
    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        # Debug: list what we actually got
        all_files = list(extract_dir.rglob("*"))
        file_list = [str(f.relative_to(extract_dir)) for f in all_files if f.is_file()]
        raise NhgisExtractError(
            f"No shapefile found in extracted content. Files found: {file_list[:20]}"
            + (f" ... and {len(file_list) - 20} more" if len(file_list) > 20 else "")
        )

    # Return the first (should be only) shapefile
    return shp_files[0], raw_content


def _normalize_to_schema(gdf: gpd.GeoDataFrame, year: int) -> gpd.GeoDataFrame:
    """Normalize NHGIS tract data to match TIGER tract schema.

    Args:
        gdf: Raw GeoDataFrame from NHGIS shapefile
        year: Census year

    Returns:
        GeoDataFrame with standardized schema matching TIGER tracts
    """
    # NHGIS uses GISJOIN as the primary identifier, but also has GEOID
    # Column names vary by year, handle both cases
    geoid_col = None
    for col in ["GEOID", "GEOID10", "GEOID20", "GISJOIN"]:
        if col in gdf.columns:
            geoid_col = col
            break

    if geoid_col is None:
        raise ValueError(f"Could not find GEOID column. Available: {list(gdf.columns)}")

    # GISJOIN has a different format (G + state + county + tract with extra chars)
    # GEOID is the standard FIPS format we want
    if geoid_col == "GISJOIN":
        # Convert GISJOIN to GEOID format
        # GISJOIN format: G[SS][0][CCC][0][TTTTTT] where SS=state, CCC=county, TTTTTT=tract
        # GEOID format: SSCCCTTTTT (11 chars)
        def gisjoin_to_geoid(gj: str) -> str:
            # Remove 'G' prefix and internal zeros
            # G 01 0 001 0 020100 -> 01001020100
            if gj.startswith("G"):
                gj = gj[1:]
            # State (2 chars), skip 0, County (3 chars), skip 0, Tract (6 chars)
            state = gj[0:2]
            county = gj[3:6]
            tract = gj[7:13]
            return f"{state}{county}{tract}"

        geoid_values = gdf[geoid_col].apply(gisjoin_to_geoid)
    else:
        geoid_values = gdf[geoid_col].astype(str)

    # Ensure GEOIDs are properly zero-padded (11 characters for tracts)
    geoid_values = geoid_values.str.zfill(11)

    # Reproject to EPSG:4326 if needed; reject missing CRS
    if gdf.crs is None:
        msg = "Source GeoDataFrame has no CRS; cannot safely assume EPSG:4326."
        raise ValueError(msg)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Build standardized output matching TIGER schema
    ingested_at = datetime.now(UTC)
    result = gpd.GeoDataFrame(
        {
            "geo_vintage": str(year),
            "geoid": geoid_values,
            "geometry": gdf["geometry"],
            "source": "nhgis",
            "ingested_at": ingested_at,
        },
        crs="EPSG:4326",
    )

    return result


def ingest_nhgis_tracts(
    year: int,
    api_key: str,
    poll_interval_minutes: int = 2,
    max_wait_minutes: int = 60,
    progress_callback=None,
) -> Path:
    """Ingest census tract boundaries from NHGIS.

    Submits an extract request to NHGIS, waits for completion, downloads
    the shapefile, and saves as GeoParquet with standardized schema.

    Args:
        year: Census year (2010 or 2020)
        api_key: IPUMS API key
        poll_interval_minutes: Minutes between status checks while waiting
        max_wait_minutes: Maximum time to wait for extract completion
        progress_callback: Optional callback(message) for progress updates

    Returns:
        Path to saved GeoParquet file

    Raises:
        ValueError: If year is not supported
        NhgisExtractError: If extract fails
    """
    from ipumspy import IpumsApiClient

    if year not in SUPPORTED_YEARS:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_YEARS))
        raise ValueError(f"Year {year} not supported. Supported years: {supported}")

    # Create API client
    client = IpumsApiClient(api_key)

    # Create extract definition
    if progress_callback:
        progress_callback(f"Creating extract for {year} tracts...")

    extract = _create_extract(year)

    # Submit extract
    if progress_callback:
        progress_callback("Submitting extract to NHGIS...")

    extract = client.submit_extract(extract)

    if progress_callback:
        progress_callback("Extract submitted, waiting for completion...")

    # Wait for completion
    _wait_for_extract(
        client,
        extract,
        poll_interval_minutes=poll_interval_minutes,
        max_wait_minutes=max_wait_minutes,
        progress_callback=progress_callback,
    )

    if progress_callback:
        progress_callback("Extract complete, downloading...")

    # Download and process
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        shp_path, raw_content = _download_and_extract(client, extract, tmppath)

        if progress_callback:
            progress_callback("Reading shapefile...")

        # Read shapefile
        gdf = gpd.read_file(shp_path)

        if progress_callback:
            progress_callback(f"Loaded {len(gdf)} tracts, normalizing schema...")

        # Normalize to standard schema
        gdf = _normalize_to_schema(gdf, year)

    # Persist raw ZIP under data/raw/nhgis/<year>/tracts/
    shapefile_name = _get_shapefile_name(year)
    raw_path, content_sha256, content_size = persist_file_snapshot(
        raw_content,
        "nhgis",
        f"{shapefile_name}.zip",
        subdirs=(str(year), "tracts"),
    )

    # Save to output
    curated_dir("tiger").mkdir(parents=True, exist_ok=True)
    from hhplab.naming import tract_filename

    output_path = curated_dir("tiger") / tract_filename(year)
    gdf.to_parquet(output_path, index=False)

    if progress_callback:
        progress_callback(f"Saved {len(gdf)} tracts to {output_path}")

    # Register in source registry (local_path → raw snapshot)
    source_url = f"nhgis://shapefiles/{shapefile_name}"

    register_source(
        source_type="nhgis_tract",
        source_url=source_url,
        source_name=f"NHGIS Census Tracts {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "year": year,
            "shapefile": shapefile_name,
            "tract_count": len(gdf),
            "source": "nhgis",
            "curated_path": str(output_path),
        },
    )

    logger.info(f"Ingested {len(gdf)} NHGIS tracts for {year} to {output_path}")

    return output_path


def _normalize_county_to_schema(gdf: gpd.GeoDataFrame, year: int) -> gpd.GeoDataFrame:
    """Normalize NHGIS county data to match TIGER county schema.

    Args:
        gdf: Raw GeoDataFrame from NHGIS shapefile
        year: Census year

    Returns:
        GeoDataFrame with standardized schema matching TIGER counties
    """
    # NHGIS uses GISJOIN as the primary identifier, but also has GEOID
    # Column names vary by year, handle both cases
    geoid_col = None
    for col in ["GEOID", "GEOID10", "GEOID20", "GISJOIN"]:
        if col in gdf.columns:
            geoid_col = col
            break

    if geoid_col is None:
        raise ValueError(f"Could not find GEOID column. Available: {list(gdf.columns)}")

    # GISJOIN has a different format for counties: G[SS][0][CCC]
    # GEOID is the standard FIPS format we want (5 chars: SSCCC)
    if geoid_col == "GISJOIN":
        def gisjoin_to_county_geoid(gj: str) -> str:
            # Remove 'G' prefix and internal zero
            # G 01 0 001 -> 01001
            if gj.startswith("G"):
                gj = gj[1:]
            # State (2 chars), skip 0, County (3 chars)
            state = gj[0:2]
            county = gj[3:6]
            return f"{state}{county}"

        geoid_values = gdf[geoid_col].apply(gisjoin_to_county_geoid)
    else:
        geoid_values = gdf[geoid_col].astype(str)

    # Ensure GEOIDs are properly zero-padded (5 characters for counties)
    geoid_values = geoid_values.str.zfill(5)

    # Reproject to EPSG:4326 if needed; reject missing CRS
    if gdf.crs is None:
        msg = "Source GeoDataFrame has no CRS; cannot safely assume EPSG:4326."
        raise ValueError(msg)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Build standardized output matching TIGER schema
    ingested_at = datetime.now(UTC)
    result = gpd.GeoDataFrame(
        {
            "geo_vintage": str(year),
            "geoid": geoid_values,
            "geometry": gdf["geometry"],
            "source": "nhgis",
            "ingested_at": ingested_at,
        },
        crs="EPSG:4326",
    )

    return result


def ingest_nhgis_counties(
    year: int,
    api_key: str,
    poll_interval_minutes: int = 2,
    max_wait_minutes: int = 60,
    progress_callback=None,
) -> Path:
    """Ingest county boundaries from NHGIS.

    Submits an extract request to NHGIS, waits for completion, downloads
    the shapefile, and saves as GeoParquet with standardized schema.

    Args:
        year: Census year (2010 or 2020)
        api_key: IPUMS API key
        poll_interval_minutes: Minutes between status checks while waiting
        max_wait_minutes: Maximum time to wait for extract completion
        progress_callback: Optional callback(message) for progress updates

    Returns:
        Path to saved GeoParquet file

    Raises:
        ValueError: If year is not supported
        NhgisExtractError: If extract fails
    """
    from ipumspy import IpumsApiClient

    from hhplab.naming import county_filename

    if year not in SUPPORTED_YEARS:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_YEARS))
        raise ValueError(f"Year {year} not supported. Supported years: {supported}")

    # Create API client
    client = IpumsApiClient(api_key)

    # Create extract definition
    if progress_callback:
        progress_callback(f"Creating extract for {year} counties...")

    extract = _create_extract(year, geo_type="counties")

    # Submit extract
    if progress_callback:
        progress_callback("Submitting extract to NHGIS...")

    extract = client.submit_extract(extract)

    if progress_callback:
        progress_callback("Extract submitted, waiting for completion...")

    # Wait for completion
    _wait_for_extract(
        client,
        extract,
        poll_interval_minutes=poll_interval_minutes,
        max_wait_minutes=max_wait_minutes,
        progress_callback=progress_callback,
    )

    if progress_callback:
        progress_callback("Extract complete, downloading...")

    # Download and process
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        shp_path, raw_content = _download_and_extract(client, extract, tmppath)

        if progress_callback:
            progress_callback("Reading shapefile...")

        # Read shapefile
        gdf = gpd.read_file(shp_path)

        if progress_callback:
            progress_callback(f"Loaded {len(gdf)} counties, normalizing schema...")

        # Normalize to standard schema
        gdf = _normalize_county_to_schema(gdf, year)

    # Persist raw ZIP under data/raw/nhgis/<year>/counties/
    shapefile_name = _get_shapefile_name(year, geo_type="counties")
    raw_path, content_sha256, content_size = persist_file_snapshot(
        raw_content,
        "nhgis",
        f"{shapefile_name}.zip",
        subdirs=(str(year), "counties"),
    )

    # Save to output
    curated_dir("tiger").mkdir(parents=True, exist_ok=True)
    output_path = curated_dir("tiger") / county_filename(year)
    gdf.to_parquet(output_path, index=False)

    if progress_callback:
        progress_callback(f"Saved {len(gdf)} counties to {output_path}")

    # Register in source registry (local_path → raw snapshot)
    source_url = f"nhgis://shapefiles/{shapefile_name}"

    register_source(
        source_type="nhgis_county",
        source_url=source_url,
        source_name=f"NHGIS Census Counties {year}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(raw_path),
        metadata={
            "year": year,
            "shapefile": shapefile_name,
            "county_count": len(gdf),
            "source": "nhgis",
            "curated_path": str(output_path),
        },
    )

    logger.info(f"Ingested {len(gdf)} NHGIS counties for {year} to {output_path}")

    return output_path
