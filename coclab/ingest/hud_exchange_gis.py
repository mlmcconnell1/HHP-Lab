"""HUD Exchange CoC GIS Tools ingester.

Downloads and processes year-specific CoC boundary shapefiles from HUD Exchange
and outputs canonicalized GeoParquet files.

Primary source: HUD ArcGIS Open Data (services.arcgis.com)
Fallback source: https://www.hudexchange.info/programs/coc/gis-tools/

Note: As of 2024, HUD Exchange no longer provides a single national shapefile.
The ArcGIS FeatureServer is now the most reliable source for current data.
"""

from __future__ import annotations

import hashlib
import json
import logging
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from shapely.geometry import shape

from coclab.raw_snapshot import make_run_id, write_api_snapshot
from coclab.source_registry import check_source_changed, register_source
from coclab.sources import (
    HUD_ARCGIS_COC_FEATURE_SERVICE,
    HUD_ARCGIS_COC_SOURCE_REF,
    HUD_EXCHANGE_COC_GDB_TEMPLATE,
)

if TYPE_CHECKING:
    import geopandas as gpd

logger = logging.getLogger(__name__)

# HUD ArcGIS feature service endpoint (primary source)
ARCGIS_FEATURE_SERVICE_URL = HUD_ARCGIS_COC_FEATURE_SERVICE

# Source reference for ArcGIS data
ARCGIS_SOURCE_REF = HUD_ARCGIS_COC_SOURCE_REF

# URL template for HUD Exchange CoC GIS geodatabase downloads (legacy fallback)
# Pattern observed from historical files: CoC_GIS_NatlTerrDC_Shapefile_{YEAR}.zip
HUD_EXCHANGE_GDB_URL_TEMPLATE = HUD_EXCHANGE_COC_GDB_TEMPLATE

# Pagination settings for ArcGIS API
# Smaller page size for more frequent progress updates
ARCGIS_PAGE_SIZE = 50
ARCGIS_REQUEST_TIMEOUT = 120.0

# Known field name mappings across different vintage years
# HUD has changed column names over time, so we map all known variants
COC_ID_FIELDS = ["COCNUM", "COC_NUM", "COCNUMBER", "CocNum", "coc_number", "CoC_Num"]
COC_NAME_FIELDS = ["COCNAME", "COC_NAME", "CocName", "coc_name", "CoC_Name"]
STATE_FIELDS = ["ST", "STATE", "State", "state", "STATE_ABBR", "state_abbrev"]


def _find_field(columns: list[str], candidates: list[str]) -> str | None:
    """Find the first matching field name from a list of candidates."""
    columns_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate in columns:
            return candidate
        # Case-insensitive fallback
        if candidate.lower() in columns_lower:
            return columns_lower[candidate.lower()]
    return None


def _extract_state_from_coc_id(coc_id: str) -> str:
    """Extract state abbreviation from CoC ID (e.g., 'CO-500' -> 'CO')."""
    if coc_id and "-" in coc_id:
        return coc_id.split("-")[0]
    return ""


# =============================================================================
# ArcGIS FeatureServer Functions (Primary Source)
# =============================================================================


def _get_arcgis_feature_count(client: httpx.Client) -> int:
    """Get the total feature count from the ArcGIS service.

    Args:
        client: HTTP client for making requests

    Returns:
        Total number of features available
    """
    params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    }
    response = client.get(ARCGIS_FEATURE_SERVICE_URL, params=params, timeout=ARCGIS_REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("count", 0)


def _fetch_arcgis_page(
    client: httpx.Client,
    offset: int = 0,
    page_size: int = ARCGIS_PAGE_SIZE,
    max_retries: int = 3,
) -> dict[str, Any]:
    """Fetch a single page of features from the ArcGIS feature service.

    Args:
        client: HTTP client for making requests
        offset: Record offset for pagination
        page_size: Number of records to fetch
        max_retries: Maximum retry attempts for transient errors

    Returns:
        JSON response containing features and metadata

    Raises:
        httpx.HTTPStatusError: If the request fails after retries
    """
    import time

    params = {
        "where": "1=1",  # All features
        "outFields": "COCNUM,COCNAME,STUSAB,STATE_NAME",
        "outSR": "4326",  # WGS84 lat/lon
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": page_size,
    }

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = client.get(
                ARCGIS_FEATURE_SERVICE_URL, params=params, timeout=ARCGIS_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            last_error = e
            # Retry on 5xx errors (server-side issues)
            if e.response.status_code >= 500 and attempt < max_retries - 1:
                time.sleep(2**attempt)  # Exponential backoff
                continue
            raise
        except (httpx.TimeoutException, httpx.RemoteProtocolError) as e:
            # Retry on timeout or connection reset errors
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
                continue
            raise

    raise last_error  # type: ignore[misc]


def _fetch_all_arcgis_features(
    client: httpx.Client,
    progress_callback: Any | None = None,
) -> tuple[list[dict[str, Any]], list[bytes]]:
    """Fetch all features from the ArcGIS service with pagination.

    Args:
        client: HTTP client for making requests
        progress_callback: Optional callback(count) called after each page fetch

    Returns:
        Tuple of (list of all GeoJSON features, list of raw response page payloads)
    """
    all_features: list[dict[str, Any]] = []
    all_raw_content: list[bytes] = []
    offset = 0

    while True:
        data = _fetch_arcgis_page(client, offset=offset)
        features = data.get("features", [])

        if not features:
            break

        all_features.extend(features)
        all_raw_content.append(json.dumps(data, sort_keys=True).encode("utf-8"))

        if progress_callback:
            progress_callback(len(features))

        # Check if we've received fewer than PAGE_SIZE, indicating last page
        if len(features) < ARCGIS_PAGE_SIZE:
            break

        offset += ARCGIS_PAGE_SIZE

    return all_features, all_raw_content


def _arcgis_features_to_geodataframe(
    features: list[dict[str, Any]],
) -> gpd.GeoDataFrame:
    """Convert GeoJSON features from ArcGIS to a GeoDataFrame.

    Args:
        features: List of GeoJSON feature dictionaries

    Returns:
        GeoDataFrame with geometry and properties
    """
    import geopandas as gpd

    if not features:
        raise ValueError("No features to convert")

    records = []
    geometries = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")

        if geom:
            geometries.append(shape(geom))
            records.append(props)

    return gpd.GeoDataFrame(records, geometry=geometries, crs="EPSG:4326")


def _map_arcgis_to_canonical_schema(
    gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
) -> gpd.GeoDataFrame:
    """Map ArcGIS fields to the canonical boundary schema.

    Args:
        gdf: Input GeoDataFrame with ArcGIS field names
        boundary_vintage: Version identifier for this ingestion

    Returns:
        GeoDataFrame with canonical column names
    """
    import geopandas as gpd

    # Handle state field - ArcGIS uses STUSAB, but values may be null
    # In that case, extract state abbreviation from CoC ID (e.g., "AK-500" -> "AK")
    state_values = None
    for field in ["STUSAB", "STATE", "ST"]:
        if field in gdf.columns and gdf[field].notna().any():
            state_values = gdf[field].astype(str)
            break

    if state_values is None:
        # Extract from CoC ID if state field not available or all null
        state_values = gdf["COCNUM"].apply(_extract_state_from_coc_id)
    else:
        # Fill 'nan' string entries (from NaN→astype(str)) using CoC ID fallback
        nan_mask = state_values == "nan"
        if nan_mask.any():
            state_values = state_values.where(
                ~nan_mask, gdf["COCNUM"].apply(_extract_state_from_coc_id)
            )

    result = gpd.GeoDataFrame(
        {
            "coc_id": gdf["COCNUM"].astype(str),
            "coc_name": gdf["COCNAME"].astype(str),
            "state_abbrev": state_values,
            "boundary_vintage": boundary_vintage,
            "source": "hud_exchange",
            "source_ref": ARCGIS_SOURCE_REF,
            "ingested_at": datetime.now(UTC),
        },
        geometry=gdf.geometry,
        crs="EPSG:4326",
    )

    return result


def fetch_from_arcgis(
    boundary_vintage: str,
    show_progress: bool = False,
) -> tuple[gpd.GeoDataFrame, list[bytes]]:
    """Fetch CoC boundaries from HUD ArcGIS FeatureServer.

    This is the primary data source for current CoC boundary data.

    Args:
        boundary_vintage: Version identifier (e.g., "2025" or "FY2024")
        show_progress: If True, display a progress bar

    Returns:
        Tuple of (GeoDataFrame with canonical schema, raw response page payloads)

    Raises:
        httpx.HTTPStatusError: If API request fails
        ValueError: If no features are returned
    """
    import click

    with httpx.Client() as client:
        if show_progress:
            total_count = _get_arcgis_feature_count(client)
            with click.progressbar(
                length=total_count,
                label="Fetching CoC boundaries",
                show_pos=True,
            ) as bar:
                features, raw_content = _fetch_all_arcgis_features(
                    client, progress_callback=bar.update
                )
        else:
            features, raw_content = _fetch_all_arcgis_features(client)

    if not features:
        raise ValueError("No features returned from HUD ArcGIS API")

    gdf = _arcgis_features_to_geodataframe(features)
    return _map_arcgis_to_canonical_schema(gdf, boundary_vintage), raw_content


def _hash_file(path: Path) -> tuple[str, int]:
    """Hash a file by content. Returns (sha256, size)."""
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def _hash_directory(path: Path) -> tuple[str, int]:
    """Hash a directory by hashing all files in sorted path order."""
    hasher = hashlib.sha256()
    size = 0
    for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
        hasher.update(str(file_path.relative_to(path)).encode("utf-8"))
        file_hash, file_size = _hash_file(file_path)
        hasher.update(file_hash.encode("utf-8"))
        size += file_size
    return hasher.hexdigest(), size


def _hash_shapefile(path: Path) -> tuple[str, int]:
    """Hash a shapefile by including sibling component files."""
    components = [".shp", ".shx", ".dbf", ".prj", ".cpg"]
    files = [path.with_suffix(ext) for ext in components]
    hasher = hashlib.sha256()
    size = 0
    for file_path in files:
        if file_path.exists():
            hasher.update(file_path.name.encode("utf-8"))
            file_hash, file_size = _hash_file(file_path)
            hasher.update(file_hash.encode("utf-8"))
            size += file_size
    return hasher.hexdigest(), size


def _hash_local_path(path: Path) -> tuple[str, int]:
    """Hash a local path (file, shapefile, or directory)."""
    if path.is_dir():
        return _hash_directory(path)
    if path.suffix.lower() == ".shp":
        return _hash_shapefile(path)
    return _hash_file(path)


# =============================================================================
# HUD Exchange ZIP Download Functions (Legacy Fallback)
# =============================================================================


def download_hud_exchange_gdb(
    boundary_vintage: str,
    output_dir: Path | str | None = None,
    url: str | None = None,
) -> Path:
    """Download CoC GIS geodatabase/shapefile from HUD Exchange.

    Args:
        boundary_vintage: Year of the boundary data (e.g., "2024")
        output_dir: Directory to save the downloaded file. Defaults to
            data/raw/hud_exchange/{vintage}/
        url: Override URL for downloading. If not provided, uses the standard
            HUD Exchange URL template.

    Returns:
        Path to the extracted geodatabase/shapefile directory

    Raises:
        httpx.HTTPStatusError: If download fails
        zipfile.BadZipFile: If downloaded file is not a valid zip
    """
    if output_dir is None:
        output_dir = Path("data/raw/hud_exchange") / boundary_vintage
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    if url is None:
        url = HUD_EXCHANGE_GDB_URL_TEMPLATE.format(vintage=boundary_vintage)

    zip_path = output_dir / f"CoC_GIS_{boundary_vintage}.zip"

    # Download the file
    with httpx.Client(follow_redirects=True, timeout=120.0) as client:
        response = client.get(url)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            f.write(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(output_dir)

    # Find the extracted .gdb or shapefile directory
    # The zip typically contains a .gdb folder or .shp files
    gdb_dirs = list(output_dir.glob("*.gdb"))
    if gdb_dirs:
        return gdb_dirs[0]

    # Look for shapefiles instead
    shp_files = list(output_dir.glob("*.shp"))
    if shp_files:
        return shp_files[0]

    # Return the output directory itself if no .gdb found
    return output_dir


def read_coc_boundaries(path: Path | str) -> gpd.GeoDataFrame:
    """Read CoC boundary data from a geodatabase or shapefile.

    Args:
        path: Path to .gdb directory or .shp file

    Returns:
        GeoDataFrame with raw boundary data
    """
    import geopandas as gpd

    path = Path(path)

    if path.suffix == ".gdb" or path.is_dir():
        # For geodatabases, we need to find the layer
        import fiona

        layers = fiona.listlayers(str(path))
        # Look for a layer with "CoC" or "boundary" in the name
        coc_layer = None
        for layer in layers:
            if "coc" in layer.lower() or "boundary" in layer.lower():
                coc_layer = layer
                break
        if coc_layer is None and layers:
            coc_layer = layers[0]

        if coc_layer:
            return gpd.read_file(path, layer=coc_layer)
        raise ValueError(f"No suitable layer found in geodatabase: {path}")

    # Regular shapefile
    return gpd.read_file(path)


def map_to_canonical_schema(
    gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    source_url: str,
) -> gpd.GeoDataFrame:
    """Map source fields to canonical boundary schema.

    Args:
        gdf: Raw GeoDataFrame from HUD Exchange
        boundary_vintage: Vintage year string
        source_url: URL the data was downloaded from

    Returns:
        GeoDataFrame with canonical schema columns
    """
    import geopandas as gpd

    columns = list(gdf.columns)

    # Find the CoC ID field
    coc_id_field = _find_field(columns, COC_ID_FIELDS)
    if coc_id_field is None:
        raise ValueError(f"Could not find CoC ID field. Available columns: {columns}")

    # Find the CoC name field
    coc_name_field = _find_field(columns, COC_NAME_FIELDS)

    # Find the state field
    state_field = _find_field(columns, STATE_FIELDS)

    # Build canonical DataFrame
    result = gpd.GeoDataFrame(geometry=gdf.geometry, crs=gdf.crs)

    result["coc_id"] = gdf[coc_id_field].astype(str).str.strip()

    if coc_name_field:
        result["coc_name"] = gdf[coc_name_field].astype(str).str.strip()
    else:
        result["coc_name"] = ""

    if state_field:
        result["state_abbrev"] = gdf[state_field].astype(str).str.strip()
    else:
        # Extract from coc_id if state field not available
        result["state_abbrev"] = result["coc_id"].apply(_extract_state_from_coc_id)

    result["boundary_vintage"] = boundary_vintage
    result["source"] = "hud_exchange"
    result["source_ref"] = source_url
    result["ingested_at"] = datetime.now(UTC)

    return result


def ingest_hud_exchange(
    boundary_vintage: str,
    *,
    url: str | None = None,
    raw_dir: Path | str | None = None,
    curated_dir: Path | str | None = None,
    skip_download: bool = False,
    use_legacy_source: bool = False,
    show_progress: bool = False,
) -> Path:
    """Ingest CoC boundaries from HUD data sources.

    By default, fetches data from the HUD ArcGIS FeatureServer (most reliable).
    Falls back to legacy HUD Exchange ZIP download if use_legacy_source=True
    or if a specific URL is provided.

    Args:
        boundary_vintage: Year of the boundary data (e.g., "2024", "2025")
        url: Override URL for downloading from HUD Exchange (implies legacy source)
        raw_dir: Directory for raw downloads (legacy source only)
        curated_dir: Base directory for curated output. Defaults to data/
        skip_download: If True, reads from local files in raw_dir (legacy source)
        use_legacy_source: Force use of HUD Exchange ZIP download instead of ArcGIS
        show_progress: If True, display a progress bar

    Returns:
        Path to the output GeoParquet file

    Raises:
        httpx.HTTPStatusError: If API/download fails
        ValueError: If data cannot be parsed or validated
    """
    import click

    from coclab.geo import normalize_boundaries, validate_boundaries
    from coclab.geo.io import curated_boundary_path, write_geoparquet

    if curated_dir is None:
        curated_dir = Path("data")
    else:
        curated_dir = Path(curated_dir)

    # Determine which source to use
    use_arcgis = not use_legacy_source and url is None and not skip_download

    source_url: str | None = None
    content_sha256: str | None = None
    content_size: int | None = None

    if use_arcgis:
        # Primary path: fetch from ArcGIS FeatureServer
        gdf, raw_pages = fetch_from_arcgis(boundary_vintage, show_progress=show_progress)
        source_url = ARCGIS_FEATURE_SERVICE_URL

        # Persist raw API snapshot per retention policy
        run_id = make_run_id()
        snap_dir, content_sha256, content_size = write_api_snapshot(
            raw_pages,
            "hud_exchange",
            year=boundary_vintage,
            variant=run_id,
            request_metadata={
                "url": ARCGIS_FEATURE_SERVICE_URL,
                "params": {
                    "where": "1=1",
                    "outFields": "COCNUM,COCNAME,STUSAB,STATE_NAME",
                    "outSR": "4326",
                    "f": "geojson",
                    "resultRecordCount": ARCGIS_PAGE_SIZE,
                },
                "page_size": ARCGIS_PAGE_SIZE,
            },
            record_count=len(gdf),
        )
        raw_local_path = str(snap_dir)

        if show_progress:
            click.echo(f"Fetched {len(gdf)} CoC boundaries.")
    else:
        # Legacy path: download ZIP from HUD Exchange
        if raw_dir is None:
            raw_dir = Path("data/raw/hud_exchange") / boundary_vintage
        else:
            raw_dir = Path(raw_dir)

        if url is None:
            url = HUD_EXCHANGE_GDB_URL_TEMPLATE.format(vintage=boundary_vintage)
        source_url = url

        if not skip_download:
            data_path = download_hud_exchange_gdb(
                boundary_vintage=boundary_vintage,
                output_dir=raw_dir,
                url=url,
            )
            zip_path = raw_dir / f"CoC_GIS_{boundary_vintage}.zip"
            if zip_path.exists():
                from coclab.raw_snapshot import hash_zip_contents

                raw_bytes = zip_path.read_bytes()
                content_sha256 = hash_zip_contents(raw_bytes)
                content_size = len(raw_bytes)
        else:
            # Find existing data in raw_dir
            gdb_dirs = list(raw_dir.glob("*.gdb"))
            shp_files = list(raw_dir.glob("*.shp"))
            if gdb_dirs:
                data_path = gdb_dirs[0]
            elif shp_files:
                data_path = shp_files[0]
            else:
                raise ValueError(f"No .gdb or .shp found in {raw_dir}")

        gdf = read_coc_boundaries(data_path)
        gdf = map_to_canonical_schema(gdf, boundary_vintage, url)

        if content_sha256 is None or content_size is None:
            content_sha256, content_size = _hash_local_path(Path(data_path))

        raw_local_path = str(raw_dir)

    # Normalize boundaries (CRS, fix geometries, compute geom_hash)
    if show_progress:
        click.echo("Normalizing boundaries...")
    gdf = normalize_boundaries(gdf)

    # Validate boundaries
    if show_progress:
        click.echo("Validating boundaries...")
    validation_result = validate_boundaries(gdf)
    if not validation_result.is_valid:
        raise ValueError(f"Validation failed:\n{validation_result}")

    # Write curated GeoParquet
    if show_progress:
        click.echo("Writing GeoParquet file...")
    output_path = curated_boundary_path(boundary_vintage, base_dir=curated_dir)
    write_geoparquet(gdf, output_path)

    # Register in boundary registry
    from coclab.registry import register_vintage

    source = "hud_exchange"
    register_vintage(
        boundary_vintage=boundary_vintage,
        source=source,
        path=output_path,
        feature_count=len(gdf),
    )

    # Register in source registry
    if source_url is None or content_sha256 is None or content_size is None:
        raise ValueError("Source registry metadata missing for HUD Exchange ingest.")

    changed, details = check_source_changed(
        source_type="boundary",
        source_url=source_url,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            "UPSTREAM DATA CHANGED: HUD Exchange boundaries have changed since last download! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info("First time tracking HUD Exchange boundaries in source registry")

    register_source(
        source_type="boundary",
        source_url=source_url,
        source_name=f"HUD Exchange CoC Boundaries {boundary_vintage}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=raw_local_path,
        metadata={
            "boundary_vintage": boundary_vintage,
            "feature_count": len(gdf),
            "source": source,
            "curated_path": str(output_path),
        },
    )

    return output_path
