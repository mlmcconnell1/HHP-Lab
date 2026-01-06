"""HUD Exchange CoC GIS Tools ingester.

Downloads and processes year-specific CoC boundary shapefiles from HUD Exchange
and outputs canonicalized GeoParquet files.

Primary source: HUD ArcGIS Open Data (services.arcgis.com)
Fallback source: https://www.hudexchange.info/programs/coc/gis-tools/

Note: As of 2024, HUD Exchange no longer provides a single national shapefile.
The ArcGIS FeatureServer is now the most reliable source for current data.
"""

from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from shapely.geometry import shape

if TYPE_CHECKING:
    import geopandas as gpd

# HUD ArcGIS feature service endpoint (primary source)
ARCGIS_FEATURE_SERVICE_URL = (
    "https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services"
    "/Continuum_of_Care_Grantee_Areas/FeatureServer/0/query"
)

# Source reference for ArcGIS data
ARCGIS_SOURCE_REF = (
    "https://hudgis-hud.opendata.arcgis.com/datasets"
    "/HUD::continuum-of-care-coc-grantee-areas"
)

# URL template for HUD Exchange CoC GIS geodatabase downloads (legacy fallback)
# Pattern observed from historical files: CoC_GIS_NatlTerrDC_Shapefile_{YEAR}.zip
HUD_EXCHANGE_GDB_URL_TEMPLATE = (
    "https://files.hudexchange.info/resources/documents/"
    "CoC_GIS_NatlTerrDC_Shapefile_{vintage}.zip"
)

# Pagination settings for ArcGIS API
# Smaller page size to avoid timeout on large geometry payloads
ARCGIS_PAGE_SIZE = 250
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
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
        except (httpx.TimeoutException, httpx.RemoteProtocolError) as e:
            # Retry on timeout or connection reset errors
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise

    raise last_error  # type: ignore[misc]


def _fetch_all_arcgis_features(
    client: httpx.Client,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """Fetch all features from the ArcGIS service with pagination.

    Args:
        client: HTTP client for making requests
        verbose: If True, print progress messages

    Returns:
        List of all GeoJSON features
    """
    all_features: list[dict[str, Any]] = []
    offset = 0
    page_num = 1

    while True:
        if verbose:
            print(f"  Fetching page {page_num} (offset {offset})...", flush=True)

        data = _fetch_arcgis_page(client, offset=offset)
        features = data.get("features", [])

        if not features:
            break

        all_features.extend(features)

        if verbose:
            print(f"  Received {len(features)} features (total: {len(all_features)})")

        # Check if we've received fewer than PAGE_SIZE, indicating last page
        if len(features) < ARCGIS_PAGE_SIZE:
            break

        offset += ARCGIS_PAGE_SIZE
        page_num += 1

    return all_features


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

    result = gpd.GeoDataFrame(
        {
            "coc_id": gdf["COCNUM"].astype(str),
            "coc_name": gdf["COCNAME"].astype(str),
            "state_abbrev": state_values,
            "boundary_vintage": boundary_vintage,
            "source": "hud_arcgis_featureserver",
            "source_ref": ARCGIS_SOURCE_REF,
            "ingested_at": datetime.now(UTC),
        },
        geometry=gdf.geometry,
        crs="EPSG:4326",
    )

    return result


def fetch_from_arcgis(
    boundary_vintage: str,
    verbose: bool = False,
) -> gpd.GeoDataFrame:
    """Fetch CoC boundaries from HUD ArcGIS FeatureServer.

    This is the primary data source for current CoC boundary data.

    Args:
        boundary_vintage: Version identifier (e.g., "2025" or "FY2024")
        verbose: If True, print progress messages

    Returns:
        GeoDataFrame with canonical schema

    Raises:
        httpx.HTTPStatusError: If API request fails
        ValueError: If no features are returned
    """
    with httpx.Client() as client:
        features = _fetch_all_arcgis_features(client, verbose=verbose)

    if not features:
        raise ValueError("No features returned from HUD ArcGIS API")

    gdf = _arcgis_features_to_geodataframe(features)
    return _map_arcgis_to_canonical_schema(gdf, boundary_vintage)


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
        raise ValueError(
            f"Could not find CoC ID field. Available columns: {columns}"
        )

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
    result["source"] = "hud_exchange_gis_tools"
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
    verbose: bool = False,
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
        verbose: If True, print progress messages

    Returns:
        Path to the output GeoParquet file

    Raises:
        httpx.HTTPStatusError: If API/download fails
        ValueError: If data cannot be parsed or validated
    """
    from coclab.geo import normalize_boundaries, validate_boundaries
    from coclab.geo.io import curated_boundary_path, write_geoparquet

    if curated_dir is None:
        curated_dir = Path("data")
    else:
        curated_dir = Path(curated_dir)

    # Determine which source to use
    use_arcgis = not use_legacy_source and url is None and not skip_download

    if use_arcgis:
        # Primary path: fetch from ArcGIS FeatureServer
        if verbose:
            print("Fetching from HUD ArcGIS FeatureServer...")
        gdf = fetch_from_arcgis(boundary_vintage, verbose=verbose)
        if verbose:
            print(f"Fetched {len(gdf)} CoC boundaries.")
    else:
        # Legacy path: download ZIP from HUD Exchange
        if raw_dir is None:
            raw_dir = Path("data/raw/hud_exchange") / boundary_vintage
        else:
            raw_dir = Path(raw_dir)

        if url is None:
            url = HUD_EXCHANGE_GDB_URL_TEMPLATE.format(vintage=boundary_vintage)

        if not skip_download:
            data_path = download_hud_exchange_gdb(
                boundary_vintage=boundary_vintage,
                output_dir=raw_dir,
                url=url,
            )
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

    # Normalize boundaries (CRS, fix geometries, compute geom_hash)
    if verbose:
        print("Normalizing boundaries...")
    gdf = normalize_boundaries(gdf)

    # Validate boundaries
    if verbose:
        print("Validating boundaries...")
    validation_result = validate_boundaries(gdf)
    if not validation_result.is_valid:
        raise ValueError(f"Validation failed:\n{validation_result}")

    # Write curated GeoParquet
    if verbose:
        print("Writing GeoParquet file...")
    output_path = curated_boundary_path(boundary_vintage, base_dir=curated_dir)
    write_geoparquet(gdf, output_path)

    return output_path
