"""HUD Open Data ArcGIS Hub ingester for CoC Grantee Areas.

Fetches CoC boundary geometries from the HUD ArcGIS feature service,
normalizes them to the canonical schema, and writes to GeoParquet.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import httpx
from shapely.geometry import shape

from coclab.geo import normalize_boundaries, validate_boundaries
from coclab.geo.io import curated_boundary_path, write_geoparquet
from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

# HUD ArcGIS feature service endpoint
FEATURE_SERVICE_URL = (
    "https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services"
    "/Continuum_of_Care_Grantee_Areas/FeatureServer/0/query"
)

# Source reference for metadata
SOURCE_REF = (
    "https://hudgis-hud.opendata.arcgis.com/datasets"
    "/HUD::continuum-of-care-coc-grantee-areas"
)

# Maximum records per request (API limit is 1000)
PAGE_SIZE = 1000

# Request timeout in seconds
REQUEST_TIMEOUT = 60.0


def _fetch_page(
    client: httpx.Client,
    offset: int = 0,
    page_size: int = PAGE_SIZE,
) -> dict[str, Any]:
    """Fetch a single page of features from the ArcGIS feature service.

    Args:
        client: HTTP client for making requests
        offset: Record offset for pagination
        page_size: Number of records to fetch

    Returns:
        JSON response containing features and metadata

    Raises:
        httpx.HTTPStatusError: If the request fails
    """
    params = {
        "where": "1=1",  # All features
        "outFields": "COCNUM,COCNAME,STUSAB,STATE_NAME",
        "outSR": "4326",  # WGS84 lat/lon
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": page_size,
    }

    response = client.get(FEATURE_SERVICE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _fetch_all_features(client: httpx.Client) -> tuple[list[dict[str, Any]], bytes]:
    """Fetch all features from the service with pagination.

    Args:
        client: HTTP client for making requests

    Returns:
        Tuple of (list of all GeoJSON features, combined raw response content)
    """
    all_features = []
    all_raw_content = []
    offset = 0

    while True:
        data = _fetch_page(client, offset=offset)
        features = data.get("features", [])

        if not features:
            break

        all_features.extend(features)
        # Store serialized JSON for consistent hashing
        all_raw_content.append(json.dumps(data, sort_keys=True).encode("utf-8"))

        # Check if we've received fewer than PAGE_SIZE, indicating last page
        if len(features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    combined_content = b"".join(all_raw_content)
    return all_features, combined_content


def _features_to_geodataframe(features: list[dict[str, Any]]) -> gpd.GeoDataFrame:
    """Convert GeoJSON features to a GeoDataFrame.

    Args:
        features: List of GeoJSON feature dictionaries

    Returns:
        GeoDataFrame with geometry and properties
    """
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

    gdf = gpd.GeoDataFrame(records, geometry=geometries, crs="EPSG:4326")
    return gdf


def _map_to_canonical_schema(
    gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    ingested_at: datetime,
) -> gpd.GeoDataFrame:
    """Map ArcGIS fields to the canonical boundary schema.

    Args:
        gdf: Input GeoDataFrame with ArcGIS field names
        boundary_vintage: Version identifier for this ingestion
        ingested_at: Timestamp of ingestion

    Returns:
        GeoDataFrame with canonical column names
    """
    result = gpd.GeoDataFrame(
        {
            "boundary_vintage": boundary_vintage,
            "coc_id": gdf["COCNUM"].astype(str),
            "coc_name": gdf["COCNAME"].astype(str),
            "state_abbrev": gdf["STUSAB"].astype(str),
            "source": "hud_opendata",
            "source_ref": SOURCE_REF,
            "ingested_at": ingested_at,
        },
        geometry=gdf.geometry,
        crs="EPSG:4326",
    )

    return result


def ingest_hud_opendata(
    snapshot_tag: str = "latest",
    *,
    base_dir: Path | str | None = None,
    http_client: httpx.Client | None = None,
) -> Path:
    """Ingest CoC boundaries from HUD Open Data ArcGIS feature service.

    Fetches all CoC Grantee Area boundaries, normalizes them to the
    canonical schema, validates the data, and writes to GeoParquet.

    Args:
        snapshot_tag: Snapshot identifier. Use "latest" to generate a
            date-based vintage like "HUDOpenData_2025-01-04".
        base_dir: Base data directory (defaults to "data")
        http_client: Optional HTTP client for testing/injection

    Returns:
        Path to the written GeoParquet file

    Raises:
        httpx.HTTPStatusError: If the API request fails
        ValueError: If no features are returned or validation fails with errors
    """
    ingested_at = datetime.now(UTC)

    # Generate boundary vintage from snapshot_tag
    if snapshot_tag == "latest":
        date_str = ingested_at.strftime("%Y-%m-%d")
        boundary_vintage = f"HUDOpenData_{date_str}"
    else:
        boundary_vintage = snapshot_tag

    # Fetch features from the API
    client = http_client or httpx.Client()
    should_close = http_client is None

    try:
        features, raw_content = _fetch_all_features(client)
    finally:
        if should_close:
            client.close()

    if not features:
        raise ValueError("No features returned from HUD Open Data API")

    # Compute SHA-256 hash of raw content for source registry
    content_sha256 = hashlib.sha256(raw_content).hexdigest()
    content_size = len(raw_content)

    # Check for upstream changes
    changed, details = check_source_changed(
        source_type="boundary",
        source_url=FEATURE_SERVICE_URL,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: HUD OpenData boundaries have changed since last download! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info("First time tracking HUD OpenData boundaries in source registry")

    # Convert to GeoDataFrame
    gdf = _features_to_geodataframe(features)

    # Map to canonical schema
    gdf = _map_to_canonical_schema(gdf, boundary_vintage, ingested_at)

    # Normalize geometries (adds geom_hash column)
    gdf = normalize_boundaries(gdf)

    # Validate the data
    validation_result = validate_boundaries(gdf)
    if not validation_result.is_valid:
        error_msgs = [str(e) for e in validation_result.errors]
        raise ValueError("Validation failed with errors:\n" + "\n".join(error_msgs))

    # Write to curated location
    output_path = curated_boundary_path(boundary_vintage, base_dir=base_dir)
    write_geoparquet(gdf, output_path)

    # Register this download in source registry
    register_source(
        source_type="boundary",
        source_url=FEATURE_SERVICE_URL,
        source_name=f"HUD OpenData CoC Boundaries {boundary_vintage}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(output_path),
        metadata={
            "boundary_vintage": boundary_vintage,
            "feature_count": len(features),
            "source": "hud_opendata",
        },
    )

    return output_path
