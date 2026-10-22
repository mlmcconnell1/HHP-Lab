"""HUD Exchange CoC GIS Tools ingester.

Downloads and processes year-specific CoC boundary shapefiles from HUD Exchange
and outputs canonicalized GeoParquet files.

Year-specific source: HUD Exchange national/per-state boundary archives.
Current snapshot source: HUD ArcGIS Open Data (services.arcgis.com), labeled by
the UTC retrieval date rather than an inferred fiscal year.

Note: As of 2024, HUD Exchange no longer provides a single national shapefile.
The ArcGIS FeatureServer is now the most reliable source for current data.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from shapely.geometry import shape

from hhplab.geographies.boundaries.hud.urls import (
    HUD_ARCGIS_COC_FEATURE_SERVICE,
    HUD_ARCGIS_COC_SOURCE_REF,
    HUD_EXCHANGE_COC_GDB_TEMPLATE,
    HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE,
    HUD_EXCHANGE_COC_STATE_SHAPEFILE_TEMPLATE,
)
from hhplab.registry.source_registry import check_source_changed, register_source
from hhplab.storage.paths import raw_root
from hhplab.storage.raw_snapshot import make_run_id, write_api_snapshot

if TYPE_CHECKING:
    import geopandas as gpd

logger = logging.getLogger(__name__)

# HUD ArcGIS feature service endpoint (primary source)
ARCGIS_FEATURE_SERVICE_URL = HUD_ARCGIS_COC_FEATURE_SERVICE

# Source reference for ArcGIS data
ARCGIS_SOURCE_REF = HUD_ARCGIS_COC_SOURCE_REF

# URL templates for HUD Exchange CoC GIS geodatabase downloads (legacy fallback).
# The national boundary URL is now the primary legacy source; the original
# NatlTerrDC shapefile template no longer works for any vintage year.
HUD_EXCHANGE_GDB_URL_TEMPLATE = HUD_EXCHANGE_COC_GDB_TEMPLATE
HUD_EXCHANGE_NATIONAL_BOUNDARY_TEMPLATE = HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE
HUD_EXCHANGE_STATE_SHAPEFILE_TEMPLATE = HUD_EXCHANGE_COC_STATE_SHAPEFILE_TEMPLATE

# State/territory abbreviations used by HUD Exchange per-state shapefiles.
# Covers all 50 states, DC, and the five populated US territories.
_HUD_STATE_ABBREVIATIONS = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "GU",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MP",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "PR",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VI",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]

# Pagination settings for ArcGIS API
# Smaller page size for more frequent progress updates
ARCGIS_PAGE_SIZE = 50
ARCGIS_REQUEST_TIMEOUT = 120.0


@dataclass(frozen=True)
class _ArchiveSource:
    """One successfully downloaded HUD archive and its consumed-byte identity."""

    url: str
    raw_sha256: str
    file_size: int


@dataclass(frozen=True)
class _HudExchangeDownload:
    """Extracted data path plus provenance for the archive inputs it uses."""

    path: Path
    archive_sources: tuple[_ArchiveSource, ...]

    @property
    def source_url(self) -> str:
        """Return a source reference that preserves every consumed archive URL."""
        if len(self.archive_sources) == 1:
            return self.archive_sources[0].url
        return json.dumps([source.url for source in self.archive_sources], separators=(",", ":"))

    @property
    def raw_sha256(self) -> str:
        """Return archive SHA-256 or a digest of ordered component digests.

        One archive uses its raw ZIP SHA-256 directly. Multiple archives use
        SHA-256 over the concatenated binary SHA-256 digests, in download
        order; per-archive raw ZIP hashes remain available in archive_sources.
        """
        if len(self.archive_sources) == 1:
            return self.archive_sources[0].raw_sha256
        hasher = hashlib.sha256()
        for source in self.archive_sources:
            hasher.update(bytes.fromhex(source.raw_sha256))
        return hasher.hexdigest()

    @property
    def file_size(self) -> int:
        return sum(source.file_size for source in self.archive_sources)


_ARCGIS_SNAPSHOT_LABEL = re.compile(r"^HUDOpenData_(\d{4}-\d{2}-\d{2})$")


def _should_use_arcgis_source(boundary_vintage: str) -> bool:
    """Return whether a label explicitly requests a dated live ArcGIS snapshot.

    Numeric labels always describe a year-specific boundary vintage and are
    sourced from that year's archive. The live FeatureServer can only be used
    with a retrieval-date label, which is validated again before the request.
    """
    return boundary_vintage.startswith("HUDOpenData_")


def _validate_arcgis_snapshot_label(boundary_vintage: str) -> str:
    """Validate and return the UTC retrieval date encoded in a snapshot label."""
    match = _ARCGIS_SNAPSHOT_LABEL.fullmatch(boundary_vintage)
    if match is None:
        raise ValueError(
            "Live HUD ArcGIS fetches require a retrieval-date label in the form "
            "'HUDOpenData_YYYY-MM-DD'. Numeric boundary vintages use year-specific "
            "HUD Exchange archives through ingest_hud_exchange()."
        )

    try:
        requested_date = date.fromisoformat(match.group(1))
    except ValueError as exc:
        raise ValueError(
            "Invalid HUD ArcGIS snapshot date; use a valid 'HUDOpenData_YYYY-MM-DD' "
            "label matching the current UTC date."
        ) from exc

    current_date = datetime.now(UTC).date()
    if requested_date != current_date:
        raise ValueError(
            f"HUD ArcGIS snapshot label {boundary_vintage!r} does not match today's "
            f"UTC date ({current_date.isoformat()}); use 'HUDOpenData_{current_date.isoformat()}' "
            "for a live snapshot or ingest a year-specific archive."
        )
    return requested_date.isoformat()

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
    """Fetch a same-day CoC snapshot from HUD ArcGIS FeatureServer.

    This live service does not establish historical geometry for an arbitrary
    year. Its retrieval date must be explicit and match today's UTC date.

    Args:
        boundary_vintage: Current retrieval-date label, e.g. "HUDOpenData_2026-10-21".
        show_progress: If True, display a progress bar

    Returns:
        Tuple of (GeoDataFrame with canonical schema, raw response page payloads)

    Raises:
        httpx.HTTPStatusError: If API request fails
        ValueError: If no features are returned
    """
    import click

    _validate_arcgis_snapshot_label(boundary_vintage)

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

    _validate_arcgis_snapshot_label(boundary_vintage)

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


def _download_and_extract_zip(
    url: str,
    output_dir: Path,
    zip_name: str,
) -> tuple[Path, _ArchiveSource] | None:
    """Download a ZIP, extract it, and return the path to geo data inside.

    Returns ``None`` if the download fails with an HTTP error (e.g. 404).
    """
    zip_path = output_dir / zip_name
    try:
        with httpx.Client(follow_redirects=True, timeout=120.0) as client:
            response = client.get(url)
            response.raise_for_status()
            raw_sha256 = hashlib.sha256(response.content).hexdigest()
            file_size = len(response.content)
            with open(zip_path, "wb") as f:
                f.write(response.content)
    except httpx.HTTPStatusError as exc:
        logger.debug(
            "Download failed for %s: %s %s",
            url,
            exc.response.status_code,
            exc.response.reason_phrase,
        )
        return None
    except (httpx.TimeoutException, httpx.ConnectError, OSError) as exc:
        logger.debug("Download failed for %s: %s", url, exc)
        return None

    archive_source = _ArchiveSource(url=url, raw_sha256=raw_sha256, file_size=file_size)
    # Isolate extraction by the bytes just downloaded so stale files already
    # present in output_dir cannot be mistaken for members of this archive.
    extract_dir = output_dir / f"{Path(zip_name).stem}_{raw_sha256[:12]}"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    gdb_dirs = list(extract_dir.rglob("*.gdb"))
    if gdb_dirs:
        return gdb_dirs[0], archive_source
    shp_files = list(extract_dir.glob("*.shp"))
    if shp_files:
        return shp_files[0], archive_source
    nested_shp_files = list(extract_dir.rglob("*.shp"))
    if nested_shp_files:
        return extract_dir, archive_source
    return extract_dir, archive_source


def _download_per_state_shapefiles(
    boundary_vintage: str,
    output_dir: Path,
) -> _HudExchangeDownload | None:
    """Download per-state shapefiles from HUD Exchange and merge them.

    Returns the path to the merged shapefile directory, or ``None`` if too
    few states succeed.
    """
    import geopandas as gpd

    state_dir = output_dir / "per_state"
    state_dir.mkdir(parents=True, exist_ok=True)

    gdfs: list[gpd.GeoDataFrame] = []
    archive_sources: list[_ArchiveSource] = []
    failed: list[str] = []

    for state in _HUD_STATE_ABBREVIATIONS:
        url = HUD_EXCHANGE_STATE_SHAPEFILE_TEMPLATE.format(
            state=state,
            vintage=boundary_vintage,
        )
        sub_dir = state_dir / state
        sub_dir.mkdir(parents=True, exist_ok=True)
        result = _download_and_extract_zip(
            url,
            sub_dir,
            f"CoC_GIS_{state}_{boundary_vintage}.zip",
        )
        if result is None:
            failed.append(state)
            continue
        try:
            data_path, archive_source = result
            gdf = read_coc_boundaries(data_path)
            gdfs.append(gdf)
            archive_sources.append(archive_source)
        except Exception:
            logger.warning("Failed to read shapefile for state %s", state)
            failed.append(state)

    if not gdfs:
        logger.warning(
            "Per-state shapefile download yielded no data for vintage %s",
            boundary_vintage,
        )
        return None

    total = len(_HUD_STATE_ABBREVIATIONS)
    success_rate = len(gdfs) / total

    if failed:
        logger.info(
            "Per-state downloads: %d/%d succeeded, %d failed (%s)",
            len(gdfs),
            total,
            len(failed),
            ", ".join(failed),
        )

    # Require at least 90% of states to succeed to avoid silent partial data.
    min_success_rate = 0.9
    if success_rate < min_success_rate:
        logger.warning(
            "Per-state fallback incomplete: only %d/%d states succeeded "
            "(%.0f%%, threshold %.0f%%). Returning None.",
            len(gdfs),
            total,
            success_rate * 100,
            min_success_rate * 100,
        )
        return None

    merged = gpd.pd.concat(gdfs, ignore_index=True)
    merged_gdf = gpd.GeoDataFrame(merged, geometry="geometry")
    merged_path = state_dir / f"merged_{boundary_vintage}.shp"
    merged_gdf.to_file(merged_path)
    return _HudExchangeDownload(path=merged_path, archive_sources=tuple(archive_sources))


def download_hud_exchange_gdb(
    boundary_vintage: str,
    output_dir: Path | str | None = None,
    url: str | None = None,
    *,
    return_provenance: bool = False,
) -> Path | _HudExchangeDownload:
    """Download CoC GIS geodatabase/shapefile from HUD Exchange.

    Tries the following sources in order:

    1. Explicit *url* (if provided).
    2. National boundary ZIP
       (``CoC_GIS_National_Boundary_{vintage}.zip`` — preferred, most complete).
    3. Legacy NatlTerrDC shapefile ZIP (original template, now broken for most
       vintages — kept for backward compatibility).
    4. Per-state shapefiles downloaded and merged.

    Args:
        boundary_vintage: Year of the boundary data (e.g., "2024")
        output_dir: Directory to save the downloaded file. Defaults to
            ``data/raw/hud_exchange/{vintage}/``
        url: Override URL for downloading. Skips the fallback chain.
        return_provenance: If True, return archive source URL/hash metadata with
            the extracted path. Defaults to the historical Path return value.

    Returns:
        Path to the extracted geodatabase/shapefile directory

    Raises:
        RuntimeError: If all download sources fail.
    """
    if output_dir is None:
        output_dir = raw_root() / "hud_exchange" / boundary_vintage
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Explicit URL (skip fallback chain) ---
    if url is not None:
        result = _download_and_extract_zip(
            url,
            output_dir,
            f"CoC_GIS_{boundary_vintage}.zip",
        )
        if result is not None:
            data_path, archive_source = result
            download = _HudExchangeDownload(data_path, (archive_source,))
            return download if return_provenance else download.path
        raise RuntimeError(f"Download from explicit URL failed: {url}")

    # --- Fallback chain ---
    urls_tried: list[str] = []

    # 1. National boundary ZIP (preferred)
    national_url = HUD_EXCHANGE_NATIONAL_BOUNDARY_TEMPLATE.format(
        vintage=boundary_vintage,
    )
    urls_tried.append(national_url)
    result = _download_and_extract_zip(
        national_url,
        output_dir,
        f"CoC_GIS_National_Boundary_{boundary_vintage}.zip",
    )
    if result is not None:
        data_path, archive_source = result
        download = _HudExchangeDownload(data_path, (archive_source,))
        return download if return_provenance else download.path
    logger.info(
        "National boundary URL unavailable for vintage %s, trying legacy URL",
        boundary_vintage,
    )

    # 2. Legacy NatlTerrDC shapefile ZIP
    legacy_url = HUD_EXCHANGE_GDB_URL_TEMPLATE.format(vintage=boundary_vintage)
    urls_tried.append(legacy_url)
    result = _download_and_extract_zip(
        legacy_url,
        output_dir,
        f"CoC_GIS_{boundary_vintage}.zip",
    )
    if result is not None:
        data_path, archive_source = result
        download = _HudExchangeDownload(data_path, (archive_source,))
        return download if return_provenance else download.path
    logger.info(
        "Legacy shapefile URL unavailable for vintage %s, trying per-state downloads",
        boundary_vintage,
    )

    # 3. Per-state shapefiles
    urls_tried.append(
        HUD_EXCHANGE_STATE_SHAPEFILE_TEMPLATE.format(
            state="<STATE>",
            vintage=boundary_vintage,
        )
    )
    result = _download_per_state_shapefiles(boundary_vintage, output_dir)
    if result is not None:
        return result if return_provenance else result.path

    raise RuntimeError(
        f"All HUD Exchange download sources failed for vintage {boundary_vintage}.\n"
        f"URLs tried:\n"
        + "\n".join(f"  - {u}" for u in urls_tried)
        + "\n\nYou can manually download boundary data from:\n"
        f"  National:   {national_url}\n"
        f"  Per-state:  {
            HUD_EXCHANGE_STATE_SHAPEFILE_TEMPLATE.format(
                state='<ST>',
                vintage=boundary_vintage,
            )
        }"
    )


def read_coc_boundaries(path: Path | str) -> gpd.GeoDataFrame:
    """Read CoC boundary data from a geodatabase or shapefile.

    Args:
        path: Path to .gdb directory or .shp file

    Returns:
        GeoDataFrame with raw boundary data
    """
    import geopandas as gpd

    path = Path(path)

    if path.suffix == ".gdb":
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

    if path.is_dir():
        gdb_dirs = sorted(path.glob("*.gdb"))
        if gdb_dirs:
            return read_coc_boundaries(gdb_dirs[0])

        shp_files = sorted(path.rglob("*.shp"))
        if not shp_files:
            raise ValueError(f"No .gdb or .shp found in {path}")
        boundary_shp_files = [
            shp_path
            for shp_path in shp_files
            if not any("_CDBG" in part.upper() for part in shp_path.parts)
        ]
        if boundary_shp_files:
            shp_files = boundary_shp_files

        frames = [gpd.read_file(shp_path) for shp_path in shp_files]
        crs = next((frame.crs for frame in frames if frame.crs is not None), None)
        merged = gpd.pd.concat(frames, ignore_index=True)
        return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)

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

    Numeric boundary vintages use year-specific HUD Exchange archives by
    default. The live ArcGIS FeatureServer is used only when ``boundary_vintage``
    is an explicit ``HUDOpenData_YYYY-MM-DD`` retrieval label matching today's
    UTC date; it is never assigned an arbitrary year.

    Args:
        boundary_vintage: Archived year (e.g., "2020") or today's explicit live
            snapshot label (e.g., "HUDOpenData_2026-10-21").
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

    from hhplab.geographies.coc import normalize_boundaries, validate_boundaries
    from hhplab.geographies.coc.coc_io import curated_boundary_path, write_geoparquet

    if curated_dir is None:
        curated_dir = Path("data")
    else:
        curated_dir = Path(curated_dir)

    # Numeric vintages always use year-specific archives. The current ArcGIS
    # layer is valid only under a same-day retrieval label, never an FY/year.
    use_arcgis = (
        not use_legacy_source
        and url is None
        and not skip_download
        and _should_use_arcgis_source(boundary_vintage)
    )

    source_url: str | None = None
    content_sha256: str | None = None
    content_size: int | None = None
    archive_sources: list[dict[str, str | int]] | None = None

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
            raw_dir = raw_root() / "hud_exchange" / boundary_vintage
        else:
            raw_dir = Path(raw_dir)

        if not skip_download:
            download = download_hud_exchange_gdb(
                boundary_vintage=boundary_vintage,
                output_dir=raw_dir,
                url=url,
                return_provenance=True,
            )
            data_path = download.path
            source_url = download.source_url
            content_sha256 = download.raw_sha256
            content_size = download.file_size
            archive_sources = [
                {
                    "source_url": source.url,
                    "raw_sha256": source.raw_sha256,
                    "file_size": source.file_size,
                }
                for source in download.archive_sources
            ]
        else:
            # Find existing data in raw_dir
            merged_path = raw_dir / "per_state" / f"merged_{boundary_vintage}.shp"
            gdb_dirs = sorted(
                path for path in raw_dir.rglob("*.gdb") if "per_state" not in path.parts
            )
            shp_files = sorted(
                path for path in raw_dir.rglob("*.shp") if "per_state" not in path.parts
            )
            if merged_path.exists():
                data_path = merged_path
            elif gdb_dirs:
                data_path = gdb_dirs[0]
            elif shp_files:
                data_path = shp_files[0]
            else:
                raise ValueError(f"No .gdb or .shp found in {raw_dir}")

            local_path = Path(data_path).resolve()
            source_url = local_path.as_uri()

        gdf = read_coc_boundaries(data_path)
        gdf = map_to_canonical_schema(gdf, boundary_vintage, source_url)

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
    from hhplab.storage.provenance import ProvenanceBlock

    provenance_extra: dict[str, Any] = {
        "source_type": "hud_exchange_archive" if archive_sources is not None else (
            "local_boundary_input" if not use_arcgis else "hud_arcgis_api"
        ),
        "source_url": source_url,
        "raw_sha256": content_sha256,
        "file_size": content_size,
    }
    if archive_sources is not None:
        provenance_extra["archive_sources"] = archive_sources
        provenance_extra["raw_sha256_algorithm"] = (
            "single archive raw ZIP SHA-256"
            if len(archive_sources) == 1
            else "SHA-256 of concatenated binary component ZIP SHA-256 digests in download order"
        )
    elif not use_arcgis:
        provenance_extra["local_path"] = str(Path(data_path).resolve())

    write_geoparquet(
        gdf,
        output_path,
        provenance=ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            geo_type="coc",
            extra=provenance_extra,
        ),
    )

    # Register in boundary registry
    from hhplab.registry import register_vintage

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
            **({"archive_sources": archive_sources} if archive_sources is not None else {}),
            "raw_sha256_algorithm": provenance_extra["raw_sha256_algorithm"]
            if archive_sources is not None
            else "SHA-256 of consumed local boundary files or persisted API snapshot",
        },
    )

    return output_path


def ingest_hud_exchange_state_archives(
    boundary_vintage: str,
    archive_dir: Path | str,
    manifest_path: Path | str,
    *,
    curated_dir: Path | str | None = None,
    force: bool = False,
) -> Path:
    """Normalize an audited set of historical per-state HUD CoC archives.

    This entry point is for the older state ZIP distribution, whose archive
    structure differs from the current national and per-state downloader. It
    reads the 51 state/DC ZIPs and available territory ZIPs named in the
    acquisition manifest, checks their recorded hashes, and sends canonical
    one-CoC layers through the same normalization, validation, provenance, and
    registry path as other HUD Exchange boundaries.

    Noncanonical layer IDs are quarantined in provenance. Duplicate canonical
    IDs are rejected because choosing one would make geometry assignment
    ambiguous. The manifest listing is recorded as a coverage reference, not
    treated as proof that a geometry exists for each listed ID.
    """
    import geopandas as gpd

    from hhplab.geographies.coc import normalize_boundaries, validate_boundaries
    from hhplab.geographies.coc.coc_io import curated_boundary_path, write_geoparquet
    from hhplab.storage.provenance import ProvenanceBlock

    archive_dir = Path(archive_dir)
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    state_rows = manifest.get("state_dc_archives")
    territory_rows = manifest.get("territory_archives")
    if not isinstance(state_rows, list) or len(state_rows) != 51:
        raise ValueError("Expected an acquisition manifest with exactly 51 state/DC archives.")
    if not isinstance(territory_rows, list) or len(territory_rows) != 5:
        raise ValueError("Expected an acquisition manifest with territory archive attempts.")
    expected_state_codes = set(_HUD_STATE_ABBREVIATIONS) - {"GU", "MP", "PR", "VI"}
    actual_state_codes = {str(row.get("code")) for row in state_rows}
    if actual_state_codes != expected_state_codes:
        raise ValueError(
            "The acquisition manifest must contain exactly the 50 states and DC; "
            f"missing={sorted(expected_state_codes - actual_state_codes)}, "
            f"unexpected={sorted(actual_state_codes - expected_state_codes)}."
        )
    unavailable_territories = [
        str(row["code"])
        for row in territory_rows
        if not row.get("sha256") or not row.get("size_bytes")
    ]
    available_territory_rows = [
        row for row in territory_rows if row.get("sha256") and row.get("size_bytes")
    ]
    archive_rows = state_rows + available_territory_rows
    expected_territory_codes = {"AS", "GU", "MP", "PR", "VI"}
    actual_territory_codes = {str(row.get("code")) for row in territory_rows}
    if actual_territory_codes != expected_territory_codes:
        raise ValueError(
            "The acquisition manifest must include territory attempts for AS, GU, MP, PR, and VI."
        )
    archive_years = {
        match.group(1)
        for row in archive_rows
        if (match := re.search(r"_(\d{4})\.zip$", str(row.get("url", ""))))
    }
    if archive_years != {str(boundary_vintage)}:
        raise ValueError(
            f"Boundary vintage {boundary_vintage!r} does not match archive year(s) "
            f"{sorted(archive_years)}."
        )

    native_ids = {
        str(coc_id)
        for coc_id in manifest.get("native_2010_listing", {}).get("coc_ids", [])
    }
    if not native_ids:
        raise ValueError("Manifest has no native 2010 CoC IDs for coverage comparison.")

    frames: list[gpd.GeoDataFrame] = []
    archive_sources: list[dict[str, str | int]] = []
    archive_ids: list[str] = []
    quarantined_layers: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for archive in sorted(archive_rows, key=lambda row: str(row["code"])):
        code = str(archive["code"])
        zip_path = archive_dir / Path(str(archive["path"])).name
        if not zip_path.is_file():
            raise FileNotFoundError(f"Missing audited {code} archive: {zip_path}")
        raw_sha256, file_size = _hash_file(zip_path)
        if raw_sha256 != archive.get("sha256") or file_size != archive.get("size_bytes"):
            raise ValueError(f"Audited archive identity changed for {code}: {zip_path}")
        with zipfile.ZipFile(zip_path) as zf:
            bad_member = zf.testzip()
            if bad_member is not None:
                raise ValueError(f"Archive {code} has a CRC failure in {bad_member}")
            shp_members = sorted(
                name for name in zf.namelist() if name.lower().endswith(".shp")
            )
            if len(shp_members) != int(archive.get("shapefile_layer_count", -1)):
                raise ValueError(f"Shapefile layer count changed for {code}: {zip_path}")

            layer_ids: set[str] = set()
            for member in shp_members:
                if "_CDBG" in member.upper():
                    continue
                source_path = f"/vsizip/{zip_path.resolve()}/{member}"
                source_gdf = gpd.read_file(source_path)
                if len(source_gdf) != 1:
                    raise ValueError(
                        f"Expected one geometry in {code}/{member}; found {len(source_gdf)}."
                    )
                id_field = _find_field(list(source_gdf.columns), COC_ID_FIELDS)
                if id_field is None:
                    raise ValueError(f"No CoC ID field in {code}/{member}.")
                coc_id = str(source_gdf.iloc[0][id_field]).strip()
                layer_ids.add(coc_id)
                if not _is_canonical_historical_layer_id(coc_id):
                    quarantined_layers.append(
                        {"archive_code": code, "layer": member, "source_coc_id": coc_id}
                    )
                    continue
                if coc_id.split("-", maxsplit=1)[0] != code:
                    raise ValueError(
                        f"Layer {code}/{member} contains out-of-jurisdiction ID {coc_id}."
                    )
                if coc_id in seen_ids:
                    raise ValueError(f"Duplicate canonical CoC ID {coc_id} in state archives.")
                seen_ids.add(coc_id)
                archive_ids.append(coc_id)
                mapped = map_to_canonical_schema(source_gdf, boundary_vintage, archive["url"])
                frames.append(mapped)
            if layer_ids != {str(coc_id) for coc_id in archive.get("coc_ids", [])}:
                raise ValueError(f"CoC IDs changed in audited {code} archive: {zip_path}")

        archive_sources.append(
            {
                "source_url": str(archive["url"]),
                "raw_sha256": raw_sha256,
                "file_size": file_size,
                "archive_code": code,
                "archive_scope": (
                    "territory" if code in {"AS", "GU", "MP", "PR", "VI"} else "state_dc"
                ),
            }
        )

    if not frames:
        raise ValueError("No canonical single-CoC polygons were found in the state archives.")

    merged = gpd.GeoDataFrame(
        gpd.pd.concat(frames, ignore_index=True),
        geometry="geometry",
        crs=frames[0].crs,
    )
    if any(frame.crs != merged.crs for frame in frames):
        raise ValueError("Historical CoC archives have inconsistent coordinate systems.")
    merged = normalize_boundaries(merged)
    validation_result = validate_boundaries(merged)
    if not validation_result.is_valid:
        raise ValueError(f"Validation failed:\n{validation_result}")

    observed_ids = set(archive_ids)
    coverage = _historical_coverage_summary(native_ids, observed_ids)
    provenance_extra: dict[str, Any] = {
        "source_type": "hud_exchange_historical_state_archives",
        "mapping_version": "hud_exchange_historical_state_archives/1",
        "acquisition_manifest_sha256": _hash_file(manifest_path)[0],
        "source_url": json.dumps(
            [source["source_url"] for source in archive_sources], separators=(",", ":")
        ),
        "raw_sha256": hashlib.sha256(
            b"".join(bytes.fromhex(str(source["raw_sha256"])) for source in archive_sources)
        ).hexdigest(),
        "raw_sha256_algorithm": (
            "SHA-256 of concatenated binary component ZIP SHA-256 digests in archive-code order"
        ),
        "file_size": sum(int(source["file_size"]) for source in archive_sources),
        "archive_sources": archive_sources,
        **coverage,
        "available_territory_archive_codes": sorted(
            str(row["code"]) for row in available_territory_rows
        ),
        "unavailable_territory_archive_codes": sorted(unavailable_territories),
        "quarantined_layers": quarantined_layers,
        "geometry_repair_method": (
            "hhplab.geographies.coc.normalize.normalize_boundaries "
            "(shapely.make_valid only when invalid)"
        ),
    }

    output_base = Path("data") if curated_dir is None else Path(curated_dir)
    output_path = curated_boundary_path(boundary_vintage, base_dir=output_base)
    if output_path.exists() and not force:
        raise FileExistsError(
            f"Refusing to overwrite existing curated boundary artifact: {output_path}"
        )
    write_geoparquet(
        merged,
        output_path,
        provenance=ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            geo_type="coc",
            extra=provenance_extra,
        ),
    )

    from hhplab.registry import register_vintage

    register_vintage(
        boundary_vintage=boundary_vintage,
        source="hud_exchange",
        path=output_path,
        feature_count=len(merged),
    )

    source_url = str(provenance_extra["source_url"])
    raw_sha256 = str(provenance_extra["raw_sha256"])
    file_size = int(provenance_extra["file_size"])
    changed, details = check_source_changed(
        source_type="boundary",
        source_url=source_url,
        current_sha256=raw_sha256,
    )
    if changed:
        logger.warning(
            "UPSTREAM DATA CHANGED: historical HUD Exchange boundary archives changed; "
            "previous hash %s, current hash %s",
            details["previous_sha256"][:16],
            raw_sha256[:16],
        )
    register_source(
        source_type="boundary",
        source_url=source_url,
        source_name=f"HUD Exchange CoC Boundaries {boundary_vintage} historical state archives",
        raw_sha256=raw_sha256,
        file_size=file_size,
        local_path=str(archive_dir.resolve()),
        metadata={
            "boundary_vintage": boundary_vintage,
            "feature_count": len(merged),
            "source": "hud_exchange",
            "curated_path": str(output_path),
            "coverage_status": coverage["coverage_status"],
            "archive_sources": archive_sources,
        },
    )
    return output_path


def _historical_coverage_summary(
    native_ids: set[str], observed_ids: set[str]
) -> dict[str, Any]:
    """Summarize historical archive coverage without treating the listing as ground truth."""
    return {
        "coverage_status": "partial_unresolved",
        "coverage_reference": "native_2010_listing",
        "native_listing_id_count": len(native_ids),
        "observed_archive_id_count": len(observed_ids),
        "shared_native_listing_id_count": len(native_ids & observed_ids),
        "native_listing_missing_ids": sorted(native_ids - observed_ids),
        "archive_only_ids": sorted(observed_ids - native_ids),
    }


def _is_canonical_historical_layer_id(coc_id: str) -> bool:
    """Return whether a historic layer ID names exactly one canonical CoC."""
    return re.fullmatch(r"[A-Z]{2}-\d{3}", coc_id) is not None
