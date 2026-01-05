"""HUD Exchange CoC GIS Tools ingester.

Downloads and processes year-specific CoC boundary shapefiles from HUD Exchange
and outputs canonicalized GeoParquet files.

Source: https://www.hudexchange.info/programs/coc/gis-tools/
"""

from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    import geopandas as gpd

# URL template for HUD Exchange CoC GIS geodatabase downloads
# Pattern observed from historical files: CoC_GIS_NatlTerrDC_Shapefile_{YEAR}.zip
HUD_EXCHANGE_GDB_URL_TEMPLATE = (
    "https://files.hudexchange.info/resources/documents/"
    "CoC_GIS_NatlTerrDC_Shapefile_{vintage}.zip"
)

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
) -> Path:
    """Ingest CoC boundaries from HUD Exchange GIS Tools.

    Downloads the year-specific CoC shapefile/geodatabase, maps fields to the
    canonical schema, normalizes geometries, validates the data, and writes
    a curated GeoParquet file.

    Args:
        boundary_vintage: Year of the boundary data (e.g., "2024")
        url: Override URL for downloading. If not provided, uses the standard
            HUD Exchange URL template.
        raw_dir: Directory for raw downloads. Defaults to data/raw/hud_exchange/{vintage}/
        curated_dir: Base directory for curated output. Defaults to data/curated/
        skip_download: If True, assumes data is already downloaded to raw_dir

    Returns:
        Path to the output GeoParquet file

    Raises:
        httpx.HTTPStatusError: If download fails
        ValueError: If data cannot be parsed or validated
    """
    from coclab.geo import normalize_boundaries, validate_boundaries
    from coclab.geo.io import curated_boundary_path, write_geoparquet

    # Set up paths
    if raw_dir is None:
        raw_dir = Path("data/raw/hud_exchange") / boundary_vintage
    else:
        raw_dir = Path(raw_dir)

    if curated_dir is None:
        curated_dir = Path("data")
    else:
        curated_dir = Path(curated_dir)

    # Determine source URL
    if url is None:
        url = HUD_EXCHANGE_GDB_URL_TEMPLATE.format(vintage=boundary_vintage)

    # Step 1: Download if needed
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

    # Step 2: Read the shapefile/geodatabase
    gdf = read_coc_boundaries(data_path)

    # Step 3: Map to canonical schema
    gdf = map_to_canonical_schema(gdf, boundary_vintage, url)

    # Step 4: Normalize boundaries (CRS, fix geometries, compute geom_hash)
    gdf = normalize_boundaries(gdf)

    # Step 5: Validate boundaries
    validation_result = validate_boundaries(gdf)
    if not validation_result.is_valid:
        raise ValueError(f"Validation failed:\n{validation_result}")

    # Step 6: Write curated GeoParquet
    output_path = curated_boundary_path(boundary_vintage, base_dir=curated_dir)
    write_geoparquet(gdf, output_path)

    return output_path
