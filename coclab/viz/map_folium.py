"""Folium-based interactive map rendering for CoC boundaries."""

from pathlib import Path

import folium
import geopandas as gpd

from coclab.registry import latest_vintage

# Default output directory for map files
DEFAULT_MAPS_DIR = Path("data/curated/maps")


def _normalize_coc_id(coc_id: str) -> str:
    """Normalize CoC ID for case/whitespace robust matching."""
    return coc_id.strip().upper()


def _find_coc_boundary_file(vintage: str) -> Path:
    """Find the GeoParquet file for a given vintage.

    Checks both new temporal shorthand and legacy naming patterns.
    """
    from coclab.naming import boundary_path

    # Try new naming first
    new_path = boundary_path(vintage)
    if new_path.exists():
        return new_path

    # Fall back to legacy naming
    legacy_path = Path(f"data/curated/coc_boundaries/coc_boundaries__{vintage}.parquet")
    if legacy_path.exists():
        return legacy_path

    raise FileNotFoundError(f"Boundary file not found: tried {new_path} and {legacy_path}")


def render_coc_map(
    coc_id: str,
    vintage: str | None = None,
    out_html: Path | None = None,
) -> Path:
    """Render an interactive Folium map for a single CoC boundary.

    Args:
        coc_id: CoC identifier (e.g., 'CO-500'). Case/whitespace insensitive.
        vintage: Boundary vintage to use. If None, uses latest_vintage().
        out_html: Output path for HTML file. If None, uses default location.

    Returns:
        Path to the generated HTML map file.

    Raises:
        FileNotFoundError: If no boundary file exists for the vintage.
        ValueError: If no matching CoC found or no vintages available.
    """
    # Resolve vintage
    if vintage is None:
        vintage = latest_vintage()
        if vintage is None:
            raise ValueError("No boundary vintages available in registry")

    # Load boundary data
    boundary_path = _find_coc_boundary_file(vintage)
    gdf = gpd.read_parquet(boundary_path)

    # Normalize coc_id column for matching
    normalized_input = _normalize_coc_id(coc_id)
    gdf["_coc_id_normalized"] = gdf["coc_id"].apply(_normalize_coc_id)

    # Filter to the requested CoC
    match = gdf[gdf["_coc_id_normalized"] == normalized_input]
    if match.empty:
        available = sorted(gdf["coc_id"].unique().tolist())
        raise ValueError(
            f"CoC '{coc_id}' not found in vintage '{vintage}'. "
            f"Available: {available[:10]}{'...' if len(available) > 10 else ''}"
        )

    # Use the first match (should be unique per vintage)
    row = match.iloc[0]
    geometry = row.geometry

    # Calculate centroid for map center
    centroid = geometry.centroid
    center_lat, center_lon = centroid.y, centroid.x

    # Create Folium map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10)

    # Build tooltip content
    tooltip_html = f"""
    <b>CoC ID:</b> {row["coc_id"]}<br>
    <b>Name:</b> {row.get("coc_name", "N/A")}<br>
    <b>Vintage:</b> {row.get("boundary_vintage", vintage)}<br>
    <b>Source:</b> {row.get("source", "N/A")}
    """

    # Add the boundary polygon
    folium.GeoJson(
        geometry.__geo_interface__,
        style_function=lambda x: {
            "fillColor": "#3388ff",
            "color": "#3388ff",
            "weight": 2,
            "fillOpacity": 0.3,
        },
        tooltip=folium.Tooltip(tooltip_html),
    ).add_to(m)

    # Fit map bounds to the geometry
    bounds = geometry.bounds  # (minx, miny, maxx, maxy)
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    # Determine output path
    if out_html is None:
        DEFAULT_MAPS_DIR.mkdir(parents=True, exist_ok=True)
        out_html = DEFAULT_MAPS_DIR / f"{row['coc_id']}__{vintage}.html"
    else:
        out_html = Path(out_html)
        out_html.parent.mkdir(parents=True, exist_ok=True)

    # Save the map
    m.save(str(out_html))

    return out_html
