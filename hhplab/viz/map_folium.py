"""Folium-based interactive map rendering for CoC, MSA, and metro overlays."""

from __future__ import annotations

import colorsys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import folium
import geopandas as gpd
import pandas as pd

from hhplab.naming import tract_path
from hhplab.paths import curated_dir
from hhplab.registry import latest_vintage

if TYPE_CHECKING:
    from hhplab.recipe.recipe_schema import MapLayerSpec, MapViewportSpec, TargetSpec


DEFAULT_MAP_CENTER = [39.5, -98.35]
DEFAULT_MAP_ZOOM = 4
SUPPORTED_BASEMAPS = {
    "cartodbpositron": "CartoDB positron",
    "openstreetmap": "OpenStreetMap",
}
IDENTIFIER_COLUMNS = {
    "coc": "coc_id",
    "msa": "msa_id",
    "metro": "metro_id",
    "tract": "tract_geoid",
}
DEFAULT_TOOLTIP_FIELDS = {
    "coc": ["coc_id", "coc_name", "boundary_vintage", "source"],
    "msa": ["msa_id", "msa_name", "cbsa_code", "definition_version"],
    "metro": ["metro_id", "metro_name", "definition_version"],
    "tract": ["tract_geoid", "geo_vintage", "source"],
}
DISTINCT_FILL_PALETTE = (
    "#f4a261",
    "#2a9d8f",
    "#e76f51",
    "#457b9d",
    "#8d99ae",
    "#ffb703",
    "#90be6d",
    "#b56576",
    "#577590",
    "#fb8500",
)
STYLE_FILL_COLOR_FIELD = "__map_fill_color"
STYLE_STROKE_COLOR_FIELD = "__map_stroke_color"


@dataclass
class ResolvedMapLayer:
    """Resolved layer ready for Folium rendering."""

    name: str
    gdf: gpd.GeoDataFrame
    tooltip_fields: list[str]
    show: bool
    style: dict[str, Any]
    id_column: str


def _normalize_selector(value: object) -> str:
    return str(value).strip().upper()


def _resolve_basemap(name: str) -> str:
    key = name.strip().lower()
    if key not in SUPPORTED_BASEMAPS:
        raise ValueError(f"Unsupported basemap '{name}'. Available: {sorted(SUPPORTED_BASEMAPS)}")
    return SUPPORTED_BASEMAPS[key]


def _normalize_coc_id(coc_id: str) -> str:
    """Normalize CoC ID for case/whitespace robust matching."""
    return _normalize_selector(coc_id)


def _find_coc_boundary_file(vintage: str, *, base_dir: Path) -> Path:
    """Find the GeoParquet file for a given vintage."""
    from hhplab.geo.geo_io import resolve_curated_boundary_path

    return resolve_curated_boundary_path(vintage, base_dir)


def _load_coc_boundaries(
    boundary_vintage: str,
    *,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    boundary_path = _find_coc_boundary_file(str(boundary_vintage), base_dir=base_dir)
    gdf = gpd.read_parquet(boundary_path)
    if "coc_id" not in gdf.columns:
        raise ValueError("CoC boundary artifact must contain 'coc_id'.")
    return gdf


def _load_msa_boundaries(
    *,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    from hhplab.msa.msa_boundaries import read_msa_boundaries

    gdf = read_msa_boundaries(definition_version, base_dir)
    if "geometry_vintage" in gdf.columns:
        vintages = set(gdf["geometry_vintage"].astype(str))
        expected = str(county_vintage)
        if expected not in vintages:
            raise ValueError(
                f"MSA boundaries artifact for {definition_version} does not match "
                f"requested county geometry vintage {county_vintage}. "
                f"Available geometry_vintage values: {sorted(vintages)}"
            )
    return gdf


def _load_metro_boundaries(
    *,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    from hhplab.metro.metro_boundaries import read_metro_boundaries

    return read_metro_boundaries(
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )


def _load_tract_boundaries(
    tract_vintage: str | int,
    *,
    base_dir: Path,
) -> gpd.GeoDataFrame:
    canonical_path = tract_path(tract_vintage, base_dir)
    legacy_path = canonical_path.with_name(f"tracts__{tract_vintage}.parquet")
    path = canonical_path if canonical_path.exists() else legacy_path
    gdf = gpd.read_parquet(path)

    geoid_column = next(
        (
            column
            for column in ("tract_geoid", "geoid", "GEOID", "GEOID10", "GEOID20")
            if column in gdf.columns
        ),
        None,
    )
    if geoid_column is None:
        raise ValueError(
            "Tract geometry artifact must contain one of "
            "'tract_geoid', 'geoid', 'GEOID', 'GEOID10', or 'GEOID20'."
        )

    if "tract_geoid" not in gdf.columns:
        gdf = gdf.rename(columns={geoid_column: "tract_geoid"})
    gdf["tract_geoid"] = gdf["tract_geoid"].astype(str).str.strip()
    return gdf


def _load_layer_geometries(layer: MapLayerSpec, *, base_dir: Path) -> gpd.GeoDataFrame:
    geo_type = layer.geometry.type
    vintage = layer.geometry.vintage

    if geo_type == "coc":
        boundary_vintage = str(vintage) if vintage is not None else latest_vintage()
        if boundary_vintage is None:
            raise ValueError("No boundary vintages available in registry")
        return _load_coc_boundaries(boundary_vintage, base_dir=base_dir)

    if geo_type == "msa":
        if not layer.geometry.source:
            raise ValueError(
                "MSA map layers require geometry.source to name the definition version."
            )
        if vintage is None:
            raise ValueError(
                "MSA map layers require geometry.vintage to name the county geometry vintage."
            )
        return _load_msa_boundaries(
            definition_version=layer.geometry.source,
            county_vintage=vintage,
            base_dir=base_dir,
        )

    if geo_type == "metro":
        if not layer.geometry.source:
            raise ValueError(
                "Metro map layers require geometry.source to name the definition version."
            )
        if vintage is None:
            raise ValueError(
                "Metro map layers require geometry.vintage to name the county geometry vintage."
            )
        return _load_metro_boundaries(
            definition_version=layer.geometry.source,
            county_vintage=vintage,
            base_dir=base_dir,
        )

    if geo_type == "tract":
        if vintage is None:
            raise ValueError(
                "Tract map layers require geometry.vintage to name the tract geometry vintage."
            )
        return _load_tract_boundaries(vintage, base_dir=base_dir)

    raise ValueError(
        f"Unsupported map layer geometry '{geo_type}'. Supported types: coc, msa, metro, tract."
    )


def _select_layer_rows(
    gdf: gpd.GeoDataFrame,
    *,
    id_column: str,
    selector_ids: list[str],
) -> gpd.GeoDataFrame:
    normalized_series = gdf[id_column].astype(str).map(_normalize_selector)
    index = pd.Index(normalized_series)
    normalized_ids = [_normalize_selector(item) for item in selector_ids]
    missing = [selector_ids[i] for i, item in enumerate(normalized_ids) if item not in index]
    if missing:
        available = sorted(gdf[id_column].astype(str).unique().tolist())
        raise ValueError(
            f"Map layer selector values not found for '{id_column}': {missing}. "
            f"Available examples: {available[:10]}{'...' if len(available) > 10 else ''}"
        )
    positions = [index.get_loc(item) for item in normalized_ids]
    if any(isinstance(position, slice) for position in positions):
        raise ValueError(f"Map layer '{id_column}' contains duplicate normalized identifiers.")
    return gdf.iloc[positions].copy().reset_index(drop=True)


def _resolve_tooltip_fields(
    *,
    geo_type: str,
    gdf: gpd.GeoDataFrame,
    tooltip_fields: list[str],
) -> list[str]:
    fields = tooltip_fields or DEFAULT_TOOLTIP_FIELDS[geo_type]
    missing = [field for field in fields if field not in gdf.columns]
    if missing:
        raise ValueError(
            f"Map tooltip fields not available for {geo_type} layer: {missing}. "
            f"Available columns: {sorted(gdf.columns.tolist())}"
        )
    return fields


def _darken_hex(color: str, factor: float = 0.65) -> str:
    """Return a darker hex color suitable for feature outlines."""
    value = color.lstrip("#")
    if len(value) != 6:
        return color
    channels = [max(0, min(255, int(int(value[i : i + 2], 16) * factor))) for i in range(0, 6, 2)]
    return "#{:02x}{:02x}{:02x}".format(*channels)


def _rgb_to_hex(red: float, green: float, blue: float) -> str:
    """Convert normalized RGB values to a hex color string."""
    return f"#{round(red * 255):02x}{round(green * 255):02x}{round(blue * 255):02x}"


def _generate_distinct_colors(count: int) -> list[str]:
    """Generate a deterministic list of visually distinct colors."""
    if count <= 0:
        return []
    if count <= len(DISTINCT_FILL_PALETTE):
        return list(DISTINCT_FILL_PALETTE[:count])

    colors: list[str] = []
    for index in range(count):
        hue = (index * 0.618033988749895) % 1.0
        lightness = 0.58 if index % 2 == 0 else 0.66
        saturation = 0.62 if index % 3 else 0.74
        colors.append(_rgb_to_hex(*colorsys.hls_to_rgb(hue, lightness, saturation)))
    return colors


def _nth_distinct_color(index: int) -> str:
    """Return the deterministic color at one palette index."""
    if index < len(DISTINCT_FILL_PALETTE):
        return DISTINCT_FILL_PALETTE[index]
    overflow_index = index - len(DISTINCT_FILL_PALETTE)
    hue = (overflow_index * 0.618033988749895) % 1.0
    lightness = 0.58 if overflow_index % 2 == 0 else 0.66
    saturation = 0.62 if overflow_index % 3 else 0.74
    return _rgb_to_hex(*colorsys.hls_to_rgb(hue, lightness, saturation))


def _distinct_palette_colors_for_ids(feature_ids: list[str]) -> dict[str, str]:
    """Assign deterministic unique colors within one rendered layer."""
    normalized_ids = sorted({_normalize_selector(feature_id) for feature_id in feature_ids})
    return dict(zip(normalized_ids, _generate_distinct_colors(len(normalized_ids)), strict=True))


def _build_feature_adjacency(gdf: gpd.GeoDataFrame) -> dict[int, set[int]]:
    """Build an undirected adjacency map for features that touch or overlap."""
    adjacency = {index: set() for index in range(len(gdf))}
    if gdf.empty:
        return adjacency

    geometries = gdf.geometry.reset_index(drop=True)
    spatial_index = geometries.sindex
    for index, geometry in geometries.items():
        if geometry is None or geometry.is_empty:
            continue
        candidate_indexes = spatial_index.query(geometry)
        for candidate in candidate_indexes:
            neighbor = int(candidate)
            if neighbor <= index:
                continue
            other = geometries.iloc[neighbor]
            if other is None or other.is_empty:
                continue
            if geometry.disjoint(other):
                continue
            adjacency[index].add(neighbor)
            adjacency[neighbor].add(index)
    return adjacency


def _distinct_palette_colors_for_layer(
    gdf: gpd.GeoDataFrame,
    *,
    id_column: str,
) -> dict[str, str]:
    """Assign deterministic colors while reusing palette entries for non-neighbors."""
    normalized_ids = gdf[id_column].astype(str).map(_normalize_selector).reset_index(drop=True)
    adjacency = _build_feature_adjacency(gdf)
    order = sorted(range(len(gdf)), key=lambda index: (normalized_ids.iloc[index], index))
    color_by_index: dict[int, str] = {}
    palette = [_nth_distinct_color(index) for index in range(len(DISTINCT_FILL_PALETTE))]

    for index in order:
        used_colors = {
            color_by_index[neighbor] for neighbor in adjacency[index] if neighbor in color_by_index
        }
        palette_index = 0
        while True:
            if palette_index == len(palette):
                palette.append(_nth_distinct_color(palette_index))
            color = palette[palette_index]
            if color not in used_colors:
                color_by_index[index] = color
                break
            palette_index += 1

    return {normalized_ids.iloc[index]: color_by_index[index] for index in order}


def _apply_distinct_feature_styles(
    gdf: gpd.GeoDataFrame,
    *,
    id_column: str,
) -> gpd.GeoDataFrame:
    """Assign deterministic fill and stroke colors to each selected feature."""
    styled = gdf.copy()
    normalized_ids = styled[id_column].astype(str).map(_normalize_selector)
    color_map = _distinct_palette_colors_for_layer(styled, id_column=id_column)
    styled[STYLE_FILL_COLOR_FIELD] = normalized_ids.map(color_map)
    styled[STYLE_STROKE_COLOR_FIELD] = styled[STYLE_FILL_COLOR_FIELD].map(_darken_hex)
    return styled


def _resolve_map_layer(layer: MapLayerSpec, *, base_dir: Path) -> ResolvedMapLayer:
    geo_type = layer.geometry.type
    if geo_type not in IDENTIFIER_COLUMNS:
        raise ValueError(
            f"Unsupported map layer geometry '{geo_type}'. Supported types: coc, msa, metro, tract."
        )

    gdf = _load_layer_geometries(layer, base_dir=base_dir)
    id_column = IDENTIFIER_COLUMNS[geo_type]
    selected = _select_layer_rows(
        gdf,
        id_column=id_column,
        selector_ids=layer.selector_ids,
    )
    tooltip_fields = _resolve_tooltip_fields(
        geo_type=geo_type,
        gdf=selected,
        tooltip_fields=layer.tooltip_fields,
    )
    label = layer.label or layer.group or f"{geo_type}:{','.join(layer.selector_ids)}"
    style = {
        "fillColor": layer.style.fill_color,
        "color": layer.style.stroke_color,
        "weight": layer.style.line_weight,
        "fillOpacity": layer.style.fill_opacity,
        "opacity": layer.style.stroke_opacity,
    }
    if layer.style_mode == "distinct":
        selected = _apply_distinct_feature_styles(
            selected,
            id_column=id_column,
        )
    return ResolvedMapLayer(
        name=label,
        gdf=selected.to_crs(epsg=4326),
        tooltip_fields=tooltip_fields,
        show=layer.initial_visibility,
        style=style,
        id_column=id_column,
    )


def _initial_map_view(layers: list[ResolvedMapLayer]) -> list[float]:
    if not layers:
        return DEFAULT_MAP_CENTER
    geometries = pd.concat([layer.gdf.geometry for layer in layers], ignore_index=True)
    centroid = geometries.union_all().centroid
    return [centroid.y, centroid.x]


def _fit_map_to_layers(
    m: folium.Map,
    layers: list[ResolvedMapLayer],
    *,
    padding: int,
) -> None:
    geometries = gpd.GeoSeries(
        pd.concat([layer.gdf.geometry for layer in layers], ignore_index=True),
        crs="EPSG:4326",
    )
    minx, miny, maxx, maxy = geometries.total_bounds
    m.fit_bounds(
        [[miny, minx], [maxy, maxx]],
        padding=(padding, padding),
    )


def render_overlay_map(
    *,
    layers: list[ResolvedMapLayer],
    basemap: str,
    viewport: MapViewportSpec,
    out_html: Path,
) -> Path:
    """Render multiple resolved overlay layers to one HTML map."""
    map_location = (
        list(viewport.center) if viewport.center is not None else _initial_map_view(layers)
    )
    zoom_start = viewport.zoom if viewport.zoom is not None else DEFAULT_MAP_ZOOM
    m = folium.Map(
        location=map_location,
        zoom_start=zoom_start,
        tiles=_resolve_basemap(basemap),
    )

    for layer in layers:
        feature_group = folium.FeatureGroup(name=layer.name, show=layer.show)
        geojson_tooltip = None
        if layer.tooltip_fields:
            geojson_tooltip = folium.GeoJsonTooltip(
                fields=layer.tooltip_fields,
                aliases=[f"{field}:" for field in layer.tooltip_fields],
                sticky=False,
            )
        layer_data = layer.gdf.copy()
        for column in layer_data.columns:
            if column == layer_data.geometry.name:
                continue
            if pd.api.types.is_datetime64_any_dtype(layer_data[column]):
                layer_data[column] = layer_data[column].astype(str)
                continue
            if layer_data[column].dtype == "object":
                layer_data[column] = layer_data[column].map(
                    lambda value: (
                        value.isoformat()
                        if isinstance(value, (pd.Timestamp, datetime, date))
                        else value
                    )
                )

        def _style_function(
            feature: dict[str, Any],
            base_style: dict[str, Any] = layer.style,
        ) -> dict[str, Any]:
            properties = feature.get("properties", {})
            style = dict(base_style)
            fill_color = properties.get(STYLE_FILL_COLOR_FIELD)
            stroke_color = properties.get(STYLE_STROKE_COLOR_FIELD)
            if fill_color is not None:
                style["fillColor"] = fill_color
            if stroke_color is not None:
                style["color"] = stroke_color
            return style

        folium.GeoJson(
            data=layer_data.__geo_interface__,
            style_function=_style_function,
            tooltip=geojson_tooltip,
        ).add_to(feature_group)
        feature_group.add_to(m)

    if len(layers) > 1:
        folium.LayerControl(collapsed=False).add_to(m)

    if viewport.fit_layers:
        _fit_map_to_layers(m, layers, padding=viewport.padding)

    out_html.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_html))
    return out_html


def render_recipe_map(
    target: TargetSpec,
    *,
    project_root: Path,
    out_html: Path,
) -> Path:
    """Render a recipe-native map target to its HTML artifact."""
    if target.map_spec is None:
        raise ValueError("Target does not define map_spec.")
    base_dir = project_root / "data"
    layers = [_resolve_map_layer(layer, base_dir=base_dir) for layer in target.map_spec.layers]
    return render_overlay_map(
        layers=layers,
        basemap=target.map_spec.basemap,
        viewport=target.map_spec.viewport,
        out_html=out_html,
    )


def render_coc_map(
    coc_id: str,
    vintage: str | None = None,
    out_html: Path | None = None,
) -> Path:
    """Render an interactive Folium map for a single CoC boundary."""
    boundary_vintage = vintage
    if boundary_vintage is None:
        boundary_vintage = latest_vintage()
        if boundary_vintage is None:
            raise ValueError("No boundary vintages available in registry")
    gdf = _load_coc_boundaries(str(boundary_vintage), base_dir=Path("data"))
    selected_gdf = _select_layer_rows(
        gdf,
        id_column="coc_id",
        selector_ids=[coc_id],
    ).to_crs(epsg=4326)
    selected = ResolvedMapLayer(
        name=str(coc_id).strip(),
        gdf=selected_gdf,
        tooltip_fields=_resolve_tooltip_fields(
            geo_type="coc",
            gdf=selected_gdf,
            tooltip_fields=[],
        ),
        show=True,
        style={
            "fillColor": "#3388ff",
            "color": "#3388ff",
            "weight": 2.0,
            "fillOpacity": 0.3,
            "opacity": 1.0,
        },
        id_column="coc_id",
    )

    if out_html is None:
        curated_dir("maps").mkdir(parents=True, exist_ok=True)
        normalized = selected.gdf.iloc[0]["coc_id"]
        out_html = curated_dir("maps") / f"{normalized}__{boundary_vintage}.html"
    else:
        out_html = Path(out_html)

    from hhplab.recipe.recipe_schema import MapViewportSpec

    return render_overlay_map(
        layers=[selected],
        basemap="cartodbpositron",
        viewport=MapViewportSpec(fit_layers=True),
        out_html=out_html,
    )
