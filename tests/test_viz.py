"""Tests for visualization module."""

from datetime import UTC, datetime

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from hhplab.recipe.recipe_schema import (
    GeometryRef,
    MapLayerSpec,
    MapLayerStyle,
    MapSpec,
    MapViewportSpec,
    TargetSpec,
)
from hhplab.registry import register_vintage
from hhplab.viz import render_coc_map, render_recipe_map
from hhplab.viz.map_folium import (
    DISTINCT_FILL_PALETTE,
    _apply_distinct_feature_styles,
    _distinct_palette_colors_for_ids,
)


@pytest.fixture
def temp_data_dir(tmp_path, monkeypatch):
    """Create a temporary data directory structure."""
    # Create curated directory
    curated_dir = tmp_path / "data" / "curated"
    curated_dir.mkdir(parents=True)

    # Create coc_boundaries directory for boundary parquet files
    boundaries_dir = curated_dir / "coc_boundaries"
    boundaries_dir.mkdir(parents=True)

    # Monkeypatch the working directory context
    monkeypatch.chdir(tmp_path)

    return tmp_path


@pytest.fixture
def sample_boundaries(temp_data_dir):
    """Create a sample GeoParquet file with test boundaries."""
    # Create sample polygons (simple squares)
    # CO-500: around Denver area
    co_500_poly = Polygon(
        [
            (-105.0, 39.5),
            (-104.5, 39.5),
            (-104.5, 40.0),
            (-105.0, 40.0),
            (-105.0, 39.5),
        ]
    )

    # NY-600: around NYC area
    ny_600_poly = Polygon(
        [
            (-74.5, 40.5),
            (-73.5, 40.5),
            (-73.5, 41.0),
            (-74.5, 41.0),
            (-74.5, 40.5),
        ]
    )

    gdf = gpd.GeoDataFrame(
        {
            "coc_id": ["CO-500", "NY-600"],
            "coc_name": ["Colorado Balance of State CoC", "New York City CoC"],
            "boundary_vintage": ["2025", "2025"],
            "source": ["hud_exchange", "hud_exchange"],
            "source_ref": ["https://example.com", "https://example.com"],
            "state_abbrev": ["CO", "NY"],
            "ingested_at": [datetime.now(UTC), datetime.now(UTC)],
            "geom_hash": ["abc123", "def456"],
        },
        geometry=[co_500_poly, ny_600_poly],
        crs="EPSG:4326",
    )

    # Save as GeoParquet
    vintage = "2025"
    parquet_path = (
        temp_data_dir / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
    )
    gdf.to_parquet(parquet_path)

    # Register the vintage
    registry_path = temp_data_dir / "data" / "curated" / "boundary_registry.parquet"
    register_vintage(
        boundary_vintage=vintage,
        source="hud_exchange",
        path=parquet_path,
        feature_count=len(gdf),
        registry_path=registry_path,
        _allow_temp_path=True,
    )

    return {"vintage": vintage, "parquet_path": parquet_path, "gdf": gdf}


@pytest.fixture
def sample_overlay_artifacts(temp_data_dir):
    """Create minimal county, MSA, metro, and tract artifacts for mixed overlays."""
    tiger_dir = temp_data_dir / "data" / "curated" / "tiger"
    msa_dir = temp_data_dir / "data" / "curated" / "msa"
    metro_dir = temp_data_dir / "data" / "curated" / "metro"
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)
    metro_dir.mkdir(parents=True, exist_ok=True)

    gpd.GeoDataFrame(
        {
            "GEOID": ["08001", "08005"],
            "county_name": ["Adams", "Arapahoe"],
            "state_name": ["Colorado", "Colorado"],
            "source": ["tiger_line", "tiger_line"],
            "geo_vintage": ["2025", "2025"],
        },
        geometry=[
            Polygon(
                [
                    (-105.2, 39.4),
                    (-104.85, 39.4),
                    (-104.85, 39.8),
                    (-105.2, 39.8),
                    (-105.2, 39.4),
                ]
            ),
            Polygon(
                [
                    (-104.85, 39.4),
                    (-104.45, 39.4),
                    (-104.45, 39.8),
                    (-104.85, 39.8),
                    (-104.85, 39.4),
                ]
            ),
        ],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2025.parquet")
    gpd.GeoDataFrame(
        {
            "geoid": ["08001000100", "08001000200", "08001000300"],
            "geo_vintage": ["2020", "2020", "2020"],
            "source": ["tiger_line", "tiger_line", "tiger_line"],
        },
        geometry=[
            Polygon(
                [
                    (-105.20, 39.40),
                    (-105.00, 39.40),
                    (-105.00, 39.60),
                    (-105.20, 39.60),
                    (-105.20, 39.40),
                ]
            ),
            Polygon(
                [
                    (-105.00, 39.40),
                    (-104.80, 39.40),
                    (-104.80, 39.60),
                    (-105.00, 39.60),
                    (-105.00, 39.40),
                ]
            ),
            Polygon(
                [
                    (-105.20, 39.80),
                    (-105.00, 39.80),
                    (-105.00, 40.00),
                    (-105.20, 40.00),
                    (-105.20, 39.80),
                ]
            ),
        ],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "tracts__T2020.parquet")

    pd.DataFrame(
        {
            "msa_id": ["19740"],
            "cbsa_code": ["19740"],
            "msa_name": ["Denver-Aurora-Lakewood, CO"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "source": ["census_msa_delineation_2023"],
            "source_ref": ["https://example.com/msa"],
        }
    ).to_parquet(msa_dir / "msa_definitions__census_msa_2023.parquet")
    pd.DataFrame(
        {
            "msa_id": ["19740", "19740"],
            "cbsa_code": ["19740", "19740"],
            "county_fips": ["08001", "08005"],
            "county_name": ["Adams", "Arapahoe"],
            "state_name": ["Colorado", "Colorado"],
            "central_outlying": ["Central", "Central"],
            "definition_version": ["census_msa_2023", "census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")
    gpd.GeoDataFrame(
        {
            "msa_id": ["19740"],
            "cbsa_code": ["19740"],
            "msa_name": ["Denver-Aurora-Lakewood, CO"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "geometry_vintage": ["2025"],
            "source": ["census_tiger_cbsa"],
            "source_ref": ["https://example.com/msa-boundaries"],
            "ingested_at": [datetime.now(UTC)],
        },
        geometry=[
            Polygon(
                [
                    (-105.2, 39.4),
                    (-104.45, 39.4),
                    (-104.45, 39.8),
                    (-105.2, 39.8),
                    (-105.2, 39.4),
                ]
            )
        ],
        crs="EPSG:4326",
    ).to_parquet(msa_dir / "msa_boundaries__census_msa_2023.parquet")

    pd.DataFrame(
        {
            "metro_id": ["GF21"],
            "metro_name": ["Denver, CO"],
            "membership_type": ["multi_county"],
            "definition_version": ["glynn_fox_v1"],
            "source": ["glynn_fox_2019"],
            "source_ref": ["https://example.com/metro"],
        }
    ).to_parquet(metro_dir / "metro_definitions__glynn_fox_v1.parquet")
    pd.DataFrame(
        {
            "metro_id": ["GF21", "GF21"],
            "county_fips": ["08001", "08005"],
            "definition_version": ["glynn_fox_v1", "glynn_fox_v1"],
        }
    ).to_parquet(metro_dir / "metro_county_membership__glynn_fox_v1.parquet")
    gpd.GeoDataFrame(
        {
            "metro_id": ["GF21"],
            "metro_name": ["Denver, CO"],
            "definition_version": ["glynn_fox_v1"],
            "geometry_vintage": ["2025"],
            "source": ["derived_metro_county_union"],
            "source_ref": ["https://example.com/metro"],
            "ingested_at": [datetime.now(UTC)],
        },
        geometry=[
            Polygon(
                [
                    (-105.2, 39.4),
                    (-104.45, 39.4),
                    (-104.45, 39.8),
                    (-105.2, 39.8),
                    (-105.2, 39.4),
                ]
            )
        ],
        crs="EPSG:4326",
    ).to_parquet(metro_dir / "metro_boundaries__glynn_fox_v1xC2025.parquet")


def _mixed_overlay_target() -> TargetSpec:
    return TargetSpec(
        id="overlay_map",
        geometry=GeometryRef(type="coc", vintage=2025),
        outputs=["map"],
        map_spec=MapSpec(
            layers=[
                MapLayerSpec(
                    geometry=GeometryRef(type="coc", vintage=2025),
                    selector_ids=["CO-500"],
                    label="CoC layer",
                    tooltip_fields=["coc_id", "coc_name"],
                ),
                MapLayerSpec(
                    geometry=GeometryRef(type="msa", vintage=2025, source="census_msa_2023"),
                    selector_ids=["19740"],
                    label="MSA layer",
                    tooltip_fields=["msa_id", "msa_name"],
                ),
                MapLayerSpec(
                    geometry=GeometryRef(type="metro", vintage=2025, source="glynn_fox_v1"),
                    selector_ids=["GF21"],
                    label="Metro layer",
                    tooltip_fields=["metro_id", "metro_name"],
                ),
            ],
            viewport=MapViewportSpec(fit_layers=True, padding=18),
        ),
    )


def _mixed_overlay_with_tract_target() -> TargetSpec:
    return TargetSpec(
        id="overlay_map_with_tracts",
        geometry=GeometryRef(type="coc", vintage=2025),
        outputs=["map"],
        map_spec=MapSpec(
            layers=[
                MapLayerSpec(
                    geometry=GeometryRef(type="coc", vintage=2025),
                    selector_ids=["CO-500"],
                    label="CoC layer",
                    tooltip_fields=["coc_id", "coc_name"],
                ),
                MapLayerSpec(
                    geometry=GeometryRef(type="msa", vintage=2025, source="census_msa_2023"),
                    selector_ids=["19740"],
                    label="MSA layer",
                    tooltip_fields=["msa_id", "msa_name"],
                ),
                MapLayerSpec(
                    geometry=GeometryRef(type="tract", vintage=2020),
                    selector_ids=["08001000100", "08001000200", "08001000300"],
                    label="Tract layer",
                    style_mode="distinct",
                    tooltip_fields=["tract_geoid", "geo_vintage"],
                ),
            ],
            viewport=MapViewportSpec(fit_layers=True, padding=18),
        ),
    )


def _mixed_overlay_with_county_target() -> TargetSpec:
    return TargetSpec(
        id="overlay_map_with_counties",
        geometry=GeometryRef(type="coc", vintage=2025),
        outputs=["map"],
        map_spec=MapSpec(
            layers=[
                MapLayerSpec(
                    geometry=GeometryRef(type="coc", vintage=2025),
                    selector_ids=["CO-500"],
                    label="CoC layer",
                    tooltip_fields=["coc_id", "coc_name"],
                ),
                MapLayerSpec(
                    geometry=GeometryRef(type="county", vintage=2025),
                    selector_ids=["08001", "08005"],
                    label="County layer",
                    style_mode="distinct",
                    tooltip_fields=["county_fips", "county_name", "geo_vintage"],
                ),
            ],
            viewport=MapViewportSpec(fit_layers=True, padding=18),
        ),
    )


def _distinct_coc_target() -> TargetSpec:
    return TargetSpec(
        id="distinct_coc_map",
        geometry=GeometryRef(type="coc", vintage=2025),
        outputs=["map"],
        map_spec=MapSpec(
            layers=[
                MapLayerSpec(
                    geometry=GeometryRef(type="coc", vintage=2025),
                    selector_ids=["CO-500", "NY-600"],
                    label="Distinct CoCs",
                    tooltip_fields=["coc_id", "coc_name"],
                    style_mode="distinct",
                ),
            ],
            viewport=MapViewportSpec(fit_layers=True, padding=18),
        ),
    )


class TestRenderCocMap:
    """Tests for render_coc_map function."""

    def test_render_basic_map(self, sample_boundaries, temp_data_dir):
        """Test rendering a basic CoC map produces an HTML file."""
        out_path = render_coc_map("CO-500", vintage="2025")

        assert out_path.exists()
        assert out_path.suffix == ".html"
        assert "CO-500" in out_path.name
        assert "2025" in out_path.name

        # Check HTML content contains expected elements
        content = out_path.read_text()
        assert "folium" in content.lower() or "leaflet" in content.lower()

    def test_render_case_insensitive(self, sample_boundaries, temp_data_dir):
        """Test that coc_id matching is case insensitive."""
        out_path = render_coc_map("co-500", vintage="2025")
        assert out_path.exists()

    def test_render_whitespace_tolerant(self, sample_boundaries, temp_data_dir):
        """Test that coc_id matching ignores leading/trailing whitespace."""
        out_path = render_coc_map("  CO-500  ", vintage="2025")
        assert out_path.exists()

    def test_render_custom_output_path(self, sample_boundaries, temp_data_dir):
        """Test rendering to a custom output path."""
        custom_path = temp_data_dir / "custom" / "my_map.html"
        out_path = render_coc_map("CO-500", vintage="2025", out_html=custom_path)

        assert out_path == custom_path
        assert out_path.exists()

    def test_render_different_cocs(self, sample_boundaries, temp_data_dir):
        """Test rendering different CoCs produces different files."""
        path1 = render_coc_map("CO-500", vintage="2025")
        path2 = render_coc_map("NY-600", vintage="2025")

        assert path1 != path2
        assert path1.exists()
        assert path2.exists()

    def test_render_uses_latest_vintage(self, sample_boundaries, temp_data_dir):
        """Test that None vintage uses latest_vintage()."""
        out_path = render_coc_map("CO-500")

        assert out_path.exists()
        assert "2025" in out_path.name

    def test_render_invalid_coc_raises(self, sample_boundaries, temp_data_dir):
        """Test that invalid coc_id raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            render_coc_map("INVALID-123", vintage="2025")

    def test_render_invalid_vintage_raises(self, sample_boundaries, temp_data_dir):
        """Test that invalid vintage raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            render_coc_map("CO-500", vintage="1999")

    def test_render_supports_coc_base_filename(self, sample_boundaries, temp_data_dir):
        """Render should resolve new coc__B* boundary filename."""
        legacy_path = sample_boundaries["parquet_path"]
        gdf = sample_boundaries["gdf"]

        coc_path = temp_data_dir / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet"
        gdf.to_parquet(coc_path)
        legacy_path.unlink()

        out_path = render_coc_map("CO-500", vintage="2025")
        assert out_path.exists()

    def test_render_no_vintages_raises(self, temp_data_dir):
        """Test that missing vintages raises ValueError."""
        with pytest.raises(ValueError, match="No boundary vintages available"):
            render_coc_map("CO-500")

    def test_map_contains_tooltip_info(self, sample_boundaries, temp_data_dir):
        """Test that rendered map contains tooltip with expected info."""
        out_path = render_coc_map("CO-500", vintage="2025")
        content = out_path.read_text()

        # Check for tooltip content
        assert "CO-500" in content
        assert "Colorado Balance of State CoC" in content
        assert "2025" in content


class TestRenderRecipeMap:
    """Tests for recipe-native multi-layer map rendering."""

    def test_render_mixed_overlay_map(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        out_path = render_recipe_map(
            _mixed_overlay_target(),
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "overlay.html",
        )

        assert out_path.exists()
        content = out_path.read_text()
        assert "CoC layer" in content
        assert "MSA layer" in content
        assert "Metro layer" in content
        assert "Denver-Aurora-Lakewood, CO" in content
        assert "Denver, CO" in content
        assert "fitBounds" in content

    def test_render_map_honors_layer_z_order(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        target = _mixed_overlay_target()
        target.map_spec.layers[0].style = MapLayerStyle(z_order=20)
        target.map_spec.layers[1].style = MapLayerStyle(z_order=0)
        target.map_spec.layers[2].style = MapLayerStyle(z_order=10)

        out_path = render_recipe_map(
            target,
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "z-order.html",
        )

        content = out_path.read_text()
        assert content.index("MSA layer") < content.index("Metro layer")
        assert content.index("Metro layer") < content.index("CoC layer")

    def test_render_map_missing_selector_raises(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        target = _mixed_overlay_target()
        target.map_spec.layers[1].selector_ids = ["99999"]

        with pytest.raises(ValueError, match="selector values not found"):
            render_recipe_map(
                target,
                project_root=temp_data_dir,
                out_html=temp_data_dir / "outputs" / "missing.html",
            )

    def test_render_map_missing_metro_boundary_artifact_is_actionable(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        metro_boundary_path = (
            temp_data_dir
            / "data"
            / "curated"
            / "metro"
            / "metro_boundaries__glynn_fox_v1xC2025.parquet"
        )
        metro_boundary_path.unlink()

        with pytest.raises(FileNotFoundError, match="generate metro-boundaries"):
            render_recipe_map(
                _mixed_overlay_target(),
                project_root=temp_data_dir,
                out_html=temp_data_dir / "outputs" / "missing-metro.html",
            )

    def test_render_map_explicit_viewport_skips_fit_bounds(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        target = _mixed_overlay_target()
        target.map_spec.viewport = MapViewportSpec(
            fit_layers=False,
            center=(39.65, -104.9),
            zoom=9,
        )
        out_path = render_recipe_map(
            target,
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "explicit-view.html",
        )

        content = out_path.read_text()
        assert "fitBounds" not in content

    def test_render_map_distinct_style_mode_assigns_multiple_colors(
        self,
        sample_boundaries,
        temp_data_dir,
    ):
        out_path = render_recipe_map(
            _distinct_coc_target(),
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "distinct.html",
        )

        content = out_path.read_text()
        assert "__map_fill_color" in content
        assert "__map_stroke_color" in content
        assert any(color in content for color in DISTINCT_FILL_PALETTE)

    def test_distinct_style_mode_assigns_unique_colors_within_layer(self):
        color_map = _distinct_palette_colors_for_ids(
            ["OH-500", "OH-501", "OH-502", "OH-503", "OH-504"],
        )

        assert len(color_map) == 5
        assert len(set(color_map.values())) == 5

    def test_render_mixed_overlay_map_with_tract_layer(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        out_path = render_recipe_map(
            _mixed_overlay_with_tract_target(),
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "overlay-with-tracts.html",
        )

        content = out_path.read_text()
        assert "CoC layer" in content
        assert "MSA layer" in content
        assert "Tract layer" in content
        assert "08001000100" in content
        assert "08001000300" in content

    def test_render_mixed_overlay_map_with_county_layer(
        self,
        sample_boundaries,
        sample_overlay_artifacts,
        temp_data_dir,
    ):
        out_path = render_recipe_map(
            _mixed_overlay_with_county_target(),
            project_root=temp_data_dir,
            out_html=temp_data_dir / "outputs" / "overlay-with-counties.html",
        )

        content = out_path.read_text()
        assert "CoC layer" in content
        assert "County layer" in content
        assert "08001" in content
        assert "Arapahoe" in content

    def test_distinct_style_mode_reuses_colors_for_noncontiguous_polygons(self):
        gdf = gpd.GeoDataFrame(
            {"tract_geoid": ["08001000100", "08001000200", "08001000300"]},
            geometry=[
                Polygon(
                    [
                        (0.0, 0.0),
                        (1.0, 0.0),
                        (1.0, 1.0),
                        (0.0, 1.0),
                        (0.0, 0.0),
                    ]
                ),
                Polygon(
                    [
                        (1.0, 0.0),
                        (2.0, 0.0),
                        (2.0, 1.0),
                        (1.0, 1.0),
                        (1.0, 0.0),
                    ]
                ),
                Polygon(
                    [
                        (0.0, 2.0),
                        (1.0, 2.0),
                        (1.0, 3.0),
                        (0.0, 3.0),
                        (0.0, 2.0),
                    ]
                ),
            ],
            crs="EPSG:4326",
        )

        styled = _apply_distinct_feature_styles(gdf, id_column="tract_geoid")

        first_color = styled.loc[0, "__map_fill_color"]
        second_color = styled.loc[1, "__map_fill_color"]
        third_color = styled.loc[2, "__map_fill_color"]

        assert first_color != second_color
        assert first_color == third_color
