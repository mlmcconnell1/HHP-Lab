"""Tests for visualization module."""

from datetime import UTC, datetime

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from coclab.registry import register_vintage
from coclab.viz import render_coc_map


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
