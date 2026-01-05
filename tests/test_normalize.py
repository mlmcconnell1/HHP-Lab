"""Tests for geometry normalization utilities."""

from __future__ import annotations

import tempfile
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
)

from coclab.geo.io import (
    curated_boundary_path,
    read_geoparquet,
    registry_path,
    write_geoparquet,
)
from coclab.geo.normalize import (
    compute_geom_hash,
    ensure_polygon_type,
    fix_geometry,
    normalize_boundaries,
    normalize_crs,
)


class TestNormalizeCRS:
    """Tests for CRS normalization."""

    def test_already_4326(self):
        """GeoDataFrame already in EPSG:4326 should be returned unchanged."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )
        result = normalize_crs(gdf)
        assert result.crs.to_epsg() == 4326

    def test_convert_from_3857(self):
        """Web Mercator (3857) should be converted to 4326."""
        # A small polygon in Web Mercator
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (100000, 0), (100000, 100000), (0, 100000)])],
            crs="EPSG:3857",
        )
        result = normalize_crs(gdf)
        assert result.crs.to_epsg() == 4326
        # Coordinates should be in lat/lon range
        bounds = result.total_bounds
        assert -180 <= bounds[0] <= 180
        assert -90 <= bounds[1] <= 90

    def test_no_crs_raises(self):
        """GeoDataFrame without CRS should raise ValueError."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        )
        with pytest.raises(ValueError, match="no CRS defined"):
            normalize_crs(gdf)


class TestFixGeometry:
    """Tests for geometry validity fixes."""

    def test_valid_polygon_unchanged(self):
        """Valid polygon should be returned as-is."""
        geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = fix_geometry(geom)
        assert result.equals(geom)

    def test_none_returns_none(self):
        """None input should return None."""
        assert fix_geometry(None) is None

    def test_empty_returns_none(self):
        """Empty geometry should return None."""
        assert fix_geometry(Polygon()) is None

    def test_self_intersecting_polygon_fixed(self):
        """Self-intersecting (bowtie) polygon should be fixed."""
        # Bowtie polygon with self-intersection
        bowtie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
        assert not bowtie.is_valid
        result = fix_geometry(bowtie)
        assert result is not None
        assert result.is_valid


class TestEnsurePolygonType:
    """Tests for polygon type enforcement."""

    def test_polygon_unchanged(self):
        """Polygon should be returned as-is."""
        geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = ensure_polygon_type(geom)
        assert isinstance(result, Polygon)

    def test_multipolygon_unchanged(self):
        """MultiPolygon should be returned as-is."""
        geom = MultiPolygon([
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
        ])
        result = ensure_polygon_type(geom)
        assert isinstance(result, MultiPolygon)

    def test_point_returns_none(self):
        """Point should return None."""
        result = ensure_polygon_type(Point(0, 0))
        assert result is None

    def test_linestring_returns_none(self):
        """LineString should return None."""
        result = ensure_polygon_type(LineString([(0, 0), (1, 1)]))
        assert result is None

    def test_geometry_collection_extracts_polygons(self):
        """GeometryCollection should have polygon components extracted."""
        gc = GeometryCollection([
            Point(0, 0),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            LineString([(0, 0), (1, 1)]),
        ])
        result = ensure_polygon_type(gc)
        assert isinstance(result, Polygon)

    def test_geometry_collection_multiple_polygons(self):
        """GeometryCollection with multiple polygons should return MultiPolygon."""
        gc = GeometryCollection([
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
        ])
        result = ensure_polygon_type(gc)
        assert isinstance(result, MultiPolygon)
        assert len(result.geoms) == 2

    def test_geometry_collection_no_polygons(self):
        """GeometryCollection without polygons should return None."""
        gc = GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])])
        result = ensure_polygon_type(gc)
        assert result is None


class TestComputeGeomHash:
    """Tests for geometry hashing."""

    def test_hash_determinism(self):
        """Same geometry should produce same hash."""
        geom1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        geom2 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

        hash1 = compute_geom_hash(geom1)
        hash2 = compute_geom_hash(geom2)

        assert hash1 == hash2

    def test_different_geometries_different_hash(self):
        """Different geometries should produce different hashes."""
        geom1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        geom2 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])

        hash1 = compute_geom_hash(geom1)
        hash2 = compute_geom_hash(geom2)

        assert hash1 != hash2

    def test_hash_is_sha256_hex(self):
        """Hash should be a 64-character hex string (SHA-256)."""
        geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = compute_geom_hash(geom)

        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_none_returns_none(self):
        """None input should return None."""
        assert compute_geom_hash(None) is None

    def test_empty_returns_none(self):
        """Empty geometry should return None."""
        assert compute_geom_hash(Polygon()) is None

    def test_hash_stable_across_precision(self):
        """Hashing normalizes precision, so minor differences are ignored."""
        geom1 = Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
        # Add very small differences (below 1e-6 precision)
        geom2 = Polygon([
            (0.0000001, 0.0),
            (1.0, 0.0000001),
            (1.0, 1.0),
            (0.0, 1.0),
        ])

        hash1 = compute_geom_hash(geom1)
        hash2 = compute_geom_hash(geom2)

        # These should be equal due to precision normalization
        assert hash1 == hash2


class TestNormalizeBoundaries:
    """Tests for the full normalization pipeline."""

    def test_full_pipeline(self):
        """Full pipeline should normalize CRS, fix geometries, and add hash."""
        gdf = gpd.GeoDataFrame(
            {"coc_id": ["CO-500", "CO-501"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
            crs="EPSG:4326",
        )
        result = normalize_boundaries(gdf)

        assert result.crs.to_epsg() == 4326
        assert "geom_hash" in result.columns
        assert all(result["geom_hash"].notna())
        assert len(result) == 2

    def test_drops_non_polygon_geometries(self):
        """Non-polygon geometries should be filtered out with warning."""
        gdf = gpd.GeoDataFrame(
            {"coc_id": ["CO-500", "CO-501"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),  # This should be dropped
            ],
            crs="EPSG:4326",
        )
        with pytest.warns(UserWarning, match="Dropped 1 rows"):
            result = normalize_boundaries(gdf)

        assert len(result) == 1
        assert result.iloc[0]["coc_id"] == "CO-500"


class TestGeoParquetIO:
    """Tests for GeoParquet I/O helpers."""

    def test_write_and_read_roundtrip(self):
        """Writing and reading should preserve data."""
        gdf = gpd.GeoDataFrame(
            {"coc_id": ["CO-500"], "name": ["Test CoC"]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            written_path = write_geoparquet(gdf, path)

            assert written_path == path
            assert path.exists()

            loaded = read_geoparquet(path)
            assert len(loaded) == 1
            assert loaded.iloc[0]["coc_id"] == "CO-500"
            assert loaded.crs.to_epsg() == 4326

    def test_creates_parent_directories(self):
        """write_geoparquet should create parent directories."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "deep" / "test.parquet"
            write_geoparquet(gdf, path)
            assert path.exists()

    def test_read_nonexistent_raises(self):
        """Reading non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_geoparquet("/nonexistent/path.parquet")


class TestPathHelpers:
    """Tests for path helper functions."""

    def test_curated_boundary_path_default(self):
        """curated_boundary_path should return expected path format."""
        path = curated_boundary_path("2025")
        assert path == Path("data/curated/coc_boundaries__2025.parquet")

    def test_curated_boundary_path_custom_base(self):
        """curated_boundary_path should respect custom base_dir."""
        path = curated_boundary_path("2025", base_dir="/custom/data")
        assert path == Path("/custom/data/curated/coc_boundaries__2025.parquet")

    def test_registry_path_default(self):
        """registry_path should return expected path format."""
        path = registry_path()
        assert path == Path("data/curated/boundary_registry.parquet")

    def test_registry_path_custom_base(self):
        """registry_path should respect custom base_dir."""
        path = registry_path(base_dir="/custom/data")
        assert path == Path("/custom/data/curated/boundary_registry.parquet")
