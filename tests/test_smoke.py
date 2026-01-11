"""End-to-end smoke tests for the CoC Lab pipeline.

This module tests the complete workflow:
1. Ingest boundary data (using fixtures for offline CI)
2. Register the vintage in the registry
3. Render a CoC map

These tests can run offline using fixture data.
"""

from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Polygon
from typer.testing import CliRunner

from coclab.cli.main import app
from coclab.geo.normalize import normalize_boundaries
from coclab.registry import latest_vintage, list_boundaries, register_vintage
from coclab.viz import render_coc_map

runner = CliRunner()


@pytest.fixture
def smoke_test_env(tmp_path, monkeypatch):
    """Set up a complete test environment with fixture data.

    Creates:
    - data/raw/ directory for raw downloads
    - data/curated/ directory for processed data
    - Sample GeoParquet file with realistic CoC boundaries
    - Registry with the vintage registered
    """
    # Change to temp directory
    monkeypatch.chdir(tmp_path)

    # Create directory structure
    raw_dir = tmp_path / "data" / "raw"
    curated_dir = tmp_path / "data" / "curated"
    maps_dir = curated_dir / "maps"
    boundaries_dir = curated_dir / "coc_boundaries"
    raw_dir.mkdir(parents=True)
    curated_dir.mkdir(parents=True)
    maps_dir.mkdir(parents=True)
    boundaries_dir.mkdir(parents=True)

    return tmp_path


@pytest.fixture
def fixture_boundaries_gdf():
    """Create a realistic fixture GeoDataFrame with sample CoC boundaries.

    This fixture represents what the ingest process would produce after
    downloading and normalizing real HUD data.
    """
    # CO-500: Colorado Balance of State CoC (simplified polygon)
    co_500_poly = Polygon([
        (-109.0, 37.0),
        (-102.0, 37.0),
        (-102.0, 41.0),
        (-109.0, 41.0),
        (-109.0, 37.0),
    ])

    # NY-600: New York City CoC (multi-borough simplified)
    ny_600_poly = MultiPolygon([
        Polygon([
            (-74.3, 40.5),
            (-73.7, 40.5),
            (-73.7, 40.9),
            (-74.3, 40.9),
            (-74.3, 40.5),
        ]),
        Polygon([
            (-73.9, 40.8),
            (-73.7, 40.8),
            (-73.7, 41.0),
            (-73.9, 41.0),
            (-73.9, 40.8),
        ]),
    ])

    # CA-600: Los Angeles City & County CoC
    ca_600_poly = Polygon([
        (-118.7, 33.7),
        (-117.6, 33.7),
        (-117.6, 34.8),
        (-118.7, 34.8),
        (-118.7, 33.7),
    ])

    # TX-600: Arlington/Fort Worth CoC
    tx_600_poly = Polygon([
        (-97.5, 32.5),
        (-97.0, 32.5),
        (-97.0, 33.0),
        (-97.5, 33.0),
        (-97.5, 32.5),
    ])

    ingested_at = datetime.now(timezone.utc)

    gdf = gpd.GeoDataFrame({
        "coc_id": ["CO-500", "NY-600", "CA-600", "TX-600"],
        "coc_name": [
            "Colorado Balance of State CoC",
            "New York City CoC",
            "Los Angeles City & County CoC",
            "Arlington/Fort Worth CoC",
        ],
        "state_abbrev": ["CO", "NY", "CA", "TX"],
        "boundary_vintage": ["2025", "2025", "2025", "2025"],
        "source": ["hud_exchange_gis_tools"] * 4,
        "source_ref": ["https://www.hudexchange.info/gis-tools"] * 4,
        "ingested_at": [ingested_at] * 4,
    }, geometry=[co_500_poly, ny_600_poly, ca_600_poly, tx_600_poly], crs="EPSG:4326")

    return gdf


class TestSmokeEndToEnd:
    """End-to-end smoke tests for the complete pipeline."""

    def test_smoke_ingest_normalize_register_render(
        self, smoke_test_env, fixture_boundaries_gdf
    ):
        """Test complete pipeline: ingest -> normalize -> register -> render."""
        # Step 1: Normalize boundaries (simulates what ingest does after download)
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)

        # Verify normalization added geom_hash
        assert "geom_hash" in normalized_gdf.columns
        assert normalized_gdf["geom_hash"].notna().all()

        # Step 2: Save as GeoParquet (simulates end of ingest)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        assert parquet_path.exists()

        # Step 3: Register the vintage
        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        entry = register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        assert entry.boundary_vintage == vintage
        assert entry.feature_count == 4

        # Verify registry works
        vintages = list_boundaries(registry_path=registry_path)
        assert len(vintages) == 1
        assert vintages[0].boundary_vintage == vintage

        # Verify latest_vintage works
        latest = latest_vintage(registry_path=registry_path)
        assert latest == vintage

        # Step 4: Render a CoC map
        map_path = render_coc_map("CO-500", vintage=vintage)

        assert map_path.exists()
        assert map_path.suffix == ".html"

        # Verify map content
        content = map_path.read_text()
        assert "CO-500" in content
        assert "Colorado Balance of State CoC" in content

    def test_smoke_multiple_cocs_can_be_rendered(
        self, smoke_test_env, fixture_boundaries_gdf
    ):
        """Test that multiple CoCs can be rendered from the same vintage."""
        # Setup: normalize and save
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        # Register
        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        # Render multiple CoCs
        coc_ids = ["CO-500", "NY-600", "CA-600", "TX-600"]
        map_paths = []

        for coc_id in coc_ids:
            path = render_coc_map(coc_id, vintage=vintage)
            map_paths.append(path)
            assert path.exists()
            assert coc_id in path.name

        # All paths should be unique
        assert len(set(map_paths)) == len(coc_ids)


class TestSmokeCLI:
    """Smoke tests for CLI commands."""

    def test_cli_list_boundaries_empty(self, smoke_test_env):
        """Test list-boundaries command with empty registry."""
        result = runner.invoke(app, ["list-boundaries"])
        assert result.exit_code == 0
        assert "No vintages registered" in result.stdout

    def test_cli_list_boundaries_with_data(self, smoke_test_env, fixture_boundaries_gdf):
        """Test list-boundaries command shows registered vintages."""
        # Setup
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        # Test
        result = runner.invoke(app, ["list-boundaries"])
        assert result.exit_code == 0
        assert "2025" in result.stdout
        assert "hud_exchange_gis_tools" in result.stdout

    def test_cli_show_renders_map(self, smoke_test_env, fixture_boundaries_gdf):
        """Test show command renders a map via CLI."""
        # Setup
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        # Test show command
        result = runner.invoke(app, ["show", "--coc", "CO-500", "--vintage", "2025"])
        assert result.exit_code == 0
        assert "Map saved to" in result.stdout
        assert "CO-500" in result.stdout

    def test_cli_show_invalid_coc_fails(self, smoke_test_env, fixture_boundaries_gdf):
        """Test show command fails gracefully for invalid CoC."""
        # Setup
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        # Test with invalid CoC
        result = runner.invoke(app, ["show", "--coc", "INVALID-999", "--vintage", "2025"])
        assert result.exit_code == 1
        # Error message may be in stdout or stderr (result.output combines both)
        output = result.output.lower() if result.output else ""
        assert "not found" in output or "error" in output or result.exit_code == 1

    def test_cli_ingest_requires_options(self, smoke_test_env):
        """Test ingest command requires --source option."""
        result = runner.invoke(app, ["ingest"])
        # Should fail or show help when missing required option
        assert result.exit_code != 0 or "source" in result.stdout.lower()


class TestSmokeDataIntegrity:
    """Smoke tests for data integrity through the pipeline."""

    def test_geom_hash_is_deterministic(self, fixture_boundaries_gdf):
        """Test that geometry hashing is deterministic across runs."""
        gdf1 = normalize_boundaries(fixture_boundaries_gdf.copy())
        gdf2 = normalize_boundaries(fixture_boundaries_gdf.copy())

        # Hashes should match for same geometries
        for coc_id in gdf1["coc_id"]:
            hash1 = gdf1.loc[gdf1["coc_id"] == coc_id, "geom_hash"].iloc[0]
            hash2 = gdf2.loc[gdf2["coc_id"] == coc_id, "geom_hash"].iloc[0]
            assert hash1 == hash2, f"Hash mismatch for {coc_id}"

    def test_roundtrip_parquet_preserves_data(
        self, smoke_test_env, fixture_boundaries_gdf
    ):
        """Test that data survives GeoParquet round-trip."""
        # Normalize and save
        original_gdf = normalize_boundaries(fixture_boundaries_gdf)
        parquet_path = smoke_test_env / "data" / "curated" / "test_roundtrip.parquet"
        original_gdf.to_parquet(parquet_path)

        # Load back
        loaded_gdf = gpd.read_parquet(parquet_path)

        # Verify schema preserved
        assert set(original_gdf.columns) == set(loaded_gdf.columns)

        # Verify data preserved
        assert len(original_gdf) == len(loaded_gdf)
        assert set(original_gdf["coc_id"]) == set(loaded_gdf["coc_id"])

        # Verify geometries preserved
        for coc_id in original_gdf["coc_id"]:
            orig_geom = original_gdf.loc[
                original_gdf["coc_id"] == coc_id, "geometry"
            ].iloc[0]
            loaded_geom = loaded_gdf.loc[
                loaded_gdf["coc_id"] == coc_id, "geometry"
            ].iloc[0]
            assert orig_geom.equals(loaded_geom), f"Geometry mismatch for {coc_id}"

    def test_crs_preserved_through_pipeline(
        self, smoke_test_env, fixture_boundaries_gdf
    ):
        """Test that CRS (EPSG:4326) is preserved through the pipeline."""
        # Normalize
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        assert normalized_gdf.crs.to_epsg() == 4326

        # Save and reload
        parquet_path = smoke_test_env / "data" / "curated" / "test_crs.parquet"
        normalized_gdf.to_parquet(parquet_path)
        loaded_gdf = gpd.read_parquet(parquet_path)

        assert loaded_gdf.crs.to_epsg() == 4326


class TestSmokeEdgeCases:
    """Smoke tests for edge cases and error handling."""

    def test_render_with_no_registry_fails_gracefully(self, smoke_test_env):
        """Test that rendering without registry gives clear error."""
        with pytest.raises(ValueError, match="No boundary vintages available"):
            render_coc_map("CO-500")

    def test_render_nonexistent_vintage_fails_gracefully(
        self, smoke_test_env, fixture_boundaries_gdf
    ):
        """Test that rendering nonexistent vintage gives clear error."""
        # Setup with vintage 2025
        normalized_gdf = normalize_boundaries(fixture_boundaries_gdf)
        vintage = "2025"
        parquet_path = smoke_test_env / "data" / "curated" / "coc_boundaries" / f"coc_boundaries__{vintage}.parquet"
        normalized_gdf.to_parquet(parquet_path)

        registry_path = smoke_test_env / "data" / "curated" / "boundary_registry.parquet"
        register_vintage(
            boundary_vintage=vintage,
            source="hud_exchange_gis_tools",
            path=parquet_path,
            feature_count=len(normalized_gdf),
            registry_path=registry_path,
        )

        # Try to render with nonexistent vintage
        with pytest.raises(FileNotFoundError):
            render_coc_map("CO-500", vintage="1999")
