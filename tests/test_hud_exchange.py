"""Tests for HUD Exchange GIS Tools ingester."""

import re
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from coclab.ingest.hud_exchange_gis import (
    _HUD_STATE_ABBREVIATIONS,
    COC_ID_FIELDS,
    _download_per_state_shapefiles,
    _extract_state_from_coc_id,
    _find_field,
    download_hud_exchange_gdb,
    ingest_hud_exchange,
    map_to_canonical_schema,
    read_coc_boundaries,
)


class TestFindField:
    """Tests for the _find_field helper function."""

    def test_find_exact_match(self):
        columns = ["COCNUM", "COCNAME", "geometry"]
        assert _find_field(columns, COC_ID_FIELDS) == "COCNUM"

    def test_find_case_insensitive(self):
        columns = ["cocnum", "cocname", "geometry"]
        assert _find_field(columns, COC_ID_FIELDS) == "cocnum"

    def test_find_alternative_name(self):
        columns = ["COC_NUM", "COC_NAME", "geometry"]
        assert _find_field(columns, COC_ID_FIELDS) == "COC_NUM"

    def test_not_found_returns_none(self):
        columns = ["some_field", "other_field"]
        assert _find_field(columns, COC_ID_FIELDS) is None


class TestExtractStateFromCocId:
    """Tests for the _extract_state_from_coc_id helper function."""

    def test_extracts_state(self):
        assert _extract_state_from_coc_id("CO-500") == "CO"
        assert _extract_state_from_coc_id("CA-600") == "CA"
        assert _extract_state_from_coc_id("NY-510") == "NY"

    def test_handles_missing_dash(self):
        assert _extract_state_from_coc_id("CO500") == ""

    def test_handles_empty_string(self):
        assert _extract_state_from_coc_id("") == ""

    def test_handles_none_like(self):
        # The function expects a string, but should handle edge cases gracefully
        assert _extract_state_from_coc_id("None") == ""


class TestMapToCanonicalSchema:
    """Tests for mapping source fields to canonical schema."""

    @pytest.fixture
    def sample_gdf(self):
        """Create a sample GeoDataFrame with typical HUD Exchange fields."""
        polygon = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
        return gpd.GeoDataFrame(
            {
                "COCNUM": ["CO-500", "CO-503"],
                "COCNAME": ["Colorado Balance of State CoC", "Metropolitan Denver CoC"],
                "ST": ["CO", "CO"],
            },
            geometry=[polygon, polygon],
            crs="EPSG:4326",
        )

    def test_maps_standard_fields(self, sample_gdf):
        result = map_to_canonical_schema(sample_gdf, "2024", "https://example.com/data.zip")

        assert "coc_id" in result.columns
        assert "coc_name" in result.columns
        assert "state_abbrev" in result.columns
        assert "boundary_vintage" in result.columns
        assert "source" in result.columns
        assert "source_ref" in result.columns
        assert "ingested_at" in result.columns

    def test_sets_correct_values(self, sample_gdf):
        result = map_to_canonical_schema(sample_gdf, "2024", "https://example.com/data.zip")

        assert list(result["coc_id"]) == ["CO-500", "CO-503"]
        assert list(result["coc_name"]) == [
            "Colorado Balance of State CoC",
            "Metropolitan Denver CoC",
        ]
        assert list(result["state_abbrev"]) == ["CO", "CO"]
        assert all(result["boundary_vintage"] == "2024")
        assert all(result["source"] == "hud_exchange")
        assert all(result["source_ref"] == "https://example.com/data.zip")
        assert all(isinstance(dt, datetime) for dt in result["ingested_at"])

    def test_handles_alternative_field_names(self):
        """Test that alternative field names are recognized."""
        polygon = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
        gdf = gpd.GeoDataFrame(
            {
                "COC_NUM": ["CO-500"],
                "COC_NAME": ["Colorado BoS"],
                "STATE": ["CO"],
            },
            geometry=[polygon],
            crs="EPSG:4326",
        )

        result = map_to_canonical_schema(gdf, "2023", "https://example.com")
        assert result["coc_id"].iloc[0] == "CO-500"
        assert result["coc_name"].iloc[0] == "Colorado BoS"

    def test_extracts_state_from_coc_id_when_missing(self):
        """Test that state is extracted from coc_id when state field is missing."""
        polygon = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
        gdf = gpd.GeoDataFrame(
            {
                "COCNUM": ["NY-510", "CA-600"],
                "COCNAME": ["New York CoC", "California CoC"],
            },
            geometry=[polygon, polygon],
            crs="EPSG:4326",
        )

        result = map_to_canonical_schema(gdf, "2024", "https://example.com")
        assert list(result["state_abbrev"]) == ["NY", "CA"]

    def test_raises_on_missing_coc_id_field(self):
        polygon = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
        gdf = gpd.GeoDataFrame(
            {"name": ["test"]},
            geometry=[polygon],
            crs="EPSG:4326",
        )

        with pytest.raises(ValueError, match="Could not find CoC ID field"):
            map_to_canonical_schema(gdf, "2024", "https://example.com")


class TestDownloadHudExchangeGdb:
    """Tests for the download function."""

    def test_download_creates_directory(self, httpx_mock):
        """Test that download creates the output directory."""
        # Create a minimal zip file in memory
        zip_content = _create_test_shapefile_zip()
        httpx_mock.add_response(content=zip_content)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            download_hud_exchange_gdb(
                boundary_vintage="2024",
                output_dir=output_dir,
                url="https://example.com/test.zip",
            )
            assert output_dir.exists()

    def test_download_extracts_zip(self, httpx_mock):
        """Test that the downloaded zip is extracted."""
        zip_content = _create_test_shapefile_zip()
        httpx_mock.add_response(content=zip_content)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            download_hud_exchange_gdb(
                boundary_vintage="2024",
                output_dir=output_dir,
                url="https://example.com/test.zip",
            )
            # Check that something was extracted
            assert output_dir.exists()
            assert any(output_dir.iterdir())


    def test_fallback_to_national_boundary_url(self, httpx_mock):
        """When no explicit URL given, tries national boundary URL first."""
        zip_content = _create_test_shapefile_zip()
        # National boundary URL succeeds
        httpx_mock.add_response(
            url=re.compile(r".*National_Boundary.*"),
            content=zip_content,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            result = download_hud_exchange_gdb(
                boundary_vintage="2020",
                output_dir=output_dir,
            )
            assert result.exists()

    def test_fallback_chain_national_fails_legacy_succeeds(self, httpx_mock):
        """National URL 404 → legacy URL succeeds."""
        zip_content = _create_test_shapefile_zip()
        # National boundary URL fails (404)
        httpx_mock.add_response(
            url=re.compile(r".*National_Boundary.*"),
            status_code=404,
        )
        # Legacy URL succeeds
        httpx_mock.add_response(
            url=re.compile(r".*NatlTerrDC.*"),
            content=zip_content,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            result = download_hud_exchange_gdb(
                boundary_vintage="2020",
                output_dir=output_dir,
            )
            assert result.exists()

    def test_all_downloads_fail_raises_runtime_error(self, httpx_mock, monkeypatch):
        """All URLs fail → RuntimeError with helpful message."""
        # National boundary URL → 404
        httpx_mock.add_response(
            url=re.compile(r".*National_Boundary.*"),
            status_code=404,
        )
        # Legacy URL → 404
        httpx_mock.add_response(
            url=re.compile(r".*NatlTerrDC.*"),
            status_code=404,
        )
        # Stub per-state fallback to avoid 55 HTTP requests in test
        monkeypatch.setattr(
            "coclab.ingest.hud_exchange_gis._download_per_state_shapefiles",
            lambda *a, **kw: None,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            with pytest.raises(RuntimeError, match="All HUD Exchange download sources failed"):
                download_hud_exchange_gdb(
                    boundary_vintage="2020",
                    output_dir=output_dir,
                )

    def test_explicit_url_failure_raises_runtime_error(self, httpx_mock):
        """Explicit URL failure raises RuntimeError, doesn't try fallbacks."""
        httpx_mock.add_response(status_code=404)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test_vintage"
            with pytest.raises(RuntimeError, match="Download from explicit URL failed"):
                download_hud_exchange_gdb(
                    boundary_vintage="2020",
                    output_dir=output_dir,
                    url="https://example.com/bad.zip",
                )


class TestReadCocBoundaries:
    """Tests for reading CoC boundary data."""

    def test_reads_shapefile(self):
        """Test reading a shapefile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            shp_path = _create_test_shapefile(Path(tmpdir))
            gdf = read_coc_boundaries(shp_path)
            assert len(gdf) == 2
            assert "COCNUM" in gdf.columns


class TestIngestHudExchange:
    """Integration tests for the full ingestion pipeline."""

    @pytest.fixture(autouse=True)
    def mock_registry(self, monkeypatch):
        """Mock the registry to avoid temp directory rejection in tests."""
        monkeypatch.setattr(
            "coclab.registry.register_vintage",
            lambda **kwargs: None,
        )
        monkeypatch.setattr(
            "coclab.ingest.hud_exchange_gis.check_source_changed",
            lambda **kwargs: (False, {"is_new": True, "previous_sha256": None}),
        )
        monkeypatch.setattr(
            "coclab.ingest.hud_exchange_gis.register_source",
            lambda **kwargs: None,
        )

    def test_ingest_with_skip_download(self):
        """Test ingestion with pre-downloaded data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            curated_dir = Path(tmpdir)

            # Create test shapefile
            _create_test_shapefile(raw_dir)

            # Run ingestion
            result = ingest_hud_exchange(
                boundary_vintage="2024",
                raw_dir=raw_dir,
                curated_dir=curated_dir,
                skip_download=True,
            )

            # Check output exists
            assert result.exists()
            assert result.suffix == ".parquet"

            # Check contents
            gdf = gpd.read_parquet(result)
            assert "coc_id" in gdf.columns
            assert "coc_name" in gdf.columns
            assert "state_abbrev" in gdf.columns
            assert "boundary_vintage" in gdf.columns
            assert "source" in gdf.columns
            assert "source_ref" in gdf.columns
            assert "ingested_at" in gdf.columns
            assert "geom_hash" in gdf.columns
            assert "geometry" in gdf.columns

            # Verify canonical values
            assert all(gdf["source"] == "hud_exchange")
            assert all(gdf["boundary_vintage"] == "2024")

    def test_ingest_normalizes_crs(self):
        """Test that ingestion normalizes CRS to EPSG:4326."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            curated_dir = Path(tmpdir)

            # Create test shapefile with a different CRS
            _create_test_shapefile(raw_dir, crs="EPSG:3857")

            result = ingest_hud_exchange(
                boundary_vintage="2024",
                raw_dir=raw_dir,
                curated_dir=curated_dir,
                skip_download=True,
            )

            gdf = gpd.read_parquet(result)
            assert gdf.crs.to_epsg() == 4326

    def test_ingest_computes_geom_hash(self):
        """Test that geometry hashes are computed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            curated_dir = Path(tmpdir)
            _create_test_shapefile(raw_dir)

            result = ingest_hud_exchange(
                boundary_vintage="2024",
                raw_dir=raw_dir,
                curated_dir=curated_dir,
                skip_download=True,
            )

            gdf = gpd.read_parquet(result)
            assert all(gdf["geom_hash"].notna())
            # Hashes should be SHA-256 hex strings (64 chars)
            assert all(len(h) == 64 for h in gdf["geom_hash"])

    def test_arcgis_path_uses_make_run_id(self):
        """hud_exchange_gis should import and use make_run_id for collision-resistant run_ids."""
        import coclab.ingest.hud_exchange_gis as mod

        assert hasattr(mod, "make_run_id"), "hud_exchange_gis should import make_run_id"
        # Verify make_run_id produces timestamp format, not date-only
        rid = mod.make_run_id()
        assert re.fullmatch(r"\d{8}-\d{6}", rid), (
            f"make_run_id() returned {rid!r}, expected YYYYMMDD-HHMMSS"
        )


class TestPerStateFallbackCompleteness:
    """Regression tests: partial per-state download must fail, not return truncated data."""

    def test_partial_success_below_threshold_returns_none(self, tmp_path, monkeypatch):
        """If most states fail, _download_per_state_shapefiles returns None."""
        # Stub the download to succeed only for the first 5 states.
        succeed_states = set(_HUD_STATE_ABBREVIATIONS[:5])

        def fake_download_and_extract(url, sub_dir, filename):
            # Parse state from the sub_dir name
            state = sub_dir.name
            if state in succeed_states:
                shp_path = _create_test_shapefile(sub_dir)
                return shp_path
            return None

        monkeypatch.setattr(
            "coclab.ingest.hud_exchange_gis._download_and_extract_zip",
            fake_download_and_extract,
        )

        result = _download_per_state_shapefiles("2020", tmp_path)
        assert result is None, (
            "Per-state fallback should return None when most states fail"
        )

    def test_full_success_returns_path(self, tmp_path, monkeypatch):
        """If all states succeed, _download_per_state_shapefiles returns a path."""

        def fake_download_and_extract(url, sub_dir, filename):
            shp_path = _create_test_shapefile(sub_dir)
            return shp_path

        monkeypatch.setattr(
            "coclab.ingest.hud_exchange_gis._download_and_extract_zip",
            fake_download_and_extract,
        )

        result = _download_per_state_shapefiles("2020", tmp_path)
        assert result is not None
        assert result.exists()


# Helper functions for creating test data


def _create_test_shapefile(directory: Path, crs: str = "EPSG:4326") -> Path:
    """Create a test shapefile with sample CoC data."""
    directory.mkdir(parents=True, exist_ok=True)

    polygon1 = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
    polygon2 = Polygon([(-106, 39), (-106, 40), (-105, 40), (-105, 39), (-106, 39)])

    gdf = gpd.GeoDataFrame(
        {
            "COCNUM": ["CO-500", "CO-503"],
            "COCNAME": ["Colorado Balance of State CoC", "Metropolitan Denver CoC"],
            "ST": ["CO", "CO"],
        },
        geometry=[polygon1, polygon2],
        crs=crs,
    )

    shp_path = directory / "test_coc.shp"
    gdf.to_file(shp_path)
    return shp_path


def _create_test_shapefile_zip() -> bytes:
    """Create a zip file containing a test shapefile."""
    import io

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = _create_test_shapefile(Path(tmpdir))

        # Get all the shapefile component files
        shp_dir = shp_path.parent
        shp_stem = shp_path.stem

        # Create zip in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                file_path = shp_dir / f"{shp_stem}{ext}"
                if file_path.exists():
                    zf.write(file_path, file_path.name)

        return zip_buffer.getvalue()
