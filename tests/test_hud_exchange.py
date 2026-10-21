"""Tests for HUD Exchange GIS Tools ingester."""

import hashlib
import re
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from hhplab.geographies.boundaries.hud.exchange_gis import (
    _HUD_STATE_ABBREVIATIONS,
    COC_ID_FIELDS,
    _ArchiveSource,
    _download_per_state_shapefiles,
    _extract_state_from_coc_id,
    _find_field,
    _HudExchangeDownload,
    _should_use_arcgis_source,
    download_hud_exchange_gdb,
    fetch_from_arcgis,
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
                return_provenance=True,
            )
            assert result.path.exists()
            assert "National_Boundary_2020.zip" in result.source_url
            assert result.archive_sources[0].raw_sha256 == hashlib.sha256(zip_content).hexdigest()

    def test_download_ignores_stale_shapefile_in_reused_output_directory(
        self, httpx_mock, tmp_path
    ):
        output_dir = tmp_path / "raw"
        stale_shp = _create_test_shapefile(output_dir)
        stale_gdf = gpd.read_file(stale_shp)
        stale_gdf["COCNUM"] = ["ZZ-999", "ZZ-998"]
        stale_gdf.to_file(stale_shp)
        httpx_mock.add_response(
            url=re.compile(r".*National_Boundary.*"),
            content=_create_test_shapefile_zip(),
        )

        download = download_hud_exchange_gdb(
            boundary_vintage="2020",
            output_dir=output_dir,
            return_provenance=True,
        )

        assert download.path != stale_shp
        assert download.path.is_relative_to(output_dir)
        assert read_coc_boundaries(download.path)["COCNUM"].tolist() == ["CO-500", "CO-503"]

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
                return_provenance=True,
            )
            assert result.path.exists()
            assert result.source_url.endswith("CoC_GIS_NatlTerrDC_Shapefile_2020.zip")
            assert result.archive_sources[0].raw_sha256 == hashlib.sha256(zip_content).hexdigest()

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
            "hhplab.geographies.boundaries.hud.exchange_gis._download_per_state_shapefiles",
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

    def test_reads_nested_shapefile_collection(self, tmp_path):
        """Extracted HUD per-state ZIPs can contain one nested shapefile per CoC."""
        _create_test_shapefile(tmp_path / "Maryland" / "MD_508")
        _create_test_shapefile(tmp_path / "Maryland" / "MD_512")

        gdf = read_coc_boundaries(tmp_path)

        assert len(gdf) == 4
        assert "COCNUM" in gdf.columns

    def test_nested_collection_ignores_cdbg_companion_shapefiles(self, tmp_path):
        """HUD per-state ZIPs can include non-boundary CDBG companions."""
        _create_test_shapefile(tmp_path / "California" / "CA_520")
        cdbg_path = _create_test_shapefile(tmp_path / "California" / "CA_520_CDBG")
        for component in cdbg_path.parent.glob("test_coc.*"):
            component.rename(component.with_name(f"CA_520_CDBG{component.suffix}"))

        gdf = read_coc_boundaries(tmp_path)

        assert len(gdf) == 2


class TestIngestHudExchange:
    """Integration tests for the full ingestion pipeline."""

    @pytest.fixture(autouse=True)
    def mock_registry(self, monkeypatch):
        """Mock the registry to avoid temp directory rejection in tests."""
        monkeypatch.setattr(
            "hhplab.registry.register_vintage",
            lambda **kwargs: None,
        )
        monkeypatch.setattr(
            "hhplab.geographies.boundaries.hud.exchange_gis.check_source_changed",
            lambda **kwargs: (False, {"is_new": True, "previous_sha256": None}),
        )
        monkeypatch.setattr(
            "hhplab.geographies.boundaries.hud.exchange_gis.register_source",
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
            assert gdf["source_ref"].str.startswith("file://").all()

            from hhplab.storage.provenance import read_provenance

            provenance = read_provenance(result)
            assert provenance.extra["source_type"] == "local_boundary_input"
            assert provenance.extra["local_path"] == str((raw_dir / "test_coc.shp").resolve())

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
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        assert hasattr(mod, "make_run_id"), "hud_exchange_gis should import make_run_id"
        # Verify make_run_id produces timestamp format, not date-only
        rid = mod.make_run_id()
        assert re.fullmatch(r"\d{8}-\d{6}", rid), (
            f"make_run_id() returned {rid!r}, expected YYYYMMDD-HHMMSS"
        )

    @pytest.mark.parametrize(
        ("boundary_vintage", "expected"),
        [
            ("2019", False),
            ("2023", False),
            ("2024", False),
            ("2025", False),
            ("HUDOpenData_2025-01-04", True),
            ("FY2024", False),
        ],
    )
    def test_arcgis_default_policy_requires_explicit_snapshot_label(
        self,
        boundary_vintage: str,
        expected: bool,
    ):
        assert _should_use_arcgis_source(boundary_vintage) is expected

    def test_arcgis_fetch_rejects_numeric_year_label_before_network(self, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        fetched = False

        def record_fetch(*args, **kwargs):
            nonlocal fetched
            fetched = True
            raise AssertionError("invalid numeric labels must fail before network access")

        monkeypatch.setattr(mod, "_fetch_all_arcgis_features", record_fetch)
        with pytest.raises(ValueError, match="retrieval-date label"):
            fetch_from_arcgis("2025")
        assert not fetched

    def test_arcgis_fetch_rejects_stale_snapshot_label_before_network(self, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        fetched = False

        def record_fetch(*args, **kwargs):
            nonlocal fetched
            fetched = True
            raise AssertionError("stale snapshot labels must fail before network access")

        monkeypatch.setattr(mod, "_fetch_all_arcgis_features", record_fetch)
        with pytest.raises(ValueError, match="does not match today's UTC date"):
            fetch_from_arcgis("HUDOpenData_2025-01-04")
        assert not fetched

    def test_arcgis_fetch_records_today_snapshot_label(self, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        today_label = f"HUDOpenData_{datetime.now(UTC).date().isoformat()}"
        feature = {
            "properties": {
                "COCNUM": "CO-500",
                "COCNAME": "Colorado Balance of State CoC",
                "STUSAB": "CO",
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-105, 39], [-105, 40], [-104, 40], [-104, 39], [-105, 39]]],
            },
        }
        monkeypatch.setattr(
            mod,
            "_fetch_all_arcgis_features",
            lambda client: ([feature], [b"page"]),
        )

        result, raw_pages = fetch_from_arcgis(today_label)

        assert result["boundary_vintage"].tolist() == [today_label]
        assert result["source_ref"].tolist() == [mod.ARCGIS_SOURCE_REF]
        assert raw_pages == [b"page"]

    def test_ingest_current_snapshot_writes_dated_canonical_path(self, tmp_path, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        today_label = f"HUDOpenData_{datetime.now(UTC).date().isoformat()}"
        raw_dir = tmp_path / "raw"
        curated_dir = tmp_path / "curated"
        polygon = Polygon([(-105, 39), (-105, 40), (-104, 40), (-104, 39), (-105, 39)])
        source = gpd.GeoDataFrame(
            {
                "COCNUM": ["CO-500"],
                "COCNAME": ["Colorado Balance of State CoC"],
                "ST": ["CO"],
            },
            geometry=[polygon],
            crs="EPSG:4326",
        )

        def fake_fetch(boundary_vintage, show_progress=False):
            assert boundary_vintage == today_label
            return map_to_canonical_schema(source, today_label, mod.ARCGIS_SOURCE_REF), [b"page"]

        monkeypatch.setattr(mod, "fetch_from_arcgis", fake_fetch)
        monkeypatch.setattr(
            mod,
            "write_api_snapshot",
            lambda *args, **kwargs: (raw_dir / "snapshot", "a" * 64, 4),
        )

        result = ingest_hud_exchange(
            boundary_vintage=today_label,
            raw_dir=raw_dir,
            curated_dir=curated_dir,
        )

        assert result.name == f"coc__B{today_label}.parquet"
        saved = gpd.read_parquet(result)
        assert saved["boundary_vintage"].tolist() == [today_label]
        assert saved["source_ref"].tolist() == [mod.ARCGIS_SOURCE_REF]

    @pytest.mark.parametrize("boundary_vintage", ["2019", "2024", "2025"])
    def test_numeric_vintage_uses_year_archive_by_default(
        self, boundary_vintage, tmp_path, monkeypatch
    ):
        """Numeric year ingests use year-specific archives, never the live layer."""
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        raw_dir = tmp_path / "raw"
        curated_dir = tmp_path / "curated"
        observed: dict[str, object] = {}

        def fail_arcgis(*args, **kwargs):
            raise AssertionError("historical ingest should not call ArcGIS by default")

        def fake_download(boundary_vintage, output_dir, url=None, return_provenance=False):
            assert return_provenance
            observed["boundary_vintage"] = boundary_vintage
            observed["url"] = url
            data_path = _create_test_shapefile(output_dir)
            source_url = f"https://example.com/actual-{boundary_vintage}.zip"
            return _HudExchangeDownload(
                data_path,
                (_ArchiveSource(source_url, "a" * 64, 123),),
            )

        def capture_check_source_changed(**kwargs):
            observed["source_url"] = kwargs["source_url"]
            return False, {"is_new": True, "previous_sha256": None}

        monkeypatch.setattr(mod, "fetch_from_arcgis", fail_arcgis)
        monkeypatch.setattr(mod, "download_hud_exchange_gdb", fake_download)
        monkeypatch.setattr(mod, "check_source_changed", capture_check_source_changed)

        result = ingest_hud_exchange(
            boundary_vintage=boundary_vintage,
            raw_dir=raw_dir,
            curated_dir=curated_dir,
        )

        assert result.exists()
        assert observed["boundary_vintage"] == boundary_vintage
        assert observed["url"] is None
        assert observed["source_url"] == f"https://example.com/actual-{boundary_vintage}.zip"

    def test_ingest_registers_per_state_archive_provenance(self, tmp_path, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        raw_dir = tmp_path / "raw"
        curated_dir = tmp_path / "curated"
        data_path = _create_test_shapefile(tmp_path / "downloaded")
        archive_sources = (
            _ArchiveSource("https://example.com/CO.zip", "a" * 64, 123),
            _ArchiveSource("https://example.com/NY.zip", "b" * 64, 456),
        )
        observed: dict[str, object] = {}

        monkeypatch.setattr(
            mod,
            "download_hud_exchange_gdb",
            lambda **kwargs: _HudExchangeDownload(data_path, archive_sources),
        )
        monkeypatch.setattr(
            mod,
            "register_source",
            lambda **kwargs: observed.update(kwargs),
        )

        result = ingest_hud_exchange(
            boundary_vintage="2024",
            raw_dir=raw_dir,
            curated_dir=curated_dir,
        )

        expected_source_url = '["https://example.com/CO.zip","https://example.com/NY.zip"]'
        combined_digests = bytes.fromhex("a" * 64) + bytes.fromhex("b" * 64)
        expected_hash = hashlib.sha256(combined_digests).hexdigest()
        saved = gpd.read_parquet(result)
        assert set(saved["source_ref"]) == {expected_source_url}
        assert observed["source_url"] == expected_source_url
        assert observed["raw_sha256"] == expected_hash
        assert observed["file_size"] == 579
        assert observed["metadata"]["archive_sources"] == [
            {
                "source_url": source.url,
                "raw_sha256": source.raw_sha256,
                "file_size": source.file_size,
            }
            for source in archive_sources
        ]

        from hhplab.storage.provenance import read_provenance

        provenance = read_provenance(result)
        assert provenance.extra["archive_sources"] == observed["metadata"]["archive_sources"]
        assert provenance.extra["raw_sha256"] == expected_hash
        assert provenance.extra["raw_sha256_algorithm"] == (
            "SHA-256 of concatenated binary component ZIP SHA-256 digests in download order"
        )

    def test_numeric_archive_failure_does_not_fall_back_to_arcgis(self, tmp_path, monkeypatch):
        import hhplab.geographies.boundaries.hud.exchange_gis as mod

        def fail_archive(*args, **kwargs):
            raise RuntimeError("year-specific archive unavailable")

        def fail_arcgis(*args, **kwargs):
            raise AssertionError("numeric vintage must not call the live ArcGIS service")

        monkeypatch.setattr(mod, "download_hud_exchange_gdb", fail_archive)
        monkeypatch.setattr(mod, "fetch_from_arcgis", fail_arcgis)

        with pytest.raises(RuntimeError, match="year-specific archive unavailable"):
            ingest_hud_exchange(
                boundary_vintage="2025",
                raw_dir=tmp_path / "raw",
                curated_dir=tmp_path / "curated",
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
                return shp_path, _ArchiveSource(url, "a" * 64, 123)
            return None

        monkeypatch.setattr(
            "hhplab.geographies.boundaries.hud.exchange_gis._download_and_extract_zip",
            fake_download_and_extract,
        )

        result = _download_per_state_shapefiles("2020", tmp_path)
        assert result is None, "Per-state fallback should return None when most states fail"

    def test_full_success_returns_path(self, tmp_path, monkeypatch):
        """If all states succeed, _download_per_state_shapefiles returns a path."""

        def fake_download_and_extract(url, sub_dir, filename):
            shp_path = _create_test_shapefile(sub_dir)
            return shp_path, _ArchiveSource(url, "a" * 64, 123)

        monkeypatch.setattr(
            "hhplab.geographies.boundaries.hud.exchange_gis._download_and_extract_zip",
            fake_download_and_extract,
        )

        result = _download_per_state_shapefiles("2020", tmp_path)
        assert result is not None
        assert result.path.exists()
        assert len(result.archive_sources) == len(_HUD_STATE_ABBREVIATIONS)


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
