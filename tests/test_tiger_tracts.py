"""Tests for TIGER tract ingest URL and schema resolution."""

import hashlib
from pathlib import Path

import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Point
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.geographies.boundaries.census.ingest import tiger_blocks
from hhplab.geographies.boundaries.census.ingest.tiger_blocks import (
    PART_RAW_SHA256_KEY,
    STATE_DOWNLOAD_ATTEMPTS,
    _block_geometry_parts_dir,
    _block_url,
    _block_zip_name,
    _download_state_blocks,
    _preserve_stale_state_part,
    _state_part_matches_source,
    _state_part_path,
    _state_part_raw_sha256,
    _stream_state_block_parts,
    _write_state_part,
    download_block_geometry,
    get_block_geometry_output_path,
    normalize_block_geometry,
    save_block_geometry,
    save_block_geometry_from_parts,
)
from hhplab.geographies.boundaries.census.ingest.tiger_tracts import (
    _resolve_geoid_column,
    _tract_2000_url,
    _tract_2000_zip_name,
    _tract_url,
    _tract_zip_name,
)
from hhplab.geographies.boundaries.census.ingest.urban_areas import (
    get_urban_area_output_path,
    normalize_urban_areas,
    save_urban_areas,
    urban_area_source,
)
from hhplab.storage.provenance import PROVENANCE_KEY, read_provenance

runner = CliRunner()


def test_tract_zip_name_uses_2010_suffix() -> None:
    """2010 tract downloads use the special tract10 filename suffix."""
    assert _tract_zip_name(2010, "51") == "tl_2010_51_tract10.zip"


def test_block_geometry_filename_uses_tabblock20_suffix() -> None:
    """2010 and 2020 tabulation blocks use their native TIGER URL patterns."""
    assert _block_zip_name(2010, "51") == "tl_2010_51_tabblock10.zip"
    assert _block_url(2010, "51").endswith("/TIGER2010/TABBLOCK/2010/tl_2010_51_tabblock10.zip")
    assert _block_zip_name(2020, "51") == "tl_2020_51_tabblock20.zip"
    assert _block_url(2020, "51").endswith("/TIGER2020/TABBLOCK20/tl_2020_51_tabblock20.zip")


def test_download_block_geometry_warns_about_in_memory_api() -> None:
    """Direct legacy callers receive a deprecation warning before the in-memory path runs."""
    with pytest.warns(DeprecationWarning, match="loads all state block geometry into memory"):
        with pytest.raises(ValueError, match="No Census tabulation block geometry rows fetched"):
            download_block_geometry(state_fips_codes=())


def test_tract_url_uses_2010_subdirectory() -> None:
    """2010 tract downloads use the extra /2010/ TIGER subdirectory."""
    assert _tract_url(2010, "51").endswith("/TIGER2010/TRACT/2010/tl_2010_51_tract10.zip")


def test_tract_url_uses_modern_pattern_after_2010() -> None:
    """Post-2010 tract downloads keep the standard state ZIP pattern."""
    assert _tract_url(2020, "51").endswith("/TIGER2020/TRACT/tl_2020_51_tract.zip")


def test_tract_2000_helpers_use_state_based_2010_archive() -> None:
    """Census 2000 tract shapefiles are state ZIPs under TIGER2010/TRACT/2000."""
    assert _tract_2000_zip_name("01") == "tl_2010_01_tract00.zip"
    assert _tract_2000_url("01").endswith("/TIGER2010/TRACT/2000/tl_2010_01_tract00.zip")


def test_resolve_geoid_column_accepts_2010_schema() -> None:
    """2010 tract shapefiles use GEOID10 instead of the modern GEOID field."""
    gdf = gpd.GeoDataFrame({"GEOID10": ["01001020100"]})
    assert _resolve_geoid_column(gdf) == "GEOID10"


def test_resolve_geoid_column_accepts_2000_schema() -> None:
    """Census 2000 tract shapefiles expose the full tract ID as CTIDFP00."""
    gdf = gpd.GeoDataFrame({"CTIDFP00": ["01001020100"]})
    assert _resolve_geoid_column(gdf) == "CTIDFP00"


def test_urban_area_source_supports_2010_and_2020() -> None:
    """Urban Area ingest uses verified national Census ZIPs for supported vintages."""
    assert urban_area_source(2010) == (
        "https://www2.census.gov/geo/pvs/tiger2010st/tl_2010_us_uac10.zip",
        "tl_2010_us_uac10.zip",
    )
    assert urban_area_source(2020) == (
        "https://www2.census.gov/geo/tiger/TIGER2020/UAC/tl_2020_us_uac20.zip",
        "tl_2020_us_uac20.zip",
    )


def test_urban_area_source_rejects_unsupported_vintage() -> None:
    """Unsupported Urban Area vintages fail with an actionable message."""
    with pytest.raises(ValueError, match="Supported vintages: 2010, 2020"):
        urban_area_source(2023)


def test_normalize_urban_areas_accepts_2010_schema() -> None:
    """2010 Urban Area shapefiles use GEOID10/NAME10/UATYP10 fields."""
    source = gpd.GeoDataFrame(
        {
            "GEOID10": ["1234"],
            "NAME10": ["Example Urban Cluster"],
            "UATYP10": ["C"],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = normalize_urban_areas(source, 2010)

    assert list(result.columns) == [
        "urban_area_geoid",
        "urban_area_name",
        "urban_area_type",
        "urban_area_vintage",
        "data_source",
        "source_ref",
        "ingested_at",
        "geometry",
    ]
    assert result.loc[0, "urban_area_geoid"] == "01234"
    assert result.loc[0, "urban_area_type"] == "urban_cluster"
    assert result.loc[0, "urban_area_vintage"] == 2010


def test_normalize_urban_areas_accepts_2020_schema() -> None:
    """2020 Urban Area shapefiles use GEOID20/NAME20/UATYP20 fields."""
    source = gpd.GeoDataFrame(
        {
            "GEOID20": ["56789"],
            "NAME20": ["Example Urban Area"],
            "UATYP20": ["U"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = normalize_urban_areas(source, 2020)

    assert result.loc[0, "urban_area_geoid"] == "56789"
    assert result.loc[0, "urban_area_type"] == "urbanized_area"
    assert result.loc[0, "source_ref"].endswith("tl_2020_us_uac20.zip")


def test_normalize_block_geometry_accepts_2020_schema() -> None:
    """2020 block shapefiles normalize to joinable block and tract identifiers."""
    source = gpd.GeoDataFrame(
        {
            "GEOID20": ["510010901011234"],
            "STATEFP20": ["51"],
            "COUNTYFP20": ["001"],
            "TRACTCE20": ["090101"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = normalize_block_geometry(source, 2020)

    assert list(result.columns) == [
        "block_geoid",
        "state_fips",
        "county_fips",
        "tract_geoid",
        "block_vintage",
        "data_source",
        "source_ref",
        "ingested_at",
        "geometry",
    ]
    assert result.loc[0, "block_geoid"] == "510010901011234"
    assert result.loc[0, "county_fips"] == "51001"
    assert result.loc[0, "tract_geoid"] == "51001090101"
    assert result.loc[0, "block_vintage"] == 2020


def test_normalize_block_geometry_rejects_unsupported_vintage() -> None:
    """Unsupported block vintages fail with an actionable message."""
    source = gpd.GeoDataFrame(
        {
            "GEOID20": ["510010901011234"],
            "STATEFP20": ["51"],
            "COUNTYFP20": ["001"],
            "TRACTCE20": ["090101"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="Unsupported block geometry vintage"):
        normalize_block_geometry(source, 2000)


def test_normalize_block_geometry_accepts_2010_schema() -> None:
    """2010 TIGER blocks retain full decennial GEOIDs for direct PL joins."""
    source = gpd.GeoDataFrame(
        {
            "GEOID10": ["510010901011234"],
            "STATEFP10": ["51"],
            "COUNTYFP10": ["001"],
            "TRACTCE10": ["090101"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    result = normalize_block_geometry(source, 2010)

    assert result.loc[0, "block_geoid"] == "510010901011234"
    assert result.loc[0, "county_fips"] == "51001"
    assert result.loc[0, "tract_geoid"] == "51001090101"
    assert result.loc[0, "block_vintage"] == 2010


def test_save_urban_areas_writes_geoparquet_with_provenance(tmp_path) -> None:
    """Saved Urban Area artifacts keep GeoParquet geometry and HHP provenance metadata."""
    gdf = gpd.GeoDataFrame(
        {
            "urban_area_geoid": ["56789"],
            "urban_area_name": ["Example Urban Area"],
            "urban_area_type": ["urbanized_area"],
            "urban_area_vintage": [2020],
            "data_source": ["census_tiger_urban_area"],
            "source_ref": ["https://example.test/uac.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    output_path = save_urban_areas(
        gdf,
        2020,
        output_dir=tmp_path,
        content_sha256="abc123",
        content_size=123,
        source_url="https://example.test/uac.zip",
    )

    assert output_path == tmp_path / "urban_areas__U2020.parquet"
    roundtrip = gpd.read_parquet(output_path)
    assert roundtrip.crs.to_epsg() == 4326
    provenance = read_provenance(output_path)
    assert provenance is not None
    assert provenance.extra["urban_area_vintage"] == 2020
    assert provenance.extra["content_sha256"] == "abc123"


def test_save_block_geometry_writes_geoparquet_with_provenance(tmp_path) -> None:
    """Saved block geometry artifacts keep GeoParquet geometry and HHP provenance."""
    gdf = gpd.GeoDataFrame(
        {
            "block_geoid": ["510010901011234"],
            "state_fips": ["51"],
            "county_fips": ["51001"],
            "tract_geoid": ["51001090101"],
            "block_vintage": [2020],
            "data_source": ["census_tiger_tabblock"],
            "source_ref": ["https://example.test/blocks.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    output_path = save_block_geometry(
        gdf,
        2020,
        output_dir=tmp_path,
        content_sha256="abc123",
        content_size=123,
        raw_paths=[tmp_path / "tl_2020_51_tabblock20.zip"],
        missing_state_fips=["72"],
    )

    assert output_path == tmp_path / "blocks__K2020.parquet"
    roundtrip = gpd.read_parquet(output_path)
    assert roundtrip.crs.to_epsg() == 4326
    provenance = read_provenance(output_path)
    assert provenance is not None
    assert provenance.extra["block_vintage"] == 2020
    assert provenance.extra["missing_state_fips"] == ["72"]


def test_save_block_geometry_does_not_publish_unprovenanced_file_on_metadata_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """A failed provenance write must not leave a final GeoParquet artifact behind."""
    gdf = gpd.GeoDataFrame(
        {
            "block_geoid": ["510010901011234"],
            "state_fips": ["51"],
            "county_fips": ["51001"],
            "tract_geoid": ["51001090101"],
            "block_vintage": [2020],
            "data_source": ["census_tiger_tabblock"],
            "source_ref": ["https://example.test/blocks.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    output_path = tmp_path / "blocks__K2020.parquet"

    def fail_write_table(*args, **kwargs):
        raise RuntimeError("metadata write interrupted")

    monkeypatch.setattr(tiger_blocks.pq, "write_table", fail_write_table)

    with pytest.raises(RuntimeError, match="metadata write interrupted"):
        save_block_geometry(gdf, 2020, output_dir=tmp_path)

    assert not output_path.exists()


def _raw_block_gdf(state_fips: str, block_suffix: str) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "GEOID20": [f"{state_fips}001090101{block_suffix}"],
            "STATEFP20": [state_fips],
            "COUNTYFP20": ["001"],
            "TRACTCE20": ["090101"],
            "geometry": [Point(float(int(state_fips)), 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )


def test_stream_state_block_parts_writes_one_part_per_state(monkeypatch, tmp_path) -> None:
    """Block geometry ingest writes per-state parts instead of retaining all states."""
    calls: list[str] = []

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        calls.append(state_fips)
        return (
            _raw_block_gdf(state_fips, "1234"),
            f"raw-{state_fips}".encode(),
            tmp_path / "raw" / f"tl_{year}_{state_fips}_tabblock20.zip",
        )

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)

    digest, content_size, raw_paths, missing, row_counts = _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01", "02"),
        raw_root=tmp_path / "raw",
    )

    assert calls == ["01", "02"]
    assert digest
    assert content_size == len(b"raw-01") + len(b"raw-02")
    assert [path.name for path in raw_paths] == [
        "tl_2020_01_tabblock20.zip",
        "tl_2020_02_tabblock20.zip",
    ]
    assert missing == []
    assert row_counts == {"01": 1, "02": 1}
    assert _state_part_path(parts_dir, "01").exists()
    assert _state_part_path(parts_dir, "02").exists()
    assert gpd.read_parquet(_state_part_path(parts_dir, "01")).loc[0, "state_fips"] == "01"


def test_stream_state_block_parts_errors_when_all_states_are_missing(
    monkeypatch,
    tmp_path,
) -> None:
    """All-missing state downloads fail instead of assembling an empty artifact."""
    calls: list[str] = []

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        calls.append(state_fips)
        return None, None, None

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )

    with pytest.raises(ValueError, match="No Census tabulation block geometry rows fetched"):
        _stream_state_block_parts(
            2020,
            parts_dir=_block_geometry_parts_dir(2020, tmp_path),
            state_fips_codes=("01", "02"),
            raw_root=tmp_path / "raw",
        )

    assert calls == ["01", "02"]


def test_download_state_blocks_retries_transient_http_errors(monkeypatch, tmp_path) -> None:
    """Transient TIGER download failures are retried before aborting a state."""
    attempts = 0

    class FailingClient:
        def get(self, url, follow_redirects):
            nonlocal attempts
            attempts += 1
            raise httpx.ReadError("peer closed connection")

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None
    )

    with pytest.raises(httpx.ReadError, match="peer closed connection"):
        _download_state_blocks(FailingClient(), 2020, "20", tmp_path)

    assert attempts == STATE_DOWNLOAD_ATTEMPTS


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.test/blocks.zip")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}",
        request=request,
        response=response,
    )


def test_download_state_blocks_retries_non_404_http_status_errors(
    monkeypatch,
    tmp_path,
) -> None:
    """Non-404 TIGER status failures retry before re-raising the final error."""
    attempts = 0

    class FailingClient:
        def get(self, url, follow_redirects):
            nonlocal attempts
            attempts += 1
            raise _http_status_error(503)

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None
    )

    with pytest.raises(httpx.HTTPStatusError, match="HTTP 503"):
        _download_state_blocks(FailingClient(), 2020, "20", tmp_path)

    assert attempts == STATE_DOWNLOAD_ATTEMPTS


def test_download_state_blocks_returns_missing_for_404_without_retry(
    monkeypatch,
    tmp_path,
) -> None:
    """A 404 means the state artifact is unavailable and should not be retried."""
    attempts = 0

    class MissingClient:
        def get(self, url, follow_redirects):
            nonlocal attempts
            attempts += 1
            raise _http_status_error(404)

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None
    )

    assert _download_state_blocks(MissingClient(), 2020, "20", tmp_path) == (
        None,
        None,
        None,
    )
    assert attempts == 1


def test_stream_state_block_parts_resumes_completed_parts(monkeypatch, tmp_path) -> None:
    """Retries skip state parts that already match the current raw ZIP bytes."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    raw_bytes = b"raw-01"
    raw_zip.write_bytes(raw_bytes)
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    _write_path = _state_part_path(parts_dir, "01")
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(normalized, _write_path, hashlib.sha256(raw_bytes).hexdigest())

    def fail_fetch_or_load_state_blocks(*args, **kwargs):
        raise AssertionError("completed state should not be fetched again")

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fail_fetch_or_load_state_blocks,
    )

    digest, content_size, raw_paths, missing, row_counts = _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
    )

    assert content_size == len(raw_bytes)
    assert digest == hashlib.sha256(raw_bytes).hexdigest()
    assert raw_paths == [raw_zip]
    assert missing == []
    assert row_counts == {"01": 1}
    # Reuse kept the parsed geometry and the recorded raw-byte hash untouched.
    reused = gpd.read_parquet(_write_path)
    assert reused.loc[0, "block_geoid"] == normalized.loc[0, "block_geoid"]
    assert _state_part_raw_sha256(_write_path) == hashlib.sha256(raw_bytes).hexdigest()


def test_stream_state_block_parts_rebuilds_on_raw_zip_change_at_same_url(
    monkeypatch,
    tmp_path,
) -> None:
    """A changed raw ZIP at the same URL must rebuild, not reuse stale geometry."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    first_bytes = b"raw-01-first"
    raw_zip.write_bytes(first_bytes)
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    part_path = _state_part_path(parts_dir, "01")
    first_normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(first_normalized, part_path, hashlib.sha256(first_bytes).hexdigest())

    second_bytes = b"raw-01-second"
    raw_zip.write_bytes(second_bytes)
    second_gdf = _raw_block_gdf("01", "9999")
    calls: list[tuple[int, str]] = []

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        calls.append((year, state_fips))
        return second_gdf, second_bytes, raw_zip

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )

    digest, content_size, raw_paths, missing, row_counts = _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
    )

    assert calls == [(2020, "01")]
    assert content_size == len(second_bytes)
    assert digest == hashlib.sha256(second_bytes).hexdigest()
    assert raw_paths == [raw_zip]
    assert missing == []
    assert row_counts == {"01": 1}
    rebuilt = gpd.read_parquet(part_path)
    assert rebuilt.loc[0, "block_geoid"] == "010010901019999"
    assert _state_part_raw_sha256(part_path) == hashlib.sha256(second_bytes).hexdigest()
    # The previous geometry part was preserved, not deleted.
    retained_geoids = [
        gpd.read_parquet(path).loc[0, "block_geoid"]
        for path in parts_dir.glob(f"{part_path.name}.sha256_*")
    ]
    assert retained_geoids == [first_normalized.loc[0, "block_geoid"]]


def test_stream_state_block_parts_rebuilds_legacy_part_without_hash(
    monkeypatch,
    tmp_path,
) -> None:
    """Parts recorded before content hashing existed are untrusted and rebuilt."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    raw_bytes = b"raw-01"
    raw_zip.write_bytes(raw_bytes)
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    part_path = _state_part_path(parts_dir, "01")
    legacy_normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    part_path.parent.mkdir(parents=True)
    legacy_normalized.to_parquet(part_path, index=False)

    calls: list[str] = []

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        calls.append(state_fips)
        return _raw_block_gdf(state_fips, "5678"), raw_bytes, raw_zip

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )

    _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
    )

    assert calls == ["01"]
    rebuilt = gpd.read_parquet(part_path)
    assert rebuilt.loc[0, "block_geoid"] == "010010901015678"
    assert _state_part_raw_sha256(part_path) == hashlib.sha256(raw_bytes).hexdigest()
    # The legacy part (which had no recorded hash) was preserved, not deleted.
    retained_geoids = [
        gpd.read_parquet(path).loc[0, "block_geoid"]
        for path in parts_dir.glob(f"{part_path.name}.sha256_*")
    ]
    assert retained_geoids == [legacy_normalized.loc[0, "block_geoid"]]


def test_stream_state_block_parts_force_retains_parts_dir_and_preserves_parts(
    monkeypatch,
    tmp_path,
) -> None:
    """Force rebuilds parts without deleting the parts directory or prior parts."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    raw_bytes = b"raw-01"
    raw_zip.write_bytes(raw_bytes)
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    part_path = _state_part_path(parts_dir, "01")
    first_normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(first_normalized, part_path, hashlib.sha256(raw_bytes).hexdigest())
    unrelated = parts_dir / "unrelated.park"
    unrelated.write_bytes(b"keep-me")

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        return _raw_block_gdf(state_fips, "4321"), raw_bytes, raw_zip

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )

    _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
        force=True,
    )

    assert unrelated.exists()
    assert part_path.exists()
    rebuilt = gpd.read_parquet(part_path)
    assert rebuilt.loc[0, "block_geoid"] == "010010901014321"
    assert _state_part_raw_sha256(part_path) == hashlib.sha256(raw_bytes).hexdigest()
    retained_geoids = [
        gpd.read_parquet(path).loc[0, "block_geoid"]
        for path in parts_dir.glob(f"{part_path.name}.sha256_*")
    ]
    assert retained_geoids == [first_normalized.loc[0, "block_geoid"]]


def test_state_part_matches_source_binds_to_raw_content_hash(tmp_path) -> None:
    """Part reuse requires both the source URL and the current raw byte hash."""
    raw_zip = tmp_path / "raw.zip"
    raw_bytes = b"raw-bytes"
    raw_zip.write_bytes(raw_bytes)
    hash_hex = hashlib.sha256(raw_bytes).hexdigest()
    part_path = tmp_path / "part.parquet"
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)

    _write_state_part(normalized, part_path, hash_hex)
    assert _state_part_matches_source(part_path, 2020, raw_zip) is True

    raw_zip.write_bytes(b"different-bytes")
    assert _state_part_matches_source(part_path, 2020, raw_zip) is False

    raw_zip.write_bytes(raw_bytes)
    legacy = tmp_path / "legacy.parquet"
    normalized.to_parquet(legacy, index=False)
    assert _state_part_matches_source(legacy, 2020, raw_zip) is False


def test_preserve_stale_state_part_keeps_distinct_versions_with_same_raw_hash(
    tmp_path,
) -> None:
    """Every distinct part version is retained even when raw hashes are identical."""
    part_path = tmp_path / "state_fips=01.parquet"
    raw_hash = hashlib.sha256(b"same-raw").hexdigest()
    retained_paths = []
    for suffix in ("1234", "2345", "3456"):
        normalized = normalize_block_geometry(_raw_block_gdf("01", suffix), 2020)
        _write_state_part(normalized, part_path, raw_hash)
        retained_paths.append(_preserve_stale_state_part(part_path))

    assert len({path.name for path in retained_paths}) == 3
    geoids = {gpd.read_parquet(path).loc[0, "block_geoid"] for path in retained_paths}
    assert geoids == {"010010901011234", "010010901012345", "010010901013456"}
    # Re-preserving the current part reuses its identical retained copy.
    assert _preserve_stale_state_part(part_path) == retained_paths[-1]


def test_preserve_stale_state_part_keeps_distinct_legacy_parts(tmp_path) -> None:
    """Distinct legacy parts (no recorded hash) are all retained, none lost."""
    part_path = tmp_path / "state_fips=01.parquet"
    first = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    first.to_parquet(part_path, index=False)
    first_retained = _preserve_stale_state_part(part_path)

    second = normalize_block_geometry(_raw_block_gdf("01", "2345"), 2020)
    second.to_parquet(part_path, index=False)
    second_retained = _preserve_stale_state_part(part_path)

    assert first_retained != second_retained
    assert first_retained.exists()
    assert second_retained.exists()
    assert (
        gpd.read_parquet(first_retained).loc[0, "block_geoid"] == "010010901011234"
    )
    assert (
        gpd.read_parquet(second_retained).loc[0, "block_geoid"] == "010010901012345"
    )


def test_preserve_stale_state_part_keeps_conflicting_retained_file(tmp_path) -> None:
    part_path = tmp_path / "state_fips=01.parquet"
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(normalized, part_path, hashlib.sha256(b"raw").hexdigest())
    original_bytes = part_path.read_bytes()
    digest = hashlib.sha256(original_bytes).hexdigest()
    conflicting_path = part_path.with_name(f"{part_path.name}.sha256_{digest}")
    conflicting_bytes = b"unrelated retained bytes"
    conflicting_path.write_bytes(conflicting_bytes)

    retained = _preserve_stale_state_part(part_path)

    assert retained != conflicting_path
    assert retained.read_bytes() == original_bytes
    assert conflicting_path.read_bytes() == conflicting_bytes


def test_stream_state_block_parts_stops_if_preservation_fails(monkeypatch, tmp_path) -> None:
    parts_dir = tmp_path / "parts"
    part_path = _state_part_path(parts_dir, "01")
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(normalized, part_path, hashlib.sha256(b"raw").hexdigest())
    original_bytes = part_path.read_bytes()
    original_open = Path.open

    def refuse_part_read(path, *args, **kwargs):
        if path == part_path:
            raise PermissionError("Cannot read existing state part for preservation")
        return original_open(path, *args, **kwargs)

    def unexpected_fetch(*args, **kwargs):
        pytest.fail("A rebuild must not start until the existing part is preserved")

    with monkeypatch.context() as blocked:
        blocked.setattr(Path, "open", refuse_part_read)
        blocked.setattr(tiger_blocks, "_fetch_or_load_state_blocks", unexpected_fetch)
        with pytest.raises(PermissionError, match="for preservation"):
            _stream_state_block_parts(
                2020,
                parts_dir=parts_dir,
                state_fips_codes=("01",),
                raw_root=tmp_path / "raw",
                force=True,
            )

    assert part_path.read_bytes() == original_bytes


def test_state_part_raw_sha256_rejects_malformed_metadata(tmp_path) -> None:
    """Malformed recorded hashes are untrusted and never crash or name files."""
    part_path = tmp_path / "part.parquet"
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    raw_zip = tmp_path / "raw.zip"
    raw_zip.write_bytes(b"raw")

    _write_state_part(normalized, part_path, hashlib.sha256(b"raw").hexdigest())
    assert _state_part_matches_source(part_path, 2020, raw_zip) is True
    well_formed = _state_part_raw_sha256(part_path)
    assert well_formed == hashlib.sha256(b"raw").hexdigest()

    # Undecodable bytes in the metadata key.
    table = pq.read_table(part_path)
    bad_meta = dict(table.schema.metadata or {})
    bad_meta[PART_RAW_SHA256_KEY] = b"\xff\xfe\x00"
    table = table.replace_schema_metadata(bad_meta)
    pq.write_table(table, part_path)
    assert _state_part_raw_sha256(part_path) is None
    assert _state_part_matches_source(part_path, 2020, raw_zip) is False

    # Valid UTF-8 that is not a 64-character hex digest.
    table = pq.read_table(part_path)
    bad_meta = dict(table.schema.metadata or {})
    bad_meta[PART_RAW_SHA256_KEY] = b"not-a-hex-digest"
    table = table.replace_schema_metadata(bad_meta)
    pq.write_table(table, part_path)
    assert _state_part_raw_sha256(part_path) is None
    assert _state_part_matches_source(part_path, 2020, raw_zip) is False


def test_stream_state_block_parts_rebuilds_part_with_malformed_hash_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """Malformed part hash metadata triggers a safe, preserving rebuild."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    raw_bytes = b"raw-01"
    raw_zip.write_bytes(raw_bytes)
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    part_path = _state_part_path(parts_dir, "01")
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_state_part(normalized, part_path, hashlib.sha256(raw_bytes).hexdigest())
    table = pq.read_table(part_path)
    bad_meta = dict(table.schema.metadata or {})
    bad_meta[PART_RAW_SHA256_KEY] = b"\xff\xfe"
    table = table.replace_schema_metadata(bad_meta)
    pq.write_table(table, part_path)

    calls: list[str] = []

    def fake_fetch_or_load_state_blocks(client, *, year, state_fips, tmpdir, raw_root, force):
        calls.append(state_fips)
        return _raw_block_gdf(state_fips, "5678"), raw_bytes, raw_zip

    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fake_fetch_or_load_state_blocks,
    )

    _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
    )

    assert calls == ["01"]
    rebuilt = gpd.read_parquet(part_path)
    assert rebuilt.loc[0, "block_geoid"] == "010010901015678"
    assert _state_part_raw_sha256(part_path) == hashlib.sha256(raw_bytes).hexdigest()
    # The malformed-metadata part was preserved under its full content hash.
    retained_geoids = [
        gpd.read_parquet(path).loc[0, "block_geoid"]
        for path in parts_dir.glob(f"{part_path.name}.sha256_*")
    ]
    assert "010010901011234" in retained_geoids


def test_save_block_geometry_from_parts_streams_final_geoparquet(tmp_path) -> None:
    """Per-state parts assemble into the canonical GeoParquet with provenance."""
    parts_dir = tmp_path / "parts"
    part_paths = []
    for state_fips in ("01", "02"):
        part_path = _state_part_path(parts_dir, state_fips)
        normalized = normalize_block_geometry(_raw_block_gdf(state_fips, "1234"), 2020)
        _write_state_part(
            normalized,
            part_path,
            hashlib.sha256(f"raw-{state_fips}".encode()).hexdigest(),
        )
        part_paths.append(part_path)

    output_path = save_block_geometry_from_parts(
        part_paths,
        2020,
        output_dir=tmp_path,
        content_sha256="abc123",
        content_size=12,
        raw_paths=[tmp_path / "tl_2020_01_tabblock20.zip"],
    )

    assert output_path == tmp_path / "blocks__K2020.parquet"
    roundtrip = gpd.read_parquet(output_path)
    assert list(roundtrip["state_fips"]) == ["01", "02"]
    assert roundtrip.crs.to_epsg() == 4326
    provenance = read_provenance(output_path)
    assert provenance is not None
    assert provenance.extra["content_sha256"] == "abc123"
    assert provenance.extra["part_paths"] == [str(path) for path in part_paths]
    # Per-state raw-byte hashes must not leak into the national artifact; the
    # final provenance block remains the authoritative aggregate record.
    output_metadata = pq.read_metadata(output_path).metadata or {}
    assert PART_RAW_SHA256_KEY not in output_metadata
    assert PROVENANCE_KEY in output_metadata
    # Each part keeps its own raw-byte hash binding.
    assert (
        _state_part_raw_sha256(part_paths[0])
        == hashlib.sha256(b"raw-01").hexdigest()
    )
    assert (
        _state_part_raw_sha256(part_paths[1])
        == hashlib.sha256(b"raw-02").hexdigest()
    )


def test_save_block_geometry_from_parts_rejects_empty_part_list(tmp_path) -> None:
    """Empty part assembly fails with an actionable message instead of writing output."""
    with pytest.raises(
        ValueError,
        match="No block geometry part files were available to assemble",
    ):
        save_block_geometry_from_parts([], 2020, output_dir=tmp_path)

    assert not (tmp_path / "blocks__K2020.parquet").exists()


def test_save_block_geometry_from_parts_casts_every_part_to_unified_schema(
    tmp_path,
) -> None:
    """Resume assembly tolerates castable schema drift in later state part files."""
    parts_dir = tmp_path / "parts"
    part_paths = []
    for state_fips in ("01", "02"):
        part_path = _state_part_path(parts_dir, state_fips)
        normalized = normalize_block_geometry(_raw_block_gdf(state_fips, "1234"), 2020)
        part_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(part_path, index=False)
        part_paths.append(part_path)

    second_part = pq.read_table(part_paths[1])
    state_fips_index = second_part.schema.get_field_index("state_fips")
    second_part = second_part.set_column(
        state_fips_index,
        pa.field("state_fips", pa.large_string()),
        second_part["state_fips"].cast(pa.large_string()),
    )
    pq.write_table(second_part, part_paths[1])

    output_path = save_block_geometry_from_parts(
        part_paths,
        2020,
        output_dir=tmp_path,
        content_sha256="abc123",
        content_size=12,
    )

    roundtrip = gpd.read_parquet(output_path)
    assert list(roundtrip["state_fips"]) == ["01", "02"]
    assert read_provenance(output_path) is not None


def test_ingest_urban_areas_cli_cached_json(monkeypatch, tmp_path) -> None:
    """Cached Urban Area CLI runs emit machine-readable JSON."""
    cached_path = get_urban_area_output_path(2020, tmp_path)
    gdf = gpd.GeoDataFrame(
        {
            "urban_area_geoid": ["56789"],
            "urban_area_name": ["Example Urban Area"],
            "urban_area_type": ["urbanized_area"],
            "urban_area_vintage": [2020],
            "data_source": ["census_tiger_urban_area"],
            "source_ref": ["https://example.test/uac.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(cached_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_urban_area_output_path",
        lambda year: cached_path,
    )

    result = runner.invoke(app, ["ingest", "urban-areas", "--year", "2020", "--json"])

    assert result.exit_code == 0
    assert '"cached": true' in result.output
    assert '"urban_area_count": 1' in result.output


def test_ingest_urban_areas_cli_cached_json_uses_parquet_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """Cached Urban Area row counts come from metadata, not full GeoDataFrame reads."""
    cached_path = get_urban_area_output_path(2020, tmp_path)
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    cached_path.touch()
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_urban_area_output_path",
        lambda year: cached_path,
    )
    monkeypatch.setattr("hhplab.cli.ingest.census._parquet_row_count", lambda path: 7)

    result = runner.invoke(app, ["ingest", "urban-areas", "--year", "2020", "--json"])

    assert result.exit_code == 0
    assert '"cached": true' in result.output
    assert '"urban_area_count": 7' in result.output


def test_ingest_block_geometry_cli_cached_json(monkeypatch, tmp_path) -> None:
    """Cached block geometry CLI runs emit machine-readable JSON."""
    cached_path = get_block_geometry_output_path(2020, tmp_path)
    gdf = gpd.GeoDataFrame(
        {
            "block_geoid": ["510010901011234"],
            "state_fips": ["51"],
            "county_fips": ["51001"],
            "tract_geoid": ["51001090101"],
            "block_vintage": [2020],
            "data_source": ["census_tiger_tabblock"],
            "source_ref": ["https://example.test/blocks.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(cached_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_block_geometry_output_path",
        lambda year, base_dir=None: cached_path,
    )

    result = runner.invoke(app, ["ingest", "block-geometry", "--year", "2020", "--json"])

    assert result.exit_code == 0
    assert '"cached": true' in result.output
    assert '"block_count": 1' in result.output


def test_ingest_urban_areas_cli_fresh_json(monkeypatch, tmp_path) -> None:
    """Fresh Urban Area CLI runs call ingest and emit machine-readable JSON."""
    output_path = get_urban_area_output_path(2010, tmp_path)
    gdf = gpd.GeoDataFrame(
        {
            "urban_area_geoid": ["01234"],
            "urban_area_name": ["Example Urban Cluster"],
            "urban_area_type": ["urban_cluster"],
            "urban_area_vintage": [2010],
            "data_source": ["census_tiger_urban_area"],
            "source_ref": ["https://example.test/uac.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(0, 0)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_urban_area_output_path",
        lambda year: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.urban_areas.ingest_urban_areas",
        lambda year, force=False: output_path,
    )

    result = runner.invoke(app, ["ingest", "urban-areas", "--year", "2010", "--json"])

    assert result.exit_code == 0
    assert '"cached": false' in result.output
    assert '"urban_area_vintage": 2010' in result.output


def test_ingest_urban_areas_cli_fresh_json_uses_parquet_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """Fresh Urban Area row counts come from metadata after ingest returns a path."""
    output_path = get_urban_area_output_path(2010, tmp_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.touch()
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_urban_area_output_path",
        lambda year: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.urban_areas.ingest_urban_areas",
        lambda year, force=False: output_path,
    )
    monkeypatch.setattr("hhplab.cli.ingest.census._parquet_row_count", lambda path: 9)

    result = runner.invoke(app, ["ingest", "urban-areas", "--year", "2010", "--json"])

    assert result.exit_code == 0
    assert '"cached": false' in result.output
    assert '"urban_area_count": 9' in result.output


def test_ingest_block_geometry_cli_fresh_json(monkeypatch, tmp_path) -> None:
    """Fresh block geometry CLI runs call ingest and emit machine-readable JSON."""
    output_path = get_block_geometry_output_path(2020, tmp_path)
    gdf = gpd.GeoDataFrame(
        {
            "block_geoid": ["510010901011234"],
            "state_fips": ["51"],
            "county_fips": ["51001"],
            "tract_geoid": ["51001090101"],
            "block_vintage": [2020],
            "data_source": ["census_tiger_tabblock"],
            "source_ref": ["https://example.test/blocks.zip"],
            "ingested_at": ["2026-05-31T00:00:00Z"],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.census.get_block_geometry_output_path",
        lambda year, base_dir=None: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.geographies.boundaries.census.ingest.tiger_blocks.ingest_block_geometry",
        lambda year, force=False, output_dir=None, raw_root=None: output_path,
    )

    result = runner.invoke(app, ["ingest", "block-geometry", "--year", "2020", "--json"])

    assert result.exit_code == 0
    assert '"cached": false' in result.output
    assert '"block_vintage": 2020' in result.output
