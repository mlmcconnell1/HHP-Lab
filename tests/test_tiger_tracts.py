"""Tests for TIGER tract ingest URL and schema resolution."""

import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Point
from typer.testing import CliRunner

from hhplab.census.ingest.tiger_blocks import (
    STATE_DOWNLOAD_ATTEMPTS,
    _block_geometry_parts_dir,
    _block_url,
    _block_zip_name,
    _download_state_blocks,
    _state_part_path,
    _stream_state_block_parts,
    download_block_geometry,
    get_block_geometry_output_path,
    normalize_block_geometry,
    save_block_geometry,
    save_block_geometry_from_parts,
)
from hhplab.census.ingest import tiger_blocks
from hhplab.census.ingest.tiger_tracts import (
    _resolve_geoid_column,
    _tract_2000_url,
    _tract_2000_zip_name,
    _tract_url,
    _tract_zip_name,
)
from hhplab.census.ingest.urban_areas import (
    get_urban_area_output_path,
    normalize_urban_areas,
    save_urban_areas,
    urban_area_source,
)
from hhplab.cli.main import app
from hhplab.provenance import read_provenance

runner = CliRunner()


def test_tract_zip_name_uses_2010_suffix() -> None:
    """2010 tract downloads use the special tract10 filename suffix."""
    assert _tract_zip_name(2010, "51") == "tl_2010_51_tract10.zip"


def test_block_geometry_filename_uses_tabblock20_suffix() -> None:
    """2020 tabulation block downloads use the TIGER TABBLOCK20 pattern."""
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
        "hhplab.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
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
        "hhplab.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
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

    monkeypatch.setattr("hhplab.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None)

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

    monkeypatch.setattr("hhplab.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None)

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

    monkeypatch.setattr("hhplab.census.ingest.tiger_blocks.time.sleep", lambda _seconds: None)

    assert _download_state_blocks(MissingClient(), 2020, "20", tmp_path) == (
        None,
        None,
        None,
    )
    assert attempts == 1


def test_stream_state_block_parts_resumes_completed_parts(monkeypatch, tmp_path) -> None:
    """Retries skip state parts that already have the matching raw ZIP."""
    raw_root = tmp_path / "raw"
    raw_zip = raw_root / "tiger" / "2020" / "blocks" / "tl_2020_01_tabblock20.zip"
    raw_zip.parent.mkdir(parents=True)
    raw_zip.write_bytes(b"raw-01")
    parts_dir = _block_geometry_parts_dir(2020, tmp_path)
    _write_path = _state_part_path(parts_dir, "01")
    normalized = normalize_block_geometry(_raw_block_gdf("01", "1234"), 2020)
    _write_path.parent.mkdir(parents=True)
    normalized.to_parquet(_write_path, index=False)

    def fail_fetch_or_load_state_blocks(*args, **kwargs):
        raise AssertionError("completed state should not be fetched again")

    monkeypatch.setattr(
        "hhplab.census.ingest.tiger_blocks._fetch_or_load_state_blocks",
        fail_fetch_or_load_state_blocks,
    )

    _digest, content_size, raw_paths, missing, row_counts = _stream_state_block_parts(
        2020,
        parts_dir=parts_dir,
        state_fips_codes=("01",),
        raw_root=raw_root,
    )

    assert content_size == len(b"raw-01")
    assert raw_paths == [raw_zip]
    assert missing == []
    assert row_counts == {"01": 1}


def test_save_block_geometry_from_parts_streams_final_geoparquet(tmp_path) -> None:
    """Per-state parts assemble into the canonical GeoParquet with provenance."""
    parts_dir = tmp_path / "parts"
    part_paths = []
    for state_fips in ("01", "02"):
        part_path = _state_part_path(parts_dir, state_fips)
        normalized = normalize_block_geometry(_raw_block_gdf(state_fips, "1234"), 2020)
        part_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_parquet(part_path, index=False)
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
        "hhplab.cli.ingest_census.get_urban_area_output_path",
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
        "hhplab.cli.ingest_census.get_urban_area_output_path",
        lambda year: cached_path,
    )
    monkeypatch.setattr("hhplab.cli.ingest_census._parquet_row_count", lambda path: 7)

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
        "hhplab.cli.ingest_census.get_block_geometry_output_path",
        lambda year: cached_path,
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
        "hhplab.cli.ingest_census.get_urban_area_output_path",
        lambda year: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.census.ingest.urban_areas.ingest_urban_areas",
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
        "hhplab.cli.ingest_census.get_urban_area_output_path",
        lambda year: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.census.ingest.urban_areas.ingest_urban_areas",
        lambda year, force=False: output_path,
    )
    monkeypatch.setattr("hhplab.cli.ingest_census._parquet_row_count", lambda path: 9)

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
        "hhplab.cli.ingest_census.get_block_geometry_output_path",
        lambda year: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.census.ingest.tiger_blocks.ingest_block_geometry",
        lambda year, force=False: output_path,
    )

    result = runner.invoke(app, ["ingest", "block-geometry", "--year", "2020", "--json"])

    assert result.exit_code == 0
    assert '"cached": false' in result.output
    assert '"block_vintage": 2020' in result.output
