"""Tests for TIGER tract ingest URL and schema resolution."""

import geopandas as gpd
import pytest
from shapely.geometry import Point
from typer.testing import CliRunner

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
