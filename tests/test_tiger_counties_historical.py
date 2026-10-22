"""Vintage-specific county identifiers and retained spatial provenance."""
import geopandas as gpd
import pytest
from shapely.geometry import box

from hhplab.geographies.boundaries.census.ingest import tiger_counties
from hhplab.storage.provenance import read_provenance

COUNTY_FIPS = "01001"


@pytest.mark.parametrize("year,column", [(2010, "GEOID10"), (2023, "GEOID")])
def test_normalize_county_identifier(year, column):
    source = gpd.GeoDataFrame({column: [COUNTY_FIPS]}, geometry=[box(0, 0, 1, 1)], crs=4326)
    result = tiger_counties.normalize_county_geometry(source, year)
    assert result.geoid.tolist() == [COUNTY_FIPS]
    assert result.geo_vintage.tolist() == [str(year)]
    assert result.geometry.equals(source.geometry)


def test_reject_county_vintage_mismatch():
    source = gpd.GeoDataFrame({"GEOID": [COUNTY_FIPS]}, geometry=[box(0, 0, 1, 1)], crs=4326)
    with pytest.raises(ValueError, match="require GEOID10"):
        tiger_counties.normalize_county_geometry(source, 2010)


def test_county_geoparquet_preserves_provenance(tmp_path, monkeypatch):
    monkeypatch.setattr(tiger_counties, "curated_dir", lambda _: tmp_path)
    source = gpd.GeoDataFrame({"GEOID10": [COUNTY_FIPS]}, geometry=[box(0, 0, 1, 1)], crs=4326)
    normalized = tiger_counties.normalize_county_geometry(source, 2010)
    normalized.attrs["source_provenance"] = {"raw_sha256": "fixture-hash"}
    path = tiger_counties.save_counties(normalized, 2010)
    assert gpd.read_parquet(path).geometry.equals(normalized.geometry)
    provenance = read_provenance(path)
    assert provenance.county_vintage == "2010"
    assert provenance.extra["raw_sha256"] == "fixture-hash"
