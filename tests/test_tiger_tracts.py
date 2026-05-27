"""Tests for TIGER tract ingest URL and schema resolution."""

import geopandas as gpd

from hhplab.census.ingest.tiger_tracts import (
    _resolve_geoid_column,
    _tract_2000_url,
    _tract_2000_zip_name,
    _tract_url,
    _tract_zip_name,
)


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
