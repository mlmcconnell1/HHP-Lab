"""Tests for NHGIS ingest module.

Tests cover:
- _get_shapefile_name: year/geo_type validation and lookup
- _normalize_to_schema: GEOID column detection, GISJOIN conversion,
  zero-padding, CRS handling, output schema
- _normalize_county_to_schema: same as above but for counties (5-char GEOID)
- _wait_for_extract: polling, timeout, failure detection
- NhgisExtractError: custom exception
"""

from __future__ import annotations

from unittest.mock import MagicMock

import geopandas as gpd
import pytest
from shapely.geometry import Point

from coclab.nhgis.ingest import (
    NHGIS_COUNTY_SHAPEFILES,
    NHGIS_TRACT_SHAPEFILES,
    NhgisExtractError,
    _get_shapefile_name,
    _normalize_county_to_schema,
    _normalize_to_schema,
    _wait_for_extract,
)

# ---------------------------------------------------------------------------
# Helpers: build synthetic GeoDataFrames
# ---------------------------------------------------------------------------

def _make_tract_gdf(columns: dict, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Build a small GeoDataFrame with a geometry column and given CRS."""
    n = len(next(iter(columns.values())))
    geom = [Point(i, i) for i in range(n)]
    return gpd.GeoDataFrame({**columns, "geometry": geom}, crs=crs)


# ===================================================================
# _get_shapefile_name
# ===================================================================


class TestGetShapefileName:
    """Tests for _get_shapefile_name."""

    def test_tract_2010(self):
        assert _get_shapefile_name(2010, "tracts") == NHGIS_TRACT_SHAPEFILES[2010]

    def test_tract_2020(self):
        assert _get_shapefile_name(2020, "tracts") == NHGIS_TRACT_SHAPEFILES[2020]

    def test_county_2010(self):
        assert _get_shapefile_name(2010, "counties") == NHGIS_COUNTY_SHAPEFILES[2010]

    def test_county_2020(self):
        assert _get_shapefile_name(2020, "counties") == NHGIS_COUNTY_SHAPEFILES[2020]

    def test_default_geo_type_is_tracts(self):
        assert _get_shapefile_name(2020) == NHGIS_TRACT_SHAPEFILES[2020]

    def test_unsupported_year_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            _get_shapefile_name(1990)

    def test_unsupported_geo_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported geo_type"):
            _get_shapefile_name(2020, "blocks")


# ===================================================================
# _normalize_to_schema (tracts)
# ===================================================================


class TestNormalizeToSchema:
    """Tests for _normalize_to_schema (tract normalization)."""

    def test_geoid_column_detected(self):
        """Standard GEOID column is used when present."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100", "06037100200"]})
        result = _normalize_to_schema(gdf, 2020)
        assert list(result["geoid"]) == ["01001020100", "06037100200"]

    def test_geoid10_column_detected(self):
        """GEOID10 column is used when GEOID is absent."""
        gdf = _make_tract_gdf({"GEOID10": ["01001020100"]})
        result = _normalize_to_schema(gdf, 2010)
        assert result["geoid"].iloc[0] == "01001020100"

    def test_geoid20_column_detected(self):
        """GEOID20 column is used when GEOID and GEOID10 are absent."""
        gdf = _make_tract_gdf({"GEOID20": ["06037100200"]})
        result = _normalize_to_schema(gdf, 2020)
        assert result["geoid"].iloc[0] == "06037100200"

    def test_gisjoin_converted_to_geoid(self):
        """GISJOIN format is correctly parsed into 11-char tract GEOID."""
        # G 01 0 001 0 020100 -> 01001020100
        gdf = _make_tract_gdf({"GISJOIN": ["G0100010020100"]})
        result = _normalize_to_schema(gdf, 2010)
        assert result["geoid"].iloc[0] == "01001020100"

    def test_gisjoin_multiple_rows(self):
        """GISJOIN conversion works for multiple rows."""
        gdf = _make_tract_gdf(
            {"GISJOIN": ["G0100010020100", "G0600370100200"]}
        )
        result = _normalize_to_schema(gdf, 2020)
        assert list(result["geoid"]) == ["01001020100", "06037100200"]

    def test_geoid_zfill_pads_short_values(self):
        """Short GEOID strings are zero-padded to 11 characters."""
        # A GEOID missing its leading zero: "1001020100" (10 chars)
        gdf = _make_tract_gdf({"GEOID": ["1001020100"]})
        result = _normalize_to_schema(gdf, 2020)
        assert result["geoid"].iloc[0] == "01001020100"
        assert len(result["geoid"].iloc[0]) == 11

    def test_numeric_geoid_converted_to_string(self):
        """Numeric GEOID values are cast to string and zero-padded."""
        gdf = _make_tract_gdf({"GEOID": [1001020100]})
        result = _normalize_to_schema(gdf, 2020)
        assert result["geoid"].iloc[0] == "01001020100"

    def test_no_geoid_column_raises(self):
        """ValueError raised when no recognised GEOID column exists."""
        gdf = _make_tract_gdf({"RANDOM_COL": ["abc"]})
        with pytest.raises(ValueError, match="Could not find GEOID column"):
            _normalize_to_schema(gdf, 2020)

    def test_missing_crs_raises(self):
        """ValueError raised when source GeoDataFrame has no CRS."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100"]}, crs=None)
        with pytest.raises(ValueError, match="no CRS"):
            _normalize_to_schema(gdf, 2020)

    def test_reprojection_from_non_4326(self):
        """GeoDataFrame in a different CRS is reprojected to EPSG:4326."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100"]}, crs="EPSG:3857")
        result = _normalize_to_schema(gdf, 2020)
        assert result.crs.to_epsg() == 4326

    def test_output_schema_columns(self):
        """Output has exactly the expected columns."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100"]})
        result = _normalize_to_schema(gdf, 2020)
        assert set(result.columns) == {
            "geo_vintage",
            "geoid",
            "geometry",
            "source",
            "ingested_at",
        }

    def test_source_column_is_nhgis(self):
        """Source column is always 'nhgis'."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100"]})
        result = _normalize_to_schema(gdf, 2020)
        assert result["source"].iloc[0] == "nhgis"

    def test_geo_vintage_is_string_of_year(self):
        """geo_vintage is the string representation of the year."""
        gdf = _make_tract_gdf({"GEOID": ["01001020100"]})
        result = _normalize_to_schema(gdf, 2010)
        assert result["geo_vintage"].iloc[0] == "2010"

    def test_geoid_column_priority_order(self):
        """When multiple GEOID-like columns exist, GEOID is preferred."""
        gdf = _make_tract_gdf(
            {"GEOID": ["01001020100"], "GISJOIN": ["G0100010020100"]}
        )
        result = _normalize_to_schema(gdf, 2020)
        # Should use GEOID (the first match), not GISJOIN
        assert result["geoid"].iloc[0] == "01001020100"


# ===================================================================
# _normalize_county_to_schema
# ===================================================================


class TestNormalizeCountyToSchema:
    """Tests for _normalize_county_to_schema (county normalization)."""

    def test_geoid_column_detected(self):
        gdf = _make_tract_gdf({"GEOID": ["01001", "06037"]})
        result = _normalize_county_to_schema(gdf, 2020)
        assert list(result["geoid"]) == ["01001", "06037"]

    def test_gisjoin_converted_to_county_geoid(self):
        """GISJOIN county format G[SS][0][CCC] -> SSCCC (5 chars)."""
        gdf = _make_tract_gdf({"GISJOIN": ["G010001", "G060037"]})
        result = _normalize_county_to_schema(gdf, 2020)
        assert list(result["geoid"]) == ["01001", "06037"]

    def test_county_geoid_zfill_pads_to_5(self):
        """Short county GEOID strings are zero-padded to 5 characters."""
        gdf = _make_tract_gdf({"GEOID": ["1001"]})
        result = _normalize_county_to_schema(gdf, 2020)
        assert result["geoid"].iloc[0] == "01001"
        assert len(result["geoid"].iloc[0]) == 5

    def test_no_geoid_column_raises(self):
        gdf = _make_tract_gdf({"OTHER": ["x"]})
        with pytest.raises(ValueError, match="Could not find GEOID column"):
            _normalize_county_to_schema(gdf, 2020)

    def test_missing_crs_raises(self):
        gdf = _make_tract_gdf({"GEOID": ["01001"]}, crs=None)
        with pytest.raises(ValueError, match="no CRS"):
            _normalize_county_to_schema(gdf, 2020)

    def test_reprojection_from_non_4326(self):
        gdf = _make_tract_gdf({"GEOID": ["01001"]}, crs="EPSG:3857")
        result = _normalize_county_to_schema(gdf, 2020)
        assert result.crs.to_epsg() == 4326

    def test_output_schema_columns(self):
        gdf = _make_tract_gdf({"GEOID": ["01001"]})
        result = _normalize_county_to_schema(gdf, 2020)
        assert set(result.columns) == {
            "geo_vintage",
            "geoid",
            "geometry",
            "source",
            "ingested_at",
        }

    def test_geoid10_column_detected(self):
        gdf = _make_tract_gdf({"GEOID10": ["01001"]})
        result = _normalize_county_to_schema(gdf, 2010)
        assert result["geoid"].iloc[0] == "01001"


# ===================================================================
# _wait_for_extract
# ===================================================================


class TestWaitForExtract:
    """Tests for _wait_for_extract polling logic."""

    def test_completed_returns_immediately(self):
        """Extract with 'completed' status returns without error."""
        client = MagicMock()
        client.extract_status.return_value = "completed"
        extract = MagicMock()

        # Should not raise
        _wait_for_extract(client, extract, poll_interval_minutes=0, max_wait_minutes=1)
        client.extract_status.assert_called_once_with(extract)

    def test_failed_raises_error(self):
        """Extract with 'failed' status raises NhgisExtractError."""
        client = MagicMock()
        client.extract_status.return_value = "failed"
        extract = MagicMock()

        with pytest.raises(NhgisExtractError, match="failed"):
            _wait_for_extract(
                client, extract, poll_interval_minutes=0, max_wait_minutes=1
            )

    def test_timeout_raises_error(self):
        """Extract that never completes raises NhgisExtractError after timeout."""
        client = MagicMock()
        client.extract_status.return_value = "queued"
        extract = MagicMock()

        with pytest.raises(NhgisExtractError, match="timed out"):
            _wait_for_extract(
                client,
                extract,
                poll_interval_minutes=0,
                max_wait_minutes=0,  # immediate timeout
            )

    def test_progress_callback_called(self):
        """Progress callback receives status messages."""
        client = MagicMock()
        client.extract_status.return_value = "completed"
        extract = MagicMock()
        messages: list[str] = []

        _wait_for_extract(
            client,
            extract,
            poll_interval_minutes=0,
            max_wait_minutes=1,
            progress_callback=messages.append,
        )

        assert len(messages) == 1
        assert "completed" in messages[0].lower()

    def test_case_insensitive_status(self):
        """Status comparison is case-insensitive."""
        client = MagicMock()
        client.extract_status.return_value = "Completed"
        extract = MagicMock()

        # Should not raise despite capitalised status string
        _wait_for_extract(client, extract, poll_interval_minutes=0, max_wait_minutes=1)


# ===================================================================
# NhgisExtractError
# ===================================================================


class TestNhgisExtractError:
    """Basic tests for the custom exception."""

    def test_is_exception(self):
        assert issubclass(NhgisExtractError, Exception)

    def test_message_preserved(self):
        err = NhgisExtractError("download failed")
        assert str(err) == "download failed"
