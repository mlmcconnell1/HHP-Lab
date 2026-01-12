"""Tests for the HUD Open Data ArcGIS ingester."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import geopandas as gpd
import httpx
import pytest

from coclab.ingest.hud_opendata_arcgis import (
    FEATURE_SERVICE_URL,
    PAGE_SIZE,
    _features_to_geodataframe,
    _fetch_all_features,
    _fetch_page,
    _map_to_canonical_schema,
    ingest_hud_opendata,
)

# Sample polygon coordinates (kept short for readability)
SAMPLE_COORDS_1 = [
    [-105.0, 40.0],
    [-104.0, 40.0],
    [-104.0, 39.0],
    [-105.0, 39.0],
    [-105.0, 40.0],
]
SAMPLE_COORDS_2 = [
    [-105.5, 39.5],
    [-104.5, 39.5],
    [-104.5, 38.5],
    [-105.5, 38.5],
    [-105.5, 39.5],
]


@pytest.fixture
def sample_geojson_response():
    """Sample GeoJSON response matching HUD Open Data format."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [SAMPLE_COORDS_1],
                },
                "properties": {
                    "COCNUM": "CO-500",
                    "COCNAME": "Colorado Balance of State CoC",
                    "STUSAB": "CO",
                    "STATE_NAME": "Colorado",
                },
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [SAMPLE_COORDS_2],
                },
                "properties": {
                    "COCNUM": "CO-503",
                    "COCNAME": "Metropolitan Denver Homeless Initiative",
                    "STUSAB": "CO",
                    "STATE_NAME": "Colorado",
                },
            },
        ],
    }


@pytest.fixture
def mock_http_client(sample_geojson_response):
    """Create a mock HTTP client that returns sample data."""
    client = MagicMock(spec=httpx.Client)
    response = MagicMock()
    response.json.return_value = sample_geojson_response
    response.raise_for_status = MagicMock()
    client.get.return_value = response
    return client


class TestFetchPage:
    """Tests for _fetch_page function."""

    def test_fetch_page_makes_correct_request(self, mock_http_client):
        """Verify the API request parameters are correct."""
        _fetch_page(mock_http_client, offset=0)

        mock_http_client.get.assert_called_once()
        call_args = mock_http_client.get.call_args

        assert call_args[0][0] == FEATURE_SERVICE_URL
        params = call_args[1]["params"]
        assert params["where"] == "1=1"
        assert params["outFields"] == "COCNUM,COCNAME,STUSAB,STATE_NAME"
        assert params["outSR"] == "4326"
        assert params["f"] == "geojson"
        assert params["resultOffset"] == 0
        assert params["resultRecordCount"] == PAGE_SIZE

    def test_fetch_page_with_offset(self, mock_http_client):
        """Verify pagination offset is passed correctly."""
        _fetch_page(mock_http_client, offset=1000)

        call_args = mock_http_client.get.call_args
        params = call_args[1]["params"]
        assert params["resultOffset"] == 1000


class TestFetchAllFeatures:
    """Tests for _fetch_all_features function."""

    def test_fetch_all_features_single_page(self, mock_http_client, sample_geojson_response):
        """Test fetching when all features fit in one page."""
        features, raw_content = _fetch_all_features(mock_http_client)

        assert len(features) == 2
        assert features[0]["properties"]["COCNUM"] == "CO-500"
        assert features[1]["properties"]["COCNUM"] == "CO-503"
        assert isinstance(raw_content, bytes)

    def test_fetch_all_features_handles_empty_response(self):
        """Test handling of empty response."""
        client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.json.return_value = {"type": "FeatureCollection", "features": []}
        response.raise_for_status = MagicMock()
        client.get.return_value = response

        features, raw_content = _fetch_all_features(client)

        assert features == []
        assert raw_content == b""

    def test_fetch_all_features_paginates(self):
        """Test that pagination works correctly."""
        client = MagicMock(spec=httpx.Client)

        # First page returns PAGE_SIZE features, second page returns fewer
        page1_features = [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [SAMPLE_COORDS_1]},
                "properties": {
                    "COCNUM": f"XX-{i:03d}",
                    "COCNAME": f"CoC {i}",
                    "STUSAB": "XX",
                    "STATE_NAME": "Test",
                },
            }
            for i in range(PAGE_SIZE)
        ]
        page2_features = [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [SAMPLE_COORDS_1]},
                "properties": {
                    "COCNUM": "XX-999",
                    "COCNAME": "Last CoC",
                    "STUSAB": "XX",
                    "STATE_NAME": "Test",
                },
            }
        ]

        resp1 = MagicMock()
        resp1.json.return_value = {"features": page1_features}
        resp1.raise_for_status = MagicMock()

        resp2 = MagicMock()
        resp2.json.return_value = {"features": page2_features}
        resp2.raise_for_status = MagicMock()

        client.get.side_effect = [resp1, resp2]

        features, raw_content = _fetch_all_features(client)

        assert len(features) == PAGE_SIZE + 1
        assert client.get.call_count == 2
        assert isinstance(raw_content, bytes)


class TestFeaturesToGeoDataFrame:
    """Tests for _features_to_geodataframe function."""

    def test_converts_features_to_gdf(self, sample_geojson_response):
        """Test conversion of GeoJSON features to GeoDataFrame."""
        gdf = _features_to_geodataframe(sample_geojson_response["features"])

        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) == 2
        assert gdf.crs.to_epsg() == 4326
        assert "COCNUM" in gdf.columns
        assert "COCNAME" in gdf.columns
        assert gdf.iloc[0]["COCNUM"] == "CO-500"

    def test_raises_on_empty_features(self):
        """Test that empty features list raises ValueError."""
        with pytest.raises(ValueError, match="No features to convert"):
            _features_to_geodataframe([])


class TestMapToCanonicalSchema:
    """Tests for _map_to_canonical_schema function."""

    def test_maps_fields_correctly(self, sample_geojson_response):
        """Test that fields are mapped to canonical schema."""
        gdf = _features_to_geodataframe(sample_geojson_response["features"])
        ingested_at = datetime(2025, 1, 4, 12, 0, 0, tzinfo=UTC)

        result = _map_to_canonical_schema(gdf, "HUDOpenData_2025-01-04", ingested_at)

        assert "boundary_vintage" in result.columns
        assert "coc_id" in result.columns
        assert "coc_name" in result.columns
        assert "state_abbrev" in result.columns
        assert "source" in result.columns
        assert "source_ref" in result.columns
        assert "ingested_at" in result.columns

        assert result.iloc[0]["coc_id"] == "CO-500"
        assert result.iloc[0]["coc_name"] == "Colorado Balance of State CoC"
        assert result.iloc[0]["state_abbrev"] == "CO"
        assert result.iloc[0]["source"] == "hud_opendata"
        assert result.iloc[0]["boundary_vintage"] == "HUDOpenData_2025-01-04"


class TestIngestHudOpendata:
    """Tests for ingest_hud_opendata function."""

    def test_ingest_creates_geoparquet(self, mock_http_client, tmp_path):
        """Test that ingestion creates a GeoParquet file."""
        output_path = ingest_hud_opendata(
            snapshot_tag="HUDOpenData_2025-01-04",
            base_dir=tmp_path,
            http_client=mock_http_client,
        )

        assert output_path.exists()
        assert output_path.suffix == ".parquet"
        assert "HUDOpenData_2025-01-04" in str(output_path)

        # Read back and verify
        gdf = gpd.read_parquet(output_path)
        assert len(gdf) == 2
        assert "geom_hash" in gdf.columns
        assert gdf.iloc[0]["coc_id"] == "CO-500"

    def test_ingest_with_latest_tag(self, mock_http_client, tmp_path):
        """Test that 'latest' generates a date-based vintage."""
        output_path = ingest_hud_opendata(
            snapshot_tag="latest",
            base_dir=tmp_path,
            http_client=mock_http_client,
        )

        # Should contain today's date
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        assert f"HUDOpenData_{today}" in str(output_path)

    def test_ingest_raises_on_empty_response(self, tmp_path):
        """Test that empty API response raises ValueError."""
        client = MagicMock(spec=httpx.Client)
        response = MagicMock()
        response.json.return_value = {"features": []}
        response.raise_for_status = MagicMock()
        client.get.return_value = response

        with pytest.raises(ValueError, match="No features returned"):
            ingest_hud_opendata(
                snapshot_tag="test",
                base_dir=tmp_path,
                http_client=client,
            )

    def test_ingest_output_has_canonical_columns(self, mock_http_client, tmp_path):
        """Test that output contains all canonical schema columns."""
        output_path = ingest_hud_opendata(
            snapshot_tag="test",
            base_dir=tmp_path,
            http_client=mock_http_client,
        )

        gdf = gpd.read_parquet(output_path)
        required_columns = [
            "boundary_vintage",
            "coc_id",
            "coc_name",
            "state_abbrev",
            "source",
            "source_ref",
            "ingested_at",
            "geom_hash",
            "geometry",
        ]
        for col in required_columns:
            assert col in gdf.columns, f"Missing column: {col}"
