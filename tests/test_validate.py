"""Tests for boundary validation utilities."""

from datetime import UTC, datetime

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from coclab.geo.validate import (
    MIN_AREA_SQ_DEG,
    Severity,
    ValidationIssue,
    ValidationResult,
    validate_boundaries,
)


def make_valid_gdf(
    coc_ids: list[str] | None = None,
    geometries: list | None = None,
    boundary_vintage: str = "2025",
) -> gpd.GeoDataFrame:
    """Create a valid GeoDataFrame for testing."""
    if coc_ids is None:
        coc_ids = ["CO-500", "CO-501"]

    n = len(coc_ids)

    if geometries is None:
        # Create simple valid polygons for each coc_id
        geometries = [
            Polygon([(-105 - i, 39), (-105 - i, 40), (-104 - i, 40), (-104 - i, 39)])
            for i in range(n)
        ]
    return gpd.GeoDataFrame(
        {
            "boundary_vintage": [boundary_vintage] * n,
            "coc_id": coc_ids,
            "coc_name": [f"CoC {cid}" for cid in coc_ids],
            "state_abbrev": ["CO"] * n,
            "source": ["hud_exchange"] * n,
            "source_ref": ["https://example.com"] * n,
            "ingested_at": [datetime.now(UTC)] * n,
            "geom_hash": ["abc123"] * n,
            "geometry": geometries[: len(coc_ids)],
        },
        crs="EPSG:4326",
    )


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_empty_result_is_valid(self):
        result = ValidationResult()
        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_add_error_makes_invalid(self):
        result = ValidationResult()
        result.add_error("TEST", "test error")
        assert not result.is_valid
        assert len(result.errors) == 1

    def test_add_warning_keeps_valid(self):
        result = ValidationResult()
        result.add_warning("TEST", "test warning")
        assert result.is_valid
        assert len(result.warnings) == 1

    def test_to_list_formats_issues(self):
        result = ValidationResult()
        result.add_error("E1", "error message", row_index=5)
        result.add_warning("W1", "warning message", column="coc_id")
        lines = result.to_list()
        assert len(lines) == 2
        assert "[ERROR]" in lines[0]
        assert "row 5" in lines[0]
        assert "[WARNING]" in lines[1]
        assert "coc_id" in lines[1]

    def test_str_representation(self):
        result = ValidationResult()
        assert "no issues" in str(result)

        result.add_error("E1", "error")
        assert "1 error(s)" in str(result)


class TestValidationIssue:
    """Tests for ValidationIssue class."""

    def test_str_with_row_and_column(self):
        issue = ValidationIssue(
            severity=Severity.ERROR,
            code="TEST",
            message="test message",
            row_index=10,
            column="coc_id",
        )
        s = str(issue)
        assert "[ERROR]" in s
        assert "TEST" in s
        assert "test message" in s
        assert "row 10" in s
        assert "coc_id" in s


class TestValidateColumns:
    """Tests for column validation."""

    def test_valid_schema_passes(self):
        gdf = make_valid_gdf()
        result = validate_boundaries(gdf)
        # Should have no column-related errors
        column_errors = [e for e in result.errors if "COLUMN" in e.code or "DTYPE" in e.code]
        assert len(column_errors) == 0

    def test_missing_required_column(self):
        gdf = make_valid_gdf()
        gdf = gdf.drop(columns=["coc_id"])
        result = validate_boundaries(gdf)
        assert any(e.code == "MISSING_COLUMN" and e.column == "coc_id" for e in result.errors)

    def test_wrong_datetime_type(self):
        gdf = make_valid_gdf()
        gdf["ingested_at"] = "not a datetime"
        result = validate_boundaries(gdf)
        assert any(e.code == "WRONG_DTYPE" and e.column == "ingested_at" for e in result.errors)


class TestValidateUniqueness:
    """Tests for coc_id uniqueness validation."""

    def test_unique_coc_ids_pass(self):
        gdf = make_valid_gdf(coc_ids=["CO-500", "CO-501", "CO-502"])
        result = validate_boundaries(gdf)
        assert not any(e.code == "DUPLICATE_COC_ID" for e in result.errors)

    def test_duplicate_coc_ids_in_same_vintage(self):
        gdf = make_valid_gdf(coc_ids=["CO-500", "CO-500"])
        result = validate_boundaries(gdf)
        assert any(e.code == "DUPLICATE_COC_ID" for e in result.errors)

    def test_same_coc_id_different_vintages_ok(self):
        gdf1 = make_valid_gdf(coc_ids=["CO-500"], boundary_vintage="2024")
        gdf2 = make_valid_gdf(coc_ids=["CO-500"], boundary_vintage="2025")
        gdf = pd.concat([gdf1, gdf2], ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf, crs="EPSG:4326")
        result = validate_boundaries(gdf)
        assert not any(e.code == "DUPLICATE_COC_ID" for e in result.errors)


class TestValidateGeometries:
    """Tests for geometry validation."""

    def test_valid_geometries_pass(self):
        gdf = make_valid_gdf()
        result = validate_boundaries(gdf)
        assert not any(
            e.code in ("NULL_GEOMETRY", "EMPTY_GEOMETRY", "NO_GEOMETRY") for e in result.errors
        )

    def test_null_geometry_flagged(self):
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[None])
        result = validate_boundaries(gdf)
        assert any(e.code == "NULL_GEOMETRY" for e in result.errors)

    def test_empty_geometry_flagged(self):
        empty_poly = Polygon()
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[empty_poly])
        result = validate_boundaries(gdf)
        assert any(e.code == "EMPTY_GEOMETRY" for e in result.errors)

    def test_invalid_geometry_warned(self):
        # Self-intersecting polygon (bowtie)
        bowtie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[bowtie])
        result = validate_boundaries(gdf)
        assert any(w.code == "INVALID_GEOMETRY" for w in result.warnings)


class TestValidateAnomalies:
    """Tests for anomaly detection."""

    def test_normal_area_no_warning(self):
        gdf = make_valid_gdf()
        result = validate_boundaries(gdf)
        assert not any(w.code == "SMALL_AREA" for w in result.warnings)

    def test_tiny_polygon_warned(self):
        # Create a very small polygon (smaller than MIN_AREA_SQ_DEG)
        tiny = Polygon(
            [
                (-105, 39),
                (-105, 39.0001),
                (-104.9999, 39.0001),
                (-104.9999, 39),
                (-105, 39),
            ]
        )
        assert tiny.area < MIN_AREA_SQ_DEG
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[tiny])
        result = validate_boundaries(gdf)
        assert any(w.code == "SMALL_AREA" for w in result.warnings)

    def test_invalid_longitude_flagged(self):
        # Polygon with longitude > 180
        bad_poly = Polygon([(175, 39), (175, 40), (185, 40), (185, 39), (175, 39)])
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[bad_poly])
        result = validate_boundaries(gdf)
        assert any(e.code == "INVALID_LONGITUDE" for e in result.errors)

    def test_invalid_latitude_flagged(self):
        # Polygon with latitude > 90
        bad_poly = Polygon([(-105, 85), (-105, 95), (-104, 95), (-104, 85), (-105, 85)])
        gdf = make_valid_gdf(coc_ids=["CO-500"], geometries=[bad_poly])
        result = validate_boundaries(gdf)
        assert any(e.code == "INVALID_LATITUDE" for e in result.errors)


class TestEmptyData:
    """Tests for empty/null data handling."""

    def test_empty_gdf(self):
        gdf = gpd.GeoDataFrame()
        result = validate_boundaries(gdf)
        assert any(e.code == "EMPTY_DATA" for e in result.errors)

    def test_none_input(self):
        result = validate_boundaries(None)
        assert any(e.code == "EMPTY_DATA" for e in result.errors)
