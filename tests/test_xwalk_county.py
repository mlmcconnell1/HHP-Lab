"""Tests for CoC-county crosswalk generation.

Tests the area-weighted crosswalk between CoC boundaries and counties,
including:
- Schema validation (required columns present)
- Area invariants (intersection <= min(county, coc))
- Share bounds (0 <= area_share <= 1)
- Reproducibility of denominators
"""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hhplab.xwalks import apply_crosswalk
from hhplab.xwalks.county import build_coc_county_crosswalk


class TestApplyCrosswalk:
    """Tests for the shared crosswalk application helper."""

    def test_weighted_sum_with_group_columns(self):
        xwalk = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500", "CO-501"],
                "county_fips": ["08001", "08002", "08002"],
                "area_share": [0.25, 0.5, 0.5],
            }
        )
        data = pd.DataFrame(
            {
                "county_fips": ["08001", "08001", "08002", "08002"],
                "year": [2020, 2021, 2020, 2021],
                "population": [100, 120, 200, 240],
            }
        )

        result = apply_crosswalk(
            data,
            xwalk,
            value_cols=["population"],
            weight_col="area_share",
            geo_id_col="coc_id",
            source_id_col="county_fips",
            group_cols=["year"],
        ).sort_values(["coc_id", "year"]).reset_index(drop=True)

        assert result[["coc_id", "year", "population"]].to_dict("records") == [
            {"coc_id": "CO-500", "year": 2020, "population": 125.0},
            {"coc_id": "CO-500", "year": 2021, "population": 150.0},
            {"coc_id": "CO-501", "year": 2020, "population": 100.0},
            {"coc_id": "CO-501", "year": 2021, "population": 120.0},
        ]
        assert list(result["covered_weight"]) == [0.75, 0.75, 0.5, 0.5]
        assert list(result["source_count"]) == [2, 2, 1, 1]

    def test_normalized_weighted_mean_reports_available_coverage(self):
        xwalk = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500"],
                "county_fips": ["08001", "08002"],
                "weight": [0.25, 0.75],
            }
        )
        data = pd.DataFrame(
            {
                "geo_id": ["08001", "08002"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "zori": [1000.0, 2000.0],
            }
        )

        result = apply_crosswalk(
            data,
            xwalk,
            value_cols=["zori"],
            output_cols=["zori_coc"],
            weight_col="weight",
            geo_id_col="coc_id",
            source_id_col="county_fips",
            data_source_id_col="geo_id",
            group_cols=["date"],
            normalize=True,
        )

        row = result.iloc[0]
        assert row["zori_coc"] == 1750.0
        assert row["covered_weight"] == 1.0
        assert row["max_source_weight"] == 0.75

    def test_all_missing_values_preserve_missing_aggregate(self):
        xwalk = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500"],
                "county_fips": ["08001", "08002"],
                "area_share": [0.25, 0.75],
            }
        )
        data = pd.DataFrame(
            {
                "county_fips": ["08001", "08002"],
                "year": [2024, 2024],
                "population": [None, None],
            }
        )

        result = apply_crosswalk(
            data,
            xwalk,
            value_cols=["population"],
            weight_col="area_share",
            geo_id_col="coc_id",
            source_id_col="county_fips",
            group_cols=["year"],
        )

        row = result.iloc[0]
        assert pd.isna(row["population"])
        assert row["covered_weight"] == 1.0
        assert row["source_count"] == 2

    def test_zero_weight_rows_count_as_sources_but_not_coverage(self):
        xwalk = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500"],
                "county_fips": ["08001", "08002"],
                "weight": [0.0, 0.0],
            }
        )
        data = pd.DataFrame(
            {
                "geo_id": ["08001", "08002"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "zori": [1000.0, 2000.0],
            }
        )

        result = apply_crosswalk(
            data,
            xwalk,
            value_cols=["zori"],
            output_cols=["zori_coc"],
            weight_col="weight",
            geo_id_col="coc_id",
            source_id_col="county_fips",
            data_source_id_col="geo_id",
            group_cols=["date"],
            normalize=True,
        )

        row = result.iloc[0]
        assert pd.isna(row["zori_coc"])
        assert pd.isna(row["max_source_weight"])
        assert row["covered_weight"] == 0.0
        assert row["source_count"] == 2

    def test_missing_required_columns_are_actionable(self):
        with pytest.raises(
            ValueError,
            match="crosswalk missing required column\\(s\\): area_share",
        ):
            apply_crosswalk(
                pd.DataFrame({"county_fips": ["08001"], "population": [100]}),
                pd.DataFrame({"coc_id": ["CO-500"], "county_fips": ["08001"]}),
                value_cols=["population"],
                weight_col="area_share",
                geo_id_col="coc_id",
                source_id_col="county_fips",
            )


class TestCountyCrosswalkSchema:
    """Tests for county crosswalk output schema."""

    @pytest.fixture
    def simple_coc_gdf(self):
        """Create simple CoC geometry for testing."""
        return gpd.GeoDataFrame(
            {
                "coc_id": ["COC-001"],
                "geometry": [box(-105, 39, -104, 40)],  # 1x1 degree box
            },
            crs="EPSG:4326",
        )

    @pytest.fixture
    def simple_county_gdf(self):
        """Create simple county geometries for testing."""
        return gpd.GeoDataFrame(
            {
                "GEOID": ["08001", "08002"],
                "geometry": [
                    box(-105, 39, -104.5, 40),  # Left half of CoC
                    box(-104.5, 39, -104, 40),  # Right half of CoC
                ],
            },
            crs="EPSG:4326",
        )

    def test_has_required_columns(self, simple_coc_gdf, simple_county_gdf):
        """Test that crosswalk has all required columns."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        required_cols = {
            "coc_id",
            "boundary_vintage",
            "county_fips",
            "area_share",
            "intersection_area",
            "county_area",
            "coc_area",
        }
        assert required_cols.issubset(set(xwalk.columns))

    def test_areas_are_positive(self, simple_coc_gdf, simple_county_gdf):
        """Test that all area columns are positive."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        assert (xwalk["intersection_area"] > 0).all()
        assert (xwalk["county_area"] > 0).all()
        assert (xwalk["coc_area"] > 0).all()

    def test_intersection_not_larger_than_parts(self, simple_coc_gdf, simple_county_gdf):
        """Test that intersection area <= min(county_area, coc_area)."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        for _, row in xwalk.iterrows():
            min_area = min(row["county_area"], row["coc_area"])
            # Allow small tolerance for floating point
            assert row["intersection_area"] <= min_area * 1.001

    def test_area_share_in_valid_range(self, simple_coc_gdf, simple_county_gdf):
        """Test that area_share is between 0 and 1."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        assert (xwalk["area_share"] >= 0).all()
        assert (xwalk["area_share"] <= 1.0).all()

    def test_area_share_equals_intersection_over_county(
        self, simple_coc_gdf, simple_county_gdf
    ):
        """Test that area_share = intersection_area / county_area."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        computed_share = xwalk["intersection_area"] / xwalk["county_area"]
        pd.testing.assert_series_equal(
            xwalk["area_share"],
            computed_share,
            check_names=False,
        )

    def test_coc_share_derivable(self, simple_coc_gdf, simple_county_gdf):
        """Test that coc_share can be derived from stored columns."""
        xwalk = build_coc_county_crosswalk(
            simple_coc_gdf, simple_county_gdf, "2024"
        )

        # coc_share = intersection_area / coc_area (for CoC→county disaggregation)
        coc_share = xwalk["intersection_area"] / xwalk["coc_area"]

        # Should be in valid range
        assert (coc_share >= 0).all()
        assert (coc_share <= 1.0).all()


class TestCountyCrosswalkWithOverlap:
    """Tests with overlapping geometries (partial intersections)."""

    @pytest.fixture
    def partial_overlap_coc(self):
        """Create CoC that partially overlaps with counties."""
        return gpd.GeoDataFrame(
            {
                "coc_id": ["COC-001"],
                "geometry": [box(-105, 39.5, -104.5, 40)],  # Shifted up
            },
            crs="EPSG:4326",
        )

    @pytest.fixture
    def two_counties(self):
        """Create two counties, one fully in CoC, one partially."""
        return gpd.GeoDataFrame(
            {
                "GEOID": ["08001", "08002"],
                "geometry": [
                    box(-105, 39.5, -104.75, 40),  # Fully inside CoC
                    box(-104.75, 39, -104.5, 40),  # Only half in CoC
                ],
            },
            crs="EPSG:4326",
        )

    def test_partial_overlap_area_share_less_than_one(
        self, partial_overlap_coc, two_counties
    ):
        """Test that partial overlaps produce area_share < 1."""
        xwalk = build_coc_county_crosswalk(
            partial_overlap_coc, two_counties, "2024"
        )

        # County 08001 is fully in CoC, area_share should be ~1.0
        county_08001 = xwalk[xwalk["county_fips"] == "08001"]
        assert len(county_08001) == 1
        assert county_08001["area_share"].iloc[0] == pytest.approx(1.0, rel=0.01)

        # County 08002 is half in CoC, area_share should be ~0.5
        county_08002 = xwalk[xwalk["county_fips"] == "08002"]
        assert len(county_08002) == 1
        assert county_08002["area_share"].iloc[0] == pytest.approx(0.5, rel=0.01)

    def test_invalid_projected_geometry_is_dropped_without_runtime_warning(self):
        coc_gdf = gpd.GeoDataFrame(
            {
                "coc_id": ["COC-001"],
                "geometry": [box(100, 100, 110, 110)],
            },
            crs="EPSG:4326",
        )
        county_gdf = gpd.GeoDataFrame(
            {
                "GEOID": ["08001"],
                "geometry": [box(-105, 39, -104, 40)],
            },
            crs="EPSG:4326",
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            xwalk = build_coc_county_crosswalk(coc_gdf, county_gdf, "2024")

        assert xwalk.empty

    def test_boundary_only_contact_is_not_a_crosswalk_relationship(self):
        coc_gdf = gpd.GeoDataFrame(
            {
                "coc_id": ["COC-001"],
                "geometry": [box(-105, 39, -104, 40)],
            },
            crs="EPSG:4326",
        )
        county_gdf = gpd.GeoDataFrame(
            {
                "GEOID": ["08001"],
                "geometry": [box(-104, 39, -103, 40)],
            },
            crs="EPSG:4326",
        )

        xwalk = build_coc_county_crosswalk(coc_gdf, county_gdf, "2024")

        assert xwalk.empty


class TestCountyCrosswalkIntegration:
    """Integration tests using real crosswalk files (if available)."""

    @pytest.fixture
    def county_xwalk_path(self):
        """Get path to a county crosswalk if it exists."""
        xwalk_dir = Path("data/curated/xwalks")
        if not xwalk_dir.exists():
            pytest.skip("Crosswalk directory not available")

        files = list(xwalk_dir.glob("xwalk__B*xC*.parquet"))
        if not files:
            pytest.skip("County crosswalks not available - run 'hhplab generate xwalks' first")
        return files[0]

    def test_real_xwalk_has_expected_columns(self, county_xwalk_path):
        """Test that real crosswalk has expected columns."""
        df = pd.read_parquet(county_xwalk_path)

        required_cols = {
            "coc_id",
            "boundary_vintage",
            "county_fips",
            "area_share",
        }
        assert required_cols.issubset(set(df.columns))

    def test_real_xwalk_area_share_valid(self, county_xwalk_path):
        """Test that area_share values are valid in real crosswalk."""
        df = pd.read_parquet(county_xwalk_path)

        assert (df["area_share"] > 0).all()
        # Allow small tolerance for floating-point precision in geometry operations
        assert (df["area_share"] <= 1.01).all()

    def test_real_xwalk_coc_count_reasonable(self, county_xwalk_path):
        """Test that CoC count is reasonable (380-420 CoCs)."""
        df = pd.read_parquet(county_xwalk_path)

        coc_count = df["coc_id"].nunique()
        assert coc_count >= 350
        assert coc_count <= 450

    def test_real_xwalk_county_count_reasonable(self, county_xwalk_path):
        """Test that county count is reasonable (~3200 counties)."""
        df = pd.read_parquet(county_xwalk_path)

        county_count = df["county_fips"].nunique()
        assert county_count >= 3000
        assert county_count <= 3500

    def test_real_xwalk_has_area_columns_if_new(self, county_xwalk_path):
        """Test that new crosswalks have the additional area columns."""
        df = pd.read_parquet(county_xwalk_path)

        # If this crosswalk was built with the new code, it should have area columns
        if "intersection_area" in df.columns:
            assert "county_area" in df.columns
            assert "coc_area" in df.columns

            # Verify invariants
            assert (df["intersection_area"] > 0).all()
            assert (df["county_area"] > 0).all()
            assert (df["coc_area"] > 0).all()

            # Verify intersection_area <= min(county, coc)
            min_areas = df[["county_area", "coc_area"]].min(axis=1)
            assert (df["intersection_area"] <= min_areas * 1.001).all()

            # Verify area_share is reproducible
            computed_share = df["intersection_area"] / df["county_area"]
            pd.testing.assert_series_equal(
                df["area_share"],
                computed_share,
                check_names=False,
            )
