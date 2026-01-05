"""Phase 2 integration/smoke tests for CoC-PIT.

These tests validate the crosswalk and measures modules work correctly
with synthetic data that has known intersection properties.
"""

import numpy as np
import pandas as pd
import pytest
import geopandas as gpd
from shapely.geometry import box
from unittest.mock import patch

from coclab.xwalks.tract import (
    build_coc_tract_crosswalk,
    add_population_weights,
    validate_population_shares,
)
from coclab.xwalks.county import build_coc_county_crosswalk
from coclab.measures.diagnostics import (
    compute_crosswalk_diagnostics,
    summarize_diagnostics,
)
from coclab.measures.acs import aggregate_to_coc


# =============================================================================
# Fixtures: Synthetic GeoDataFrames with known geometry properties
# =============================================================================


@pytest.fixture
def simple_coc_gdf() -> gpd.GeoDataFrame:
    """Create a simple CoC boundary covering a 10x10 square.

    The CoC boundary is a single square from (0,0) to (10,10).
    Total area = 100 square units.
    """
    return gpd.GeoDataFrame(
        {
            "coc_number": ["XX-500"],
            "geometry": [box(0, 0, 10, 10)],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def simple_tract_gdf() -> gpd.GeoDataFrame:
    """Create tract geometries that partially overlap with the CoC.

    Tract 1: box(0,0,5,10) - fully inside CoC, area = 50
    Tract 2: box(5,0,15,10) - 50% inside CoC (5-10 x 0-10), area = 100, overlap = 50

    Total CoC coverage: Tract 1 contributes 100% (50/50), Tract 2 contributes 50% (50/100)
    """
    return gpd.GeoDataFrame(
        {
            "GEOID": ["01001000100", "01001000200"],
            "geometry": [box(0, 0, 5, 10), box(5, 0, 15, 10)],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def multi_coc_gdf() -> gpd.GeoDataFrame:
    """Create multiple CoC boundaries for testing population share sums.

    CoC XX-500: box(0,0,10,10)
    CoC XX-501: box(10,0,20,10)

    These two CoCs tile the space from 0-20 on x-axis, 0-10 on y-axis.
    """
    return gpd.GeoDataFrame(
        {
            "coc_number": ["XX-500", "XX-501"],
            "geometry": [box(0, 0, 10, 10), box(10, 0, 20, 10)],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def multi_tract_gdf() -> gpd.GeoDataFrame:
    """Create tracts that span across multiple CoCs.

    Tract 1: box(0,0,5,10) - fully in XX-500
    Tract 2: box(5,0,15,10) - split between XX-500 (50%) and XX-501 (50%)
    Tract 3: box(15,0,20,10) - fully in XX-501
    """
    return gpd.GeoDataFrame(
        {
            "GEOID": ["01001000100", "01001000200", "01001000300"],
            "geometry": [
                box(0, 0, 5, 10),
                box(5, 0, 15, 10),
                box(15, 0, 20, 10),
            ],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def population_data() -> pd.DataFrame:
    """Create synthetic population data for tracts.

    Tract 1: 1000 population
    Tract 2: 2000 population
    Tract 3: 1500 population
    """
    return pd.DataFrame(
        {
            "GEOID": ["01001000100", "01001000200", "01001000300"],
            "total_population": [1000, 2000, 1500],
        }
    )


@pytest.fixture
def acs_tract_data() -> pd.DataFrame:
    """Create synthetic ACS tract data for aggregation testing.

    Provides tract-level measures that can be aggregated to CoC level.
    """
    return pd.DataFrame(
        {
            "GEOID": ["01001000100", "01001000200", "01001000300"],
            "total_population": [1000, 2000, 1500],
            "adult_population": [800, 1600, 1200],
            "median_household_income": [50000, 60000, 55000],
            "median_gross_rent": [1200, 1400, 1300],
            "population_below_poverty": [100, 200, 150],
            "poverty_universe": [950, 1900, 1400],
        }
    )


@pytest.fixture
def simple_crosswalk(simple_coc_gdf, simple_tract_gdf) -> pd.DataFrame:
    """Pre-built crosswalk from simple fixtures."""
    return build_coc_tract_crosswalk(
        simple_coc_gdf, simple_tract_gdf, "2024", "2020"
    )


@pytest.fixture
def multi_crosswalk(multi_coc_gdf, multi_tract_gdf) -> pd.DataFrame:
    """Pre-built crosswalk from multi-CoC fixtures."""
    return build_coc_tract_crosswalk(
        multi_coc_gdf, multi_tract_gdf, "2024", "2020"
    )


@pytest.fixture
def crosswalk_with_pop(multi_crosswalk, population_data) -> pd.DataFrame:
    """Crosswalk with population weights added."""
    return add_population_weights(multi_crosswalk, population_data)


# =============================================================================
# Test: Tract Crosswalk Reproducibility
# =============================================================================


def test_tract_crosswalk_reproducibility(simple_coc_gdf, simple_tract_gdf):
    """Verify that same inputs produce identical crosswalk outputs.

    This test ensures the crosswalk building process is deterministic.
    Running the same inputs through build_coc_tract_crosswalk multiple
    times should produce byte-identical results.
    """
    # Build crosswalk twice with identical inputs
    xwalk1 = build_coc_tract_crosswalk(
        simple_coc_gdf, simple_tract_gdf, "2024", "2020"
    )
    xwalk2 = build_coc_tract_crosswalk(
        simple_coc_gdf, simple_tract_gdf, "2024", "2020"
    )

    # Verify structure is identical
    assert list(xwalk1.columns) == list(xwalk2.columns)
    assert len(xwalk1) == len(xwalk2)

    # Verify values are identical
    pd.testing.assert_frame_equal(xwalk1, xwalk2)


def test_tract_crosswalk_reproducibility_multi_coc(multi_coc_gdf, multi_tract_gdf):
    """Verify reproducibility with multiple CoCs and tracts.

    More complex scenario with multiple overlapping geometries.
    """
    xwalk1 = build_coc_tract_crosswalk(
        multi_coc_gdf, multi_tract_gdf, "2024", "2020"
    )
    xwalk2 = build_coc_tract_crosswalk(
        multi_coc_gdf, multi_tract_gdf, "2024", "2020"
    )

    pd.testing.assert_frame_equal(xwalk1, xwalk2)


# =============================================================================
# Test: Population Shares Sum to One
# =============================================================================


def test_population_shares_sum_to_one(crosswalk_with_pop):
    """Verify that pop_share values sum to approximately 1 per CoC.

    For a valid crosswalk with population weights, each CoC should have
    all its pop_share values sum to 1.0 (within floating point tolerance).
    This is a fundamental invariant of the population weighting calculation.
    """
    # Validate using the module's validation function
    validation = validate_population_shares(crosswalk_with_pop)

    # All CoCs should be valid (sum between 0.99 and 1.01)
    assert validation["is_valid"].all(), (
        f"Some CoCs have invalid pop_share sums: "
        f"{validation[~validation['is_valid']]}"
    )

    # Check actual sums are very close to 1.0
    for coc_id in crosswalk_with_pop["coc_id"].unique():
        coc_data = crosswalk_with_pop[crosswalk_with_pop["coc_id"] == coc_id]
        pop_share_sum = coc_data["pop_share"].sum()
        assert abs(pop_share_sum - 1.0) < 0.001, (
            f"CoC {coc_id} has pop_share sum of {pop_share_sum}, expected ~1.0"
        )


def test_population_shares_sum_to_one_simple(simple_crosswalk, population_data):
    """Verify pop_share sum with simple 2-tract scenario.

    Uses the simple crosswalk fixture with just 2 tracts to validate
    the basic mechanics of population weighting.
    """
    # Filter population data to match simple crosswalk
    pop_data = population_data[
        population_data["GEOID"].isin(["01001000100", "01001000200"])
    ]

    xwalk_with_pop = add_population_weights(simple_crosswalk, pop_data)

    # Single CoC, pop_share should sum to 1.0
    pop_share_sum = xwalk_with_pop["pop_share"].sum()
    assert abs(pop_share_sum - 1.0) < 0.001, (
        f"pop_share sum is {pop_share_sum}, expected ~1.0"
    )


# =============================================================================
# Test: Area Shares Valid Range
# =============================================================================


def test_area_shares_valid(simple_crosswalk):
    """Verify that area_share values are between 0 and 1.

    Area share represents the fraction of a tract's area that falls
    within a CoC. This must always be between 0 (no overlap) and 1
    (tract fully contained in CoC).
    """
    area_shares = simple_crosswalk["area_share"]

    # All values must be >= 0
    assert (area_shares >= 0).all(), (
        f"Found negative area_share values: {area_shares[area_shares < 0]}"
    )

    # All values must be <= 1
    assert (area_shares <= 1).all(), (
        f"Found area_share values > 1: {area_shares[area_shares > 1]}"
    )


def test_area_shares_valid_multi_coc(multi_crosswalk):
    """Verify area_share bounds with multiple CoCs.

    More complex scenario where tracts may be split across CoCs.
    Each individual area_share should still be in [0, 1].
    """
    area_shares = multi_crosswalk["area_share"]

    assert (area_shares >= 0).all()
    assert (area_shares <= 1).all()


def test_area_shares_known_values(simple_crosswalk):
    """Verify area_share calculation with known geometry overlaps.

    Tract 1 (box 0,0,5,10) is fully contained in CoC (box 0,0,10,10)
    -> area_share should be ~1.0

    Tract 2 (box 5,0,15,10) has 50% overlap with CoC
    -> area_share should be ~0.5

    Note: We use 0.02 tolerance because the Albers Equal Area projection
    causes slight distortion when projecting from EPSG:4326 lat/lon boxes.
    """
    xwalk = simple_crosswalk.set_index("tract_geoid")

    # Tract 1 should be fully inside CoC (area_share ~ 1.0)
    tract1_share = xwalk.loc["01001000100", "area_share"]
    assert abs(tract1_share - 1.0) < 0.02, (
        f"Tract 1 area_share is {tract1_share}, expected ~1.0"
    )

    # Tract 2 should be ~50% inside CoC (area_share ~ 0.5)
    tract2_share = xwalk.loc["01001000200", "area_share"]
    assert abs(tract2_share - 0.5) < 0.02, (
        f"Tract 2 area_share is {tract2_share}, expected ~0.5"
    )


# =============================================================================
# Test: Crosswalk Diagnostics Schema
# =============================================================================


def test_crosswalk_diagnostics_schema(multi_crosswalk):
    """Verify diagnostics have expected columns.

    The compute_crosswalk_diagnostics function should return a DataFrame
    with specific columns for analyzing crosswalk quality.
    """
    diagnostics = compute_crosswalk_diagnostics(multi_crosswalk)

    # Check required columns exist
    expected_cols = [
        "coc_id",
        "num_tracts",
        "max_tract_contribution",
        "coverage_ratio_area",
        "coverage_ratio_pop",
    ]

    for col in expected_cols:
        assert col in diagnostics.columns, (
            f"Missing expected column: {col}. "
            f"Found columns: {list(diagnostics.columns)}"
        )


def test_crosswalk_diagnostics_values(multi_crosswalk):
    """Verify diagnostics have sensible values.

    Validates the diagnostic calculations produce expected results
    for our known synthetic data.
    """
    diagnostics = compute_crosswalk_diagnostics(multi_crosswalk)

    # Should have one row per CoC
    assert len(diagnostics) == 2  # XX-500 and XX-501

    # num_tracts should be positive integers
    assert (diagnostics["num_tracts"] > 0).all()
    assert diagnostics["num_tracts"].dtype in [np.int64, np.int32, int]

    # max_tract_contribution should be in [0, 1]
    assert (diagnostics["max_tract_contribution"] >= 0).all()
    assert (diagnostics["max_tract_contribution"] <= 1).all()

    # coverage_ratio_area should be positive
    assert (diagnostics["coverage_ratio_area"] > 0).all()


def test_crosswalk_diagnostics_with_pop(crosswalk_with_pop):
    """Verify diagnostics include population coverage when available."""
    diagnostics = compute_crosswalk_diagnostics(crosswalk_with_pop)

    # coverage_ratio_pop should have values (not all NA)
    assert diagnostics["coverage_ratio_pop"].notna().any()

    # Pop coverage should be close to 1.0 for complete crosswalks
    pop_coverage = diagnostics["coverage_ratio_pop"].dropna()
    assert (pop_coverage > 0.99).all() and (pop_coverage < 1.01).all()


def test_summarize_diagnostics_output(multi_crosswalk):
    """Verify summarize_diagnostics produces a string summary."""
    diagnostics = compute_crosswalk_diagnostics(multi_crosswalk)
    summary = summarize_diagnostics(diagnostics)

    # Should return a non-empty string
    assert isinstance(summary, str)
    assert len(summary) > 0

    # Should contain expected section headers
    assert "CROSSWALK DIAGNOSTICS SUMMARY" in summary
    assert "Total CoCs:" in summary


# =============================================================================
# Test: CoC Measures Schema
# =============================================================================


def test_coc_measures_schema(multi_crosswalk, acs_tract_data):
    """Verify measures output has expected columns and types.

    The aggregate_to_coc function should produce a DataFrame with
    specific columns for CoC-level demographic measures.
    """
    measures = aggregate_to_coc(acs_tract_data, multi_crosswalk, weighting="area")

    # Check required columns exist
    expected_cols = [
        "coc_id",
        "total_population",
        "adult_population",
        "median_household_income",
        "median_gross_rent",
        "coverage_ratio",
        "weighting_method",
        "source",
    ]

    for col in expected_cols:
        assert col in measures.columns, (
            f"Missing expected column: {col}. "
            f"Found columns: {list(measures.columns)}"
        )


def test_coc_measures_types(multi_crosswalk, acs_tract_data):
    """Verify measures have correct data types."""
    measures = aggregate_to_coc(acs_tract_data, multi_crosswalk, weighting="area")

    # coc_id should be string
    assert measures["coc_id"].dtype == object or pd.api.types.is_string_dtype(
        measures["coc_id"]
    )

    # Population columns should be numeric
    assert pd.api.types.is_numeric_dtype(measures["total_population"])
    assert pd.api.types.is_numeric_dtype(measures["adult_population"])

    # weighting_method should be a string
    assert measures["weighting_method"].iloc[0] == "area"

    # source should be a string
    assert measures["source"].iloc[0] == "acs_5yr"


def test_coc_measures_values(multi_crosswalk, acs_tract_data):
    """Verify measures aggregation produces sensible values.

    Checks that aggregated values are reasonable given the input data.
    """
    measures = aggregate_to_coc(acs_tract_data, multi_crosswalk, weighting="area")

    # Should have one row per CoC
    assert len(measures) == 2

    # Total population should be positive for non-empty CoCs
    assert (measures["total_population"] > 0).all()

    # Adult population should be roughly proportional to total
    # (about 80% in our test data, but weighted aggregation may vary)
    adult_ratio = measures["adult_population"] / measures["total_population"]
    assert (adult_ratio > 0.5).all() and (adult_ratio < 1.5).all()

    # Coverage ratio should be in reasonable range
    assert (measures["coverage_ratio"] >= 0).all()
    assert (measures["coverage_ratio"] <= 2).all()  # Can exceed 1 due to area weighting


def test_coc_measures_weighting_methods(multi_crosswalk, acs_tract_data, population_data):
    """Verify both area and population weighting methods work."""
    # Area weighting
    area_measures = aggregate_to_coc(
        acs_tract_data, multi_crosswalk, weighting="area"
    )
    assert area_measures["weighting_method"].iloc[0] == "area"

    # Population weighting (requires pop_share in crosswalk)
    xwalk_with_pop = add_population_weights(multi_crosswalk, population_data)
    pop_measures = aggregate_to_coc(
        acs_tract_data, xwalk_with_pop, weighting="population"
    )
    assert pop_measures["weighting_method"].iloc[0] == "population"


# =============================================================================
# Test: County Crosswalk
# =============================================================================


@pytest.fixture
def simple_county_gdf() -> gpd.GeoDataFrame:
    """Create county geometries for testing county crosswalk."""
    return gpd.GeoDataFrame(
        {
            "GEOID": ["01001", "01003"],
            "geometry": [box(0, 0, 8, 10), box(8, 0, 20, 10)],
        },
        crs="EPSG:4326",
    )


def test_county_crosswalk_schema(simple_coc_gdf, simple_county_gdf):
    """Verify county crosswalk has expected schema."""
    xwalk = build_coc_county_crosswalk(simple_coc_gdf, simple_county_gdf, "2024")

    expected_cols = ["coc_id", "boundary_vintage", "county_fips", "area_share"]

    for col in expected_cols:
        assert col in xwalk.columns


def test_county_crosswalk_area_shares(simple_coc_gdf, simple_county_gdf):
    """Verify county area_share values are valid."""
    xwalk = build_coc_county_crosswalk(simple_coc_gdf, simple_county_gdf, "2024")

    assert (xwalk["area_share"] >= 0).all()
    assert (xwalk["area_share"] <= 1).all()


# =============================================================================
# Test: Edge Cases
# =============================================================================


def test_empty_intersection():
    """Verify handling of non-intersecting geometries.

    When a tract does not intersect a CoC, it should not appear
    in the crosswalk.
    """
    coc_gdf = gpd.GeoDataFrame(
        {"coc_number": ["XX-500"], "geometry": [box(0, 0, 10, 10)]},
        crs="EPSG:4326",
    )
    # Tract completely outside CoC
    tract_gdf = gpd.GeoDataFrame(
        {"GEOID": ["99999999999"], "geometry": [box(100, 100, 110, 110)]},
        crs="EPSG:4326",
    )

    xwalk = build_coc_tract_crosswalk(coc_gdf, tract_gdf, "2024", "2020")

    # Should produce empty crosswalk
    assert len(xwalk) == 0


def test_tract_fully_contained():
    """Verify handling of tract fully contained in CoC.

    A tract completely inside a CoC should have area_share close to 1.0.
    Uses small coordinates to avoid projection distortion issues.
    """
    coc_gdf = gpd.GeoDataFrame(
        {"coc_number": ["XX-500"], "geometry": [box(0, 0, 5, 5)]},
        crs="EPSG:4326",
    )
    tract_gdf = gpd.GeoDataFrame(
        {"GEOID": ["01001000100"], "geometry": [box(1, 1, 4, 4)]},
        crs="EPSG:4326",
    )

    xwalk = build_coc_tract_crosswalk(coc_gdf, tract_gdf, "2024", "2020")

    assert len(xwalk) == 1
    # Use wider tolerance due to projection distortion
    assert abs(xwalk.iloc[0]["area_share"] - 1.0) < 0.02


def test_missing_columns_raises():
    """Verify appropriate errors for missing required columns."""
    # Missing coc_number
    bad_coc = gpd.GeoDataFrame(
        {"bad_col": ["XX-500"], "geometry": [box(0, 0, 10, 10)]},
        crs="EPSG:4326",
    )
    good_tract = gpd.GeoDataFrame(
        {"GEOID": ["01001000100"], "geometry": [box(0, 0, 5, 10)]},
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="coc_number"):
        build_coc_tract_crosswalk(bad_coc, good_tract, "2024", "2020")

    # Missing GEOID
    good_coc = gpd.GeoDataFrame(
        {"coc_number": ["XX-500"], "geometry": [box(0, 0, 10, 10)]},
        crs="EPSG:4326",
    )
    bad_tract = gpd.GeoDataFrame(
        {"bad_col": ["01001000100"], "geometry": [box(0, 0, 5, 10)]},
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="GEOID"):
        build_coc_tract_crosswalk(good_coc, bad_tract, "2024", "2020")
