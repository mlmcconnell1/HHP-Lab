"""Tests for ACS measure builder."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import pytest

from coclab.measures.acs import (
    ACS_VARS,
    ADULT_VARS,
    aggregate_to_coc,
    fetch_acs_tract_data,
)


def make_census_response(
    tracts: list[dict[str, Any]],
    state_fips: str = "08",
) -> list[list[str]]:
    """Create a mock Census API response.

    Parameters
    ----------
    tracts : list[dict]
        List of tract data dicts with keys like 'county', 'tract', and ACS variable codes.
    state_fips : str
        State FIPS code to include in response.

    Returns
    -------
    list[list[str]]
        Census API-style response with header row and data rows.
    """
    # Build header row
    all_vars = list(ACS_VARS.keys()) + ADULT_VARS
    headers = ["NAME"] + all_vars + ["state", "county", "tract"]

    rows = [headers]
    for tract in tracts:
        row = [tract.get("NAME", "Census Tract")]
        for var in all_vars:
            row.append(str(tract.get(var, "0")))
        row.append(state_fips)
        row.append(tract.get("county", "001"))
        row.append(tract.get("tract", "000100"))
        rows.append(row)

    return rows


class TestFetchACSTractData:
    """Tests for fetch_acs_tract_data function."""

    def test_parses_response_correctly(self, httpx_mock):
        """Test that Census API response is parsed into correct DataFrame structure."""
        # Create mock response with one tract
        response_data = make_census_response(
            [
                {
                    "NAME": "Census Tract 1, Test County, Colorado",
                    "county": "001",
                    "tract": "000100",
                    "B01003_001E": "1000",  # total_population
                    "B19013_001E": "50000",  # median_household_income
                    "B25064_001E": "1200",  # median_gross_rent
                    "C17002_001E": "950",  # poverty_universe
                    "C17002_002E": "50",  # below_50pct_poverty
                    "C17002_003E": "75",  # 50_to_99pct_poverty
                    # Add a few adult vars
                    "B01001_007E": "100",  # Male 18-19
                    "B01001_008E": "150",  # Male 20
                    "B01001_031E": "100",  # Female 18-19
                    "B01001_032E": "140",  # Female 20
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2022/acs/acs5.*"),
            json=response_data,
        )

        df = fetch_acs_tract_data(2022, "08")

        assert len(df) == 1
        assert "GEOID" in df.columns
        assert df.iloc[0]["GEOID"] == "08001000100"
        assert df.iloc[0]["total_population"] == 1000
        assert df.iloc[0]["median_household_income"] == 50000
        assert df.iloc[0]["median_gross_rent"] == 1200
        assert df.iloc[0]["population_below_poverty"] == 125  # 50 + 75

    def test_handles_missing_values(self, httpx_mock):
        """Test that negative values (Census missing indicator) are converted to NA."""
        response_data = make_census_response(
            [
                {
                    "county": "001",
                    "tract": "000100",
                    "B01003_001E": "1000",
                    "B19013_001E": "-666666666",  # Missing value indicator
                    "B25064_001E": "1200",
                    "C17002_001E": "950",
                    "C17002_002E": "50",
                    "C17002_003E": "75",
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2022/acs/acs5.*"),
            json=response_data,
        )

        df = fetch_acs_tract_data(2022, "08")

        assert pd.isna(df.iloc[0]["median_household_income"])

    def test_calculates_adult_population(self, httpx_mock):
        """Test that adult population is correctly summed from age groups."""
        # Create response with specific adult age values
        tract_data = {
            "county": "001",
            "tract": "000100",
            "B01003_001E": "1000",
            "B19013_001E": "50000",
            "B25064_001E": "1200",
            "C17002_001E": "950",
            "C17002_002E": "50",
            "C17002_003E": "75",
        }
        # Set all adult male vars (007-025) to 10 each = 19 vars * 10 = 190
        for i in range(7, 26):
            tract_data[f"B01001_{i:03d}E"] = "10"
        # Set all adult female vars (031-049) to 10 each = 19 vars * 10 = 190
        for i in range(31, 50):
            tract_data[f"B01001_{i:03d}E"] = "10"

        response_data = make_census_response([tract_data])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2022/acs/acs5.*"),
            json=response_data,
        )

        df = fetch_acs_tract_data(2022, "08")

        # 19 male vars + 19 female vars, each = 10
        expected_adults = 38 * 10
        assert df.iloc[0]["adult_population"] == expected_adults


class TestAggregateToCoC:
    """Tests for aggregate_to_coc function."""

    def test_area_weighted_aggregation(self):
        """Test aggregation with area weighting."""
        # Create mock ACS data
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [1000, 2000, 3000],
                "adult_population": [800, 1600, 2400],
                "population_below_poverty": [100, 200, 300],
                "median_household_income": [50000, 60000, 70000],
                "median_gross_rent": [1000, 1200, 1400],
            }
        )

        # Create crosswalk - two tracts in CO-500, one in CO-501
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200", "08001000300"],
                "coc_id": ["CO-500", "CO-500", "CO-501"],
                "area_share": [0.8, 0.5, 1.0],  # tract 1: 80% in CO-500, tract 2: 50% in CO-500
                "pop_share": [0.8, 0.5, 1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        assert len(result) == 2
        assert set(result["coc_id"]) == {"CO-500", "CO-501"}

        # Check CO-500 weighted population
        # 1000 * 0.8 + 2000 * 0.5 = 800 + 1000 = 1800
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["total_population"] == 1800
        assert co500["weighting_method"] == "area"

        # Check CO-501
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["total_population"] == 3000  # 3000 * 1.0

    def test_population_weighted_aggregation(self):
        """Test aggregation with population weighting."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.4, 0.6],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="population")

        assert len(result) == 1
        co500 = result.iloc[0]
        # 1000 * 0.4 + 2000 * 0.6 = 400 + 1200 = 1600
        assert co500["total_population"] == 1600
        assert co500["weighting_method"] == "population"

    def test_coverage_ratio_calculation(self):
        """Test that coverage_ratio correctly computes area-weighted coverage."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, pd.NA],  # Second tract has no data
                "adult_population": [800, pd.NA],
                "population_below_poverty": [100, pd.NA],
                "median_household_income": [50000, pd.NA],
                "median_gross_rent": [1000, pd.NA],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [0.6, 0.4],
                "pop_share": [0.6, 0.4],
                "intersection_area": [600.0, 400.0],  # Areas in arbitrary units
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        # Only first tract has data (intersection_area=600)
        # Total area = 600 + 400 = 1000
        # Coverage = 600 / 1000 = 0.6
        assert result.iloc[0]["coverage_ratio"] == 0.6

    def test_missing_weight_column_raises(self):
        """Test that missing weight column raises ValueError."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                # Missing area_share column
            }
        )

        with pytest.raises(ValueError, match="missing required column"):
            aggregate_to_coc(acs_data, crosswalk, weighting="area")

    def test_adds_metadata_columns(self):
        """Test that result includes source metadata."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
                "adult_population": [800],
                "population_below_poverty": [100],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        assert "source" in result.columns
        assert result.iloc[0]["source"] == "acs_5yr"
        assert result.iloc[0]["weighting_method"] == "area"


class TestACSSchemaMeasures:
    """Tests to ensure the output schema matches requirements."""

    def test_output_schema_columns(self):
        """Test that aggregate output has all required schema columns."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
                "adult_population": [800],
                "population_below_poverty": [100],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        # Required columns per schema
        required_columns = [
            "coc_id",
            "weighting_method",
            "total_population",
            "adult_population",
            "population_below_poverty",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
            "source",
        ]

        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"
