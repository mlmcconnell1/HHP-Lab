"""Tests for county-level ACS weight computation for ZORI aggregation."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from coclab.provenance import read_provenance
from coclab.rents.weights import (
    ACS_WEIGHT_VARS,
    build_county_weights,
    fetch_county_acs_totals,
    fetch_state_county_acs,
    get_county_weights_path,
    load_county_weights,
    normalize_county_fips,
    parse_acs_vintage,
)


def make_census_county_response(
    counties: list[dict[str, Any]],
    state_fips: str = "08",
    variable: str = "B25003_003E",
) -> list[list[str]]:
    """Create a mock Census API response for county-level data.

    Parameters
    ----------
    counties : list[dict]
        List of county data dicts with keys like 'county' and variable codes.
    state_fips : str
        State FIPS code to include in response.
    variable : str
        ACS variable code included in response.

    Returns
    -------
    list[list[str]]
        Census API-style response with header row and data rows.
    """
    headers = ["NAME", variable, "state", "county"]

    rows = [headers]
    for county in counties:
        row = [
            county.get("NAME", "Test County, Colorado"),
            str(county.get(variable, "0")),
            state_fips,
            county.get("county", "001"),
        ]
        rows.append(row)

    return rows


class TestParseAcsVintage:
    """Tests for parse_acs_vintage function."""

    def test_parses_range_format(self):
        """Test parsing of range format like '2019-2023'."""
        assert parse_acs_vintage("2019-2023") == 2023
        assert parse_acs_vintage("2018-2022") == 2022
        assert parse_acs_vintage("2017-2021") == 2021

    def test_parses_single_year_format(self):
        """Test parsing of single year format."""
        assert parse_acs_vintage("2023") == 2023
        assert parse_acs_vintage("2022") == 2022

    def test_invalid_range_raises(self):
        """Test that invalid range format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid ACS vintage"):
            parse_acs_vintage("2019-2024")  # Wrong span

        with pytest.raises(ValueError, match="Invalid ACS vintage"):
            parse_acs_vintage("2019-2020")  # Too short span

    def test_invalid_format_raises(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid ACS vintage"):
            parse_acs_vintage("abc")

        with pytest.raises(ValueError, match="Invalid ACS vintage"):
            parse_acs_vintage("2019-abc")


class TestNormalizeCountyFips:
    """Tests for normalize_county_fips function."""

    def test_normalizes_fips_correctly(self):
        """Test that county FIPS is correctly formatted as 5 characters."""
        assert normalize_county_fips("08", "031") == "08031"
        assert normalize_county_fips("8", "31") == "08031"

    def test_preserves_leading_zeros(self):
        """Test that leading zeros are preserved in FIPS."""
        fips = normalize_county_fips("01", "001")
        assert fips == "01001"
        assert len(fips) == 5
        assert fips.startswith("01")  # Alabama starts with leading zero

    def test_pads_short_values(self):
        """Test that short values are zero-padded."""
        fips = normalize_county_fips("1", "1")
        assert fips == "01001"
        assert len(fips) == 5


class TestGetCountyWeightsPath:
    """Tests for get_county_weights_path function."""

    def test_default_path(self):
        """Test default output path generation."""
        path = get_county_weights_path("2019-2023", "renter_households")
        assert path == Path("data/curated/acs/county_weights__2019-2023__renter_households.parquet")

    def test_custom_base_dir(self):
        """Test output path with custom base directory."""
        path = get_county_weights_path("2019-2023", "renter_households", base_dir="/tmp/test")
        assert path == Path("/tmp/test/county_weights__2019-2023__renter_households.parquet")

    def test_different_methods(self):
        """Test path generation for different weighting methods."""
        assert "housing_units" in str(get_county_weights_path("2019-2023", "housing_units"))
        assert "population" in str(get_county_weights_path("2019-2023", "population"))


class TestFetchStateCountyAcs:
    """Tests for fetch_state_county_acs function."""

    def test_parses_response_correctly(self, httpx_mock):
        """Test that Census API response is parsed into correct DataFrame structure."""
        response_data = make_census_county_response(
            [
                {
                    "NAME": "Denver County, Colorado",
                    "county": "031",
                    "B25003_003E": "150000",
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, raw_content = fetch_state_county_acs(2023, "08", "B25003_003E")

        assert len(df) == 1
        assert "county_fips" in df.columns
        assert df.iloc[0]["county_fips"] == "08031"
        assert df.iloc[0]["value"] == 150000
        assert isinstance(raw_content, bytes)

    def test_handles_missing_values(self, httpx_mock):
        """Test that negative values (Census missing indicator) are converted to NA."""
        response_data = make_census_county_response(
            [
                {
                    "county": "001",
                    "B25003_003E": "-666666666",  # Missing value indicator
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_county_acs(2023, "08", "B25003_003E")

        assert pd.isna(df.iloc[0]["value"])

    def test_county_fips_leading_zeros_preserved(self, httpx_mock):
        """Test that county FIPS with leading zeros are correctly formatted."""
        response_data = make_census_county_response(
            [
                {
                    "county": "001",
                    "B25003_003E": "5000",
                }
            ],
            state_fips="01",  # Alabama - starts with 0
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_county_acs(2023, "01", "B25003_003E")

        fips = df.iloc[0]["county_fips"]
        assert fips == "01001"
        assert len(fips) == 5
        assert fips.startswith("01")


class TestFetchCountyAcsTotals:
    """Tests for fetch_county_acs_totals function."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_correct_schema_renter_households(self, httpx_mock):
        """Test that returned DataFrame has the correct schema for renter_households."""
        response_data = make_census_county_response(
            [
                {
                    "NAME": "Denver County, Colorado",
                    "county": "031",
                    "B25003_003E": "150000",
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, sha256, file_size = fetch_county_acs_totals("2019-2023", "renter_households")

        # Check required columns exist
        required_cols = [
            "county_fips",
            "acs_vintage",
            "weighting_method",
            "weight_value",
            "county_name",
            "data_source",
            "source_ref",
            "ingested_at",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

        # Check column values
        assert df.iloc[0]["acs_vintage"] == "2019-2023"
        assert df.iloc[0]["weighting_method"] == "renter_households"
        assert df.iloc[0]["data_source"] == "acs_5yr"
        assert "B25003" in df.iloc[0]["source_ref"]

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_correct_schema_housing_units(self, httpx_mock):
        """Test correct schema for housing_units weighting method."""
        response_data = make_census_county_response(
            [{"county": "031", "B25001_001E": "250000"}],
            variable="B25001_001E",
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "housing_units")

        assert df.iloc[0]["weighting_method"] == "housing_units"
        assert "B25001" in df.iloc[0]["source_ref"]

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_correct_schema_population(self, httpx_mock):
        """Test correct schema for population weighting method."""
        response_data = make_census_county_response(
            [{"county": "031", "B01003_001E": "700000"}],
            variable="B01003_001E",
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "population")

        assert df.iloc[0]["weighting_method"] == "population"
        assert "B01003" in df.iloc[0]["source_ref"]

    def test_invalid_method_raises(self):
        """Test that invalid weighting method raises ValueError."""
        with pytest.raises(ValueError, match="Invalid weighting method"):
            fetch_county_acs_totals("2019-2023", "invalid_method")

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_weight_values_non_negative(self, httpx_mock):
        """Test that weight values are non-negative (or NA)."""
        response_data = make_census_county_response(
            [
                {"county": "031", "B25003_003E": "150000"},
                {"county": "001", "B25003_003E": "0"},  # Zero is valid
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "renter_households")

        # All non-NA values should be >= 0
        valid_weights = df["weight_value"].dropna()
        assert (valid_weights >= 0).all()

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_non_empty_dataset(self, httpx_mock):
        """Test that the dataset is non-empty when API returns data."""
        response_data = make_census_county_response([{"county": "031", "B25003_003E": "150000"}])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "renter_households")

        assert len(df) > 0

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_raises_when_no_data_fetched(self, httpx_mock):
        """Test that ValueError is raised when no data can be fetched."""
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=500,
        )

        with pytest.raises(ValueError, match="No county ACS data"):
            fetch_county_acs_totals("2019-2023", "renter_households")


class TestBuildCountyWeights:
    """Tests for build_county_weights function."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_creates_output_file(self, httpx_mock, tmp_path):
        """Test that build creates the output Parquet file."""
        response_data = make_census_county_response([{"county": "031", "B25003_003E": "150000"}])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df = build_county_weights(
            "2019-2023",
            "renter_households",
            output_dir=tmp_path,
        )

        output_path = tmp_path / "county_weights__2019-2023__renter_households.parquet"
        assert output_path.exists()
        assert len(df) > 0

    def test_uses_cache_when_exists(self, tmp_path):
        """Test that cached file is used when it exists."""
        # Create a dummy cached file
        cached_path = tmp_path / "county_weights__2019-2023__renter_households.parquet"
        cached_path.parent.mkdir(parents=True, exist_ok=True)

        # Write a simple DataFrame
        df = pd.DataFrame(
            {
                "county_fips": ["08031"],
                "acs_vintage": ["2019-2023"],
                "weighting_method": ["renter_households"],
                "weight_value": [150000],
                "county_name": ["Denver County, Colorado"],
                "data_source": ["acs_5yr"],
                "source_ref": ["cached"],
                "ingested_at": [datetime.now(UTC)],
            }
        )
        df.to_parquet(cached_path)

        # Call build without force - should use cache
        result = build_county_weights(
            "2019-2023",
            "renter_households",
            force=False,
            output_dir=tmp_path,
        )

        assert result.iloc[0]["source_ref"] == "cached"

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_force_refetch_ignores_cache(self, httpx_mock, tmp_path):
        """Test that force=True refetches even with cache."""
        # Create a dummy cached file
        cached_path = tmp_path / "county_weights__2019-2023__renter_households.parquet"
        cached_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(
            {
                "county_fips": ["08031"],
                "acs_vintage": ["2019-2023"],
                "weighting_method": ["renter_households"],
                "weight_value": [150000],
                "county_name": ["Denver County, Colorado"],
                "data_source": ["acs_5yr"],
                "source_ref": ["cached"],
                "ingested_at": [datetime.now(UTC)],
            }
        )
        df.to_parquet(cached_path)

        # Setup mock for refetch
        response_data = make_census_county_response(
            [
                {"county": "031", "B25003_003E": "160000"}  # Different value
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        # Call build with force=True
        result = build_county_weights(
            "2019-2023",
            "renter_households",
            force=True,
            output_dir=tmp_path,
        )

        # Verify new data was written
        assert result.iloc[0]["weight_value"] == 160000

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_includes_provenance_metadata(self, httpx_mock, tmp_path):
        """Test that output file includes provenance metadata."""
        response_data = make_census_county_response([{"county": "031", "B25003_003E": "150000"}])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        build_county_weights(
            "2019-2023",
            "renter_households",
            output_dir=tmp_path,
        )

        output_path = tmp_path / "county_weights__2019-2023__renter_households.parquet"

        # Read provenance from file
        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.acs_vintage == "2019-2023"
        assert provenance.extra.get("dataset") == "county_weights"
        assert provenance.extra.get("weighting_method") == "renter_households"
        assert provenance.extra.get("table") == "B25003"


class TestLoadCountyWeights:
    """Tests for load_county_weights function."""

    def test_loads_existing_file(self, tmp_path):
        """Test loading an existing weights file."""
        # Create a weights file
        weights_path = tmp_path / "county_weights__2019-2023__renter_households.parquet"
        df = pd.DataFrame(
            {
                "county_fips": ["08031"],
                "acs_vintage": ["2019-2023"],
                "weighting_method": ["renter_households"],
                "weight_value": [150000],
                "county_name": ["Denver County"],
                "data_source": ["acs_5yr"],
                "source_ref": ["test"],
                "ingested_at": [datetime.now(UTC)],
            }
        )
        df.to_parquet(weights_path)

        # Load it
        result = load_county_weights("2019-2023", "renter_households", base_dir=tmp_path)

        assert len(result) == 1
        assert result.iloc[0]["county_fips"] == "08031"

    def test_raises_when_file_not_found(self, tmp_path):
        """Test that FileNotFoundError is raised when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="County weights file not found"):
            load_county_weights("2019-2023", "renter_households", base_dir=tmp_path)


class TestSchemaValidation:
    """Tests for output schema validation."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_county_fips_length(self, httpx_mock):
        """Test that county_fips is exactly 5 characters."""
        response_data = make_census_county_response(
            [
                {"county": "031", "B25003_003E": "150000"},
                {"county": "001", "B25003_003E": "50000"},
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "renter_households")

        # All county FIPS should be exactly 5 characters
        assert all(len(fips) == 5 for fips in df["county_fips"])

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_data_source_is_acs_5yr(self, httpx_mock):
        """Test that data_source is always 'acs_5yr'."""
        response_data = make_census_county_response([{"county": "031", "B25003_003E": "150000"}])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "renter_households")

        assert all(df["data_source"] == "acs_5yr")

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_ingested_at_is_utc(self, httpx_mock):
        """Test that ingested_at is a UTC timestamp."""
        response_data = make_census_county_response([{"county": "031", "B25003_003E": "150000"}])

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _ = fetch_county_acs_totals("2019-2023", "renter_households")

        # Check that timestamp is timezone-aware
        ts = df.iloc[0]["ingested_at"]
        assert ts.tzinfo is not None
        assert ts.tzinfo == UTC


class TestAcsWeightVarsConfiguration:
    """Tests for ACS_WEIGHT_VARS configuration."""

    def test_all_methods_have_required_keys(self):
        """Test that all weighting methods have required configuration keys."""
        required_keys = {"table", "variable", "description"}
        for method, config in ACS_WEIGHT_VARS.items():
            assert required_keys.issubset(config.keys()), f"Method {method} missing keys"

    def test_variable_codes_are_valid_format(self):
        """Test that variable codes follow Census naming convention."""
        for method, config in ACS_WEIGHT_VARS.items():
            var = config["variable"]
            # Census variables follow pattern like B25003_003E
            assert re.match(r"^[BC]\d{5}_\d{3}[EM]$", var), (
                f"Invalid variable format for {method}: {var}"
            )
