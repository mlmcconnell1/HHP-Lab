"""Tests for ACS tract population data fetcher."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from coclab.acs.ingest.tract_population import (
    fetch_state_tract_data,
    fetch_tract_data,
    get_output_path,
    ingest_tract_data,
    normalize_geoid,
    parse_acs_vintage,
)
from coclab.acs.variables import ALL_API_VARS, TRACT_OUTPUT_COLUMNS
from coclab.provenance import read_provenance


def make_census_response(
    tracts: list[dict[str, Any]],
    state_fips: str = "08",
) -> list[list[str]]:
    """Create a mock Census API response for ACS tract data.

    Parameters
    ----------
    tracts : list[dict]
        List of tract data dicts with keys like 'county', 'tract', and variable codes.
    state_fips : str
        State FIPS code to include in response.

    Returns
    -------
    list[list[str]]
        Census API-style response with header row and data rows.
    """
    # Build header row with all API variables
    headers = ["NAME"] + ALL_API_VARS + ["state", "county", "tract"]

    rows = [headers]
    for tract in tracts:
        row = [tract.get("NAME", "Census Tract")]
        for var in ALL_API_VARS:
            row.append(str(tract.get(var, "0")))
        row.append(state_fips)
        row.append(tract.get("county", "001"))
        row.append(tract.get("tract", "000100"))
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


class TestNormalizeGeoid:
    """Tests for normalize_geoid function."""

    def test_normalizes_geoid_correctly(self):
        """Test that GEOID is correctly formatted as 11 characters."""
        assert normalize_geoid("08", "031", "001000") == "08031001000"
        assert normalize_geoid("8", "31", "1000") == "08031001000"

    def test_preserves_leading_zeros(self):
        """Test that leading zeros are preserved in GEOID."""
        geoid = normalize_geoid("01", "001", "000100")
        assert geoid == "01001000100"
        assert len(geoid) == 11
        assert geoid.startswith("01")  # Alabama starts with leading zero

    def test_pads_short_values(self):
        """Test that short values are zero-padded."""
        geoid = normalize_geoid("1", "1", "100")
        assert geoid == "01001000100"
        assert len(geoid) == 11


class TestGetOutputPath:
    """Tests for get_output_path function."""

    def test_default_path(self):
        """Test default output path generation with new temporal shorthand."""
        path = get_output_path("2019-2023", "2023")
        assert path == Path("data/curated/acs/acs5_tracts__A2023xT2023.parquet")

    def test_custom_base_dir(self):
        """Test output path with custom base directory."""
        path = get_output_path("2019-2023", "2023", base_dir="/tmp/test")
        assert path == Path("/tmp/test/acs5_tracts__A2023xT2023.parquet")


class TestFetchStateTractPopulation:
    """Tests for fetch_state_tract_data function."""

    def test_parses_response_correctly(self, httpx_mock):
        """Test that Census API response is parsed into correct DataFrame structure."""
        response_data = make_census_response(
            [
                {
                    "NAME": "Census Tract 1, Test County, Colorado",
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",  # total_population
                    "B01003_001M": "150",  # margin of error
                    "B19013_001E": "65000",  # median_household_income
                    "B25064_001E": "1400",  # median_gross_rent
                    "C17002_001E": "4800",  # poverty_universe
                    "C17002_002E": "200",  # below_50pct_poverty
                    "C17002_003E": "350",  # 50_to_99pct_poverty
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, raw_content = fetch_state_tract_data(2023, "08")

        assert len(df) == 1
        assert "tract_geoid" in df.columns
        assert df.iloc[0]["tract_geoid"] == "08031001000"
        assert df.iloc[0]["total_population"] == 5000
        assert df.iloc[0]["moe_total_population"] == 150
        assert df.iloc[0]["median_household_income"] == 65000
        assert df.iloc[0]["median_gross_rent"] == 1400
        assert df.iloc[0]["poverty_universe"] == 4800
        assert df.iloc[0]["below_50pct_poverty"] == 200
        assert df.iloc[0]["50_to_99pct_poverty"] == 350
        assert df.iloc[0]["population_below_poverty"] == 550  # 200 + 350
        assert isinstance(raw_content, bytes)

    def test_handles_missing_values(self, httpx_mock):
        """Test that negative values (Census missing indicator) are converted to NA."""
        response_data = make_census_response(
            [
                {
                    "county": "001",
                    "tract": "000100",
                    "B01003_001E": "-666666666",  # Missing value indicator
                    "B01003_001M": "-666666666",  # Missing MOE
                    "B19013_001E": "-666666666",  # Missing income
                    "B25064_001E": "-666666666",  # Missing rent
                }
            ]
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "08")

        assert pd.isna(df.iloc[0]["total_population"])
        assert pd.isna(df.iloc[0]["moe_total_population"])
        assert pd.isna(df.iloc[0]["median_household_income"])
        assert pd.isna(df.iloc[0]["median_gross_rent"])

    def test_derives_adult_population(self, httpx_mock):
        """Test that adult population is derived from B01001 age groups."""
        tract_data = {
            "county": "031",
            "tract": "001000",
            "B01003_001E": "1000",
            "B01003_001M": "50",
        }
        # Male 18+ (007-025): 19 vars at 10 each = 190
        for i in range(7, 26):
            tract_data[f"B01001_{i:03d}E"] = "10"
        # Female 18+ (031-049): 19 vars at 10 each = 190
        for i in range(31, 50):
            tract_data[f"B01001_{i:03d}E"] = "10"

        response_data = make_census_response([tract_data])
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "08")

        assert df.iloc[0]["adult_population"] == 380  # 38 vars * 10

    def test_derives_population_below_poverty(self, httpx_mock):
        """Test that population_below_poverty is derived from poverty components."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "C17002_001E": "4800",
                    "C17002_002E": "120",  # below 50%
                    "C17002_003E": "230",  # 50-99%
                }
            ]
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "08")

        assert df.iloc[0]["population_below_poverty"] == 350  # 120 + 230

    def test_geoid_leading_zeros_preserved(self, httpx_mock):
        """Test that GEOIDs with leading zeros are correctly formatted."""
        response_data = make_census_response(
            [
                {
                    "county": "001",
                    "tract": "000100",
                    "B01003_001E": "1000",
                    "B01003_001M": "50",
                }
            ],
            state_fips="01",  # Alabama - starts with 0
        )

        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "01")

        geoid = df.iloc[0]["tract_geoid"]
        assert geoid == "01001000100"
        assert len(geoid) == 11
        assert geoid.startswith("01")


class TestFetchTractPopulation:
    """Tests for fetch_tract_data function."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_correct_schema(self, httpx_mock):
        """Test that returned DataFrame has all canonical columns."""
        # Mock response with the full variable set
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
                    "B19013_001E": "65000",
                    "B25064_001E": "1400",
                    "C17002_001E": "4800",
                    "C17002_002E": "200",
                    "C17002_003E": "350",
                }
            ]
        )

        # Mock all state requests - return data for state 08, 404 for others
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*state%3A08.*"),
            json=response_data,
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=404,
        )

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        # Output columns must match the canonical TRACT_OUTPUT_COLUMNS order
        assert list(df.columns) == TRACT_OUTPUT_COLUMNS

        # Spot-check values
        row = df.iloc[0]
        assert row["acs_vintage"] == "2019-2023"
        assert row["tract_vintage"] == "2023"
        assert row["data_source"] == "acs_5yr"
        assert row["total_population"] == 5000
        assert row["median_household_income"] == 65000
        assert row["median_gross_rent"] == 1400
        assert row["poverty_universe"] == 4800
        assert row["population_below_poverty"] == 550  # 200 + 350

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_population_values_non_negative(self, httpx_mock):
        """Test that population values are non-negative (or NA)."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
                },
                {
                    "county": "031",
                    "tract": "001100",
                    "B01003_001E": "0",  # Zero population is valid
                    "B01003_001M": "0",
                },
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

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        # All non-NA values should be >= 0
        valid_pops = df["total_population"].dropna()
        assert (valid_pops >= 0).all()

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_returns_non_empty_dataset(self, httpx_mock):
        """Test that the dataset is non-empty when API returns data."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
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

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        assert len(df) > 0

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_raises_when_no_data_fetched(self, httpx_mock):
        """Test that ValueError is raised when no data can be fetched."""
        # Mock all state requests to fail
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            status_code=500,
        )

        with pytest.raises(ValueError, match="No ACS tract data"):
            fetch_tract_data("2019-2023", "2023")


class TestIngestTractPopulation:
    """Tests for ingest_tract_data function."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_creates_output_file(self, httpx_mock, tmp_path):
        """Test that ingest creates the output Parquet file."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
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

        output_path = ingest_tract_data(
            "2019-2023",
            "2023",
            output_dir=tmp_path,
        )

        assert output_path.exists()
        assert output_path.suffix == ".parquet"

    def test_uses_cache_when_exists(self, tmp_path):
        """Test that cached file is used when it exists."""
        # Create a dummy cached file with new temporal shorthand naming
        cached_path = tmp_path / "acs5_tracts__A2023xT2023.parquet"
        cached_path.parent.mkdir(parents=True, exist_ok=True)

        # Write a simple DataFrame
        df = pd.DataFrame(
            {
                "tract_geoid": ["08031001000"],
                "acs_vintage": ["2019-2023"],
                "tract_vintage": ["2023"],
                "total_population": [5000],
                "data_source": ["acs_5yr"],
                "source_ref": ["cached"],
                "ingested_at": [datetime.now(UTC)],
            }
        )
        df.to_parquet(cached_path)

        # Call ingest without force - should use cache
        result_path = ingest_tract_data(
            "2019-2023",
            "2023",
            force=False,
            output_dir=tmp_path,
        )

        assert result_path == cached_path

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_force_refetch_ignores_cache(self, httpx_mock, tmp_path):
        """Test that force=True refetches even with cache."""
        # Create a dummy cached file with new temporal shorthand naming
        cached_path = tmp_path / "acs5_tracts__A2023xT2023.parquet"
        cached_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(
            {
                "tract_geoid": ["08031001000"],
                "acs_vintage": ["2019-2023"],
                "tract_vintage": ["2023"],
                "total_population": [5000],
                "data_source": ["acs_5yr"],
                "source_ref": ["cached"],
                "ingested_at": [datetime.now(UTC)],
            }
        )
        df.to_parquet(cached_path)

        # Setup mock for refetch
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "6000",  # Different value
                    "B01003_001M": "200",
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

        # Call ingest with force=True
        result_path = ingest_tract_data(
            "2019-2023",
            "2023",
            force=True,
            output_dir=tmp_path,
        )

        # Verify new data was written
        result_df = pd.read_parquet(result_path)
        assert result_df.iloc[0]["total_population"] == 6000

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_includes_provenance_metadata(self, httpx_mock, tmp_path):
        """Test that output file includes provenance metadata."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
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

        output_path = ingest_tract_data(
            "2019-2023",
            "2023",
            output_dir=tmp_path,
        )

        # Read provenance from file
        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.acs_vintage == "2019-2023"
        assert provenance.tract_vintage == "2023"
        assert provenance.extra.get("dataset") == "acs5_tract_data"
        assert "B01003" in provenance.extra.get("tables", [])


class TestSchemaValidation:
    """Tests for output schema validation."""

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_tract_geoid_length(self, httpx_mock):
        """Test that tract_geoid is exactly 11 characters."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
                },
                {
                    "county": "001",
                    "tract": "000100",
                    "B01003_001E": "3000",
                    "B01003_001M": "100",
                },
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

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        # All GEOIDs should be exactly 11 characters
        assert all(len(geoid) == 11 for geoid in df["tract_geoid"])

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_data_source_is_acs_5yr(self, httpx_mock):
        """Test that data_source is always 'acs_5yr'."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
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

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        assert all(df["data_source"] == "acs_5yr")

    @pytest.mark.httpx_mock(can_send_already_matched_responses=True)
    def test_ingested_at_is_utc(self, httpx_mock):
        """Test that ingested_at is a UTC timestamp."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B01003_001M": "150",
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

        df, _, _, _ = fetch_tract_data("2019-2023", "2023")

        # Check that timestamp is timezone-aware
        ts = df.iloc[0]["ingested_at"]
        assert ts.tzinfo is not None
        assert ts.tzinfo == UTC


class TestUnemploymentIngest:
    """Tests for B23025 unemployment variable ingestion."""

    def test_unemployment_columns_parsed(self, httpx_mock):
        """B23025 variables are included in tract output."""
        response_data = make_census_response(
            [
                {
                    "county": "031",
                    "tract": "001000",
                    "B01003_001E": "5000",
                    "B23025_003E": "3200",  # civilian_labor_force
                    "B23025_005E": "160",  # unemployed_count
                }
            ]
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "08")

        assert "civilian_labor_force" in df.columns
        assert "unemployed_count" in df.columns
        assert df.iloc[0]["civilian_labor_force"] == 3200
        assert df.iloc[0]["unemployed_count"] == 160

    def test_unemployment_missing_values_become_na(self, httpx_mock):
        """Negative Census values for B23025 become NA."""
        response_data = make_census_response(
            [
                {
                    "county": "001",
                    "tract": "000100",
                    "B23025_003E": "-666666666",
                    "B23025_005E": "-666666666",
                }
            ]
        )
        httpx_mock.add_response(
            url=re.compile(r"https://api\.census\.gov/data/2023/acs/acs5.*"),
            json=response_data,
        )

        df, _ = fetch_state_tract_data(2023, "08")

        assert pd.isna(df.iloc[0]["civilian_labor_force"])
        assert pd.isna(df.iloc[0]["unemployed_count"])

    def test_unemployment_in_tract_output_columns(self):
        """TRACT_OUTPUT_COLUMNS includes unemployment fields."""
        assert "civilian_labor_force" in TRACT_OUTPUT_COLUMNS
        assert "unemployed_count" in TRACT_OUTPUT_COLUMNS
