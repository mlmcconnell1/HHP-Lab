"""Tests for PEP (Population Estimates Program) ingestion and aggregation.

Tests the county-level PEP data pipeline including:
- Parsing wide-to-long format Census Bureau files
- FIPS code validation (5-char county FIPS with leading zeros)
- Aggregation to CoC boundaries
- Coverage ratio computation
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import pandas as pd
import pytest

from coclab.pep.ingest import (
    INTERCENSAL_SERIES,
    PEP_URLS,
    POSTCENSAL_SERIES,
    VINTAGE_YEARS,
    get_output_path,
    ingest_pep_county,
    parse_pep_county,
)

# Sample Census PEP CSV data in wide format (mimics co-est2024-alldata.csv structure)
# Note: Census files use STATE and COUNTY as separate columns, SUMLEV to indicate geography level
# fmt: off
SAMPLE_PEP_CSV = """SUMLEV,REGION,DIVISION,STATE,COUNTY,STNAME,CTYNAME,ESTIMATESBASE2020,POPESTIMATE2020,POPESTIMATE2021,POPESTIMATE2022,POPESTIMATE2023
40,3,6,01,000,Alabama,Alabama,5024356,5031362,5039877,5074296,5108468
50,3,6,01,001,Alabama,Autauga County,58805,58239,58443,58920,59534
50,3,6,01,003,Alabama,Baldwin County,231767,231640,239294,246118,251122
50,3,6,01,005,Alabama,Barbour County,25223,25026,24670,24381,24148
40,1,2,09,000,Connecticut,Connecticut,3605597,3605597,3609001,3626205,3617176
50,1,2,09,001,Connecticut,Fairfield County,943332,943640,944198,948153,949921
50,1,2,09,003,Connecticut,Hartford County,894730,894730,896573,899498,895391
"""

# Sample with state-only rows (SUMLEV=40) and county rows (SUMLEV=50)
SAMPLE_PEP_CSV_MIXED = """SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2020,POPESTIMATE2021
40,01,000,Alabama,Alabama,5031362,5039877
50,01,001,Alabama,Autauga County,58239,58443
50,01,003,Alabama,Baldwin County,231640,239294
50,01,005,Alabama,Barbour County,25026,24670
40,09,000,Connecticut,Connecticut,3605597,3609001
50,09,001,Connecticut,Fairfield County,943640,944198
"""

# Sample with leading zeros needing preservation
SAMPLE_PEP_CSV_LEADING_ZEROS = """SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2020
50,01,001,Alabama,Autauga County,58239
50,02,020,Alaska,Anchorage Municipality,291247
50,06,001,California,Alameda County,1682353
"""
# fmt: on


class TestParsePepCounty:
    """Tests for parse_pep_county function."""

    def test_parses_wide_to_long_format(self, tmp_path):
        """Test that wide format is correctly converted to long format."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV)

        df = parse_pep_county(csv_path, vintage=2024)

        # Should only include county rows (SUMLEV=50), not state totals
        # 5 counties * 4 years = 20 rows
        assert len(df) == 20

        # Check required columns exist
        required_cols = {"county_fips", "state_fips", "year", "population"}
        assert required_cols.issubset(set(df.columns))

    def test_filters_county_rows_only(self, tmp_path):
        """Test that only county-level rows (SUMLEV=50) are included."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV_MIXED)

        df = parse_pep_county(csv_path, vintage=2024)

        # Should have 4 counties * 2 years = 8 rows (not state totals)
        assert len(df) == 8

        # All county FIPS should be 5 characters (state rows would have 000 county code)
        assert all(len(fips) == 5 for fips in df["county_fips"])
        assert all(fips[-3:] != "000" for fips in df["county_fips"])

    def test_geo_id_format_5_chars(self, tmp_path):
        """Test that county_fips is 5-character FIPS code."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV)

        df = parse_pep_county(csv_path, vintage=2024)

        # All county_fips should be exactly 5 characters
        assert all(len(fips) == 5 for fips in df["county_fips"])

        # Check specific FIPS codes
        assert "01001" in df["county_fips"].values  # Autauga County, AL
        assert "01003" in df["county_fips"].values  # Baldwin County, AL
        assert "09001" in df["county_fips"].values  # Fairfield County, CT

    def test_preserves_leading_zeros(self, tmp_path):
        """Test that leading zeros are preserved in FIPS codes."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV_LEADING_ZEROS)

        df = parse_pep_county(csv_path, vintage=2024)

        # Check that leading zeros are preserved
        assert "01001" in df["county_fips"].values  # Alabama (01)
        assert "02020" in df["county_fips"].values  # Alaska (02)
        assert "06001" in df["county_fips"].values  # California (06)

        # State FIPS should also have leading zeros
        assert "01" in df["state_fips"].values
        assert "02" in df["state_fips"].values
        assert "06" in df["state_fips"].values

    def test_population_values_extracted(self, tmp_path):
        """Test that population values are correctly extracted."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV_LEADING_ZEROS)

        df = parse_pep_county(csv_path, vintage=2024)

        # Check population values
        autauga = df[df["county_fips"] == "01001"]
        assert autauga["population"].iloc[0] == 58239

        anchorage = df[df["county_fips"] == "02020"]
        assert anchorage["population"].iloc[0] == 291247

    def test_year_extraction(self, tmp_path):
        """Test that years are correctly extracted from column names."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV)

        df = parse_pep_county(csv_path, vintage=2024)

        # Should have years 2020-2023
        years = sorted(df["year"].unique())
        assert years == [2020, 2021, 2022, 2023]

    def test_reference_date_is_july_1(self, tmp_path):
        """Test that reference_date is July 1 of each year."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_PEP_CSV)

        df = parse_pep_county(csv_path, vintage=2024)

        # All reference dates should be July 1
        for _, row in df.iterrows():
            ref_date = row["reference_date"]
            assert ref_date.month == 7
            assert ref_date.day == 1
            assert ref_date.year == row["year"]


class TestGetOutputPath:
    """Tests for get_output_path function."""

    def test_vintage_path(self, tmp_path):
        """Test output path for specific vintage."""
        path = get_output_path(2024, output_dir=tmp_path)
        assert path == tmp_path / "pep_county__v2024.parquet"

    def test_combined_path(self, tmp_path):
        """Test output path for combined data."""
        path = get_output_path("combined", output_dir=tmp_path)
        assert path == tmp_path / "pep_county__combined.parquet"


class TestPepUrls:
    """Tests for PEP URL configuration."""

    def test_vintage_2020_url_exists(self):
        """Test that vintage 2020 URL is configured."""
        assert 2020 in PEP_URLS
        assert "2010-2020" in PEP_URLS[2020]
        assert "co-est2020-alldata.csv" in PEP_URLS[2020]

    def test_vintage_2024_url_exists(self):
        """Test that vintage 2024 URL is configured."""
        assert 2024 in PEP_URLS
        assert "2020-2024" in PEP_URLS[2024]
        assert "co-est2024-alldata.csv" in PEP_URLS[2024]

    def test_vintage_years_configured(self):
        """Test that year ranges are configured for each vintage."""
        assert 2020 in VINTAGE_YEARS
        assert VINTAGE_YEARS[2020] == list(range(2010, 2021))

        assert 2024 in VINTAGE_YEARS
        assert VINTAGE_YEARS[2024] == list(range(2020, 2025))


class TestSeriesValidation:
    """Tests for series validation and availability."""

    def test_intercensal_unavailable_raises(self):
        with pytest.raises(ValueError):
            ingest_pep_county(series=INTERCENSAL_SERIES)

    def test_postcensal_invalid_vintage_raises(self):
        with pytest.raises(ValueError):
            ingest_pep_county(series=POSTCENSAL_SERIES, vintage=1999)


class TestIntegration:
    """Integration tests using actual downloaded data (if available)."""

    @pytest.fixture
    def pep_data_path(self):
        """Get path to PEP data if it exists."""
        path = Path("data/curated/pep/pep_county__combined.parquet")
        if not path.exists():
            pytest.skip("PEP data not available - run 'coclab ingest pep' first")
        return path

    def test_loaded_data_has_expected_columns(self, pep_data_path):
        """Test that loaded PEP data has expected columns."""
        df = pd.read_parquet(pep_data_path)

        required_cols = {
            "county_fips",
            "state_fips",
            "year",
            "population",
            "vintage",
            "data_source",
        }
        assert required_cols.issubset(set(df.columns))

    def test_loaded_data_covers_expected_years(self, pep_data_path):
        """Test that loaded data covers 2010-2024."""
        df = pd.read_parquet(pep_data_path)

        years = sorted(df["year"].unique())
        assert min(years) <= 2010
        assert max(years) >= 2024

    def test_county_count_reasonable(self, pep_data_path):
        """Test that county count is reasonable (3100+ counties)."""
        df = pd.read_parquet(pep_data_path)

        county_count = df["county_fips"].nunique()
        # US has ~3143 counties/equivalents
        assert county_count >= 3100
        assert county_count <= 3300

    def test_all_county_fips_valid_length(self, pep_data_path):
        """Test that all county FIPS codes are 5 characters."""
        df = pd.read_parquet(pep_data_path)

        invalid = df[df["county_fips"].str.len() != 5]
        assert len(invalid) == 0, f"Found {len(invalid)} rows with invalid FIPS length"

    def test_population_values_positive(self, pep_data_path):
        """Test that all population values are positive."""
        df = pd.read_parquet(pep_data_path)

        negative = df[df["population"] < 0]
        assert len(negative) == 0, f"Found {len(negative)} rows with negative population"

        # Most should have population > 100
        small_pop = df[df["population"] < 100]
        assert len(small_pop) / len(df) < 0.01  # Less than 1% very small
