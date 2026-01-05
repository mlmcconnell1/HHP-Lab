"""Tests for PIT data parsing and canonicalization (WP-3B).

Tests cover:
- CoC ID normalization with various input formats
- PIT file parsing (CSV and Excel)
- Parquet output with provenance metadata
- Edge cases and error handling
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from coclab.pit.ingest.parser import (
    CANONICAL_COLUMNS,
    InvalidCoCIdError,
    get_canonical_output_path,
    normalize_coc_id,
    parse_pit_file,
    write_pit_parquet,
)
from coclab.provenance import read_provenance


class TestNormalizeCocId:
    """Tests for normalize_coc_id function."""

    def test_standard_format(self):
        """Test that standard format passes through."""
        assert normalize_coc_id("CO-500") == "CO-500"

    def test_lowercase(self):
        """Test lowercase conversion."""
        assert normalize_coc_id("co-500") == "CO-500"

    def test_no_dash(self):
        """Test missing hyphen handling."""
        assert normalize_coc_id("CO500") == "CO-500"

    def test_whitespace(self):
        """Test whitespace trimming."""
        assert normalize_coc_id(" CO-500 ") == "CO-500"

    def test_space_separator(self):
        """Test space as separator."""
        assert normalize_coc_id("CO 500") == "CO-500"

    def test_underscore_separator(self):
        """Test underscore as separator."""
        assert normalize_coc_id("CO_500") == "CO-500"

    def test_california(self):
        """Test California CoC."""
        assert normalize_coc_id("CA-600") == "CA-600"

    def test_new_york(self):
        """Test New York CoC."""
        assert normalize_coc_id("NY-600") == "NY-600"

    def test_short_number_padding(self):
        """Test that short numbers are zero-padded."""
        assert normalize_coc_id("CO-5") == "CO-005"
        assert normalize_coc_id("CA-50") == "CA-050"

    def test_us_territories(self):
        """Test US territory codes are valid."""
        assert normalize_coc_id("DC-500") == "DC-500"  # District of Columbia
        assert normalize_coc_id("PR-502") == "PR-502"  # Puerto Rico
        assert normalize_coc_id("GU-500") == "GU-500"  # Guam
        assert normalize_coc_id("VI-500") == "VI-500"  # US Virgin Islands

    def test_empty_raises(self):
        """Test empty string raises error."""
        with pytest.raises(InvalidCoCIdError, match="empty or null"):
            normalize_coc_id("")

    def test_none_raises(self):
        """Test None raises error."""
        with pytest.raises(InvalidCoCIdError, match="empty or null"):
            normalize_coc_id(None)

    def test_invalid_format_raises(self):
        """Test invalid format raises error."""
        with pytest.raises(InvalidCoCIdError, match="Cannot normalize"):
            normalize_coc_id("INVALID")

    def test_too_many_digits_raises(self):
        """Test that 4+ digits raises error."""
        with pytest.raises(InvalidCoCIdError, match="Cannot normalize"):
            normalize_coc_id("CO-5000")

    def test_too_long_raises(self):
        """Test that strings >7 chars are rejected early (footnotes, etc.)."""
        with pytest.raises(InvalidCoCIdError, match="too long"):
            normalize_coc_id("CO-500 Denver Metro")
        with pytest.raises(InvalidCoCIdError, match="too long"):
            normalize_coc_id("This is a footnote about MO-604")

    def test_invalid_state_code_raises(self):
        """Test that invalid state codes raise error."""
        with pytest.raises(InvalidCoCIdError, match="Invalid state code"):
            normalize_coc_id("XX-500")
        with pytest.raises(InvalidCoCIdError, match="Invalid state code"):
            normalize_coc_id("ZZ-123")

    def test_invalid_state_code_skip_validation(self):
        """Test that validation can be skipped."""
        result = normalize_coc_id("XX-500", validate_state=False)
        assert result == "XX-500"

    def test_letter_suffix_stripped(self):
        """Test that letter suffixes (e.g., MO-604a) are stripped.

        MO-604a represents the Kansas City metro area CoC which spans
        both Missouri and Kansas. The 'a' suffix in HUD data indicates
        the combined territory total.
        """
        assert normalize_coc_id("MO-604a") == "MO-604"
        assert normalize_coc_id("MO-604A") == "MO-604"
        assert normalize_coc_id("mo-604a") == "MO-604"

    def test_letter_suffix_various_separators(self):
        """Test letter suffix with various separators."""
        assert normalize_coc_id("MO604a") == "MO-604"
        assert normalize_coc_id("MO 604a") == "MO-604"
        assert normalize_coc_id("MO_604a") == "MO-604"


class TestParsePitFile:
    """Tests for parse_pit_file function."""

    @pytest.fixture
    def sample_csv_file(self, tmp_path):
        """Create a sample CSV file for testing."""
        csv_path = tmp_path / "pit_data.csv"
        data = """CoC Number,CoC Name,Overall Homeless,Sheltered Total Homeless,Unsheltered Homeless
CO-500,Colorado Balance of State,1234,800,434
CA-600,Los Angeles CoC,50000,35000,15000
NY-501,New York City,80000,60000,20000
"""
        csv_path.write_text(data)
        return csv_path

    @pytest.fixture
    def sample_csv_with_year_column(self, tmp_path):
        """Create a CSV file with year column."""
        csv_path = tmp_path / "pit_data_years.csv"
        data = """Year,CoC Number,Overall Homeless,Sheltered Total Homeless,Unsheltered Homeless
2023,CO-500,1200,780,420
2023,CA-600,49000,34000,15000
2024,CO-500,1234,800,434
2024,CA-600,50000,35000,15000
"""
        csv_path.write_text(data)
        return csv_path

    @pytest.fixture
    def sample_csv_various_formats(self, tmp_path):
        """Create CSV with various CoC ID formats."""
        csv_path = tmp_path / "pit_various.csv"
        data = """coc_code,total homeless
CO-500,1234
ca600,2000
NY 501,3000
TX-5,500
"""
        csv_path.write_text(data)
        return csv_path

    def test_parse_csv_basic(self, sample_csv_file):
        """Test basic CSV parsing."""
        result = parse_pit_file(sample_csv_file, year=2024)

        assert len(result) == 3
        assert set(result.columns) >= {"pit_year", "coc_id", "pit_total"}
        assert list(result["coc_id"]) == ["CO-500", "CA-600", "NY-501"]
        assert list(result["pit_total"]) == [1234, 50000, 80000]

    def test_parse_csv_with_year_filter(self, sample_csv_with_year_column):
        """Test CSV parsing with year filtering."""
        result = parse_pit_file(sample_csv_with_year_column, year=2024)

        # Should only get 2024 data
        assert len(result) == 2
        assert all(result["pit_year"] == 2024)

    def test_parse_csv_normalizes_coc_ids(self, sample_csv_various_formats):
        """Test that CoC IDs are normalized."""
        result = parse_pit_file(sample_csv_various_formats, year=2024)

        coc_ids = list(result["coc_id"])
        assert "CO-500" in coc_ids
        assert "CA-600" in coc_ids
        assert "NY-501" in coc_ids
        assert "TX-005" in coc_ids

    def test_parse_sets_metadata(self, sample_csv_file):
        """Test that metadata fields are set."""
        result = parse_pit_file(
            sample_csv_file,
            year=2024,
            source="hud_exchange",
            source_ref="https://example.com/pit.csv",
        )

        assert all(result["pit_year"] == 2024)
        assert all(result["data_source"] == "hud_exchange")
        assert all(result["source_ref"] == "https://example.com/pit.csv")
        assert all(pd.notna(result["ingested_at"]))

    def test_parse_unsupported_format(self, tmp_path):
        """Test error on unsupported file format."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("some data")
        with pytest.raises(ValueError, match="Unsupported file format"):
            parse_pit_file(txt_file, year=2024)

    def test_parse_missing_coc_column(self, tmp_path):
        """Test error when CoC ID column not found."""
        csv_path = tmp_path / "no_coc.csv"
        csv_path.write_text("name,count\nSome Place,100\n")
        with pytest.raises(ValueError, match="Cannot find CoC ID column"):
            parse_pit_file(csv_path, year=2024)

    def test_parse_missing_total_column(self, tmp_path):
        """Test error when total homeless column not found."""
        csv_path = tmp_path / "no_total.csv"
        csv_path.write_text("coc_code,name\nCO-500,Colorado\n")
        with pytest.raises(ValueError, match="Cannot find total homeless column"):
            parse_pit_file(csv_path, year=2024)


class TestWritePitParquet:
    """Tests for write_pit_parquet function."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            "pit_year": [2024, 2024, 2024],
            "coc_id": ["CO-500", "CA-600", "NY-501"],
            "pit_total": [1234, 50000, 80000],
            "pit_sheltered": [800, 35000, 60000],
            "pit_unsheltered": [434, 15000, 20000],
            "data_source": ["hud_exchange"] * 3,
            "source_ref": ["https://example.com"] * 3,
            "ingested_at": [datetime.now(timezone.utc)] * 3,
            "notes": [None, None, None],
        })

    def test_write_creates_file(self, sample_dataframe, tmp_path):
        """Test that Parquet file is created."""
        output_path = tmp_path / "pit_counts__2024.parquet"
        result = write_pit_parquet(sample_dataframe, output_path)

        assert result.exists()
        assert result == output_path

    def test_write_creates_parent_dirs(self, sample_dataframe, tmp_path):
        """Test that parent directories are created."""
        output_path = tmp_path / "nested" / "dirs" / "pit_counts__2024.parquet"
        result = write_pit_parquet(sample_dataframe, output_path)

        assert result.exists()

    def test_write_data_readable(self, sample_dataframe, tmp_path):
        """Test that written data can be read back."""
        output_path = tmp_path / "pit_counts__2024.parquet"
        write_pit_parquet(sample_dataframe, output_path)

        df = pd.read_parquet(output_path)
        assert len(df) == 3
        assert list(df["coc_id"]) == ["CO-500", "CA-600", "NY-501"]

    def test_write_has_provenance(self, sample_dataframe, tmp_path):
        """Test that provenance metadata is embedded."""
        output_path = tmp_path / "pit_counts__2024.parquet"
        write_pit_parquet(sample_dataframe, output_path)

        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.extra.get("pit_year") == 2024
        assert provenance.extra.get("row_count") == 3

    def test_write_missing_columns(self, tmp_path):
        """Test error when required columns missing."""
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "pit_total": [1234],
        })
        output_path = tmp_path / "pit_counts.parquet"
        with pytest.raises(ValueError, match="Missing required columns"):
            write_pit_parquet(df, output_path)

    def test_write_nullable_integers(self, tmp_path):
        """Test handling of nullable integer columns."""
        df = pd.DataFrame({
            "pit_year": [2024, 2024],
            "coc_id": ["CO-500", "CA-600"],
            "pit_total": [1234, 5000],
            "pit_sheltered": [800, None],  # One null value
            "pit_unsheltered": [None, 3000],  # One null value
            "data_source": ["hud_exchange"] * 2,
            "source_ref": ["https://example.com"] * 2,
            "ingested_at": [datetime.now(timezone.utc)] * 2,
            "notes": [None, None],
        })
        output_path = tmp_path / "pit_counts__2024.parquet"
        write_pit_parquet(df, output_path)

        result = pd.read_parquet(output_path)
        assert pd.isna(result.loc[0, "pit_unsheltered"])
        assert pd.isna(result.loc[1, "pit_sheltered"])


class TestGetCanonicalOutputPath:
    """Tests for get_canonical_output_path function."""

    def test_default_path(self):
        """Test default base directory."""
        path = get_canonical_output_path(2024)
        assert path == Path("data/curated/pit/pit_counts__2024.parquet")

    def test_custom_base_dir(self, tmp_path):
        """Test custom base directory."""
        path = get_canonical_output_path(2024, base_dir=tmp_path)
        assert path == tmp_path / "pit_counts__2024.parquet"

    def test_year_in_filename(self):
        """Test that year appears in filename."""
        path = get_canonical_output_path(2023)
        assert "2023" in str(path)
        path = get_canonical_output_path(2024)
        assert "2024" in str(path)


class TestCanonicalColumns:
    """Tests for CANONICAL_COLUMNS constant."""

    def test_required_columns_present(self):
        """Test that required columns are defined."""
        required = [
            "pit_year",
            "coc_id",
            "pit_total",
            "pit_sheltered",
            "pit_unsheltered",
            "data_source",
            "source_ref",
            "ingested_at",
            "notes",
        ]
        for col in required:
            assert col in CANONICAL_COLUMNS
