"""Tests for ZORI ingestion and normalization.

Tests the county-level ZORI data pipeline including:
- Downloading and caching
- Wide-to-long format parsing
- Geo_id validation (5-char county FIPS)
- Monthly continuity validation
- Parquet output with provenance
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from hhplab.provenance import read_provenance
from hhplab.rents.zori_ingest import (
    ZORI_URLS,
    _validate_monthly_continuity,
    download_zori,
    get_output_path,
    ingest_zori,
    parse_zori_county,
    parse_zori_zip,
)

# Sample Zillow CSV data in wide format (county)
# fmt: off
SAMPLE_COUNTY_CSV = (
    "RegionID,SizeRank,RegionName,RegionType,StateName,"
    "StateCodeFIPS,MunicipalCodeFIPS,Metro,2023-01-31,2023-02-28,2023-03-31\n"
    "1234,1,Adams County,county,Colorado,08,001,Denver-Aurora-Lakewood,"
    "1500.00,1520.00,1540.00\n"
    "5678,2,Arapahoe County,county,Colorado,08,005,Denver-Aurora-Lakewood,"
    "1600.00,1620.00,1650.00\n"
    "9012,3,Autauga County,county,Alabama,01,001,Montgomery,"
    "900.00,910.00,920.00\n"
)

# Sample with gaps in date sequence
SAMPLE_CSV_WITH_GAPS = (
    "RegionID,SizeRank,RegionName,RegionType,StateName,"
    "StateCodeFIPS,MunicipalCodeFIPS,Metro,2023-01-31,2023-03-31,2023-05-31\n"
    "1234,1,Adams County,county,Colorado,08,001,Denver-Aurora-Lakewood,"
    "1500.00,1540.00,1580.00\n"
)

# Sample with invalid geo_ids (short FIPS)
SAMPLE_CSV_INVALID_GEOID = (
    "RegionID,SizeRank,RegionName,RegionType,StateName,"
    "StateCodeFIPS,MunicipalCodeFIPS,Metro,2023-01-31\n"
    "1234,1,Test County,county,Colorado,8,1,Denver,1500.00\n"
)
# fmt: on


class TestParseZoriCounty:
    """Tests for parse_zori_county function."""

    def test_parses_wide_to_long_format(self, tmp_path):
        """Test that wide format is correctly converted to long format."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        # Should have 3 counties * 3 months = 9 rows
        assert len(df) == 9

        # Check columns
        assert set(df.columns) == {"geo_id", "date", "zori", "region_name", "state"}

    def test_geo_id_format_5_chars(self, tmp_path):
        """Test that geo_id is 5-character county FIPS."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        # All geo_ids should be exactly 5 characters
        assert all(len(geo_id) == 5 for geo_id in df["geo_id"])

        # Check specific FIPS codes
        assert "08001" in df["geo_id"].values  # Adams County, CO
        assert "08005" in df["geo_id"].values  # Arapahoe County, CO
        assert "01001" in df["geo_id"].values  # Autauga County, AL

    def test_preserves_leading_zeros(self, tmp_path):
        """Test that leading zeros are preserved in FIPS codes."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        # Alabama's FIPS starts with 01
        al_rows = df[df["state"] == "Alabama"]
        assert len(al_rows) > 0
        assert al_rows["geo_id"].iloc[0].startswith("01")

    def test_date_parsing(self, tmp_path):
        """Test that dates are correctly parsed."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        # Check that dates are datetime objects
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

        # Check date values
        dates = df["date"].dt.date.unique()
        assert date(2023, 1, 31) in dates
        assert date(2023, 2, 28) in dates
        assert date(2023, 3, 31) in dates

    def test_zori_values_numeric(self, tmp_path):
        """Test that ZORI values are numeric."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        assert pd.api.types.is_numeric_dtype(df["zori"])
        assert (df["zori"] > 0).all()

    def test_sorted_by_geo_id_and_date(self, tmp_path):
        """Test that output is sorted by geo_id and date."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(SAMPLE_COUNTY_CSV)

        df = parse_zori_county(csv_path)

        # Check sorting
        assert df["geo_id"].is_monotonic_increasing or df.equals(
            df.sort_values(["geo_id", "date"]).reset_index(drop=True)
        )


class TestParseZoriMonthOnlyHeaders:
    """Regression tests for coclab-gwfa: YYYY-MM column headers."""

    MONTH_ONLY_CSV = (
        "RegionID,SizeRank,RegionName,RegionType,StateName,"
        "StateCodeFIPS,MunicipalCodeFIPS,Metro,2023-01,2023-02\n"
        "1234,1,Adams County,county,Colorado,08,001,Denver,"
        "1500.00,1520.00\n"
    )

    def test_county_parses_month_only_headers(self, tmp_path):
        """Month-only column headers like 2023-01 must parse, not be dropped."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(self.MONTH_ONLY_CSV)
        df = parse_zori_county(csv_path)
        assert len(df) == 2
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_zip_parses_month_only_headers(self, tmp_path):
        """Month-only headers must work for ZIP ZORI too."""
        csv = (
            "RegionID,SizeRank,RegionName,RegionType,StateName,"
            "Metro,2023-01,2023-02\n"
            "12345,1,80001,zip,Colorado,Denver,"
            "1400.00,1420.00\n"
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv)
        df = parse_zori_zip(csv_path)
        assert len(df) == 2


class TestValidateMonthyContinuity:
    """Tests for _validate_monthly_continuity function."""

    def test_no_warnings_for_continuous_series(self, caplog):
        """Test that no warnings are logged for continuous monthly data."""
        df = pd.DataFrame(
            {
                "geo_id": ["08001"] * 3,
                "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01"]),
            }
        )

        with caplog.at_level("WARNING"):
            _validate_monthly_continuity(df)

        # Should not have gap warnings
        assert "Gap in ZORI series" not in caplog.text

    def test_warns_on_gaps(self, caplog):
        """Test that gaps in monthly series trigger warnings."""
        df = pd.DataFrame(
            {
                "geo_id": ["08001"] * 3,
                "date": pd.to_datetime(["2023-01-01", "2023-03-01", "2023-04-01"]),  # Feb missing
            }
        )

        with caplog.at_level("WARNING"):
            _validate_monthly_continuity(df)

        # Should warn about gap
        assert "Gap in ZORI series" in caplog.text
        assert "08001" in caplog.text

    def test_handles_year_boundary(self, caplog):
        """Test that year boundaries are handled correctly."""
        df = pd.DataFrame(
            {
                "geo_id": ["08001"] * 2,
                "date": pd.to_datetime(["2022-12-01", "2023-01-01"]),
            }
        )

        with caplog.at_level("WARNING"):
            _validate_monthly_continuity(df)

        # No gap - December to January is continuous
        assert "Gap in ZORI series" not in caplog.text

    def test_truncates_many_warnings(self, caplog):
        """Test that excessive warnings are truncated."""
        # Create data with many gaps
        dates = pd.date_range("2020-01", "2023-01", freq="2ME")  # Every other month
        df = pd.DataFrame(
            {
                "geo_id": ["08001"] * len(dates),
                "date": dates,
            }
        )

        with caplog.at_level("WARNING"):
            _validate_monthly_continuity(df, max_warnings=5)

        # Should have truncation message
        assert "truncated" in caplog.text.lower()


class TestGetOutputPath:
    """Tests for get_output_path function."""

    def test_default_path_with_max_year(self, tmp_path, monkeypatch):
        """Test output path with temporal Z-year notation."""
        monkeypatch.chdir(tmp_path)
        path = get_output_path("county", max_year=2026)
        assert path == tmp_path / "data" / "curated" / "zori" / "zori__county__Z2026.parquet"

    def test_legacy_path_without_max_year(self, tmp_path, monkeypatch):
        """Test legacy output path when max_year is omitted."""
        monkeypatch.chdir(tmp_path)
        path = get_output_path("county")
        assert path == tmp_path / "data" / "curated" / "zori" / "zori__county.parquet"

    def test_custom_base_dir(self):
        """Test output path with custom base directory."""
        path = get_output_path("county", output_dir="/tmp/test", max_year=2026)
        assert path == Path("/tmp/test/zori__county__Z2026.parquet")

    def test_zip_geography(self, tmp_path, monkeypatch):
        """Test output path for ZIP geography."""
        monkeypatch.chdir(tmp_path)
        path = get_output_path("zip", max_year=2025)
        assert path == tmp_path / "data" / "curated" / "zori" / "zori__zip__Z2025.parquet"


class TestDiscoverZoriIngest:
    """Tests for discover_zori_ingest function."""

    def test_discovers_temporal_file(self, tmp_path):
        """Test discovery of Z-year named file."""
        from hhplab.naming import discover_zori_ingest

        (tmp_path / "zori__county__Z2026.parquet").touch()
        result = discover_zori_ingest("county", tmp_path)
        assert result == tmp_path / "zori__county__Z2026.parquet"

    def test_prefers_highest_year(self, tmp_path):
        """Test that highest Z-year is preferred."""
        from hhplab.naming import discover_zori_ingest

        (tmp_path / "zori__county__Z2025.parquet").touch()
        (tmp_path / "zori__county__Z2026.parquet").touch()
        result = discover_zori_ingest("county", tmp_path)
        assert result == tmp_path / "zori__county__Z2026.parquet"

    def test_falls_back_to_legacy(self, tmp_path):
        """Test fallback to legacy name."""
        from hhplab.naming import discover_zori_ingest

        (tmp_path / "zori__county.parquet").touch()
        result = discover_zori_ingest("county", tmp_path)
        assert result == tmp_path / "zori__county.parquet"

    def test_returns_none_when_empty(self, tmp_path):
        """Test None return when no file exists."""
        from hhplab.naming import discover_zori_ingest

        result = discover_zori_ingest("county", tmp_path)
        assert result is None


class TestDownloadZori:
    """Tests for download_zori function."""

    def test_uses_cache_when_exists(self, tmp_path, httpx_mock):
        """Test that cached file is used when it exists."""
        # Create a cached file
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        today = date.today().isoformat()
        cached_path = raw_dir / f"zori__county__{today}.csv"
        cached_path.write_text(SAMPLE_COUNTY_CSV)

        # Call download - should use cache, not make HTTP request
        path, sha256 = download_zori("county", raw_dir_override=raw_dir, force=False)

        assert path == cached_path
        assert len(sha256) == 64  # SHA256 hex string

    def test_downloads_when_forced(self, tmp_path, httpx_mock):
        """Test that force=True re-downloads even with cache."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        # Create a cached file
        today = date.today().isoformat()
        cached_path = raw_dir / f"zori__county__{today}.csv"
        cached_path.write_text("old content")

        # Mock the HTTP response
        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        # Force download
        path, sha256 = download_zori("county", raw_dir_override=raw_dir, force=True)

        # Should have new content
        assert SAMPLE_COUNTY_CSV in path.read_text()

    def test_custom_url(self, tmp_path, httpx_mock):
        """Test downloading from custom URL."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        custom_url = "https://example.com/zori.csv"
        httpx_mock.add_response(
            url=custom_url,
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        path, sha256 = download_zori("county", url=custom_url, raw_dir_override=raw_dir, force=True)

        assert path.exists()

    def test_invalid_geography_raises(self, tmp_path):
        """Test that invalid geography raises ValueError."""
        with pytest.raises(ValueError, match="Unknown geography"):
            download_zori("invalid_geo", raw_dir_override=tmp_path)


class TestIngestZori:
    """Tests for ingest_zori function."""

    def test_end_to_end_ingest(self, tmp_path, httpx_mock):
        """Test complete ingest pipeline."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        assert output_path.exists()
        assert output_path.suffix == ".parquet"
        assert "Z2023" in output_path.name  # Sample data max year is 2023

        # Read and verify output
        df = pd.read_parquet(output_path)

        # Check schema
        required_cols = [
            "geo_type",
            "geo_id",
            "date",
            "zori",
            "region_name",
            "state",
            "data_source",
            "metric",
            "ingested_at",
            "source_ref",
            "raw_sha256",
        ]
        for col in required_cols:
            assert col in df.columns, f"Missing required column: {col}"

        # Check geo_type is correct
        assert (df["geo_type"] == "county").all()

        # Check data_source
        assert (df["data_source"] == "Zillow Economic Research").all()

        # Check metric
        assert (df["metric"] == "ZORI").all()

    def test_date_filtering(self, tmp_path, httpx_mock):
        """Test that date filters are applied correctly."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
            start="2023-02-01",
            end="2023-02-28",
        )

        df = pd.read_parquet(output_path)

        # Should only have February data
        dates = df["date"].dt.date.unique()
        assert len(dates) == 1
        assert dates[0].month == 2

    def test_includes_provenance_metadata(self, tmp_path, httpx_mock):
        """Test that output file includes provenance metadata."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        # Read provenance from file
        provenance = read_provenance(output_path)
        assert provenance is not None

        # Check provenance fields
        assert provenance.extra.get("dataset") == "zori"
        assert provenance.extra.get("geography") == "county"
        assert provenance.extra.get("metric") == "ZORI"
        assert provenance.extra.get("source") == "Zillow Economic Research"
        assert "attribution" in provenance.extra
        assert "raw_sha256" in provenance.extra
        assert "row_count" in provenance.extra
        assert "geo_count" in provenance.extra

    def test_uses_cache_when_exists(self, tmp_path, httpx_mock):
        """Test that cached output is used when it exists."""
        output_dir = tmp_path / "curated"
        output_dir.mkdir(parents=True)

        # Create a cached output file with temporal name
        cached_path = output_dir / "zori__county__Z2023.parquet"
        df = pd.DataFrame(
            {
                "geo_type": ["county"],
                "geo_id": ["08001"],
                "date": [pd.Timestamp("2023-01-01")],
                "zori": [1500.0],
                "region_name": ["Adams County"],
                "state": ["Colorado"],
                "data_source": ["cached"],
                "metric": ["ZORI"],
                "ingested_at": [datetime.now(UTC)],
                "source_ref": ["cached"],
                "raw_sha256": ["cached"],
            }
        )
        df.to_parquet(cached_path)

        # Call ingest without force - should use cache
        result_path = ingest_zori(
            geography="county",
            output_dir=output_dir,
            force=False,
        )

        assert result_path == cached_path

        # Verify it didn't make HTTP request (httpx_mock would fail if it did)

    def test_force_reprocesses_even_with_cache(self, tmp_path, httpx_mock):
        """Test that force=True reprocesses even with cache."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"
        output_dir.mkdir(parents=True)

        # Create a cached output file with temporal name
        cached_path = output_dir / "zori__county__Z2022.parquet"
        df = pd.DataFrame(
            {
                "geo_type": ["county"],
                "geo_id": ["08001"],
                "date": [pd.Timestamp("2023-01-01")],
                "zori": [1.0],  # Different value
                "region_name": ["Old"],
                "state": ["Old"],
                "data_source": ["cached"],
                "metric": ["ZORI"],
                "ingested_at": [datetime.now(UTC)],
                "source_ref": ["cached"],
                "raw_sha256": ["cached"],
            }
        )
        df.to_parquet(cached_path)

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        # Force reprocess
        result_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        # Verify new data was written with temporal name
        result_df = pd.read_parquet(result_path)
        assert result_df["zori"].max() > 1000  # Should have real values now
        assert "Z2023" in result_path.name

        # Old file should be cleaned up
        assert not cached_path.exists()

    def test_reingest_cleans_up_legacy_file(self, tmp_path, httpx_mock):
        """Test that legacy-named file is cleaned up after re-ingest."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"
        output_dir.mkdir(parents=True)

        # Create a legacy-named cached file
        legacy_path = output_dir / "zori__county.parquet"
        legacy_path.touch()

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        result_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        assert "Z2023" in result_path.name
        assert not legacy_path.exists()


class TestSchemaValidation:
    """Tests for output schema validation."""

    def test_geo_id_length_5_chars(self, tmp_path, httpx_mock):
        """Test that geo_id is exactly 5 characters."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        df = pd.read_parquet(output_path)
        assert all(len(geo_id) == 5 for geo_id in df["geo_id"])

    def test_zori_values_positive(self, tmp_path, httpx_mock):
        """Test that all ZORI values are positive."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        df = pd.read_parquet(output_path)
        assert (df["zori"] > 0).all()

    def test_ingested_at_is_utc(self, tmp_path, httpx_mock):
        """Test that ingested_at is a UTC timestamp."""
        raw_dir = tmp_path / "raw"
        output_dir = tmp_path / "curated"

        httpx_mock.add_response(
            url=ZORI_URLS["county"],
            content=SAMPLE_COUNTY_CSV.encode(),
        )

        output_path = ingest_zori(
            geography="county",
            raw_dir=raw_dir,
            output_dir=output_dir,
            force=True,
        )

        df = pd.read_parquet(output_path)

        # Check that timestamp is timezone-aware
        ts = df.iloc[0]["ingested_at"]
        assert ts.tzinfo is not None


class TestParseZoriZip:
    """Regression: parse_zori_zip must use _DATE_COL_RE, not _date_re (coclab-fs5t)."""

    def test_parse_zori_zip_basic(self, tmp_path):
        csv_path = tmp_path / "zori_zip.csv"
        csv_path.write_text(
            "RegionName,StateName,2015-01-31,2015-02-28\n"
            "10001,New York,1200.00,1210.00\n"
            "90210,California,2500.00,2520.00\n"
        )
        df = parse_zori_zip(csv_path)
        assert len(df) == 4
        assert set(df.columns) == {"geo_id", "date", "zori", "region_name", "state"}
        assert list(df["geo_id"].unique()) == ["10001", "90210"]

    def test_happy_path_geo_id_zero_padded(self, tmp_path):
        """geo_id is 5-digit zero-padded from RegionName."""
        csv_path = tmp_path / "zip.csv"
        csv_path.write_text(
            "RegionName,StateName,2024-01-31,2024-02-29\n"
            "501,Massachusetts,1800.00,1810.00\n"
            "10001,New York,2400.00,2420.00\n"
        )
        df = parse_zori_zip(csv_path)

        assert set(df["geo_id"].unique()) == {"00501", "10001"}
        assert all(len(g) == 5 for g in df["geo_id"])

    def test_happy_path_dates_and_values(self, tmp_path):
        """Date parsing and ZORI values are correct."""
        csv_path = tmp_path / "zip.csv"
        csv_path.write_text(
            "RegionName,StateName,2024-03-31,2024-04-30\n90210,California,2500.50,2510.75\n"
        )
        df = parse_zori_zip(csv_path)

        assert len(df) == 2
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

        row_mar = df[df["date"].dt.month == 3].iloc[0]
        assert row_mar["zori"] == pytest.approx(2500.50)
        assert row_mar["date"].day == 31

        row_apr = df[df["date"].dt.month == 4].iloc[0]
        assert row_apr["zori"] == pytest.approx(2510.75)

    def test_invalid_zip_length_filtered(self, tmp_path):
        """ZIP codes that don't resolve to 5 digits after zfill are dropped."""
        csv_path = tmp_path / "zip.csv"
        csv_path.write_text(
            "RegionName,StateName,2024-01-31\n10001,New York,1500.00\n123456,Nowhere,9999.00\n"
        )
        df = parse_zori_zip(csv_path)

        # 123456 zero-filled is "123456" (6 chars) -> filtered out
        assert len(df) == 1
        assert df.iloc[0]["geo_id"] == "10001"

    def test_all_null_zori_returns_empty(self, tmp_path):
        """When every ZORI value is null, result is an empty DataFrame."""
        csv_path = tmp_path / "zip.csv"
        csv_path.write_text(
            "RegionName,StateName,2024-01-31,2024-02-29\n10001,New York,,\n90210,California,,\n"
        )
        df = parse_zori_zip(csv_path)

        assert len(df) == 0
        assert set(df.columns) == {"geo_id", "date", "zori", "region_name", "state"}
