"""Tests for PIT data ingestion from HUD User."""

import tempfile
from pathlib import Path

import pytest

from hhplab.pit.ingest.hud_exchange import (
    DownloadResult,
    download_pit_data,
    get_pit_source_url,
    list_available_years,
)


class TestGetPitSourceUrl:
    """Tests for get_pit_source_url function."""

    def test_known_year_2024(self):
        url = get_pit_source_url(2024)
        assert "2024" in url
        assert "huduser.gov" in url
        assert url.endswith(".xlsb")

    def test_known_year_2023(self):
        url = get_pit_source_url(2023)
        assert "2023" in url
        assert "huduser.gov" in url
        assert url.endswith(".xlsb")

    def test_unknown_year_constructs_url(self):
        # Future year - should construct a URL using new HUD User pattern
        url = get_pit_source_url(2030)
        assert "2030" in url
        assert "huduser.gov" in url
        assert url.endswith(".xlsb")

    def test_invalid_year_too_old(self):
        with pytest.raises(ValueError, match="not directly available"):
            get_pit_source_url(2012)  # Files only available for 2013+

    def test_invalid_year_too_new(self):
        with pytest.raises(ValueError, match="outside valid PIT data range"):
            get_pit_source_url(2031)


class TestListAvailableYears:
    """Tests for list_available_years function."""

    def test_returns_list(self):
        years = list_available_years()
        assert isinstance(years, list)

    def test_sorted_descending(self):
        years = list_available_years()
        assert years == sorted(years, reverse=True)

    def test_contains_recent_years(self):
        years = list_available_years()
        assert 2024 in years
        assert 2023 in years

    def test_all_integers(self):
        years = list_available_years()
        assert all(isinstance(y, int) for y in years)


class TestDownloadPitData:
    """Tests for download_pit_data function."""

    def test_download_creates_directory(self, httpx_mock):
        """Test that download creates the output directory."""
        # Mock the HTTP response with minimal Excel-like content
        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2023-PIT-Counts-by-CoC.xlsb",
            content=b"PK\x03\x04",  # ZIP header (Excel files are ZIP archives)
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "2023"
            result = download_pit_data(2023, output_dir=output_dir)

            assert output_dir.exists()
            assert result.path.exists()
            assert isinstance(result, DownloadResult)
            assert result.source_url.endswith(".xlsb")

    def test_download_writes_metadata(self, httpx_mock):
        """Test that download writes metadata file."""
        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb",
            content=b"PK\x03\x04test content",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "2024"
            result = download_pit_data(2024, output_dir=output_dir)

            # Check metadata file exists
            meta_path = output_dir / f"{result.path.name}.meta.json"
            assert meta_path.exists()

            # Check metadata content
            import json

            with open(meta_path) as f:
                metadata = json.load(f)

            assert metadata["pit_year"] == 2024
            assert "source_url" in metadata
            assert "downloaded_at" in metadata

    def test_download_returns_result_with_metadata(self, httpx_mock):
        """Test that download returns DownloadResult with correct metadata."""
        content = b"test excel content"
        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2022-PIT-Counts-by-CoC.xlsx",
            content=content,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = download_pit_data(2022, output_dir=tmpdir)

            assert isinstance(result, DownloadResult)
            assert result.file_size == len(content)
            assert result.source_url.endswith("2022-PIT-Counts-by-CoC.xlsx")
            assert result.downloaded_at is not None

    def test_download_skips_existing_file(self, httpx_mock):
        """Test that download skips existing file when force=False."""
        # Create an existing file
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "2021"
            output_dir.mkdir(parents=True)
            existing_file = output_dir / "2007-2021-PIT-Counts-by-CoC.xlsx"
            existing_file.write_bytes(b"existing content")

            # Should not make HTTP request
            result = download_pit_data(2021, output_dir=output_dir, force=False)

            assert result.path == existing_file
            # httpx_mock will fail if any unexpected requests are made

    def test_download_force_redownloads(self, httpx_mock):
        """Test that download re-downloads when force=True."""
        new_content = b"new content"
        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2021-PIT-Counts-by-CoC.xlsx",
            content=new_content,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "2021"
            output_dir.mkdir(parents=True)
            existing_file = output_dir / "2007-2021-PIT-Counts-by-CoC.xlsx"
            existing_file.write_bytes(b"old content")

            result = download_pit_data(2021, output_dir=output_dir, force=True)

            assert result.file_size == len(new_content)
            assert existing_file.read_bytes() == new_content

    def test_download_handles_404_error(self, httpx_mock):
        """Test that download raises FileNotFoundError on 404."""
        # Mock all URL variations that will be tried
        base = "https://www.huduser.gov/portal/sites/default/files/xls/2007-2020-"
        patterns = ["PIT-Counts-by-CoC", "Point-in-Time-Estimates-by-CoC", "PIT-Estimates-by-CoC"]
        extensions = [".xlsx", ".xlsb"]
        for pattern in patterns:
            for ext in extensions:
                httpx_mock.add_response(url=f"{base}{pattern}{ext}", status_code=404)

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(FileNotFoundError, match="not found"):
                download_pit_data(2020, output_dir=tmpdir)

    def test_download_handles_http_error(self, httpx_mock):
        """Test that download raises on non-404 HTTP error."""
        import httpx

        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2020-PIT-Counts-by-CoC.xlsx",
            status_code=500,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(httpx.HTTPStatusError):
                download_pit_data(2020, output_dir=tmpdir)

    def test_download_rejects_empty_hud_response_without_writing_file(self, httpx_mock):
        """Empty HTTP 200 responses are treated as unpublished PIT files."""
        base = "https://www.huduser.gov/portal/sites/default/files/xls/2007-2025-"
        httpx_mock.add_response(
            url=f"{base}PIT-Counts-by-CoC.xlsb",
            content=b"",
            headers={"content-length": "0"},
        )
        patterns = [
            "PIT-Counts-by-CoC",
            "Point-in-Time-Estimates-by-CoC",
            "PIT-Estimates-by-CoC",
        ]
        for pattern in patterns:
            for ext in [".xlsx", ".xlsb"]:
                url = f"{base}{pattern}{ext}"
                if url.endswith("PIT-Counts-by-CoC.xlsb"):
                    continue
                httpx_mock.add_response(url=url, status_code=404)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with pytest.raises(FileNotFoundError, match="no usable workbook content"):
                download_pit_data(2025, output_dir=output_dir)

            assert not (output_dir / "2007-2025-PIT-Counts-by-CoC.xlsb").exists()
            assert list(output_dir.glob("*.meta.json")) == []

    def test_download_redownloads_existing_empty_file(self, httpx_mock):
        """A prior zero-byte workbook is not treated as a valid cached download."""
        new_content = b"new workbook content"
        httpx_mock.add_response(
            url="https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb",
            content=new_content,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            existing_file = output_dir / "2007-2024-PIT-Counts-by-CoC.xlsb"
            existing_file.write_bytes(b"")

            result = download_pit_data(2024, output_dir=output_dir, force=False)

            assert result.file_size == len(new_content)
            assert existing_file.read_bytes() == new_content
