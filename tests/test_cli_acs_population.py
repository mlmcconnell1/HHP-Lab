"""Tests for ACS population CLI commands in the test plan."""

import json
from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from hhplab.acs.variables import acs5_registry_measure_names, acs5_registry_tables
from hhplab.cli.main import app

runner = CliRunner()


class TestIngestAcs5TractCommand:
    """Tests for ingest acs5-tract CLI command."""

    @patch("hhplab.cli.ingest.acs_population.get_output_path")
    @patch("hhplab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs5_tract_uses_cache(
        self,
        mock_read_parquet,
        mock_ingest,
        mock_get_output_path,
        tmp_path,
    ):
        """Cached file should skip ingest when --force is not used."""
        cached_path = tmp_path / "tract_population.parquet"
        cached_path.touch()
        mock_get_output_path.return_value = cached_path
        mock_read_parquet.return_value = pd.DataFrame(
            {"tract_geoid": ["01001020100"], "total_population": [100]}
        )

        result = runner.invoke(
            app, ["ingest", "acs5-tract", "--acs", "2019-2023", "--tracts", "2023"]
        )

        assert result.exit_code == 0
        assert "Cached file found" in result.output
        mock_ingest.assert_not_called()

    @patch("hhplab.cli.ingest.acs_population.get_output_path")
    @patch("hhplab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs5_tract_success(
        self,
        mock_read_parquet,
        mock_ingest,
        mock_get_output_path,
        tmp_path,
    ):
        """Ingest should summarize output on success."""
        output_path = tmp_path / "tract_population.parquet"
        mock_get_output_path.return_value = output_path
        mock_ingest.return_value = output_path
        mock_read_parquet.return_value = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [100],
                "adult_population": [80],
                "median_household_income": [50000.0],
                "median_gross_rent": [1200.0],
            }
        )

        result = runner.invoke(
            app,
            [
                "ingest",
                "acs5-tract",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert "INGEST SUMMARY" in result.output
        mock_ingest.assert_called_once()

    @patch("hhplab.cli.ingest.acs_population.get_output_path")
    @patch("hhplab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs5_tract_json_reports_registry_metadata(
        self,
        mock_read_parquet,
        mock_ingest,
        mock_get_output_path,
        tmp_path,
    ):
        """JSON output should expose registry-derived ACS5 support metadata."""
        output_path = tmp_path / "tract_population.parquet"
        mock_get_output_path.return_value = output_path
        mock_ingest.return_value = output_path
        mock_read_parquet.return_value = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [100],
                "adult_population": [80],
                "median_household_income": [50000.0],
                "median_gross_rent": [1200.0],
            }
        )

        result = runner.invoke(
            app,
            [
                "ingest",
                "acs5-tract",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--force",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["cached"] is False
        assert payload["supported_acs_tables"] == acs5_registry_tables()
        assert payload["supported_measures"] == acs5_registry_measure_names()
        assert "B05001" in payload["supported_acs_tables"]
        assert "B25056" in payload["supported_acs_tables"]
        assert "B25003" in payload["supported_acs_tables"]
        assert "nativity_citizenship" in payload["supported_measures"]

    @patch("hhplab.cli.ingest.acs_population.get_output_path")
    @patch("hhplab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs5_tract_cached_json_is_machine_readable(
        self,
        mock_read_parquet,
        mock_ingest,
        mock_get_output_path,
        tmp_path,
    ):
        """Cached JSON output should not emit human progress text."""
        cached_path = tmp_path / "tract_population.parquet"
        cached_path.touch()
        mock_get_output_path.return_value = cached_path
        mock_read_parquet.return_value = pd.DataFrame(
            {"tract_geoid": ["01001020100"], "total_population": [100]}
        )

        result = runner.invoke(
            app,
            ["ingest", "acs5-tract", "--acs", "2019-2023", "--tracts", "2023", "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["cached"] is True
        assert payload["supported_acs_tables"] == acs5_registry_tables()
        mock_ingest.assert_not_called()


class TestIngestAcs5TractHelp:
    """Tests for ingest acs5-tract help output."""

    def test_ingest_acs5_tract_help(self):
        """Primary ingest acs5-tract help should show options."""
        result = runner.invoke(app, ["ingest", "acs5-tract", "--help"])

        assert result.exit_code == 0
        assert "--acs" in result.output
        assert "--tracts" in result.output
        assert "B05001" in result.output
        assert "B25056" in result.output
        assert "B25003" in result.output
        assert "nativity_citizenship" in result.output
        assert "contract_rent_distribution" in result.output
