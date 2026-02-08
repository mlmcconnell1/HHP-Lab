"""Tests for ACS population CLI commands in the test plan."""

from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestIngestAcsPopulationCommand:
    """Tests for ingest-acs-population CLI command."""

    @patch("coclab.cli.ingest_acs_population.get_output_path")
    @patch("coclab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs_population_uses_cache(
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
            app, ["ingest-acs-population", "--acs", "2019-2023", "--tracts", "2023"]
        )

        assert result.exit_code == 0
        assert "Cached file found" in result.output
        mock_ingest.assert_not_called()

    @patch("coclab.cli.ingest_acs_population.get_output_path")
    @patch("coclab.acs.ingest.tract_population.ingest_tract_data")
    @patch("pandas.read_parquet")
    def test_ingest_acs_population_success(
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
                "ingest-acs-population",
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


class TestNestedIngestAcsCommand:
    """Tests for ingest acs nested CLI command."""

    def test_ingest_acs_help(self):
        """Nested ingest acs help should show options."""
        result = runner.invoke(app, ["ingest", "acs", "--help"])

        assert result.exit_code == 0
        assert "--acs" in result.output
        assert "--tracts" in result.output
        assert "--translate" in result.output

    def test_ingest_acs_population_still_works(self):
        """Deprecated 'ingest acs-population' should still resolve."""
        result = runner.invoke(app, ["ingest", "acs-population", "--help"])

        assert result.exit_code == 0
        assert "--acs" in result.output
