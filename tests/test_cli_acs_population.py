"""Tests for ACS population CLI commands in the test plan."""

from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestIngestAcsPopulationCommand:
    """Tests for ingest-acs-population CLI command."""

    @patch("coclab.cli.ingest_acs_population.get_output_path")
    @patch("coclab.acs.ingest.tract_population.ingest_tract_population")
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
    @patch("coclab.acs.ingest.tract_population.ingest_tract_population")
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
            {"tract_geoid": ["01001020100"], "total_population": [100]}
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


class TestRollupAcsPopulationCommand:
    """Tests for rollup-acs-population CLI command."""

    def test_rollup_acs_population_invalid_weighting(self):
        """Invalid weighting should fail."""
        result = runner.invoke(
            app,
            [
                "rollup-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "invalid",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid weighting method" in result.output

    @patch("coclab.acs.rollup.get_output_path")
    @patch("pandas.read_parquet")
    @patch("coclab.acs.rollup.build_coc_population_rollup")
    def test_rollup_acs_population_uses_cache(
        self,
        mock_build,
        mock_read_parquet,
        mock_get_output_path,
        tmp_path,
    ):
        """Cached file should skip rebuild when --force is not used."""
        cached_path = tmp_path / "rollup.parquet"
        cached_path.touch()
        mock_get_output_path.return_value = cached_path
        mock_read_parquet.return_value = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "coc_population": [100],
                "coverage_ratio": [1.0],
                "tract_count": [1],
            }
        )

        result = runner.invoke(
            app,
            [
                "rollup-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
            ],
        )

        assert result.exit_code == 0
        assert "Cached file found" in result.output
        mock_build.assert_not_called()

    @patch("coclab.acs.rollup.build_coc_population_rollup")
    @patch("pandas.read_parquet")
    def test_rollup_acs_population_success(
        self,
        mock_read_parquet,
        mock_build,
        tmp_path,
    ):
        """Rollup should print a summary on success."""
        output_path = tmp_path / "rollup.parquet"
        mock_build.return_value = output_path
        mock_read_parquet.return_value = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "coc_population": [100],
                "coverage_ratio": [1.0],
                "tract_count": [1],
            }
        )

        result = runner.invoke(
            app,
            [
                "rollup-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "area",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert "ROLLUP SUMMARY" in result.output
        mock_build.assert_called_once()


class TestCrosscheckAcsPopulationCommand:
    """Tests for crosscheck-acs-population CLI command."""

    def test_crosscheck_invalid_weighting(self):
        """Invalid weighting should fail."""
        result = runner.invoke(
            app,
            [
                "crosscheck-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "invalid",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid weighting method" in result.output

    @patch("coclab.acs.crosscheck.run_crosscheck")
    @patch("coclab.acs.crosscheck.print_crosscheck_report")
    def test_crosscheck_exit_code_from_report(
        self,
        mock_print,
        mock_run,
    ):
        """Exit code should follow print_crosscheck_report result."""
        mock_run.return_value = object()
        mock_print.return_value = 2

        result = runner.invoke(
            app,
            [
                "crosscheck-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "area",
            ],
        )

        assert result.exit_code == 2
        mock_run.assert_called_once()
        mock_print.assert_called_once()


class TestVerifyAcsPopulationCommand:
    """Tests for verify-acs-population CLI command."""

    def test_verify_invalid_weighting(self):
        """Invalid weighting should fail."""
        result = runner.invoke(
            app,
            [
                "verify-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "invalid",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid weighting method" in result.output

    @patch("coclab.acs.ingest.tract_population.get_output_path")
    @patch("coclab.acs.rollup.get_output_path")
    @patch("pandas.read_parquet")
    @patch("coclab.acs.crosscheck.run_crosscheck")
    @patch("coclab.acs.crosscheck.print_crosscheck_report")
    def test_verify_uses_cached_files(
        self,
        mock_print,
        mock_run,
        mock_read_parquet,
        mock_get_rollup_path,
        mock_get_tract_path,
        tmp_path,
    ):
        """Cached inputs should be used when --force is not set."""
        tract_path = tmp_path / "tract_population.parquet"
        rollup_path = tmp_path / "rollup.parquet"
        tract_path.touch()
        rollup_path.touch()

        mock_get_tract_path.return_value = tract_path
        mock_get_rollup_path.return_value = rollup_path
        mock_read_parquet.return_value = pd.DataFrame(
            {"coc_id": ["CO-500"], "coc_population": [100]}
        )
        mock_run.return_value = object()
        mock_print.return_value = 0

        result = runner.invoke(
            app,
            [
                "verify-acs-population",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
                "--tracts",
                "2023",
                "--weighting",
                "area",
            ],
        )

        assert result.exit_code == 0
        assert "STEP 1" in result.output
        assert "STEP 2" in result.output
        assert "STEP 3" in result.output
        mock_print.assert_called_once()
