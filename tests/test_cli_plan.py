"""Tests for CLI commands referenced in the CoC-Lab CLI test plan."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestCensusIngestCommand:
    """Tests for the ingest-census CLI command."""

    def test_ingest_census_invalid_type(self):
        """Invalid type should fail with an error."""
        result = runner.invoke(app, ["ingest-census", "--type", "invalid"])

        assert result.exit_code == 1
        assert "Invalid type" in result.output

    @patch("coclab.census.ingest.ingest_tiger_tracts")
    @patch("coclab.census.ingest.ingest_tiger_counties")
    def test_ingest_census_cached_skips_downloads(
        self,
        mock_counties,
        mock_tracts,
        tmp_path,
    ):
        """Cached files should skip downloads without --force."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            tracts_path = Path("data/curated/census/tracts__2023.parquet")
            counties_path = Path("data/curated/census/counties__2023.parquet")
            tracts_path.parent.mkdir(parents=True, exist_ok=True)
            tracts_path.touch()
            counties_path.touch()

            result = runner.invoke(app, ["ingest-census", "--year", "2023"])

        assert result.exit_code == 0
        assert "Tracts file already exists" in result.output
        assert "Counties file already exists" in result.output
        mock_tracts.assert_not_called()
        mock_counties.assert_not_called()


class TestBuildXwalksCommand:
    """Tests for the build-xwalks CLI command."""

    @patch("coclab.cli.build_xwalks.latest_vintage")
    @patch("coclab.cli.build_xwalks.list_boundaries")
    def test_build_xwalks_missing_boundary(
        self,
        mock_list_boundaries,
        mock_latest_vintage,
    ):
        """Should fail if no boundary vintages are registered."""
        mock_list_boundaries.return_value = []
        mock_latest_vintage.return_value = None

        result = runner.invoke(app, ["build-xwalks"])

        assert result.exit_code == 1
        assert "No boundary vintages found in registry" in result.output

    @patch("coclab.cli.build_xwalks.list_boundaries")
    @patch("coclab.cli.build_xwalks.gpd.read_parquet")
    @patch("coclab.cli.build_xwalks.build_coc_tract_crosswalk")
    @patch("coclab.cli.build_xwalks.save_crosswalk")
    @patch("coclab.cli.build_xwalks.compute_crosswalk_diagnostics")
    @patch("coclab.cli.build_xwalks.summarize_diagnostics")
    def test_build_xwalks_success_skips_missing_counties(
        self,
        mock_summarize,
        mock_diagnostics,
        mock_save_crosswalk,
        mock_build_crosswalk,
        mock_read_parquet,
        mock_list_boundaries,
        tmp_path,
    ):
        """Build-xwalks should succeed and skip counties when missing."""
        from datetime import UTC, datetime

        from coclab.registry.schema import RegistryEntry

        with runner.isolated_filesystem(temp_dir=tmp_path):
            boundary_path = Path("data/curated/coc_boundaries/coc_boundaries__2025.parquet")
            boundary_path.parent.mkdir(parents=True, exist_ok=True)
            boundary_path.touch()

            tracts_path = Path("data/curated/census/tracts__2023.parquet")
            tracts_path.parent.mkdir(parents=True, exist_ok=True)
            tracts_path.touch()

            mock_list_boundaries.return_value = [
                RegistryEntry(
                    boundary_vintage="2025",
                    source="hud_exchange",
                    ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
                    path=boundary_path,
                    feature_count=1,
                    hash_of_file="abc123",
                )
            ]

            mock_read_parquet.return_value = pd.DataFrame(
                {"coc_id": ["CO-500"], "geometry": ["geom"]}
            )
            mock_build_crosswalk.return_value = pd.DataFrame(
                {"coc_id": ["CO-500"], "intersection_area": [1.0]}
            )
            mock_save_crosswalk.return_value = Path(
                "data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet"
            )
            mock_diagnostics.return_value = pd.DataFrame(
                {"coc_id": ["CO-500"], "coverage_ratio_area": [1.0]}
            )
            mock_summarize.return_value = "DIAGNOSTICS SUMMARY"

            result = runner.invoke(
                app, ["build-xwalks", "--boundary", "2025", "--tracts", "2023"]
            )

        assert result.exit_code == 0
        assert "Saved tract crosswalk" in result.output
        assert "Skipping county crosswalk" in result.output
        mock_build_crosswalk.assert_called_once()
        mock_save_crosswalk.assert_called_once()


class TestDiagnosticsCommand:
    """Tests for the xwalk-diagnostics CLI command."""

    def test_diagnostics_missing_crosswalk(self, tmp_path):
        """Should fail if crosswalk file is missing."""
        crosswalk_path = tmp_path / "missing.parquet"

        result = runner.invoke(
            app, ["xwalk-diagnostics", "--crosswalk", str(crosswalk_path)]
        )

        assert result.exit_code == 1
        assert "Crosswalk file not found" in result.output

    @patch("coclab.cli.diagnostics.compute_crosswalk_diagnostics")
    @patch("coclab.cli.diagnostics.summarize_diagnostics")
    @patch("coclab.cli.diagnostics.identify_problem_cocs")
    @patch("pandas.read_parquet")
    def test_diagnostics_show_problems(
        self,
        mock_read_parquet,
        mock_identify,
        mock_summarize,
        mock_compute,
        tmp_path,
    ):
        """Should show problem CoCs when requested."""
        crosswalk_path = tmp_path / "crosswalk.parquet"
        crosswalk_path.touch()

        mock_read_parquet.return_value = pd.DataFrame(
            {"coc_id": ["CO-500"], "intersection_area": [1.0]}
        )
        mock_compute.return_value = pd.DataFrame(
            {"coc_id": ["CO-500"], "coverage_ratio_area": [0.5]}
        )
        mock_summarize.return_value = "SUMMARY"
        mock_identify.return_value = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "issues": ["low_area_coverage (0.500)"],
                "num_tracts": [1],
                "coverage_ratio_area": [0.5],
                "max_tract_contribution": [1.0],
            }
        )

        result = runner.invoke(
            app,
            [
                "xwalk-diagnostics",
                "--crosswalk",
                str(crosswalk_path),
                "--show-problems",
            ],
        )

        assert result.exit_code == 0
        assert "PROBLEM CoCs" in result.output
        assert "CO-500" in result.output


class TestBuildMeasuresCommand:
    """Tests for the build-measures CLI command."""

    def test_build_measures_invalid_weighting(self):
        """Invalid weighting should fail."""
        result = runner.invoke(
            app, ["build-measures", "--weighting", "invalid"]
        )

        assert result.exit_code == 1
        assert "Invalid weighting method" in result.output

    @patch("coclab.cli.build_measures.latest_vintage")
    def test_build_measures_missing_crosswalk(self, mock_latest, tmp_path):
        """Missing crosswalk file should fail."""
        mock_latest.return_value = "2025"
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir(parents=True, exist_ok=True)

        result = runner.invoke(
            app,
            [
                "build-measures",
                "--acs", "2019-2023",
                "--xwalk-dir", str(xwalk_dir),
            ],
        )

        assert result.exit_code == 1
        assert "Crosswalk file not found" in result.output

    @patch("coclab.cli.build_measures.build_coc_measures")
    @patch("coclab.cli.build_measures.latest_vintage")
    def test_build_measures_success(
        self,
        mock_latest,
        mock_build,
        tmp_path,
    ):
        """Build-measures should succeed with valid inputs."""
        mock_latest.return_value = "2025"
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir(parents=True, exist_ok=True)
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"
        xwalk_path.touch()

        mock_build.return_value = pd.DataFrame(
            {"coc_id": ["CO-500"], "total_population": [1000]}
        )

        result = runner.invoke(
            app,
            [
                "build-measures",
                "--acs", "2019-2023",
                "--xwalk-dir", str(xwalk_dir),
            ],
        )

        assert result.exit_code == 0
        assert "MEASURE SUMMARY" in result.output
        mock_build.assert_called_once()
