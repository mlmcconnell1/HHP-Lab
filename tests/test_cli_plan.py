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

    def test_ingest_census_invalid_type_nested(self):
        """Nested ingest census invalid type should fail with an error."""
        result = runner.invoke(app, ["ingest", "census", "--type", "invalid"])

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
            tracts_path = Path("data/curated/census/tracts__T2023.parquet")
            counties_path = Path("data/curated/census/counties__C2023.parquet")
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
    """Tests for the generate xwalks CLI command."""

    def test_build_xwalks_requires_build_flag(self):
        """Should fail without --build."""
        result = runner.invoke(app, ["generate", "xwalks"])
        # Typer shows error for missing required --build option
        assert result.exit_code != 0

    @patch("coclab.cli.build_xwalks.list_boundaries")
    @patch("coclab.cli.build_xwalks.gpd.read_parquet")
    @patch("coclab.cli.build_xwalks.build_coc_tract_crosswalk")
    @patch("coclab.cli.build_xwalks.save_crosswalk")
    @patch("coclab.cli.build_xwalks.compute_crosswalk_diagnostics")
    @patch("coclab.cli.build_xwalks.summarize_diagnostics")
    def test_build_xwalks_success_skips_missing_counties_silently(
        self,
        mock_summarize,
        mock_diagnostics,
        mock_save_crosswalk,
        mock_build_crosswalk,
        mock_read_parquet,
        mock_list_boundaries,
        tmp_path,
    ):
        """Generate xwalks should succeed and silently skip counties when missing and not explicitly requested."""
        from datetime import UTC, datetime

        from coclab.registry.schema import RegistryEntry

        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create build directory
            build_dir = Path("builds/test")
            (build_dir / "data" / "curated" / "xwalks").mkdir(parents=True, exist_ok=True)
            (build_dir / "data" / "raw").mkdir(parents=True, exist_ok=True)
            (build_dir / "base").mkdir(parents=True, exist_ok=True)
            manifest = {"schema_version": 1, "build": {"name": "test", "years": [2025]}, "base_assets": [], "aggregate_runs": []}
            (build_dir / "manifest.json").write_text(json.dumps(manifest))

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
                "builds/test/data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet"
            )
            mock_diagnostics.return_value = pd.DataFrame(
                {"coc_id": ["CO-500"], "coverage_ratio_area": [1.0]}
            )
            mock_summarize.return_value = "DIAGNOSTICS SUMMARY"

            # Without --counties, missing county file should be silently skipped (no warning)
            result = runner.invoke(
                app, ["generate", "xwalks", "--build", "test", "--boundary", "2025", "--tracts", "2023"]
            )

        assert result.exit_code == 0
        assert "Saved tract crosswalk" in result.output
        # Should NOT warn when --counties is not explicitly specified
        assert "Skipping county crosswalk" not in result.output
        assert "Warning: County file not found" not in result.output
        mock_build_crosswalk.assert_called_once()
        mock_save_crosswalk.assert_called_once()

    @patch("coclab.cli.build_xwalks.list_boundaries")
    @patch("coclab.cli.build_xwalks.gpd.read_parquet")
    @patch("coclab.cli.build_xwalks.build_coc_tract_crosswalk")
    @patch("coclab.cli.build_xwalks.save_crosswalk")
    @patch("coclab.cli.build_xwalks.compute_crosswalk_diagnostics")
    @patch("coclab.cli.build_xwalks.summarize_diagnostics")
    def test_build_xwalks_warns_when_counties_explicitly_requested_and_missing(
        self,
        mock_summarize,
        mock_diagnostics,
        mock_save_crosswalk,
        mock_build_crosswalk,
        mock_read_parquet,
        mock_list_boundaries,
        tmp_path,
    ):
        """Generate xwalks should warn when --counties is explicitly specified but file is missing."""
        from datetime import UTC, datetime

        from coclab.registry.schema import RegistryEntry

        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create build directory
            build_dir = Path("builds/test")
            (build_dir / "data" / "curated" / "xwalks").mkdir(parents=True, exist_ok=True)
            (build_dir / "data" / "raw").mkdir(parents=True, exist_ok=True)
            (build_dir / "base").mkdir(parents=True, exist_ok=True)
            manifest = {"schema_version": 1, "build": {"name": "test", "years": [2025]}, "base_assets": [], "aggregate_runs": []}
            (build_dir / "manifest.json").write_text(json.dumps(manifest))

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
                "builds/test/data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet"
            )
            mock_diagnostics.return_value = pd.DataFrame(
                {"coc_id": ["CO-500"], "coverage_ratio_area": [1.0]}
            )
            mock_summarize.return_value = "DIAGNOSTICS SUMMARY"

            # WITH --counties, missing county file SHOULD produce a warning
            result = runner.invoke(
                app,
                [
                    "generate",
                    "xwalks",
                    "--build",
                    "test",
                    "--boundary",
                    "2025",
                    "--tracts",
                    "2023",
                    "--counties",
                    "2023",
                ],
            )

        assert result.exit_code == 0
        assert "Saved tract crosswalk" in result.output
        # Should warn when --counties is explicitly specified
        assert "Warning: County file not found" in result.output
        mock_build_crosswalk.assert_called_once()
        mock_save_crosswalk.assert_called_once()


class TestDiagnosticsCommand:
    """Tests for the diagnostics xwalk CLI command."""

    def test_diagnostics_missing_crosswalk(self, tmp_path):
        """Should fail if crosswalk file is missing."""
        crosswalk_path = tmp_path / "missing.parquet"

        result = runner.invoke(app, ["diagnostics", "xwalk", "--crosswalk", str(crosswalk_path)])

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
                "diagnostics",
                "xwalk",
                "--crosswalk",
                str(crosswalk_path),
                "--show-problems",
            ],
        )

        assert result.exit_code == 0
        assert "PROBLEM CoCs" in result.output
        assert "CO-500" in result.output


class TestBuildMeasuresRemoved:
    """Tests that build measures command has been removed."""

    def test_build_measures_not_registered(self):
        """build measures should not be a registered subcommand."""
        result = runner.invoke(app, ["build", "--help"])
        assert "measures" not in result.output
