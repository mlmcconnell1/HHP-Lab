"""Tests for the Phase 3 CLI commands (ingest pit, build-panel, diagnostics-panel)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestIngestPitCommand:
    """Tests for the 'ingest pit' command."""

    def test_ingest_pit_help(self):
        """Help should show options."""
        result = runner.invoke(app, ["ingest", "pit", "--help"])

        assert result.exit_code == 0
        assert "--year" in result.output
        assert "--force" in result.output
        assert "--parse-only" in result.output

    def test_ingest_pit_help_nested(self):
        """Nested help should show options."""
        result = runner.invoke(app, ["ingest", "pit", "--help"])

        assert result.exit_code == 0
        assert "--year" in result.output
        assert "--force" in result.output
        assert "--parse-only" in result.output

    def test_ingest_pit_requires_year(self):
        """Should fail without --year option."""
        result = runner.invoke(app, ["ingest", "pit"])

        # Typer shows error for missing required option
        assert result.exit_code != 0

    @patch("coclab.pit.ingest.download_pit_data")
    @patch("coclab.pit.ingest.parse_pit_file")
    @patch("coclab.pit.ingest.write_pit_parquet")
    @patch("coclab.pit.registry.register_pit_year")
    @patch("coclab.pit.qa.validate_pit_data")
    def test_ingest_pit_success(
        self,
        mock_validate,
        mock_register,
        mock_write,
        mock_parse,
        mock_download,
    ):
        """Full ingestion workflow should succeed."""
        from datetime import UTC, datetime

        from coclab.pit.ingest import DownloadResult
        from coclab.pit.qa import QAReport
        from coclab.pit.registry import PitRegistryEntry

        # Mock download result
        mock_download.return_value = DownloadResult(
            path=Path("data/raw/pit/2024/2007-2024-PIT-Counts-by-CoC.xlsx"),
            source_url="https://example.com/pit.xlsx",
            downloaded_at=datetime.now(UTC),
            file_size=12345,
        )

        # Mock parsed result (PITParseResult with df attribute)
        from coclab.pit.ingest import PITParseResult

        mock_df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "pit_year": [2024, 2024],
                "pit_total": [100, 200],
            }
        )
        mock_parse.return_value = PITParseResult(
            df=mock_df,
            cross_state_mappings={},
            rows_read=2,
            rows_skipped=0,
        )

        # Mock write output
        mock_write.return_value = Path("data/curated/pit/pit_counts__2024.parquet")

        # Mock registry entry
        mock_register.return_value = PitRegistryEntry(
            pit_year=2024,
            source="hud_exchange",
            ingested_at=datetime.now(UTC),
            path=Path("data/curated/pit/pit_counts__2024.parquet"),
            row_count=2,
            hash_of_file="abc123",
        )

        # Mock QA report
        mock_validate.return_value = QAReport()

        result = runner.invoke(app, ["ingest", "pit", "--year", "2024"])

        assert result.exit_code == 0
        assert "Ingesting PIT data for year 2024" in result.output
        assert "Parsed 2 CoC records" in result.output
        assert "PIT ingestion complete" in result.output

    @patch("coclab.pit.ingest.download_pit_data")
    def test_ingest_pit_download_failure(self, mock_download):
        """Should handle download failure gracefully."""
        mock_download.side_effect = Exception("Network error")

        result = runner.invoke(app, ["ingest", "pit", "--year", "2024"])

        assert result.exit_code == 1
        assert "Error downloading PIT data" in result.output

    @patch("coclab.pit.ingest.download_pit_data")
    @patch("coclab.pit.ingest.parse_pit_file")
    def test_ingest_pit_parse_failure(self, mock_parse, mock_download):
        """Should handle parse failure gracefully."""
        from datetime import UTC, datetime

        from coclab.pit.ingest import DownloadResult

        mock_download.return_value = DownloadResult(
            path=Path("data/raw/pit/2024/pit.xlsx"),
            source_url="https://example.com",
            downloaded_at=datetime.now(UTC),
            file_size=100,
        )
        mock_parse.side_effect = ValueError("Cannot find CoC ID column")

        result = runner.invoke(app, ["ingest", "pit", "--year", "2024"])

        assert result.exit_code == 1
        assert "Error parsing PIT file" in result.output


class TestBuildPanelCommand:
    """Tests for the 'build-panel' command."""

    def test_build_panel_help(self):
        """Help should show options."""
        result = runner.invoke(app, ["build-panel", "--help"])

        assert result.exit_code == 0
        assert "--start" in result.output
        assert "--end" in result.output
        assert "--weighting" in result.output
        assert "--output" in result.output

    def test_build_panel_requires_years(self):
        """Should fail without year options."""
        result = runner.invoke(app, ["build-panel"])

        # Typer shows error for missing required options
        assert result.exit_code != 0

    def test_build_panel_invalid_weighting(self):
        """Should fail with invalid weighting method."""
        result = runner.invoke(
            app,
            ["build-panel", "--start", "2020", "--end", "2024", "--weighting", "invalid"],
        )

        assert result.exit_code == 1
        assert "Invalid weighting method" in result.output

    def test_build_panel_invalid_year_range(self):
        """Should fail if start > end."""
        result = runner.invoke(app, ["build-panel", "--start", "2024", "--end", "2020"])

        assert result.exit_code == 1
        assert "Start year" in result.output

    @patch("coclab.panel.build_panel")
    @patch("coclab.panel.save_panel")
    def test_build_panel_success(self, mock_save, mock_build):
        """Panel build should succeed."""
        # Mock panel DataFrame
        mock_df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500", "CA-600", "CA-600"],
                "year": [2020, 2021, 2020, 2021],
                "pit_total": [100, 110, 200, 210],
                "coverage_ratio": [0.95, 0.92, 0.88, 0.90],
                "boundary_changed": [False, True, False, False],
            }
        )
        mock_build.return_value = mock_df
        mock_save.return_value = Path("data/curated/panel/coc_panel__2020_2021.parquet")

        result = runner.invoke(app, ["build-panel", "--start", "2020", "--end", "2021"])

        assert result.exit_code == 0
        assert "Building panel for 2020-2021" in result.output
        assert "Panel Summary" in result.output
        assert "Saved panel to" in result.output

    @patch("coclab.panel.build_panel")
    def test_build_panel_empty(self, mock_build):
        """Should warn if panel is empty."""
        mock_build.return_value = pd.DataFrame()

        result = runner.invoke(app, ["build-panel", "--start", "2020", "--end", "2021"])

        assert result.exit_code == 1
        assert "Panel is empty" in result.output

    @patch("coclab.panel.build_panel")
    @patch("coclab.provenance.write_parquet_with_provenance")
    def test_build_panel_custom_output(self, mock_write, mock_build, tmp_path):
        """Panel build with custom output path."""
        mock_df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2020],
                "pit_total": [100],
                "coverage_ratio": [0.95],
                "boundary_changed": [False],
            }
        )
        mock_build.return_value = mock_df

        output_file = tmp_path / "custom_panel.parquet"

        result = runner.invoke(
            app,
            [
                "build-panel",
                "--start",
                "2020",
                "--end",
                "2020",
                "--output",
                str(output_file),
            ],
        )

        assert result.exit_code == 0
        mock_write.assert_called_once()


class TestPanelDiagnosticsCommand:
    """Tests for the 'diagnostics-panel' command."""

    def test_panel_diagnostics_help(self):
        """Help should show options."""
        result = runner.invoke(app, ["diagnostics-panel", "--help"])

        assert result.exit_code == 0
        assert "--panel" in result.output
        assert "--output-dir" in result.output
        assert "--format" in result.output

    def test_panel_diagnostics_requires_panel(self):
        """Should fail without --panel option."""
        result = runner.invoke(app, ["diagnostics-panel"])

        # Typer shows error for missing required option
        assert result.exit_code != 0

    def test_panel_diagnostics_file_not_found(self, tmp_path):
        """Should fail if panel file does not exist."""
        result = runner.invoke(
            app,
            ["diagnostics-panel", "--panel", str(tmp_path / "nonexistent.parquet")],
        )

        assert result.exit_code == 1
        assert "Panel file not found" in result.output

    def test_panel_diagnostics_invalid_format(self, tmp_path):
        """Should fail with invalid format."""
        # Create a dummy file
        panel_file = tmp_path / "panel.parquet"
        panel_file.touch()

        result = runner.invoke(
            app,
            ["diagnostics-panel", "--panel", str(panel_file), "--format", "invalid"],
        )

        assert result.exit_code == 1
        assert "Invalid format" in result.output

    @patch("pandas.read_parquet")
    @patch("coclab.panel.generate_diagnostics_report")
    def test_panel_diagnostics_success_text(
        self,
        mock_generate,
        mock_read,
        tmp_path,
    ):
        """Diagnostics in text format should succeed."""
        from coclab.panel.diagnostics import DiagnosticsReport

        # Create a dummy panel file
        panel_file = tmp_path / "panel.parquet"
        panel_file.touch()

        # Mock loaded DataFrame
        mock_df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2020, 2020],
                "pit_total": [100, 200],
            }
        )
        mock_read.return_value = mock_df

        # Mock diagnostics report
        mock_report = DiagnosticsReport(
            coverage=pd.DataFrame(
                {
                    "year": [2020],
                    "mean": [0.95],
                    "low_coverage_count": [0],
                }
            ),
            boundary_changes=pd.DataFrame(),
            missingness=pd.DataFrame(),
            panel_info={"row_count": 2, "coc_count": 2, "year_count": 1},
        )
        mock_generate.return_value = mock_report

        result = runner.invoke(
            app,
            ["diagnostics-panel", "--panel", str(panel_file), "--format", "text"],
        )

        assert result.exit_code == 0
        assert "Loading panel" in result.output
        assert "PANEL DIAGNOSTICS REPORT" in result.output

    @patch("pandas.read_parquet")
    @patch("coclab.panel.generate_diagnostics_report")
    def test_panel_diagnostics_success_csv(
        self,
        mock_generate,
        mock_read,
        tmp_path,
    ):
        """Diagnostics in CSV format should succeed."""
        from coclab.panel.diagnostics import DiagnosticsReport

        # Create a dummy panel file
        panel_file = tmp_path / "panel.parquet"
        panel_file.touch()

        # Mock loaded DataFrame
        mock_df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2020],
                "pit_total": [100],
            }
        )
        mock_read.return_value = mock_df

        # Mock diagnostics report with to_csv returning paths
        mock_report = MagicMock(spec=DiagnosticsReport)
        mock_report.to_csv.return_value = {
            "coverage": tmp_path / "coverage_summary.csv",
        }
        mock_report.summary.return_value = "Summary text"
        mock_report.panel_info = {"row_count": 1}
        mock_generate.return_value = mock_report

        output_dir = tmp_path / "diagnostics"

        result = runner.invoke(
            app,
            [
                "diagnostics-panel",
                "--panel",
                str(panel_file),
                "--format",
                "csv",
                "--output-dir",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0
        assert "Exporting diagnostics" in result.output
        mock_report.to_csv.assert_called_once()


class TestPhase3HelpOutput:
    """Tests for Phase 3 CLI help output."""

    def test_main_help_includes_phase3_commands(self):
        """Main help should show Phase 3 commands."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "ingest" in result.output
        assert "build-panel" in result.output
        assert "diagnostics-panel" in result.output

    def test_ingest_pit_help_shows_examples(self):
        """Ingest pit help should show examples."""
        result = runner.invoke(app, ["ingest", "pit", "--help"])

        assert result.exit_code == 0
        assert "coclab ingest pit --year 2024" in result.output

    def test_build_panel_help_shows_examples(self):
        """Build-panel help should show examples."""
        result = runner.invoke(app, ["build-panel", "--help"])

        assert result.exit_code == 0
        assert "coclab build-panel --start" in result.output
        assert "--weighting population" in result.output

    def test_panel_diagnostics_help_shows_examples(self):
        """Panel-diagnostics help should show examples."""
        result = runner.invoke(app, ["diagnostics-panel", "--help"])

        assert result.exit_code == 0
        assert "coclab diagnostics-panel --panel" in result.output
