"""Tests for the Phase 3 CLI commands (ingest pit, diagnostics panel)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app

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

    @patch("hhplab.pit.ingest.download_pit_data")
    @patch("hhplab.pit.ingest.parse_pit_file")
    @patch("hhplab.pit.ingest.write_pit_parquet")
    @patch("hhplab.pit.pit_registry.register_pit_year")
    @patch("hhplab.pit.qa.validate_pit_data")
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

        from hhplab.pit.ingest import DownloadResult
        from hhplab.pit.pit_registry import PitRegistryEntry
        from hhplab.pit.qa import QAReport

        # Mock download result
        mock_download.return_value = DownloadResult(
            path=Path("data/raw/pit/2024/2007-2024-PIT-Counts-by-CoC.xlsx"),
            source_url="https://example.com/pit.xlsx",
            downloaded_at=datetime.now(UTC),
            file_size=12345,
        )

        # Mock parsed result (PITParseResult with df attribute)
        from hhplab.pit.ingest import PITParseResult

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

    @patch("hhplab.pit.ingest.download_pit_data")
    def test_ingest_pit_download_failure(self, mock_download):
        """Should handle download failure gracefully."""
        mock_download.side_effect = Exception("Network error")

        result = runner.invoke(app, ["ingest", "pit", "--year", "2024"])

        assert result.exit_code == 1
        assert "Error downloading PIT data" in result.output

    @patch("hhplab.pit.ingest.download_pit_data")
    @patch("hhplab.pit.ingest.parse_pit_file")
    def test_ingest_pit_parse_failure(self, mock_parse, mock_download):
        """Should handle parse failure gracefully."""
        from datetime import UTC, datetime

        from hhplab.pit.ingest import DownloadResult

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


class TestPanelDiagnosticsCommand:
    """Tests for the 'diagnostics panel' command."""

    def test_panel_diagnostics_help(self):
        """Help should show options."""
        result = runner.invoke(app, ["diagnostics", "panel", "--help"])

        assert result.exit_code == 0
        assert "--panel" in result.output
        assert "--output-dir" in result.output
        assert "--format" in result.output

    def test_panel_diagnostics_requires_panel(self):
        """Should fail without --panel option."""
        result = runner.invoke(app, ["diagnostics", "panel"])

        # Typer shows error for missing required option
        assert result.exit_code != 0

    def test_panel_diagnostics_file_not_found(self, tmp_path):
        """Should fail if panel file does not exist."""
        result = runner.invoke(
            app,
            ["diagnostics", "panel", "--panel", str(tmp_path / "nonexistent.parquet")],
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
            ["diagnostics", "panel", "--panel", str(panel_file), "--format", "invalid"],
        )

        assert result.exit_code == 1
        assert "Invalid format" in result.output

    @patch("pandas.read_parquet")
    @patch("hhplab.panel.generate_diagnostics_report")
    def test_panel_diagnostics_success_text(
        self,
        mock_generate,
        mock_read,
        tmp_path,
    ):
        """Diagnostics in text format should succeed."""
        from hhplab.panel.panel_diagnostics import DiagnosticsReport

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
            ["diagnostics", "panel", "--panel", str(panel_file), "--format", "text"],
        )

        assert result.exit_code == 0
        assert "Loading panel" in result.output
        assert "PANEL DIAGNOSTICS REPORT" in result.output

    @patch("pandas.read_parquet")
    @patch("hhplab.panel.generate_diagnostics_report")
    def test_panel_diagnostics_success_csv(
        self,
        mock_generate,
        mock_read,
        tmp_path,
    ):
        """Diagnostics in CSV format should succeed."""
        from hhplab.panel.panel_diagnostics import DiagnosticsReport

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
                "diagnostics",
                "panel",
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
        assert "build" in result.output
        assert "diagnostics" in result.output

    def test_ingest_pit_help_shows_examples(self):
        """Ingest pit help should show examples."""
        result = runner.invoke(app, ["ingest", "pit", "--help"])

        assert result.exit_code == 0
        assert "hhplab ingest pit --year 2024" in result.output

    def test_panel_diagnostics_help_shows_examples(self):
        """Panel-diagnostics help should show examples."""
        result = runner.invoke(app, ["diagnostics", "panel", "--help"])

        assert result.exit_code == 0
        assert "hhplab diagnostics panel --panel" in result.output
