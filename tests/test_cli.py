"""Tests for the coclab CLI."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestIngestCommand:
    """Tests for the 'ingest-boundaries' command."""

    def test_ingest_unknown_source(self):
        """Ingest with unknown source should fail."""
        result = runner.invoke(app, ["ingest-boundaries", "--source", "unknown"])
        assert result.exit_code == 1
        assert "Unknown source" in result.output

    def test_ingest_hud_exchange_requires_vintage(self):
        """Ingest hud_exchange without vintage should fail."""
        result = runner.invoke(app, ["ingest-boundaries", "--source", "hud_exchange"])
        assert result.exit_code == 1
        assert "--vintage is required" in result.output

    @patch("coclab.ingest.hud_exchange_gis.ingest_hud_exchange")
    def test_ingest_hud_exchange_success(self, mock_ingest):
        """Ingest hud_exchange with vintage should call ingest_hud_exchange."""
        mock_ingest.return_value = Path("data/curated/coc_boundaries/coc_boundaries__2025.parquet")

        result = runner.invoke(
            app, ["ingest-boundaries", "--source", "hud_exchange", "--vintage", "2025", "--force"]
        )

        assert result.exit_code == 0
        assert "Successfully ingested" in result.output
        mock_ingest.assert_called_once_with("2025", show_progress=True)

    @patch("coclab.ingest.hud_exchange_gis.ingest_hud_exchange")
    def test_ingest_hud_exchange_failure(self, mock_ingest):
        """Ingest hud_exchange failure should show error."""
        mock_ingest.side_effect = ValueError("Download failed")

        result = runner.invoke(
            app, ["ingest-boundaries", "--source", "hud_exchange", "--vintage", "2025", "--force"]
        )

        assert result.exit_code == 1
        assert "Error:" in result.output

    @patch("coclab.ingest.hud_opendata_arcgis.ingest_hud_opendata")
    def test_ingest_hud_opendata_success(self, mock_ingest):
        """Ingest hud_opendata should call ingest_hud_opendata."""
        mock_ingest.return_value = Path(
            "data/curated/coc_boundaries/coc_boundaries__HUDOpenData_2025-01-04.parquet"
        )

        result = runner.invoke(app, ["ingest-boundaries", "--source", "hud_opendata"])

        assert result.exit_code == 0
        assert "Successfully ingested" in result.output
        mock_ingest.assert_called_once_with(snapshot_tag="latest")

    @patch("coclab.ingest.hud_opendata_arcgis.ingest_hud_opendata")
    def test_ingest_hud_opendata_with_snapshot(self, mock_ingest):
        """Ingest hud_opendata with custom snapshot tag."""
        mock_ingest.return_value = Path(
            "data/curated/coc_boundaries/coc_boundaries__custom_snapshot.parquet"
        )

        result = runner.invoke(
            app, ["ingest-boundaries", "--source", "hud_opendata", "--snapshot", "custom_snapshot"]
        )

        assert result.exit_code == 0
        mock_ingest.assert_called_once_with(snapshot_tag="custom_snapshot")


class TestNestedIngestCommand:
    """Tests for the nested 'ingest boundaries' command."""

    @patch("coclab.ingest.hud_exchange_gis.ingest_hud_exchange")
    def test_ingest_boundaries_nested_hud_exchange(self, mock_ingest):
        """Nested ingest boundaries should call ingest_hud_exchange."""
        mock_ingest.return_value = Path("data/curated/coc_boundaries/coc_boundaries__2025.parquet")

        result = runner.invoke(
            app,
            [
                "ingest",
                "boundaries",
                "--source",
                "hud_exchange",
                "--vintage",
                "2025",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert "Successfully ingested" in result.output
        mock_ingest.assert_called_once_with("2025", show_progress=True)


class TestListBoundariesCommand:
    """Tests for the 'list-boundaries' command."""

    @patch("coclab.registry.registry.list_boundaries")
    def test_list_boundaries_empty(self, mock_list):
        """List boundaries when no vintages registered."""
        mock_list.return_value = []

        result = runner.invoke(app, ["list-boundaries"])

        assert result.exit_code == 0
        assert "No vintages registered" in result.output

    @patch("coclab.registry.registry.list_boundaries")
    def test_list_boundaries_with_entries(self, mock_list):
        """List boundaries with registered entries."""
        from datetime import UTC, datetime

        from coclab.registry.schema import RegistryEntry

        mock_list.return_value = [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=datetime(2025, 1, 4, 12, 0, 0, tzinfo=UTC),
                path=Path("data/curated/coc_boundaries/coc_boundaries__2025.parquet"),
                feature_count=450,
                hash_of_file="abc123",
            ),
            RegistryEntry(
                boundary_vintage="HUDOpenData_2025-01-04",
                source="hud_opendata",
                ingested_at=datetime(2025, 1, 4, 10, 0, 0, tzinfo=UTC),
                path=Path(
                    "data/curated/coc_boundaries/coc_boundaries__HUDOpenData_2025-01-04.parquet"
                ),
                feature_count=448,
                hash_of_file="def456",
            ),
        ]

        result = runner.invoke(app, ["list-boundaries"])

        assert result.exit_code == 0
        assert "2025" in result.output
        assert "hud_exchange" in result.output
        assert "HUDOpenData_2025-01-04" in result.output
        assert "450" in result.output


class TestShowCommand:
    """Tests for the 'show' command."""

    @patch("coclab.viz.map_folium.render_coc_map")
    def test_show_success(self, mock_render):
        """Show CoC map successfully."""
        mock_render.return_value = Path("data/curated/maps/CO-500__2025.html")

        result = runner.invoke(app, ["show", "--coc", "CO-500"])

        assert result.exit_code == 0
        assert "Map saved to" in result.output
        mock_render.assert_called_once_with(coc_id="CO-500", vintage=None, out_html=None)

    @patch("coclab.viz.map_folium.render_coc_map")
    def test_show_with_vintage(self, mock_render):
        """Show CoC map with specific vintage."""
        mock_render.return_value = Path("data/curated/maps/CO-500__2024.html")

        result = runner.invoke(app, ["show", "--coc", "CO-500", "--vintage", "2024"])

        assert result.exit_code == 0
        mock_render.assert_called_once_with(coc_id="CO-500", vintage="2024", out_html=None)

    @patch("coclab.viz.map_folium.render_coc_map")
    def test_show_with_output_path(self, mock_render):
        """Show CoC map with custom output path."""
        custom_path = Path("/tmp/my_map.html")
        mock_render.return_value = custom_path

        result = runner.invoke(app, ["show", "--coc", "CO-500", "--output", str(custom_path)])

        assert result.exit_code == 0
        mock_render.assert_called_once_with(coc_id="CO-500", vintage=None, out_html=custom_path)

    @patch("coclab.viz.map_folium.render_coc_map")
    def test_show_coc_not_found(self, mock_render):
        """Show CoC map when CoC not found."""
        mock_render.side_effect = ValueError("CoC 'XX-999' not found")

        result = runner.invoke(app, ["show", "--coc", "XX-999"])

        assert result.exit_code == 1
        assert "Error:" in result.output

    @patch("coclab.viz.map_folium.render_coc_map")
    def test_show_file_not_found(self, mock_render):
        """Show CoC map when boundary file not found."""
        mock_render.side_effect = FileNotFoundError("Boundary file not found")

        result = runner.invoke(app, ["show", "--coc", "CO-500"])

        assert result.exit_code == 1
        assert "Error:" in result.output


class TestHelpOutput:
    """Tests for CLI help output."""

    def test_main_help(self):
        """Main help should show all commands."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "ingest" in result.output  # Nested subcommand group
        assert "list-boundaries" in result.output
        assert "show" in result.output
        # Deprecated aliases should be hidden from help
        assert "ingest-boundaries" not in result.output
        assert "check-boundaries" not in result.output

    def test_ingest_help(self):
        """Ingest help should show subcommands."""
        result = runner.invoke(app, ["ingest", "--help"])

        assert result.exit_code == 0
        assert "boundaries" in result.output
        assert "census" in result.output
        assert "pit" in result.output
        assert "zori" in result.output

    def test_ingest_boundaries_help(self):
        """Ingest boundaries help should show options."""
        result = runner.invoke(app, ["ingest", "boundaries", "--help"])

        assert result.exit_code == 0
        assert "--source" in result.output
        assert "--vintage" in result.output
        assert "--snapshot" in result.output

    def test_show_help(self):
        """Show help should show options."""
        result = runner.invoke(app, ["show", "--help"])

        assert result.exit_code == 0
        assert "--coc" in result.output
        assert "--vintage" in result.output
        assert "--output" in result.output

    def test_validate_population_help(self):
        """Validate-population help should show options."""
        result = runner.invoke(app, ["validate-population", "--help"])

        assert result.exit_code == 0
        assert "--boundary" in result.output
        assert "--acs" in result.output
        assert "--by-state" in result.output
        assert "--warn-threshold" in result.output


class TestValidatePopulation:
    """Tests for the 'validate-population' command."""

    def test_validate_population_runs(self, tmp_path):
        """Test that validate-population runs with test data."""
        import pandas as pd

        # Create test crosswalk
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir()
        xwalk = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500", "CO-501"],
                "tract_geoid": ["08001000100", "08001000200", "08005000100"],
                "area_share": [1.0, 1.0, 1.0],
                "pop_share": [0.5, 0.5, 1.0],
            }
        )
        xwalk.to_parquet(xwalk_dir / "xwalk__B2025xT2023.parquet")

        # Create test ACS data
        acs_dir = tmp_path / "acs"
        acs_dir.mkdir()
        acs = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200", "08005000100"],
                "total_population": [1000, 2000, 3000],
                "tract_vintage": ["2023", "2023", "2023"],
                "acs_vintage": ["2019-2023", "2019-2023", "2019-2023"],
            }
        )
        acs.to_parquet(acs_dir / "tract_population__2019-2023__2023.parquet")

        result = runner.invoke(
            app,
            [
                "validate-population",
                "--xwalk-dir",
                str(xwalk_dir),
                "--acs-dir",
                str(acs_dir),
            ],
        )

        assert result.exit_code == 0
        assert "POPULATION CROSSWALK VALIDATION" in result.output
        assert "NATIONAL TOTAL" in result.output
        assert "COC-AGGREGATED TOTAL" in result.output

    def test_validate_population_missing_files(self, tmp_path):
        """Test that validate-population fails gracefully with missing files."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "validate-population",
                "--xwalk-dir",
                str(empty_dir),
            ],
        )

        assert result.exit_code == 1
        assert "Error" in result.output or "No crosswalk" in result.output
