"""Tests for the hhplab CLI."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from hhplab import __version__
from hhplab.cli.main import app

runner = CliRunner()


class TestRootCommand:
    """Tests for root CLI behavior."""

    def test_version_option(self):
        result = runner.invoke(app, ["--version"])

        assert result.exit_code == 0
        assert result.output == f"hhplab {__version__}\n"


class TestNestedIngestCommand:
    """Tests for the nested 'ingest boundaries' command."""

    @patch("hhplab.hud.ingest_hud_exchange")
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
    """Tests for the 'list boundaries' command."""

    @patch("hhplab.registry.boundary_registry.list_boundaries")
    def test_list_boundaries_empty(self, mock_list):
        """List boundaries when no vintages registered."""
        mock_list.return_value = []

        result = runner.invoke(app, ["list", "boundaries"])

        assert result.exit_code == 0
        assert "No vintages registered" in result.output

    @patch("hhplab.registry.boundary_registry.list_boundaries")
    def test_list_boundaries_with_entries(self, mock_list):
        """List boundaries with registered entries."""
        from datetime import UTC, datetime

        from hhplab.registry.schema import RegistryEntry

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

        result = runner.invoke(app, ["list", "boundaries"])

        assert result.exit_code == 0
        assert "2025" in result.output
        assert "hud_exchange" in result.output
        assert "HUDOpenData_2025-01-04" in result.output
        assert "450" in result.output


class TestShowCommand:
    """Tests for the 'show' command."""

    @patch("hhplab.viz.map_folium.render_coc_map")
    def test_show_success(self, mock_render):
        """Show CoC map successfully."""
        mock_render.return_value = Path("data/curated/maps/CO-500__2025.html")

        result = runner.invoke(app, ["show", "map", "--coc", "CO-500"])

        assert result.exit_code == 0
        assert "Map saved to" in result.output
        mock_render.assert_called_once_with(coc_id="CO-500", vintage=None, out_html=None)

    @patch("hhplab.viz.map_folium.render_coc_map")
    def test_show_with_vintage(self, mock_render):
        """Show CoC map with specific vintage."""
        mock_render.return_value = Path("data/curated/maps/CO-500__2024.html")

        result = runner.invoke(app, ["show", "map", "--coc", "CO-500", "--vintage", "2024"])

        assert result.exit_code == 0
        mock_render.assert_called_once_with(coc_id="CO-500", vintage="2024", out_html=None)

    @patch("hhplab.viz.map_folium.render_coc_map")
    def test_show_with_output_path(self, mock_render):
        """Show CoC map with custom output path."""
        custom_path = Path("/tmp/my_map.html")
        mock_render.return_value = custom_path

        result = runner.invoke(
            app,
            ["show", "map", "--coc", "CO-500", "--output", str(custom_path)],
        )

        assert result.exit_code == 0
        mock_render.assert_called_once_with(coc_id="CO-500", vintage=None, out_html=custom_path)

    @patch("hhplab.viz.map_folium.render_coc_map")
    def test_show_coc_not_found(self, mock_render):
        """Show CoC map when CoC not found."""
        mock_render.side_effect = ValueError("CoC 'XX-999' not found")

        result = runner.invoke(app, ["show", "map", "--coc", "XX-999"])

        assert result.exit_code == 1
        assert "Error:" in result.output

    @patch("hhplab.viz.map_folium.render_coc_map")
    def test_show_file_not_found(self, mock_render):
        """Show CoC map when boundary file not found."""
        mock_render.side_effect = FileNotFoundError("Boundary file not found")

        result = runner.invoke(app, ["show", "map", "--coc", "CO-500"])

        assert result.exit_code == 1
        assert "Error:" in result.output


class TestHelpOutput:
    """Tests for CLI help output."""

    def test_main_help(self):
        """Main help should show all commands."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "ingest" in result.output  # Nested subcommand group
        assert "list" in result.output
        assert "validate" in result.output
        assert "generate" in result.output
        assert "build" in result.output
        assert "show" in result.output
        # Removed legacy aliases must not appear in help
        assert "ingest-boundaries" not in result.output
        assert "check-boundaries" not in result.output
        assert "list-boundaries" not in result.output
        assert "validate-population" not in result.output
        assert "aggregate-measures" not in result.output
        assert "aggregate-zori" not in result.output
        assert "build-panel" not in result.output
        assert "build-xwalks" not in result.output
        assert "export-bundle" not in result.output

    def test_ingest_help(self):
        """Ingest help should show subcommands."""
        result = runner.invoke(app, ["ingest", "--help"])

        assert result.exit_code == 0
        assert "boundaries" in result.output
        assert "tiger" in result.output
        assert "pit" in result.output
        assert "zori" in result.output

    def test_list_help(self):
        """List help should show subcommands."""
        result = runner.invoke(app, ["list", "--help"])

        assert result.exit_code == 0
        assert "boundaries" in result.output
        assert "census" in result.output
        assert "measures" in result.output
        assert "xwalks" in result.output

    def test_build_help(self):
        """Build help should show recipe subcommands."""
        result = runner.invoke(app, ["build", "--help"])

        assert result.exit_code == 0
        assert "recipe" in result.output
        assert "recipe-preflight" in result.output
        assert "recipe-plan" in result.output
        assert "recipe-export" in result.output

    def test_generate_help(self):
        """Generate help should show subcommands."""
        result = runner.invoke(app, ["generate", "--help"])

        assert result.exit_code == 0
        assert "xwalks" in result.output

    def test_ingest_boundaries_help(self):
        """Ingest boundaries help should show options."""
        result = runner.invoke(app, ["ingest", "boundaries", "--help"])

        assert result.exit_code == 0
        assert "--source" in result.output
        assert "--vintage" in result.output
        assert "--snapshot" in result.output

    def test_show_help(self):
        """Show help should show subcommands."""
        result = runner.invoke(app, ["show", "--help"])

        assert result.exit_code == 0
        assert "map" in result.output
        assert "measures" in result.output
        assert "sources" in result.output
        assert "vintage-diffs" in result.output

    def test_show_map_help(self):
        """Show map help should show options."""
        result = runner.invoke(app, ["show", "map", "--help"])

        assert result.exit_code == 0
        assert "--coc" in result.output
        assert "--vintage" in result.output
        assert "--output" in result.output

    def test_validate_help(self):
        """Validate help should show subcommands."""
        result = runner.invoke(app, ["validate", "--help"])

        assert result.exit_code == 0
        assert "boundaries" in result.output
        assert "pit-vintages" in result.output
        assert "population" in result.output

    def test_validate_population_help(self):
        """Validate population help should show options."""
        result = runner.invoke(app, ["validate", "population", "--help"])

        assert result.exit_code == 0
        assert "--boundary" in result.output
        assert "--acs" in result.output
        assert "--by-state" in result.output
        assert "--warn-threshold" in result.output


class TestValidatePopulation:
    """Tests for the 'validate population' command."""

    def test_validate_population_runs(self, tmp_path):
        """Test that validate population runs with test data."""
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
        acs.to_parquet(acs_dir / "acs5_tracts__A2023xT2023.parquet")

        result = runner.invoke(
            app,
            [
                "validate",
                "population",
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
        """Test that validate population fails gracefully with missing files."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "validate",
                "population",
                "--xwalk-dir",
                str(empty_dir),
            ],
        )

        assert result.exit_code == 1
        assert "Error" in result.output or "No crosswalk" in result.output


class TestRegistryDeleteEntry:
    """Tests for the 'registry delete-entry' command."""

    @patch("hhplab.registry.boundary_registry.delete_vintage")
    @patch("hhplab.registry.boundary_registry.list_boundaries")
    def test_registry_delete_entry_not_found(self, mock_list, mock_delete):
        """Should fail if entry not found."""
        mock_list.return_value = []

        result = runner.invoke(
            app,
            ["registry", "delete-entry", "2024", "hud_exchange"],
        )

        assert result.exit_code == 1
        assert "No entry found" in result.output

    @patch("hhplab.source_registry.delete_by_local_path")
    @patch("hhplab.registry.boundary_registry.delete_vintage")
    @patch("hhplab.registry.boundary_registry.list_boundaries")
    def test_registry_delete_entry_success(self, mock_list, mock_delete, mock_delete_src):
        """Should delete entry when found and confirmed."""
        from unittest.mock import MagicMock

        entry = MagicMock()
        entry.boundary_vintage = "2024"
        entry.source = "hud_exchange"
        entry.feature_count = 400
        entry.path = Path("data/curated/coc_boundaries/coc_boundaries__2024.parquet")
        mock_list.return_value = [entry]
        mock_delete.return_value = True
        mock_delete_src.return_value = 1

        result = runner.invoke(
            app,
            ["registry", "delete-entry", "2024", "hud_exchange", "--yes"],
        )

        assert result.exit_code == 0
        assert "Deleted registry entry" in result.output
        mock_delete.assert_called_once_with("2024", "hud_exchange")


class TestRegistryRebuild:
    """Tests for the 'registry rebuild' command."""

    def test_registry_rebuild_no_registry(self, tmp_path):
        """Should handle missing registry file."""
        result = runner.invoke(
            app,
            ["registry", "rebuild", "--registry", str(tmp_path / "nonexistent.parquet")],
        )

        assert "Registry not found" in result.output

    @patch("hhplab.cli.registry_rebuild._load_registry")
    def test_registry_rebuild_empty_registry(self, mock_load, tmp_path):
        """Should handle empty registry."""
        import pandas as pd

        registry_path = tmp_path / "source_registry.parquet"
        registry_path.touch()
        mock_load.return_value = pd.DataFrame()

        result = runner.invoke(
            app,
            ["registry", "rebuild", "--registry", str(registry_path)],
        )

        assert "empty" in result.output.lower()


# ---------------------------------------------------------------------------
# Regression: retired named-build commands must not reappear
# ---------------------------------------------------------------------------


class TestRetiredCommandRegression:
    """Guard against deleted commands reappearing in help output."""

    def _command_names(self, group: str) -> set[str]:
        """Extract registered command names from a group's help output."""
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0
        # Typer formats commands as "│ name  description │"
        names = set()
        for line in result.output.splitlines():
            stripped = line.strip().lstrip("│").strip()
            if stripped:
                token = stripped.split()[0]
                if token not in ("--help", "Usage:", "Options", "Commands"):
                    names.add(token)
        return names

    def test_build_help_excludes_retired(self):
        names = self._command_names("build")
        for retired in ("panel", "create", "list", "export"):
            assert retired not in names, f"Retired command '{retired}' in build help"

    def test_list_help_excludes_artifacts(self):
        names = self._command_names("list")
        assert "artifacts" not in names

    def test_generate_help_excludes_catalog(self):
        names = self._command_names("generate")
        assert "catalog" not in names

    def test_top_level_crosscheck_removed(self):
        """Hidden crosscheck commands should no longer be registered."""
        result = runner.invoke(app, ["crosscheck-pit-vintages", "--help"])
        assert result.exit_code == 2  # Typer returns 2 for unknown commands

    def test_aggregate_no_build_create_guidance(self):
        """Aggregate CLI should reject the retired --build option outright."""
        result = runner.invoke(app, ["aggregate", "pit", "--build", "nonexistent"])
        assert result.exit_code != 0
        assert "No such option: --build" in result.output

    def test_aggregate_works_without_build(self):
        """Aggregate commands should require explicit years."""
        result = runner.invoke(app, ["aggregate", "pit"])
        assert result.exit_code == 2
        assert "--years is required" in result.output

    def test_list_group_help_no_artifacts(self):
        """The 'list' group description must not mention retired 'artifacts'."""
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0
        assert "artifacts" not in result.output.lower()

    def test_generate_group_help_no_catalogs(self):
        """The 'generate' group description must not mention retired 'catalogs'."""
        result = runner.invoke(app, ["generate", "--help"])
        assert result.exit_code == 0
        assert "catalogs" not in result.output.lower()

    def test_main_help_no_stale_terminology(self):
        """Top-level help must not reference retired concepts."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        output_lower = result.output.lower()
        for term in ("artifacts", "catalogs"):
            assert term not in output_lower, f"Stale term '{term}' in main help"

    def test_recipe_help_marks_recipe_as_default_entrypoint(self):
        """Recipe help should present build recipe as the normal entrypoint."""
        result = runner.invoke(app, ["build", "recipe", "--help"])
        assert result.exit_code == 0
        assert "normal entrypoint" in result.output
        assert "recipe-preflight" in result.output
        assert "recipe-plan" in result.output

    def test_recipe_plan_help_marks_it_as_inspection_only(self):
        """Recipe-plan help should not present itself as the readiness gate."""
        result = runner.invoke(app, ["build", "recipe-plan", "--help"])
        assert result.exit_code == 0
        assert "authoring or debugging" in result.output
        assert "recipe-preflight" in result.output

    def test_agents_command_recommends_preflight_then_recipe(self):
        """Agent guidance should point to preflight + recipe as the default flow."""
        result = runner.invoke(app, ["agents"])
        assert result.exit_code == 0
        assert "build recipe-preflight --recipe <file> --json" in result.output
        assert "build recipe --recipe <file> --json" in result.output
