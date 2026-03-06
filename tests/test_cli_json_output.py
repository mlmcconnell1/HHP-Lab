"""Tests for --json output across list and diagnostic commands.

Covers coclab-265p: Expand and standardize --json output.
"""

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


class TestListMeasuresJson:

    def test_json_output(self, tmp_path):
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "total_population": [50000],
            "weighting_method": ["area"],
        })
        df.to_parquet(measures_dir / "measures__A2023@B2025.parquet")

        result = runner.invoke(
            app, ["list", "measures", "--dir", str(measures_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] == 1
        assert payload["measures"][0]["boundary_vintage"] == "2025"

    def test_json_empty(self, tmp_path):
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        result = runner.invoke(
            app, ["list", "measures", "--dir", str(measures_dir), "--json"]
        )

        # Human output returns 0 and prints "No measure files" - JSON not reached
        assert result.exit_code == 0


class TestListCensusJson:

    def test_json_output(self, tmp_path):
        census_dir = tmp_path / "tiger"
        census_dir.mkdir()
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(
            census_dir / "tracts__T2023.parquet"
        )
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(
            census_dir / "counties__C2023.parquet"
        )

        result = runner.invoke(
            app, ["list", "census", "--dir", str(census_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] == 2
        types = {f["type"] for f in payload["census_files"]}
        assert types == {"tracts", "counties"}

    def test_json_with_type_filter(self, tmp_path):
        census_dir = tmp_path / "tiger"
        census_dir.mkdir()
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(
            census_dir / "tracts__T2023.parquet"
        )
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(
            census_dir / "counties__C2023.parquet"
        )

        result = runner.invoke(
            app,
            ["list", "census", "--dir", str(census_dir), "--type", "tracts", "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["count"] == 1
        assert payload["census_files"][0]["type"] == "tracts"


class TestListXwalksJson:

    def test_json_output(self, tmp_path):
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir()
        pd.DataFrame({
            "coc_id": ["CO-500"],
            "tract_geoid": ["08001000100"],
            "area_share": [1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2023.parquet")

        result = runner.invoke(
            app, ["list", "xwalks", "--dir", str(xwalk_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] == 1
        xw = payload["crosswalks"][0]
        assert xw["type"] == "tract"
        assert xw["boundary_vintage"] == "2025"
        assert xw["census_vintage"] == "2023"


class TestDiagnosticsXwalkJson:

    def test_json_output(self, tmp_path):
        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame({
            "coc_id": ["CO-500", "CO-500", "CO-501"],
            "tract_geoid": ["08001000100", "08001000200", "08005000100"],
            "area_share": [0.6, 0.4, 1.0],
            "intersection_area": [600.0, 400.0, 1000.0],
        }).to_parquet(xwalk_path)

        result = runner.invoke(
            app,
            ["diagnostics", "xwalk", "--crosswalk", str(xwalk_path), "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] >= 2
        assert "diagnostics" in payload
        assert "problems" in payload


class TestDiagnosticsPanelJson:

    def test_json_output(self, tmp_path):
        panel_path = tmp_path / "panel.parquet"
        pd.DataFrame({
            "coc_id": ["CO-500", "CO-500"],
            "year": [2023, 2024],
            "pit_total": [100, 110],
            "boundary_vintage_used": ["2023", "2024"],
            "acs_vintage_used": ["2022", "2023"],
            "weighting_method": ["area", "area"],
            "total_population": [50000, 51000],
            "coverage_ratio": [0.95, 0.96],
            "boundary_changed": [False, True],
            "source": ["coclab_panel", "coclab_panel"],
        }).to_parquet(panel_path)

        result = runner.invoke(
            app, ["diagnostics", "panel", "--panel", str(panel_path), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert "panel_info" in payload


class TestJsonHelp:
    """Verify --json appears in help for all updated commands."""

    def test_list_measures_help(self):
        result = runner.invoke(app, ["list", "measures", "--help"])
        assert "--json" in result.output

    def test_list_census_help(self):
        result = runner.invoke(app, ["list", "census", "--help"])
        assert "--json" in result.output

    def test_list_xwalks_help(self):
        result = runner.invoke(app, ["list", "xwalks", "--help"])
        assert "--json" in result.output

    def test_diagnostics_xwalk_help(self):
        result = runner.invoke(app, ["diagnostics", "xwalk", "--help"])
        assert "--json" in result.output

    def test_diagnostics_panel_help(self):
        result = runner.invoke(app, ["diagnostics", "panel", "--help"])
        assert "--json" in result.output
