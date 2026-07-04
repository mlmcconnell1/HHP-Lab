"""Tests for --json output across list and diagnostic commands.

Covers coclab-265p: Expand and standardize --json output.
"""

import json

import pandas as pd
import pytest
import typer
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.cli.shared.output import cli_error, emit_result

runner = CliRunner()


class TestCliOutputHelpers:

    def test_emit_result_outputs_json_when_requested(self, capsys):
        handled = emit_result({"status": "ok", "count": 2}, json_output=True)

        assert handled is True
        assert json.loads(capsys.readouterr().out) == {"status": "ok", "count": 2}

    def test_emit_result_returns_false_for_human_output(self, capsys):
        handled = emit_result({"status": "ok"}, json_output=False)

        assert handled is False
        assert capsys.readouterr().out == ""

    def test_cli_error_outputs_json_and_exit_code(self, capsys):
        with pytest.raises(typer.Exit) as exc_info:
            cli_error("bad input", json_output=True, code=2)

        assert exc_info.value.exit_code == 2
        assert json.loads(capsys.readouterr().out) == {
            "status": "error",
            "error": "bad input",
        }


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

    def test_json_empty_dir(self, tmp_path):
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        result = runner.invoke(
            app, ["list", "measures", "--dir", str(measures_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "measures": []}

    def test_json_missing_dir(self, tmp_path):
        result = runner.invoke(
            app, ["list", "measures", "--dir", str(tmp_path / "nope"), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "measures": []}

    def test_dictionary_json_output(self):
        result = runner.invoke(app, ["list", "measures", "--dictionary", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] == len(payload["measures"])

        by_column = {entry["column"]: entry for entry in payload["measures"]}
        pit_total = by_column["pit_total"]
        assert pit_total["role_hint"] == "outcome"
        assert pit_total["source_provider"] == "hud"
        assert pit_total["source_product"] == "pit"
        assert pit_total["native_geometry"] == "coc"
        assert pit_total["coverage_years"] == {
            "first": 2007,
            "last": None,
            "last_is_ongoing": True,
        }

        zori = by_column["zori_coc"]
        assert zori["source_provider"] == "zillow"
        assert zori["coverage_years"]["first"] == 2015
        assert any("starts in January 2015" in caveat for caveat in zori["caveats"])


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

    def test_json_missing_dir(self, tmp_path):
        result = runner.invoke(
            app, ["list", "census", "--dir", str(tmp_path / "nope"), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "census_files": []}

    def test_json_empty_dir(self, tmp_path):
        census_dir = tmp_path / "tiger"
        census_dir.mkdir()

        result = runner.invoke(
            app, ["list", "census", "--dir", str(census_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "census_files": []}

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


    def test_json_missing_dir(self, tmp_path):
        result = runner.invoke(
            app, ["list", "xwalks", "--dir", str(tmp_path / "nope"), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "crosswalks": []}

    def test_json_empty_dir(self, tmp_path):
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir()

        result = runner.invoke(
            app, ["list", "xwalks", "--dir", str(xwalk_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload == {"status": "ok", "count": 0, "crosswalks": []}

    def test_json_output_msa_crosswalk(self, tmp_path):
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir()
        pd.DataFrame({
            "coc_id": ["CO-500"],
            "msa_id": ["35620"],
            "allocation_share": [1.0],
        }).to_parquet(
            xwalk_dir / "msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
        )

        result = runner.invoke(
            app, ["list", "xwalks", "--dir", str(xwalk_dir), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["count"] == 1
        xw = payload["crosswalks"][0]
        assert xw["type"] == "msa"
        assert xw["boundary_vintage"] == "2025"
        assert xw["definition_version"] == "census_msa_2023"
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
            "acs5_vintage_used": ["2022", "2023"],
            "weighting_method": ["area", "area"],
            "total_population": [50000, 51000],
            "coverage_ratio": [0.95, 0.96],
            "boundary_changed": [False, True],
            "source": ["hhplab_panel", "hhplab_panel"],
        }).to_parquet(panel_path)

        result = runner.invoke(
            app, ["diagnostics", "panel", "--panel", str(panel_path), "--json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert "panel_info" in payload


    def test_json_missing_file(self, tmp_path):
        result = runner.invoke(
            app,
            ["diagnostics", "panel", "--panel", str(tmp_path / "missing.parquet"), "--json"],
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "error"
        assert "not found" in payload["error"]


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
