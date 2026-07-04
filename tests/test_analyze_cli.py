"""Tests for hhplab analyze commands."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

runner = CliRunner()


def _panel_fixture(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "B", "B", "B"],
            "year": [2020, 2021, 2022, 2020, 2021, 2022],
            "pit_total": [10.0, 12.0, 14.0, 20.0, 19.0, 18.0],
            "total_population": [1000.0, 1000.0, 1000.0, 2000.0, 2000.0, 2000.0],
            "median_gross_rent": [900.0, 950.0, 1000.0, 1100.0, 1125.0, 1150.0],
            "unemployment_rate": [0.05, 0.06, 0.055, 0.08, 0.075, 0.07],
        }
    )
    write_parquet_with_provenance(
        df,
        path,
        ProvenanceBlock(
            geo_type="coc",
            extra={"recipe": "test-panel"},
        ),
    )
    return path


class TestAnalyzeCli:
    def test_describe_writes_json_and_provenance(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "describe.parquet"

        result = runner.invoke(
            app,
            [
                "analyze",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,total_population",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["analysis_type"] == "describe"
        assert payload["column_count"] == 2
        assert output.exists()
        manifest = output.with_suffix(".manifest.json")
        assert manifest.exists()
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["analysis_type"] == "describe"
        assert provenance.extra["input_panel"] == str(panel)
        manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
        assert manifest_payload["analysis_type"] == "describe"
        assert manifest_payload["specification"]["parameters"]["columns"] == [
            "pit_total",
            "total_population",
        ]
        assert manifest_payload["panel"]["name"] == "panel"
        assert manifest_payload["panel"]["provenance"]["extra"]["recipe"] == "test-panel"
        assert manifest_payload["result_summary"]["row_count"] == 2
        assert set(manifest_payload["result_summary"]["columns"]) >= {"column", "mean"}
        assert payload["manifest_path"] == str(manifest)

    def test_correlate_outputs_pairwise_and_partial_correlations(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "correlate.parquet"

        result = runner.invoke(
            app,
            [
                "analyze",
                "correlate",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,median_gross_rent,total_population",
                "--partial-controls",
                "unemployment_rate",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["analysis_type"] == "correlate"
        assert payload["pair_count"] == 3
        assert pd.read_parquet(output).shape[0] == 3
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["parameters"]["partial_controls"] == ["unemployment_rate"]

    def test_regress_outputs_fixed_effect_coefficients(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "regress.parquet"

        result = runner.invoke(
            app,
            [
                "analyze",
                "regress",
                "--panel",
                str(panel),
                "--outcome",
                "pit_total",
                "--predictors",
                "median_gross_rent,unemployment_rate",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["analysis_type"] == "regress"
        assert payload["std_error_type"] == "clustered:geo_id"
        terms = set(pd.read_parquet(output)["term"])
        assert {"Intercept", "median_gross_rent", "unemployment_rate"} <= terms

    def test_lagged_outputs_lagged_associations(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "lagged.parquet"

        result = runner.invoke(
            app,
            [
                "analyze",
                "lagged",
                "--panel",
                str(panel),
                "--outcome",
                "pit_total",
                "--predictors",
                "median_gross_rent,unemployment_rate",
                "--lags",
                "1,2",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["analysis_type"] == "lagged"
        assert payload["association_count"] == 4
        assert set(pd.read_parquet(output)["lag"]) == {1, 2}

    def test_ledger_list_and_show_expose_analysis_manifests(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        describe_output = tmp_path / "describe.parquet"
        correlate_output = tmp_path / "correlate.parquet"

        describe_result = runner.invoke(
            app,
            [
                "analyze",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,total_population",
                "--output",
                str(describe_output),
                "--json",
            ],
        )
        correlate_result = runner.invoke(
            app,
            [
                "analyze",
                "correlate",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,total_population",
                "--output",
                str(correlate_output),
                "--json",
            ],
        )
        assert describe_result.exit_code == 0
        assert correlate_result.exit_code == 0

        list_result = runner.invoke(
            app,
            [
                "analyze",
                "ledger",
                "list",
                "--directory",
                str(tmp_path),
                "--analysis-type",
                "describe",
                "--json",
            ],
        )
        assert list_result.exit_code == 0
        list_payload = json.loads(list_result.output)
        assert list_payload["analysis_count"] == 1
        [analysis] = list_payload["analyses"]
        assert analysis["analysis_type"] == "describe"
        assert analysis["panel_path"] == str(panel)
        assert analysis["output_path"] == str(describe_output)

        show_result = runner.invoke(
            app,
            [
                "analyze",
                "ledger",
                "show",
                "--manifest",
                analysis["manifest_path"],
                "--json",
            ],
        )
        assert show_result.exit_code == 0
        show_payload = json.loads(show_result.output)
        assert show_payload["manifest"]["analysis_type"] == "describe"
        assert show_payload["manifest"]["output"]["path"] == str(describe_output)
