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
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["analysis_type"] == "describe"
        assert provenance.extra["input_panel"] == str(panel)

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
