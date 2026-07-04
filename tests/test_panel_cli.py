"""Tests for hhplab panel inspection commands."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def _panel_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "geo_id": ["A", "A", "B", "B"],
            "year": [2020, 2021, 2020, 2021],
            "pit_total": [10.0, 12.0, 20.0, None],
            "total_population": [1000.0, 1010.0, 2000.0, 2020.0],
            "median_gross_rent": [900.0, 950.0, 1100.0, 1150.0],
        }
    ).to_parquet(path)
    return path


class TestPanelCli:
    def test_panel_describe_json_includes_missingness_and_semantics(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

        result = runner.invoke(
            app,
            [
                "panel",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,total_population",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["row_count"] == 4
        assert payload["geography_count"] == 2
        assert payload["year_min"] == 2020
        assert payload["year_max"] == 2021
        by_column = {entry["column"]: entry for entry in payload["measures"]}
        assert by_column["pit_total"]["role_hint"] == "outcome"
        assert by_column["total_population"]["source_provider"] == "census"
        missing_2021 = next(row for row in payload["missingness_by_year"] if row["year"] == 2021)
        assert missing_2021["pit_total"] == 0.5
        missing_geo_b = next(
            row for row in payload["missingness_by_geography"] if row["geo_id"] == "B"
        )
        assert missing_geo_b["pit_total"] == 0.5

    def test_panel_query_json_filters_and_selects_columns(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

        result = runner.invoke(
            app,
            [
                "panel",
                "query",
                "--panel",
                str(panel),
                "--where",
                "year == 2021",
                "--columns",
                "geo_id,year,pit_total",
                "--limit",
                "1",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["row_count"] == 1
        assert payload["columns"] == ["geo_id", "year", "pit_total"]
        assert payload["records"] == [{"geo_id": "A", "year": 2021, "pit_total": 12.0}]
