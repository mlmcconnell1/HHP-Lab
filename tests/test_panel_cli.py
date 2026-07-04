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


def _unbalanced_panel_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "B", "B", "C", "C"],
            "year": [2020, 2021, 2023, 2020, 2023, 2020, 2023],
            "pit_total": [10.0, 11.0, 13.0, 20.0, 23.0, 30.0, 33.0],
            "total_population": [1000.0, 1010.0, 1030.0, 2000.0, 2030.0, 3000.0, 3030.0],
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
        assert payload["year_coverage"]["missing_years"] == []
        assert payload["missingness_by_geo_year"]

    def test_panel_describe_json_flags_absent_geo_year_cells_and_year_gaps(
        self, tmp_path: Path
    ):
        panel = _unbalanced_panel_fixture(tmp_path / "panel.parquet")

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
        assert payload["year_coverage"]["expected_years"] == [2020, 2021, 2022, 2023]
        assert payload["year_coverage"]["missing_years"] == [2022]
        absent_cells = [
            record for record in payload["missingness_by_geo_year"] if not record["row_present"]
        ]
        assert {
            (record["geo_id"], record["year"], record["pit_total"]) for record in absent_cells
        } == {
            ("A", 2022, 1.0),
            ("B", 2021, 1.0),
            ("B", 2022, 1.0),
            ("C", 2021, 1.0),
            ("C", 2022, 1.0),
        }
        missing_2021 = next(row for row in payload["missingness_by_year"] if row["year"] == 2021)
        assert missing_2021["pit_total"] == 2 / 3
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

    def test_panel_query_json_uses_null_for_non_finite_values(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

        result = runner.invoke(
            app,
            [
                "panel",
                "query",
                "--panel",
                str(panel),
                "--where",
                "geo_id == 'B' and year == 2021",
                "--columns",
                "geo_id,year,pit_total",
                "--json",
            ],
        )

        assert result.exit_code == 0
        assert "NaN" not in result.output
        assert "Infinity" not in result.output
        payload = json.loads(result.output)
        assert payload["records"] == [{"geo_id": "B", "year": 2021, "pit_total": None}]

    def test_panel_describe_non_json_includes_summary_and_coverage(self, tmp_path: Path):
        panel = _unbalanced_panel_fixture(tmp_path / "panel.parquet")

        result = runner.invoke(
            app,
            [
                "panel",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,total_population",
            ],
        )

        assert result.exit_code == 0
        assert "Measure summary:" in result.output
        assert "Missing years: 2022" in result.output
        assert "Absent geo-year cells:" in result.output
        assert "pit_total" in result.output

    def test_panel_query_non_json_prints_records(self, tmp_path: Path):
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
            ],
        )

        assert result.exit_code == 0
        assert "Columns: geo_id, year, pit_total" in result.output
        assert "geo_id" in result.output
        assert "A" in result.output
        assert "12.0" in result.output
