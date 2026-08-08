"""Tests for hhplab panel inspection commands."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.storage.provenance import read_provenance

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


def _msa_panel_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "msa_id": ["10100", "10200", "10300", "10400", "10500"],
            "msa_name": ["Alpha, CA", "Beta, PR", "Gamma, TX", "Delta, NY", "Epsilon, WA"],
            "year": [2024, 2024, 2024, 2024, 2024],
            "total_population": [5000.0, 9999.0, 8000.0, 7000.0, 6000.0],
            "non_native_share": [0.20, 0.95, 0.40, 0.35, 0.30],
            "msa_contract_rent_p25": [900.0, 1200.0, 1300.0, 1100.0, 1000.0],
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

    def test_panel_query_sort_top_output_feeds_analyze_correlate(self, tmp_path: Path):
        panel = _msa_panel_fixture(tmp_path / "msa_measures.parquet")
        output = tmp_path / "top3.parquet"

        result = runner.invoke(
            app,
            [
                "panel",
                "query",
                "--panel",
                str(panel),
                "--where",
                "not msa_name.str.endswith(', PR')",
                "--sort",
                "total_population",
                "--desc",
                "--top",
                "3",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["row_count"] == 3
        assert payload["output_path"] == str(output)
        assert [record["msa_id"] for record in payload["records"]] == ["10300", "10400", "10500"]

        written = pd.read_parquet(output)
        assert written["msa_id"].tolist() == ["10300", "10400", "10500"]
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["dataset_type"] == "panel_query"
        assert provenance.extra["input_panel"] == str(panel)
        assert provenance.extra["parameters"]["where"] == "not msa_name.str.endswith(', PR')"
        assert provenance.extra["parameters"]["sort"] == "total_population"
        assert provenance.extra["parameters"]["descending"] is True
        assert provenance.extra["parameters"]["top"] == 3

        correlate = runner.invoke(
            app,
            [
                "analyze",
                "correlate",
                "--panel",
                str(output),
                "--columns",
                "non_native_share,msa_contract_rent_p25",
                "--json",
            ],
        )

        assert correlate.exit_code == 0
        correlate_payload = json.loads(correlate.output)
        assert correlate_payload["analysis_type"] == "correlate"
        assert correlate_payload["records"][0]["n"] == 3

    def test_panel_query_sort_validates_column_before_projection(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

        result = runner.invoke(
            app,
            [
                "panel",
                "query",
                "--panel",
                str(panel),
                "--sort",
                "missing_column",
                "--columns",
                "geo_id,year",
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "Requested panel sort column is missing: missing_column" in result.output

    def test_panel_enrich_joins_source_columns_and_derives_rate(self, tmp_path: Path):
        panel = _msa_panel_fixture(tmp_path / "msa_panel.parquet")
        source = tmp_path / "sanctuary.parquet"
        output = tmp_path / "enriched.parquet"
        pd.DataFrame(
            {
                "cbsa_code": ["10100", "10200", "10300", "10400", "10500"],
                "sanctuary_policy": [1, 0, 1, 0, 1],
            }
        ).to_parquet(source)

        result = runner.invoke(
            app,
            [
                "panel",
                "enrich",
                "--panel",
                str(panel),
                "--source",
                str(source),
                "--columns",
                "sanctuary_policy",
                "--output",
                str(output),
                "--numerator",
                "total_population",
                "--denominator",
                "msa_contract_rent_p25",
                "--rate-per",
                "1000",
                "--rate-name",
                "population_per_1000_rent_dollars",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "ok"
        assert payload["row_count"] == 5
        assert payload["matched_row_count"] == 5
        assert payload["join"]["panel_geo_column"] == "msa_id"
        assert payload["join"]["source_geo_column"] == "cbsa_code"
        assert payload["source_columns"] == ["sanctuary_policy"]
        assert payload["derived_rates"] == [
            {
                "name": "population_per_1000_rent_dollars",
                "numerator": "total_population",
                "denominator": "msa_contract_rent_p25",
                "rate_per": 1000.0,
            }
        ]

        enriched = pd.read_parquet(output)
        assert enriched["sanctuary_policy"].tolist() == [1, 0, 1, 0, 1]
        assert "cbsa_code" not in enriched.columns
        assert enriched.loc[0, "population_per_1000_rent_dollars"] == 5000.0 / 900.0 * 1000
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["dataset_type"] == "panel_enrichment"
        assert provenance.extra["input_panel"] == str(panel)
        assert provenance.extra["input_source"] == str(source)
        assert provenance.extra["join"]["include_year"] is False

    def test_panel_enrich_rejects_duplicate_source_join_keys(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        source = tmp_path / "source.parquet"
        pd.DataFrame(
            {
                "geo_id": ["A", "A", "B"],
                "year": [2020, 2020, 2020],
                "extra_covariate": [1.0, 2.0, 3.0],
            }
        ).to_parquet(source)

        result = runner.invoke(
            app,
            [
                "panel",
                "enrich",
                "--panel",
                str(panel),
                "--source",
                str(source),
                "--columns",
                "extra_covariate",
                "--output",
                str(tmp_path / "enriched.parquet"),
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "Source rows are not unique by join keys" in result.output

    def test_panel_enrich_renames_source_columns_before_collision_check(
        self, tmp_path: Path
    ):
        panel = tmp_path / "panel.parquet"
        source = tmp_path / "source.parquet"
        output = tmp_path / "enriched.parquet"
        pd.DataFrame(
            {
                "msa_id": ["10100", "10200"],
                "year": [2024, 2024],
                "coverage_ratio": [0.95, 0.90],
            }
        ).to_parquet(panel)
        pd.DataFrame(
            {
                "msa_id": ["10100", "10200"],
                "year": [2024, 2024],
                "coverage_ratio": [1.0, 0.5],
                "unauthorized_immigrant_population": [1000.0, 500.0],
            }
        ).to_parquet(source)

        result = runner.invoke(
            app,
            [
                "panel",
                "enrich",
                "--panel",
                str(panel),
                "--source",
                str(source),
                "--columns",
                "coverage_ratio:mpi_coverage_ratio,unauthorized_immigrant_population",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["source_columns"] == [
            "mpi_coverage_ratio",
            "unauthorized_immigrant_population",
        ]
        assert payload["source_column_renames"] == {
            "coverage_ratio": "mpi_coverage_ratio"
        }
        enriched = pd.read_parquet(output)
        assert enriched["coverage_ratio"].tolist() == [0.95, 0.90]
        assert enriched["mpi_coverage_ratio"].tolist() == [1.0, 0.5]
        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["source_column_renames"] == {
            "coverage_ratio": "mpi_coverage_ratio"
        }

    def test_panel_enrich_rejects_duplicate_source_rename_specs(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        source = tmp_path / "source.parquet"
        pd.DataFrame(
            {
                "geo_id": ["A", "B"],
                "year": [2020, 2020],
                "extra_covariate": [1.0, 2.0],
            }
        ).to_parquet(source)

        result = runner.invoke(
            app,
            [
                "panel",
                "enrich",
                "--panel",
                str(panel),
                "--source",
                str(source),
                "--columns",
                "extra_covariate:first,extra_covariate:second",
                "--output",
                str(tmp_path / "enriched.parquet"),
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "duplicate input columns" in result.output
