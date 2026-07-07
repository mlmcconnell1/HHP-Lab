"""Tests for hhplab analyze commands."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.covariates.mpi_contract import MPI_MEASURE_COLUMNS
from hhplab.naming import (
    analysis_manifest_filename,
    analysis_manifest_path,
    analysis_output_filename,
    analysis_output_path,
)
from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

runner = CliRunner()


def _panel_fixture(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C"],
            "year": [2020, 2021, 2022, 2023] * 3,
            "pit_total": [10.0, 12.0, 14.0, 15.0, 20.0, 19.0, 18.0, 17.0, 8.0, 9.0, 11.0, 12.0],
            "total_population": [
                1000.0,
                1000.0,
                1010.0,
                1015.0,
                2000.0,
                2000.0,
                2010.0,
                2020.0,
                900.0,
                910.0,
                915.0,
                920.0,
            ],
            "median_gross_rent": [
                900.0,
                950.0,
                1000.0,
                1040.0,
                1100.0,
                1125.0,
                1150.0,
                1175.0,
                850.0,
                875.0,
                900.0,
                925.0,
            ],
            "unemployment_rate": [
                0.05,
                0.06,
                0.055,
                0.052,
                0.08,
                0.075,
                0.07,
                0.068,
                0.045,
                0.047,
                0.049,
                0.048,
            ],
            "policy_indicator": [0, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 1],
            "constant_policy_indicator": [1] * 12,
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


def _saturated_panel_fixture(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "B", "B", "B"],
            "year": [2020, 2021, 2022, 2020, 2021, 2022],
            "pit_total": [10.0, 12.0, 14.0, 20.0, 19.0, 18.0],
            "median_gross_rent": [900.0, 950.0, 1000.0, 1100.0, 1125.0, 1150.0],
            "unemployment_rate": [0.05, 0.06, 0.055, 0.08, 0.075, 0.07],
        }
    )
    df.to_parquet(path)
    return path


class TestAnalyzeCli:
    def test_regress_help_documents_permutation_limitations(self):
        result = runner.invoke(app, ["analyze", "regress", "--help"])

        assert result.exit_code == 0
        assert "single-predictor" in result.output
        assert "correlated" in result.output
        assert "controls" in result.output

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

    def test_default_analysis_paths_round_trip_through_naming_helpers(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel__Y2020-2023@B2025.parquet")
        expected_output = analysis_output_path(panel, "describe")
        expected_manifest = analysis_manifest_path(expected_output)

        assert (
            analysis_output_filename(panel.name, "describe")
            == "panel__Y2020-2023@B2025__analysis_describe.parquet"
        )
        assert (
            analysis_manifest_filename(expected_output.name)
            == "panel__Y2020-2023@B2025__analysis_describe.manifest.json"
        )

        result = runner.invoke(
            app,
            [
                "analyze",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert Path(payload["output_path"]) == expected_output
        assert Path(payload["manifest_path"]) == expected_manifest
        assert expected_output.exists()
        assert expected_manifest.exists()

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

    def test_correlate_accepts_mpi_covariates_with_existing_panel_series(
        self,
        tmp_path: Path,
    ):
        panel = tmp_path / "mpi_panel.parquet"
        write_parquet_with_provenance(
            pd.DataFrame(
                {
                    "geo_type": ["msa", "msa", "msa"],
                    "geo_id": ["11111", "22222", "33333"],
                    "year": [2024, 2024, 2024],
                    "pit_total": [10.0, 20.0, 30.0],
                    "unauthorized_immigrant_population": [1000.0, 2500.0, 3600.0],
                    "unauthorized_immigrant_share_of_us_total": [0.01, 0.02, 0.03],
                }
            ),
            panel,
            ProvenanceBlock(
                geo_type="msa",
                extra={"recipe": "mpi-panel"},
            ),
        )
        output = tmp_path / "mpi_correlate.parquet"

        result = runner.invoke(
            app,
            [
                "analyze",
                "correlate",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,unauthorized_immigrant_population",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["analysis_type"] == "correlate"
        records = pd.read_parquet(output)
        assert records.loc[0, "left"] == "pit_total"
        assert records.loc[0, "right"] == "unauthorized_immigrant_population"
        assert records.loc[0, "n"] == 3

        conformance = run_conformance(
            pd.read_parquet(panel),
            PanelRequest(
                start_year=2024,
                end_year=2024,
                geo_type="msa",
                measure_columns=list(MPI_MEASURE_COLUMNS),
            ),
        )
        assert conformance.passed

        missing_result = runner.invoke(
            app,
            [
                "analyze",
                "correlate",
                "--panel",
                str(panel),
                "--columns",
                "pit_total,missing_mpi_measure",
                "--json",
            ],
        )
        assert missing_result.exit_code != 0
        assert "correlate references missing panel columns" in missing_result.output
        assert "missing_mpi_measure" in missing_result.output

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
        regression = pd.read_parquet(output)
        terms = set(regression["term"])
        assert {"Intercept", "median_gross_rent", "unemployment_rate"} <= terms
        assert (regression["dof"] > 0).all()
        assert "p_value" in regression.columns
        assert payload["dof"] > 0

    def test_regress_standardizes_predictors_and_flags_binary_terms(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "regress_standardized.parquet"

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
                "median_gross_rent,policy_indicator",
                "--no-entity-fe",
                "--no-year-fe",
                "--cluster-by",
                "",
                "--standardize",
                "predictors",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["standardize"] == "predictors"
        assert payload["standardized_terms"] == ["median_gross_rent"]
        assert payload["unstandardized_terms"] == ["policy_indicator"]

        regression = pd.read_parquet(output).set_index("term")
        rent = regression.loc["median_gross_rent"]
        policy = regression.loc["policy_indicator"]
        assert bool(rent["standardized"])
        assert rent["standardization"] == "predictors"
        assert rent["standardization_std"] > 0
        assert not bool(policy["standardized"])
        assert policy["standardization_note"] == "binary_indicator_not_standardized"

        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["parameters"]["standardize"] == "predictors"
        assert (
            provenance.extra["parameters"]["standardization"]["policy_indicator"]["note"]
            == "binary_indicator_not_standardized"
        )

    def test_regress_wild_cluster_bootstrap_p_values_are_recorded(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "regress_wild_cluster.parquet"

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
                "--cluster-by",
                "geo_id",
                "--inference",
                "wild-cluster",
                "--inference-reps",
                "19",
                "--inference-seed",
                "123",
                "--inference-terms",
                "median_gross_rent",
                "--output",
                str(output),
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["inference"] == "wild-cluster"
        assert payload["inference_reps"] == 19
        assert payload["inference_terms"] == ["median_gross_rent"]

        regression = pd.read_parquet(output).set_index("term")
        rent = regression.loc["median_gross_rent"]
        unemployment = regression.loc["unemployment_rate"]
        assert rent["inference_method"] == "wild-cluster"
        assert bool(rent["inference_term"])
        assert 0 <= rent["p_value"] <= 1
        assert rent["asymptotic_p_value"] != rent["p_value"]
        assert not bool(unemployment["inference_term"])
        assert unemployment["p_value"] == unemployment["asymptotic_p_value"]

        provenance = read_provenance(output)
        assert provenance is not None
        assert provenance.extra["parameters"]["inference"] == "wild-cluster"
        assert provenance.extra["parameters"]["inference_terms"] == ["median_gross_rent"]

    def test_regress_permutation_warns_when_control_predictors_are_retained(
        self,
        tmp_path: Path,
    ):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "regress_permutation.parquet"

        with pytest.warns(
            RuntimeWarning,
            match="Permutation inference is calibrated for single-predictor",
        ):
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
                    "median_gross_rent,policy_indicator",
                    "--no-entity-fe",
                    "--no-year-fe",
                    "--cluster-by",
                    "",
                    "--inference",
                    "permutation",
                    "--inference-reps",
                    "19",
                    "--inference-seed",
                    "456",
                    "--inference-terms",
                    "policy_indicator",
                    "--output",
                    str(output),
                    "--json",
                ],
            )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["inference"] == "permutation"
        assert payload["inference_terms"] == ["policy_indicator"]

        regression = pd.read_parquet(output).set_index("term")
        policy = regression.loc["policy_indicator"]
        rent = regression.loc["median_gross_rent"]
        assert policy["inference_method"] == "permutation"
        assert bool(policy["inference_term"])
        assert 0 <= policy["p_value"] <= 1
        assert policy["asymptotic_p_value"] != policy["p_value"]
        assert not bool(rent["inference_term"])
        assert rent["p_value"] == rent["asymptotic_p_value"]

    def test_regress_permutation_single_predictor_does_not_warn(
        self,
        tmp_path: Path,
    ):
        panel = _panel_fixture(tmp_path / "panel.parquet")
        output = tmp_path / "regress_permutation.parquet"

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
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
                    "policy_indicator",
                    "--no-entity-fe",
                    "--no-year-fe",
                    "--cluster-by",
                    "",
                    "--inference",
                    "permutation",
                    "--inference-reps",
                    "19",
                    "--inference-seed",
                    "456",
                    "--inference-terms",
                    "policy_indicator",
                    "--output",
                    str(output),
                    "--json",
                ],
            )

        runtime_warnings = [
            warning
            for warning in caught
            if issubclass(warning.category, RuntimeWarning)
        ]
        assert runtime_warnings == []
        assert result.exit_code == 0

        payload = json.loads(result.output)
        assert payload["inference"] == "permutation"
        assert payload["inference_terms"] == ["policy_indicator"]

    def test_regress_permutation_rejects_fixed_effect_models(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

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
                "policy_indicator",
                "--inference",
                "permutation",
                "--inference-reps",
                "9",
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "without fixed effects" in result.output

    def test_regress_standardize_rejects_constant_binary_predictor(self, tmp_path: Path):
        panel = _panel_fixture(tmp_path / "panel.parquet")

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
                "median_gross_rent,constant_policy_indicator",
                "--no-entity-fe",
                "--no-year-fe",
                "--cluster-by",
                "",
                "--standardize",
                "predictors",
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "constant_policy_indicator" in result.output
        assert "standard deviation is zero or undefined" in result.output

    def test_regress_rejects_saturated_fixed_effect_model(self, tmp_path: Path):
        panel = _saturated_panel_fixture(tmp_path / "panel.parquet")

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
                "--json",
            ],
        )

        assert result.exit_code != 0
        assert "saturated or rank-deficient" in result.output

    def test_describe_json_uses_null_for_non_finite_values(self, tmp_path: Path):
        panel = tmp_path / "panel.parquet"
        pd.DataFrame({"geo_id": ["A"], "year": [2020], "pit_total": [10.0]}).to_parquet(panel)

        result = runner.invoke(
            app,
            [
                "analyze",
                "describe",
                "--panel",
                str(panel),
                "--columns",
                "pit_total",
                "--json",
            ],
        )

        assert result.exit_code == 0
        assert "NaN" not in result.output
        assert "Infinity" not in result.output
        payload = json.loads(result.output)
        assert payload["records"][0]["std"] is None

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
        (tmp_path / "malformed.manifest.json").write_text("{not-json", encoding="utf-8")

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
