"""Tests for covariate finding sidecars and ledger CLI."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from typer.testing import CliRunner

import hhplab.results.findings as findings_module
from hhplab.cli.build_cmds.results import _run_workflow_module
from hhplab.cli.main import app
from hhplab.results.findings import finding_from_result

runner = CliRunner()


def _workflow_result() -> dict[str, object]:
    return {
        "summary": {"years": [2020, 2021], "levels_rows": 2, "msa_count": 1},
        "outputs": {"regressions_parquet": "outputs/example/regressions.parquet"},
        "regressions": [
            {
                "family": "example_family",
                "model": "example_state_year",
                "outcome": "d_log_zori",
                "fixed_effects": "primary_state_year",
                "term": "d_log_pop",
                "estimate": 0.2,
                "std_error": 0.1,
                "p_value": 0.10,
                "nobs": 20,
                "std_error_type": "clustered:msa_id",
            },
            {
                "family": "example_family",
                "model": "example_state_year",
                "outcome": "d_log_zori",
                "fixed_effects": "primary_state_year",
                "term": "example_covariate",
                "estimate": -0.3,
                "std_error": 0.1,
                "p_value": 0.01,
                "nobs": 20,
                "std_error_type": "clustered:msa_id",
            },
        ],
    }


def test_finding_from_result_derives_standard_contract() -> None:
    finding = finding_from_result(
        workflow_id="example-workflow",
        module_name="build_irs_migration_pooled_panel",
        result=_workflow_result(),
    )

    assert finding is not None
    payload = finding.to_payload()
    assert payload["workflow_id"] == "example-workflow"
    assert payload["source_id"] == "irs_soi_migration"
    assert payload["direction"] == "negative"
    assert payload["primary_spec"]["model"] == "example_state_year"
    assert payload["key_terms"][0]["term"] == "example_covariate"
    assert payload["sample_window"]["start_year"] == 2020
    assert payload["coverage_notes"]["levels_rows"] == 2


def test_run_workflow_module_writes_finding_sidecar(tmp_path) -> None:
    module = SimpleNamespace(
        __name__="hhplab.results.workflows.build_irs_migration_pooled_panel",
        run=_workflow_result,
        main=lambda: None,
    )

    with (
        patch.object(findings_module, "FINDINGS_DIR", tmp_path),
        patch("hhplab.cli.build_cmds.results.importlib.import_module", return_value=module),
    ):
        payload = _run_workflow_module(
            "build_irs_migration_pooled_panel",
            workflow_id="irs-migration-pooled",
        )

    sidecar = tmp_path / "irs-migration-pooled__build_irs_migration_pooled_panel.finding.json"
    assert payload["finding_sidecar"] == str(sidecar)
    assert sidecar.exists()
    assert json.loads(sidecar.read_text())["direction"] == "negative"


def test_covariate_findings_list_and_show_cli(tmp_path) -> None:
    finding = finding_from_result(
        workflow_id="example-workflow",
        module_name="build_irs_migration_pooled_panel",
        result=_workflow_result(),
    )
    assert finding is not None
    sidecar = tmp_path / "example.finding.json"
    sidecar.write_text(json.dumps(finding.to_payload()) + "\n", encoding="utf-8")

    list_result = runner.invoke(
        app,
        ["list", "covariate-findings", "--directory", str(tmp_path), "--json"],
    )

    assert list_result.exit_code == 0, list_result.output
    list_payload = json.loads(list_result.output)
    assert list_payload["finding_count"] == 1
    assert list_payload["findings"][0]["source_id"] == "irs_soi_migration"

    show_result = runner.invoke(
        app,
        ["show", "covariate-finding", "--finding", str(sidecar), "--json"],
    )

    assert show_result.exit_code == 0, show_result.output
    show_payload = json.loads(show_result.output)
    assert show_payload["finding"]["workflow_id"] == "example-workflow"
