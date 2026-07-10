"""CLI entrypoints for reproducing documented result workflows."""

from __future__ import annotations

import contextlib
import importlib
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import numpy as np
import typer

from hhplab.results.findings import write_finding_sidecar_from_result

WORKFLOW_MODULE_PREFIX = "hhplab.results.workflows"


@dataclass(frozen=True)
class ResultWorkflow:
    name: str
    description: str
    modules: tuple[str, ...]


RESULT_WORKFLOWS: dict[str, ResultWorkflow] = {
    "top50-msa-coc-pit-contract-rent-2010-2020": ResultWorkflow(
        name="top50-msa-coc-pit-contract-rent-2010-2020",
        description="Generate the top-50 MSA/CoC PIT contract-rent 2010-2020 panel.",
        modules=("generate_top50_msa_coc_pit_contract_rent_2010_2020",),
    ),
    "poverty-longitudinal": ResultWorkflow(
        name="poverty-longitudinal",
        description="Build the pooled MSA poverty longitudinal panel.",
        modules=("build_poverty_longitudinal_panel",),
    ),
    "irs-migration-pooled": ResultWorkflow(
        name="irs-migration-pooled",
        description="Build the pooled MSA IRS migration panel and direct-income rent screens.",
        modules=("build_irs_migration_pooled_panel",),
    ),
    "supply-iv": ResultWorkflow(
        name="supply-iv",
        description="Build the housing-supply IV analysis panel with default inputs.",
        modules=("build_supply_iv_panel",),
    ),
    "vera-hic-pit-panel": ResultWorkflow(
        name="vera-hic-pit-panel",
        description="Build the pooled Vera/HIC/PIT panel.",
        modules=("build_vera_hic_pit_panel",),
    ),
    "vera-hic-pit-longitudinal": ResultWorkflow(
        name="vera-hic-pit-longitudinal",
        description="Build the top-50 Vera/HIC/PIT longitudinal panel.",
        modules=("build_vera_hic_pit_longitudinal",),
    ),
    "vera-hic-pit-longitudinal-pooled": ResultWorkflow(
        name="vera-hic-pit-longitudinal-pooled",
        description="Build the pooled top-150 Vera/HIC/PIT longitudinal panel.",
        modules=("build_vera_hic_pit_longitudinal_pooled",),
    ),
    "vera-hic-pit-correlations": ResultWorkflow(
        name="vera-hic-pit-correlations",
        description="Run Vera/HIC/PIT correlation summaries from the built panel.",
        modules=("vera_hic_pit_correlations",),
    ),
    "vera-hic-pit": ResultWorkflow(
        name="vera-hic-pit",
        description="Reproduce documented Vera/HIC/PIT panel and correlation artifacts.",
        modules=(
            "build_vera_hic_pit_panel",
            "build_vera_hic_pit_longitudinal",
            "build_vera_hic_pit_longitudinal_pooled",
            "vera_hic_pit_correlations",
        ),
    ),
    "overdose-lag": ResultWorkflow(
        name="overdose-lag",
        description="Build the overdose lag panel.",
        modules=("build_overdose_lag_panel",),
    ),
    "overdose-hic-category-correlations": ResultWorkflow(
        name="overdose-hic-category-correlations",
        description="Run overdose/HIC category correlation summaries.",
        modules=("overdose_hic_category_correlations",),
    ),
    "overdose-hic": ResultWorkflow(
        name="overdose-hic",
        description="Reproduce documented overdose/HIC panel and correlation artifacts.",
        modules=(
            "build_overdose_lag_panel",
            "overdose_hic_category_correlations",
        ),
    ),
    "renter-household-share-composition": ResultWorkflow(
        name="renter-household-share-composition",
        description="Build the renter-household-share composition panel and regressions.",
        modules=("build_renter_household_share_composition_panel",),
    ),
    "household-size-composition": ResultWorkflow(
        name="household-size-composition",
        description="Build the household-size composition panel and regressions.",
        modules=("build_household_size_composition_panel",),
    ),
    "recent-mover-income-composition": ResultWorkflow(
        name="recent-mover-income-composition",
        description="Build the recent-mover-income composition panel and regressions.",
        modules=("build_recent_mover_income_composition_panel",),
    ),
    "local-income-composition": ResultWorkflow(
        name="local-income-composition",
        description="Build the local-income composition panel and regressions.",
        modules=("build_local_income_composition_panel",),
    ),
    "employment-labor-force-composition": ResultWorkflow(
        name="employment-labor-force-composition",
        description="Build the ACS1 employment and labor-force composition panel and regressions.",
        modules=("build_employment_labor_force_composition_panel",),
    ),
    "income-inequality-composition": ResultWorkflow(
        name="income-inequality-composition",
        description="Build the income-inequality composition panel and regressions.",
        modules=("build_income_inequality_composition_panel",),
    ),
    "housing-cost-burden-composition": ResultWorkflow(
        name="housing-cost-burden-composition",
        description="Build the housing-cost-burden composition panel and timing-aware regressions.",
        modules=("build_housing_cost_burden_composition_panel",),
    ),
    "subsidized-housing-stock": ResultWorkflow(
        name="subsidized-housing-stock",
        description="Build the HUD subsidized-housing stock panel and rent-growth regressions.",
        modules=("build_subsidized_housing_stock_panel",),
    ),
    "eviction-rate-timing": ResultWorkflow(
        name="eviction-rate-timing",
        description="Build the Eviction Lab timing-aware rent-growth panel and regressions.",
        modules=("build_eviction_rate_timing_panel",),
    ),
    "qcew-labor-market": ResultWorkflow(
        name="qcew-labor-market",
        description="Build the QCEW labor-market panel and rent-growth regressions.",
        modules=("build_qcew_labor_market_panel",),
    ),
    "bps-valuation-benchmark": ResultWorkflow(
        name="bps-valuation-benchmark",
        description="Reproduce BPS mix-adjusted permit valuation benchmark checks.",
        modules=("build_bps_valuation_benchmark",),
    ),
    "bps-valuation-rent-channel": ResultWorkflow(
        name="bps-valuation-rent-channel",
        description=(
            "Test whether BPS mix-adjusted permit valuation growth leads MSA rent growth."
        ),
        modules=("build_bps_valuation_rent_channel",),
    ),
    "composition-rent-population-robustness": ResultWorkflow(
        name="composition-rent-population-robustness",
        description="Run tracked robustness checks for composition rent-population screens.",
        modules=("analyze_composition_rent_population_robustness",),
    ),
    "composition-rent-population": ResultWorkflow(
        name="composition-rent-population",
        description="Reproduce all documented composition rent-population result artifacts.",
        modules=(
            "build_renter_household_share_composition_panel",
            "build_household_size_composition_panel",
            "build_recent_mover_income_composition_panel",
            "build_local_income_composition_panel",
            "build_employment_labor_force_composition_panel",
            "build_income_inequality_composition_panel",
            "build_housing_cost_burden_composition_panel",
            "analyze_composition_rent_population_robustness",
        ),
    ),
    "noncompositional-rent-population": ResultWorkflow(
        name="noncompositional-rent-population",
        description="Reproduce documented non-compositional rent-population result artifacts.",
        modules=(
            "build_noncompositional_rent_population_panel",
            "analyze_noncompositional_rent_population_robustness",
        ),
    ),
    "core-rent-shock-state-year-fe": ResultWorkflow(
        name="core-rent-shock-state-year-fe",
        description="Reproduce the pooled top-150 core rent-shock fixed-effects check.",
        modules=("analyze_core_rent_shock_state_year_fe",),
    ),
    "rent-growth-r2-decomposition": ResultWorkflow(
        name="rent-growth-r2-decomposition",
        description="Decompose rent-growth R-squared across all tracked covariate channels.",
        modules=("analyze_rent_growth_r2_decomposition",),
    ),
    "sanctuary-longdiff-robustness": ResultWorkflow(
        name="sanctuary-longdiff-robustness",
        description="Reproduce sanctuary long-difference state and California robustness checks.",
        modules=("analyze_sanctuary_longdiff_robustness",),
    ),
    "overdose-psh-state-year-robustness": ResultWorkflow(
        name="overdose-psh-state-year-robustness",
        description="Track PSH-to-overdose lag checks under year and state-year FE.",
        modules=("build_overdose_lag_panel", "analyze_overdose_psh_state_year_robustness"),
    ),
    "all-documented-results": ResultWorkflow(
        name="all-documented-results",
        description="Run every current package-cataloged result workflow in dependency order.",
        modules=(
            "generate_top50_msa_coc_pit_contract_rent_2010_2020",
            "build_poverty_longitudinal_panel",
            "build_irs_migration_pooled_panel",
            "build_supply_iv_panel",
            "build_vera_hic_pit_panel",
            "build_vera_hic_pit_longitudinal",
            "build_vera_hic_pit_longitudinal_pooled",
            "vera_hic_pit_correlations",
            "build_overdose_lag_panel",
            "overdose_hic_category_correlations",
            "build_renter_household_share_composition_panel",
            "build_household_size_composition_panel",
            "build_recent_mover_income_composition_panel",
            "build_local_income_composition_panel",
            "build_employment_labor_force_composition_panel",
            "build_income_inequality_composition_panel",
            "build_housing_cost_burden_composition_panel",
            "analyze_composition_rent_population_robustness",
            "build_noncompositional_rent_population_panel",
            "analyze_noncompositional_rent_population_robustness",
            "analyze_core_rent_shock_state_year_fe",
            "analyze_rent_growth_r2_decomposition",
            "analyze_sanctuary_longdiff_robustness",
            "analyze_overdose_psh_state_year_robustness",
            "build_eviction_rate_timing_panel",
            "build_qcew_labor_market_panel",
            "build_bps_valuation_benchmark",
            "build_bps_valuation_rent_channel",
        ),
    ),
}


def _workflow_choices() -> str:
    return ", ".join(sorted(RESULT_WORKFLOWS))


def _script_label(module_name: str) -> str:
    return f"scripts/{module_name}.py"


def _json_safe(value: object) -> object:
    """Recursively coerce numpy scalars/arrays into plain JSON-serializable values.

    Workflow `run()` results are built from pandas/numpy computations, which
    routinely carry numpy scalar types (e.g. int64 from `Series.unique()`)
    that the stdlib `json` module cannot serialize. This is applied once at
    the CLI boundary so no workflow module has to remember to cast values.
    """
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return _json_safe(value.tolist())
    return value


def _load_summary_from_stdout(stdout_lines: list[str]) -> dict[str, object] | None:
    for line in stdout_lines:
        prefix = "summary -> "
        if not line.startswith(prefix):
            continue
        path = Path(line.removeprefix(prefix).strip())
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
    return None


def _run_workflow_module(module_name: str) -> dict[str, object]:
    module = importlib.import_module(f"{WORKFLOW_MODULE_PREFIX}.{module_name}")
    run = getattr(module, "run", None)
    main = getattr(module, "main", None)
    callable_entrypoint = run if callable(run) else main
    if callable_entrypoint is None:
        raise AttributeError(
            f"Result workflow module {module.__name__} has no run() or main() function. "
            "Add run()/main() or update RESULT_WORKFLOWS."
        )
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        result = callable_entrypoint()

    stdout_lines = stdout.getvalue().splitlines()
    if result is None:
        result = _load_summary_from_stdout(stdout_lines)

    payload: dict[str, object] = {
        "script": _script_label(module_name),
        "module": module.__name__,
        "stdout": stdout_lines,
    }
    if result is not None:
        payload["result"] = _json_safe(result)
    return payload


def _attach_finding_sidecar(
    step: dict[str, object],
    *,
    workflow_id: str,
    module_name: str,
) -> None:
    result = step.get("result")
    if not isinstance(result, dict) or "finding_sidecar" in step:
        return
    finding_path = write_finding_sidecar_from_result(
        workflow_id=workflow_id,
        module_name=module_name,
        result=result,
    )
    if finding_path is not None:
        step["finding_sidecar"] = str(finding_path)


def build_result_cmd(
    workflow_name: Annotated[
        str,
        typer.Argument(
            help=f"Result workflow to run. Choices: {_workflow_choices()}",
            metavar="WORKFLOW",
        ),
    ],
    use_json: Annotated[
        bool,
        typer.Option("--json", help="Emit structured JSON."),
    ] = False,
) -> None:
    """Reproduce documented result artifacts through the package CLI."""

    workflow = RESULT_WORKFLOWS.get(workflow_name)
    if workflow is None:
        payload = {
            "status": "error",
            "error": (
                f"Unknown result workflow '{workflow_name}'. "
                f"Use one of: {_workflow_choices()}."
            ),
            "available_workflows": sorted(RESULT_WORKFLOWS),
        }
        if use_json:
            typer.echo(json.dumps(payload, indent=2))
        else:
            typer.echo(payload["error"], err=True)
        raise typer.Exit(2)

    steps: list[dict[str, object]] = []
    for module_name in workflow.modules:
        try:
            step = _run_workflow_module(module_name)
            _attach_finding_sidecar(step, workflow_id=workflow.name, module_name=module_name)
            steps.append(step)
        except Exception as exc:
            payload = {
                "status": "error",
                "workflow": workflow.name,
                "description": workflow.description,
                "failed_module": module_name,
                "failed_script": _script_label(module_name),
                "error": str(exc),
                "completed_steps": steps,
            }
            if use_json:
                typer.echo(json.dumps(payload, indent=2))
            else:
                typer.echo(
                    f"Error running result workflow '{workflow.name}' at "
                    f"{_script_label(module_name)}: {exc}",
                    err=True,
                )
            raise typer.Exit(1) from exc

    payload = {
        "status": "ok",
        "workflow": workflow.name,
        "description": workflow.description,
        "steps": steps,
    }
    if use_json:
        typer.echo(json.dumps(payload, indent=2))
        return

    typer.echo(f"Completed result workflow: {workflow.name}")
    for step in steps:
        typer.echo(f"- {step['script']}")
        for line in step["stdout"]:
            typer.echo(f"  {line}")
        if not step["stdout"] and step.get("result") is not None:
            # Workflows migrated to the run()-only contract print nothing to
            # stdout (main() is bypassed); fall back to the structured result
            # so plain-text mode isn't silently empty for those modules.
            typer.echo(json.dumps(step["result"], indent=2))
