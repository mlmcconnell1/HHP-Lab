"""CLI entrypoints for reproducing documented result workflows."""

from __future__ import annotations

import contextlib
import io
import json
import runpy
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts"


@dataclass(frozen=True)
class ResultWorkflow:
    name: str
    description: str
    scripts: tuple[str, ...]


RESULT_WORKFLOWS: dict[str, ResultWorkflow] = {
    "top50-msa-coc-pit-contract-rent-2010-2020": ResultWorkflow(
        name="top50-msa-coc-pit-contract-rent-2010-2020",
        description="Generate the top-50 MSA/CoC PIT contract-rent 2010-2020 panel.",
        scripts=("generate_top50_msa_coc_pit_contract_rent_2010_2020.py",),
    ),
    "poverty-longitudinal": ResultWorkflow(
        name="poverty-longitudinal",
        description="Build the pooled MSA poverty longitudinal panel.",
        scripts=("build_poverty_longitudinal_panel.py",),
    ),
    "irs-migration-pooled": ResultWorkflow(
        name="irs-migration-pooled",
        description="Build the pooled MSA IRS migration panel.",
        scripts=("build_irs_migration_pooled_panel.py",),
    ),
    "supply-iv": ResultWorkflow(
        name="supply-iv",
        description="Build the housing-supply IV analysis panel with default inputs.",
        scripts=("build_supply_iv_panel.py",),
    ),
    "vera-hic-pit-panel": ResultWorkflow(
        name="vera-hic-pit-panel",
        description="Build the pooled Vera/HIC/PIT panel.",
        scripts=("build_vera_hic_pit_panel.py",),
    ),
    "vera-hic-pit-longitudinal": ResultWorkflow(
        name="vera-hic-pit-longitudinal",
        description="Build the top-50 Vera/HIC/PIT longitudinal panel.",
        scripts=("build_vera_hic_pit_longitudinal.py",),
    ),
    "vera-hic-pit-longitudinal-pooled": ResultWorkflow(
        name="vera-hic-pit-longitudinal-pooled",
        description="Build the pooled top-150 Vera/HIC/PIT longitudinal panel.",
        scripts=("build_vera_hic_pit_longitudinal_pooled.py",),
    ),
    "vera-hic-pit-correlations": ResultWorkflow(
        name="vera-hic-pit-correlations",
        description="Run Vera/HIC/PIT correlation summaries from the built panel.",
        scripts=("vera_hic_pit_correlations.py",),
    ),
    "vera-hic-pit": ResultWorkflow(
        name="vera-hic-pit",
        description="Reproduce documented Vera/HIC/PIT panel and correlation artifacts.",
        scripts=(
            "build_vera_hic_pit_panel.py",
            "build_vera_hic_pit_longitudinal.py",
            "build_vera_hic_pit_longitudinal_pooled.py",
            "vera_hic_pit_correlations.py",
        ),
    ),
    "overdose-lag": ResultWorkflow(
        name="overdose-lag",
        description="Build the overdose lag panel.",
        scripts=("build_overdose_lag_panel.py",),
    ),
    "overdose-hic-category-correlations": ResultWorkflow(
        name="overdose-hic-category-correlations",
        description="Run overdose/HIC category correlation summaries.",
        scripts=("overdose_hic_category_correlations.py",),
    ),
    "overdose-hic": ResultWorkflow(
        name="overdose-hic",
        description="Reproduce documented overdose/HIC panel and correlation artifacts.",
        scripts=(
            "build_overdose_lag_panel.py",
            "overdose_hic_category_correlations.py",
        ),
    ),
    "renter-household-share-composition": ResultWorkflow(
        name="renter-household-share-composition",
        description="Build the renter-household-share composition panel and regressions.",
        scripts=("build_renter_household_share_composition_panel.py",),
    ),
    "household-size-composition": ResultWorkflow(
        name="household-size-composition",
        description="Build the household-size composition panel and regressions.",
        scripts=("build_household_size_composition_panel.py",),
    ),
    "recent-mover-income-composition": ResultWorkflow(
        name="recent-mover-income-composition",
        description="Build the recent-mover-income composition panel and regressions.",
        scripts=("build_recent_mover_income_composition_panel.py",),
    ),
    "composition-rent-population-robustness": ResultWorkflow(
        name="composition-rent-population-robustness",
        description="Run tracked robustness checks for composition rent-population screens.",
        scripts=("analyze_composition_rent_population_robustness.py",),
    ),
    "composition-rent-population": ResultWorkflow(
        name="composition-rent-population",
        description="Reproduce all documented composition rent-population result artifacts.",
        scripts=(
            "build_renter_household_share_composition_panel.py",
            "build_household_size_composition_panel.py",
            "build_recent_mover_income_composition_panel.py",
            "analyze_composition_rent_population_robustness.py",
        ),
    ),
    "noncompositional-rent-population": ResultWorkflow(
        name="noncompositional-rent-population",
        description="Reproduce documented non-compositional rent-population result artifacts.",
        scripts=(
            "build_noncompositional_rent_population_panel.py",
            "analyze_noncompositional_rent_population_robustness.py",
        ),
    ),
    "all-documented-results": ResultWorkflow(
        name="all-documented-results",
        description="Run every current package-cataloged result workflow in dependency order.",
        scripts=(
            "generate_top50_msa_coc_pit_contract_rent_2010_2020.py",
            "build_poverty_longitudinal_panel.py",
            "build_irs_migration_pooled_panel.py",
            "build_supply_iv_panel.py",
            "build_vera_hic_pit_panel.py",
            "build_vera_hic_pit_longitudinal.py",
            "build_vera_hic_pit_longitudinal_pooled.py",
            "vera_hic_pit_correlations.py",
            "build_overdose_lag_panel.py",
            "overdose_hic_category_correlations.py",
            "build_renter_household_share_composition_panel.py",
            "build_household_size_composition_panel.py",
            "build_recent_mover_income_composition_panel.py",
            "analyze_composition_rent_population_robustness.py",
            "build_noncompositional_rent_population_panel.py",
            "analyze_noncompositional_rent_population_robustness.py",
        ),
    ),
}


def _workflow_choices() -> str:
    return ", ".join(sorted(RESULT_WORKFLOWS))


def _run_script(script_name: str) -> dict[str, object]:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(
            f"Result workflow script not found: {script_path}. "
            "Restore the tracked script or update RESULT_WORKFLOWS."
        )

    stdout = io.StringIO()
    inserted = False
    script_dir = str(SCRIPTS_DIR)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
        inserted = True
    try:
        with contextlib.redirect_stdout(stdout):
            runpy.run_path(str(script_path), run_name="__main__")
    finally:
        if inserted:
            sys.path.remove(script_dir)

    return {
        "script": str(script_path.relative_to(REPO_ROOT)),
        "stdout": stdout.getvalue().splitlines(),
    }


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
    for script_name in workflow.scripts:
        steps.append(_run_script(script_name))

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
