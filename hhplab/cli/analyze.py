"""CLI commands for panel analysis outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal

import typer

from hhplab.analyze import (
    AnalysisError,
    _json_safe,
    correlate_panel,
    describe_panel,
    lagged_associations_panel,
    list_analysis_manifests,
    read_analysis_manifest,
    regress_panel,
)

analyze_app = typer.Typer(
    name="analyze",
    help="Run statistical analyses on panel parquet files",
    no_args_is_help=True,
)
ledger_app = typer.Typer(
    name="ledger",
    help="List and inspect analysis manifest sidecars.",
    no_args_is_help=True,
)
analyze_app.add_typer(ledger_app, name="ledger")


def _parse_columns(value: str | None, *, option: str, required: bool = False) -> list[str] | None:
    if value is None:
        if required:
            raise typer.BadParameter(f"{option} is required.")
        return None
    columns = [part.strip() for part in value.split(",") if part.strip()]
    if required and not columns:
        raise typer.BadParameter(f"{option} must include at least one column.")
    return columns


def _parse_lags(value: str) -> list[int]:
    try:
        lags = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as exc:
        raise typer.BadParameter("--lags must be a comma-separated list of integers.") from exc
    if not lags or any(lag < 1 for lag in lags):
        raise typer.BadParameter("--lags must contain positive integers.")
    return lags


def _emit(result, *, json_output: bool) -> None:
    payload = result.to_payload()
    if json_output:
        typer.echo(json.dumps(_json_safe(payload), indent=2, default=str, allow_nan=False))
        return
    typer.echo(f"Wrote {payload['analysis_type']} analysis: {payload['output_path']}")
    typer.echo(f"Manifest: {payload['manifest_path']}")
    typer.echo(f"Rows: {len(payload['records'])}")


def _emit_payload(payload: dict, *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(_json_safe(payload), indent=2, default=str, allow_nan=False))
        return
    typer.echo(json.dumps(_json_safe(payload), indent=2, default=str, allow_nan=False))


@analyze_app.command("describe")
def analyze_describe(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    columns: Annotated[
        str | None,
        typer.Option("--columns", help="Comma-separated measure columns. Defaults to numeric."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output parquet path for analysis results."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Describe numeric panel measures with data-dictionary semantics."""
    try:
        result = describe_panel(
            panel,
            columns=_parse_columns(columns, option="--columns"),
            output_path=output,
        )
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(result, json_output=json_output)


@analyze_app.command("correlate")
def analyze_correlate(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    columns: Annotated[
        str,
        typer.Option("--columns", help="Comma-separated measure columns to correlate."),
    ],
    partial_controls: Annotated[
        str | None,
        typer.Option(
            "--partial-controls",
            help="Comma-separated controls to residualize before partial correlations.",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output parquet path for analysis results."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Compute pairwise and optional partial correlations."""
    try:
        result = correlate_panel(
            panel,
            columns=_parse_columns(columns, option="--columns", required=True) or [],
            partial_controls=_parse_columns(partial_controls, option="--partial-controls"),
            output_path=output,
        )
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(result, json_output=json_output)


@analyze_app.command("regress")
def analyze_regress(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    outcome: Annotated[
        str,
        typer.Option("--outcome", help="Outcome column."),
    ],
    predictors: Annotated[
        str,
        typer.Option("--predictors", help="Comma-separated predictor columns."),
    ],
    entity_column: Annotated[
        str,
        typer.Option("--entity-column", help="Entity/geography identifier column."),
    ] = "geo_id",
    year_column: Annotated[
        str,
        typer.Option("--year-column", help="Year column."),
    ] = "year",
    entity_fe: Annotated[
        bool,
        typer.Option("--entity-fe/--no-entity-fe", help="Include entity fixed effects."),
    ] = True,
    year_fe: Annotated[
        bool,
        typer.Option("--year-fe/--no-year-fe", help="Include year fixed effects."),
    ] = True,
    cluster_by: Annotated[
        str | None,
        typer.Option(
            "--cluster-by",
            help="Cluster standard errors by this column. Use empty string to disable.",
        ),
    ] = "geo_id",
    standardize: Annotated[
        Literal["none", "predictors", "all"],
        typer.Option(
            "--standardize",
            help="Z-score model columns before fitting. One of: none, predictors, all.",
        ),
    ] = "none",
    inference: Annotated[
        Literal["none", "wild-cluster", "permutation"],
        typer.Option(
            "--inference",
            help=(
                "Small-sample inference method. Use wild-cluster with --cluster-by, "
                "or permutation for cross-sectional treatment terms."
            ),
        ),
    ] = "none",
    inference_reps: Annotated[
        int,
        typer.Option("--inference-reps", help="Bootstrap/permutation repetitions."),
    ] = 999,
    inference_seed: Annotated[
        int,
        typer.Option("--inference-seed", help="Random seed for small-sample inference."),
    ] = 0,
    inference_terms: Annotated[
        str | None,
        typer.Option(
            "--inference-terms",
            help="Comma-separated model terms to test; defaults to predictors.",
        ),
    ] = None,
    endogenous: Annotated[
        str | None,
        typer.Option(
            "--endogenous",
            help=(
                "Predictor to instrument with 2SLS. Requires --instruments; the "
                "result includes first-stage instrument coefficients and a "
                "first-stage F diagnostic."
            ),
        ),
    ] = None,
    instruments: Annotated[
        str | None,
        typer.Option(
            "--instruments",
            help="Comma-separated excluded-instrument columns for --endogenous.",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output parquet path for analysis results."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Run OLS or 2SLS with optional entity/year fixed effects and clustered errors."""
    try:
        result = regress_panel(
            panel,
            outcome=outcome,
            predictors=_parse_columns(predictors, option="--predictors", required=True) or [],
            entity_column=entity_column,
            year_column=year_column,
            entity_fe=entity_fe,
            year_fe=year_fe,
            cluster_by=cluster_by or None,
            standardize=standardize,
            inference=inference,
            inference_reps=inference_reps,
            inference_seed=inference_seed,
            inference_terms=_parse_columns(inference_terms, option="--inference-terms"),
            endogenous=endogenous,
            instruments=_parse_columns(instruments, option="--instruments"),
            output_path=output,
        )
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(result, json_output=json_output)


@analyze_app.command("lagged")
def analyze_lagged(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    outcome: Annotated[
        str,
        typer.Option("--outcome", help="Outcome column."),
    ],
    predictors: Annotated[
        str,
        typer.Option("--predictors", help="Comma-separated predictor columns."),
    ],
    lags: Annotated[
        str,
        typer.Option("--lags", help="Comma-separated positive lags."),
    ] = "1",
    entity_column: Annotated[
        str,
        typer.Option("--entity-column", help="Entity/geography identifier column."),
    ] = "geo_id",
    year_column: Annotated[
        str,
        typer.Option("--year-column", help="Year column."),
    ] = "year",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output parquet path for analysis results."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Compute outcome associations with lagged predictors."""
    try:
        result = lagged_associations_panel(
            panel,
            outcome=outcome,
            predictors=_parse_columns(predictors, option="--predictors", required=True) or [],
            lags=_parse_lags(lags),
            entity_column=entity_column,
            year_column=year_column,
            output_path=output,
        )
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(result, json_output=json_output)


@ledger_app.command("list")
def ledger_list(
    directory: Annotated[
        Path,
        typer.Option(
            "--directory",
            "-d",
            help="Directory to recursively scan for *.manifest.json analysis sidecars.",
            exists=True,
            file_okay=False,
            dir_okay=True,
        ),
    ] = Path("."),
    analysis_type: Annotated[
        str | None,
        typer.Option("--analysis-type", help="Filter to one analysis type."),
    ] = None,
    panel: Annotated[
        str | None,
        typer.Option("--panel", help="Filter by exact panel path or panel name."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """List previous analysis invocations recorded as manifest sidecars."""
    try:
        analyses = list_analysis_manifests(
            directory,
            analysis_type=analysis_type,
            panel=panel,
        )
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit_payload(
        {
            "status": "ok",
            "directory": str(directory),
            "analysis_count": len(analyses),
            "analyses": analyses,
        },
        json_output=json_output,
    )


@ledger_app.command("show")
def ledger_show(
    manifest: Annotated[
        Path,
        typer.Option("--manifest", "-m", help="Analysis manifest path.", exists=True),
    ],
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Show one analysis invocation manifest."""
    try:
        payload = read_analysis_manifest(manifest)
    except AnalysisError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit_payload({"status": "ok", "manifest": payload}, json_output=json_output)
