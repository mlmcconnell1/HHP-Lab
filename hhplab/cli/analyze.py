"""CLI commands for panel analysis outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.analyze import (
    AnalysisError,
    correlate_panel,
    describe_panel,
    lagged_associations_panel,
    regress_panel,
)

analyze_app = typer.Typer(
    name="analyze",
    help="Run statistical analyses on panel parquet files",
    no_args_is_help=True,
)


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
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(f"Wrote {payload['analysis_type']} analysis: {payload['output_path']}")
    typer.echo(f"Rows: {len(payload['records'])}")


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
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output parquet path for analysis results."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Run OLS with optional entity/year fixed effects and clustered standard errors."""
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
