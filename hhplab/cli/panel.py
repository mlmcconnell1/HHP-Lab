"""CLI commands for inspecting built panel parquet files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.panel.inspect import PanelInspectError, describe_panel_file, query_panel_file

panel_app = typer.Typer(
    name="panel",
    help="Inspect built panel parquet files",
    no_args_is_help=True,
)


def _parse_columns(value: str | None) -> list[str] | None:
    if value is None:
        return None
    columns = [part.strip() for part in value.split(",") if part.strip()]
    return columns or None


def _emit(payload: dict, *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(f"Rows: {payload.get('row_count', 0)}")


@panel_app.command("describe")
def panel_describe(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    columns: Annotated[
        str | None,
        typer.Option("--columns", help="Comma-separated measure columns. Defaults to numeric."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Summarize panel measures, missingness, and coverage."""
    try:
        payload = describe_panel_file(panel, columns=_parse_columns(columns))
    except PanelInspectError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload, json_output=json_output)


@panel_app.command("query")
def panel_query(
    panel: Annotated[
        Path,
        typer.Option("--panel", "-p", help="Input panel parquet path.", exists=True),
    ],
    columns: Annotated[
        str | None,
        typer.Option("--columns", help="Comma-separated columns to return."),
    ] = None,
    where: Annotated[
        str | None,
        typer.Option("--where", help="Pandas query expression, e.g. year == 2024."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum rows to return.", min=1),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Filter and select panel rows."""
    try:
        payload = query_panel_file(
            panel,
            columns=_parse_columns(columns),
            where=where,
            limit=limit,
        )
    except PanelInspectError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(payload, json_output=json_output)
