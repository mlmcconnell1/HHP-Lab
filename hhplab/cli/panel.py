"""CLI commands for inspecting built panel parquet files."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Annotated

import pandas as pd
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


def _json_safe(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if pd.isna(value) and not isinstance(value, (list, tuple, dict)):
        return None
    return value


def _format_describe(payload: dict) -> str:
    lines = [
        f"Panel: {payload.get('panel_path')}",
        (
            f"Rows: {payload.get('row_count', 0)} | Columns: {payload.get('column_count', 0)} | "
            f"Measures: {payload.get('measure_count', 0)}"
        ),
    ]
    geo_col = payload.get("geo_column")
    if geo_col is not None:
        lines.append(f"Geographies ({geo_col}): {payload.get('geography_count')}")
    if payload.get("year_min") is not None:
        lines.append(f"Years: {payload.get('year_min')}-{payload.get('year_max')}")

    year_coverage = payload.get("year_coverage") or {}
    missing_years = year_coverage.get("missing_years") or []
    if missing_years:
        lines.append(f"Missing years: {', '.join(str(year) for year in missing_years)}")
    else:
        lines.append("Missing years: none")

    measures = payload.get("measures") or []
    if measures:
        table = pd.DataFrame(measures)[
            ["column", "n", "missing", "missing_rate", "mean", "min", "median", "max"]
        ]
        lines.extend(["", "Measure summary:", table.to_string(index=False)])

    missing_geo_year = payload.get("missingness_by_geo_year") or []
    absent_cells = [record for record in missing_geo_year if not record.get("row_present", True)]
    if absent_cells:
        absent_table = pd.DataFrame(absent_cells[:10])
        display_columns = [
            column
            for column in [geo_col, "year", "row_present", "missing_measure_count", "measure_count"]
            if column in absent_table.columns
        ]
        lines.extend(
            ["", "Absent geo-year cells:", absent_table[display_columns].to_string(index=False)]
        )
    else:
        lines.extend(["", "Absent geo-year cells: none"])
    return "\n".join(lines)


def _format_query(payload: dict) -> str:
    lines = [
        f"Panel: {payload.get('panel_path')}",
        f"Rows: {payload.get('row_count', 0)}",
        f"Columns: {', '.join(payload.get('columns') or [])}",
    ]
    records = payload.get("records") or []
    if records:
        lines.extend(["", pd.DataFrame(records).to_string(index=False)])
    else:
        lines.append("No records matched.")
    return "\n".join(lines)


def _emit(payload: dict, *, json_output: bool) -> None:
    payload = _json_safe(payload)
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str, allow_nan=False))
        return
    if "measures" in payload:
        typer.echo(_format_describe(payload))
        return
    typer.echo(_format_query(payload))


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
