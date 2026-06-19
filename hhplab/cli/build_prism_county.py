"""CLI command for materializing PRISM monthly rasters to county parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.prism import materialize_prism_monthly_counties


def build_prism_county(
    variable: Annotated[
        str,
        typer.Option("--variable", "-v", help="PRISM temperature variable: tmin, tmean, or tmax."),
    ] = "tmin",
    year: Annotated[int, typer.Option("--year", "-y", help="PRISM data year.")] = 2024,
    month: Annotated[int, typer.Option("--month", "-m", help="PRISM data month, 1-12.")] = 1,
    county_vintage: Annotated[
        int,
        typer.Option("--county-vintage", help="Census county geometry vintage."),
    ] = 2023,
    raw_zip: Annotated[
        Path | None,
        typer.Option("--raw-zip", help="Override path to retained PRISM raw ZIP."),
    ] = None,
    county_geometry: Annotated[
        Path | None,
        typer.Option("--county-geometry", help="Override path to county geometry parquet."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Override output parquet path."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output structured JSON instead of human-readable text."),
    ] = False,
) -> None:
    """Materialize one PRISM monthly raster to county means."""
    try:
        output_path = materialize_prism_monthly_counties(
            variable=variable,
            year=year,
            month=month,
            county_vintage=county_vintage,
            raw_zip_path=raw_zip,
            county_geometry_path=county_geometry,
            output_path=output,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error materializing PRISM county data: {exc}", err=True)
        raise typer.Exit(1) from exc

    row_count = len(pd.read_parquet(output_path))
    payload = {
        "status": "ok",
        "provider": "prism",
        "product": "temperature",
        "variable": variable,
        "year": year,
        "month": month,
        "county_vintage": county_vintage,
        "output_path": str(output_path),
        "row_count": row_count,
    }
    if json_output:
        typer.echo(json.dumps(payload))
        return

    typer.echo("PRISM county materialization complete:")
    typer.echo(f"  Variable: {variable}")
    typer.echo(f"  Period: {year}-{month:02d}")
    typer.echo(f"  Counties: {row_count}")
    typer.echo(f"  Output: {output_path}")
