"""CLI command for ingesting PRISM monthly temperature raw ZIPs."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.output import JsonOutput, cli_error, emit_result
from hhplab.prism import download_prism_monthly


def ingest_prism(
    variable: Annotated[
        str,
        typer.Option(
            "--variable",
            "-v",
            help="PRISM temperature variable: tmin, tmean, or tmax.",
        ),
    ] = "tmin",
    year: Annotated[
        int,
        typer.Option("--year", "-y", help="PRISM data year."),
    ] = 2024,
    month: Annotated[
        int,
        typer.Option("--month", "-m", help="PRISM data month, 1-12."),
    ] = 1,
    force: Annotated[
        bool,
        typer.Option("--force", help="Re-download even if the raw ZIP is already cached."),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Download and register one PRISM monthly temperature ZIP."""
    try:
        result = download_prism_monthly(variable=variable, year=year, month=month, force=force)
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        cli_error(exc, json_output, human_prefix="Error ingesting PRISM data")

    payload = {
        "status": "ok",
        "provider": "prism",
        "product": "temperature",
        "variable": result.variable,
        "year": result.year,
        "month": result.month,
        "source_url": result.source_url,
        "raw_path": str(result.path),
        "file_size": result.file_size,
        "raw_sha256": result.raw_sha256,
        "cached": result.cached,
    }
    if emit_result(payload, json_output):
        return

    typer.echo("PRISM raw ingestion complete:")
    typer.echo(f"  Variable: {result.variable}")
    typer.echo(f"  Period: {result.year}-{result.month:02d}")
    typer.echo(f"  Raw: {result.path}")
    typer.echo(f"  Cached: {result.cached}")
