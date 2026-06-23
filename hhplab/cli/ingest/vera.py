"""CLI command for Vera county incarceration trends ingestion."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.paths import curated_dir
from hhplab.vera.ingest import ingest_county_incarceration_trends


def ingest_vera_incarceration(
    raw_path: Annotated[
        Path | None,
        typer.Option(
            "--raw-path",
            help="Path to staged Vera incarceration_trends_county.csv raw file.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for curated parquet.",
        ),
    ] = None,
    county_vintage: Annotated[
        int,
        typer.Option("--county-vintage", help="Census county geometry vintage."),
    ] = 2020,
    download: Annotated[
        bool,
        typer.Option(
            "--download/--no-download",
            help="Download the public Vera CSV when the raw file is not staged.",
        ),
    ] = True,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Reprocess even if the curated output already exists.",
        ),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Ingest Vera county incarceration trends to panel-ready county-year parquet."""
    if output_dir is None:
        output_dir = curated_dir("vera")

    try:
        result_path = ingest_county_incarceration_trends(
            raw_path=raw_path,
            output_dir=output_dir,
            county_vintage=county_vintage,
            download=download,
            force=force,
        )
    except FileNotFoundError as exc:
        cli_error(exc, json_output, code=2)
    except ValueError as exc:
        cli_error(f"Validation failed: {exc}", json_output, code=2)

    row_count = len(pd.read_parquet(result_path))
    payload = {
        "status": "ok",
        "provider": "vera",
        "product": "incarceration_trends",
        "county_vintage": county_vintage,
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if emit_result(payload, json_output):
        return

    typer.echo(f"Successfully ingested Vera county incarceration trends to: {result_path}")
