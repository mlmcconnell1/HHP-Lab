"""CLI command for materializing MEDSL county presidential artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.medsl.materialize import materialize_county_political_leaning
from hhplab.paths import curated_dir


def build_medsl_president_county(
    raw_path: Annotated[
        Path | None,
        typer.Option(
            "--raw-path",
            help="Path to staged MEDSL countypres_2000-2024.tab raw file.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for the materialized county measure parquet.",
        ),
    ] = None,
    county_vintage: Annotated[
        int,
        typer.Option("--county-vintage", help="Census county geometry vintage."),
    ] = 2020,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Rebuild even if the materialized output already exists.",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output structured JSON instead of human-readable text."),
    ] = False,
) -> None:
    """Materialize the MEDSL county presidential political artifact."""
    if output_dir is None:
        output_dir = curated_dir("medsl")

    try:
        result_path = materialize_county_political_leaning(
            raw_path=raw_path,
            output_dir=output_dir,
            county_vintage=county_vintage,
            force=force,
        )
    except (FileNotFoundError, ValueError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error materializing MEDSL county presidential artifact: {exc}", err=True)
        raise typer.Exit(2) from exc

    row_count = len(pd.read_parquet(result_path))
    payload = {
        "status": "ok",
        "provider": "medsl",
        "product": "president",
        "county_vintage": county_vintage,
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if json_output:
        typer.echo(json.dumps(payload))
        return

    typer.echo("MEDSL county presidential materialization complete:")
    typer.echo(f"  County vintage: {county_vintage}")
    typer.echo(f"  Rows: {row_count}")
    typer.echo(f"  Output: {result_path}")
