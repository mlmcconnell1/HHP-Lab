"""CLI command for materializing MEDSL county presidential artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.sources.medsl.medsl.materialize import materialize_county_political_leaning
from hhplab.storage.paths import curated_dir


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
    json_output: JsonOutput = False,
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
        cli_error(
            exc,
            json_output,
            code=2,
            human_prefix="Error materializing MEDSL county presidential artifact",
        )

    row_count = len(pd.read_parquet(result_path))
    payload = {
        "status": "ok",
        "provider": "medsl",
        "product": "president",
        "county_vintage": county_vintage,
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if emit_result(payload, json_output):
        return

    typer.echo("MEDSL county presidential materialization complete:")
    typer.echo(f"  County vintage: {county_vintage}")
    typer.echo(f"  Rows: {row_count}")
    typer.echo(f"  Output: {result_path}")
