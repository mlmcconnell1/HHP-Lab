"""CLI command for MEDSL county presidential returns ingestion."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.paths import curated_dir
from hhplab.sources.medsl.medsl.ingest import ingest_county_presidential_returns


def ingest_medsl_presidential(
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
            help="Output directory for curated parquet.",
        ),
    ] = None,
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
    """Normalize staged MEDSL county presidential returns to curated parquet."""
    if output_dir is None:
        output_dir = curated_dir("medsl")

    try:
        result_path = ingest_county_presidential_returns(
            raw_path=raw_path,
            output_dir=output_dir,
            force=force,
        )
    except FileNotFoundError as exc:
        cli_error(exc, json_output, code=2)
    except ValueError as exc:
        message = f"Validation failed: {exc}"
        cli_error(message, json_output, code=2)

    row_count = len(pd.read_parquet(result_path))
    payload = {
        "status": "ok",
        "provider": "medsl",
        "product": "county_presidential_returns",
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if emit_result(payload, json_output):
        return

    typer.echo(f"Successfully ingested MEDSL county presidential returns to: {result_path}")
