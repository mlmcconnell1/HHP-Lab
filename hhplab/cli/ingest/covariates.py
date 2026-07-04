"""CLI command for expanded covariate ingestion."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.paths import curated_dir


def ingest_covariate(
    source: Annotated[
        str,
        typer.Option("--source", help="Covariate source id; see `hhplab list covariates`."),
    ],
    raw_path: Annotated[
        Path,
        typer.Option("--raw-path", help="Path to staged provider CSV/TXT/Parquet."),
    ],
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Output directory for curated parquet."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Reprocess even if output already exists."),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Ingest staged hidden-cause covariate data to curated parquet."""
    try:
        result_path = ingest_covariate_source(
            source,
            raw_path,
            output_dir=output_dir or curated_dir("covariates"),
            force=force,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        cli_error(exc, json_output, code=2)

    row_count = len(pd.read_parquet(result_path))
    payload = {
        "status": "ok",
        "source_id": source,
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if emit_result(payload, json_output):
        return
    typer.echo(f"Successfully ingested covariate source {source}: {result_path}")
