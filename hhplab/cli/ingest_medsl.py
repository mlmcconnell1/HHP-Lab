"""CLI command for MEDSL county presidential returns ingestion."""

from pathlib import Path
from typing import Annotated

import typer

from hhplab.medsl.ingest import ingest_county_presidential_returns
from hhplab.paths import curated_dir


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
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    except ValueError as exc:
        typer.echo(f"Error: Validation failed: {exc}", err=True)
        raise typer.Exit(2) from exc

    typer.echo(f"Successfully ingested MEDSL county presidential returns to: {result_path}")
