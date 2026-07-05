"""CLI command for expanded covariate ingestion."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.covariates.catalog import covariate_source_spec
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.paths import curated_dir
from hhplab.provenance import read_provenance


def ingest_covariate(
    source: Annotated[
        str,
        typer.Option("--source", help="Covariate source id; see `hhplab list covariates`."),
    ],
    raw_path: Annotated[
        Path,
        typer.Option("--raw-path", help="Path to staged provider CSV/TXT/Parquet/XLSX."),
    ],
    county_reference_path: Annotated[
        Path | None,
        typer.Option(
            "--county-reference-path",
            help="County name/FIPS reference for source-specific county XLSX ingests.",
        ),
    ] = None,
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
            county_reference_path=county_reference_path,
            force=force,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        cli_error(exc, json_output, code=2)

    row_count = len(pd.read_parquet(result_path))
    spec = covariate_source_spec(source)
    provenance = read_provenance(result_path)
    extra = provenance.extra if provenance is not None else {}
    payload = {
        "status": "ok",
        "source_id": source,
        "output_path": str(result_path),
        "row_count": row_count,
        "measure_columns": list(spec.measure_columns),
        "skipped_rows": extra.get("skipped_rows", 0),
        "warnings": extra.get("skipped_reasons", {}),
    }
    if emit_result(payload, json_output):
        return
    typer.echo(f"Successfully ingested covariate source {source}: {result_path}")
