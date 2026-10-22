"""CLI command for ingesting official Census MSA boundary polygons."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.geographies.msa.msa_definitions import DEFINITION_VERSION, get_msa_source_spec


def ingest_msa_boundaries(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="MSA definition version to align the polygons to.",
        ),
    ] = DEFINITION_VERSION,
    tiger_year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="TIGER/Line CBSA geometry year to ingest.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite an existing curated MSA boundary artifact.",
        ),
    ] = False,
    source_file: Annotated[
        Path | None,
        typer.Option(
            "--source-file",
            help="Read a locally verified TIGER source archive instead of downloading it.",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ] = None,
    json_output: JsonOutput = False,
) -> None:
    """Ingest official MSA boundary polygons to curated GeoParquet."""
    import hhplab.artifacts.naming.naming as naming
    from hhplab.geographies.msa.msa_boundaries import ingest_msa_boundaries as ingest_impl
    from hhplab.geographies.msa.msa_boundaries import read_msa_boundaries

    output_path = naming.msa_boundaries_path(definition_version)
    spec = get_msa_source_spec(definition_version)
    tiger_year = tiger_year if tiger_year is not None else spec.geometry_vintage
    if output_path.exists() and not force:
        cli_error(
            f"MSA boundary artifact already exists at {output_path}. Use --force to overwrite.",
            json_output,
            json_payload={
                "status": "error",
                "error": "artifact_exists",
                "existing": str(output_path),
            },
        )

    if not json_output:
        typer.echo(
            "Ingesting official MSA boundary polygons "
            f"(definition={definition_version}, year={tiger_year})..."
        )

    try:
        if source_file is None:
            written_path = ingest_impl(definition_version, tiger_year=tiger_year)
        else:
            written_path = ingest_impl(
                definition_version,
                tiger_year=tiger_year,
                source_file=source_file,
            )
        boundaries = read_msa_boundaries(definition_version)
    except ValueError as exc:
        cli_error(
            exc,
            json_output,
            json_payload={"status": "error", "error": "validation_failed", "detail": str(exc)},
        )

    result = {
        "status": "ok",
        "definition_version": definition_version,
        "geometry_vintage": tiger_year,
        "definition_source_release_date": spec.source_release_date,
        "artifact": str(written_path),
        "msa_count": len(boundaries),
    }
    if emit_result(result, json_output):
        return

    typer.echo(f"  Written: {written_path}")
    typer.echo(f"  MSAs: {len(boundaries)}")
    typer.echo("MSA boundary ingest complete.")
