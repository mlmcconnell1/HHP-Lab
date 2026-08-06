"""CLI command for ingesting official Census MSA boundary polygons."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.geographies.msa.msa_definitions import DEFINITION_VERSION, DELINEATION_FILE_YEAR


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
        int,
        typer.Option(
            "--year",
            "-y",
            help="TIGER/Line CBSA geometry year to ingest.",
        ),
    ] = DELINEATION_FILE_YEAR,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite an existing curated MSA boundary artifact.",
        ),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Ingest official MSA boundary polygons to curated GeoParquet."""
    import hhplab.naming as naming
    from hhplab.geographies.msa.msa_boundaries import ingest_msa_boundaries as ingest_impl
    from hhplab.geographies.msa.msa_boundaries import read_msa_boundaries

    output_path = naming.msa_boundaries_path(definition_version)
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
        written_path = ingest_impl(definition_version, tiger_year=tiger_year)
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
        "artifact": str(written_path),
        "msa_count": len(boundaries),
    }
    if emit_result(result, json_output):
        return

    typer.echo(f"  Written: {written_path}")
    typer.echo(f"  MSAs: {len(boundaries)}")
    typer.echo("MSA boundary ingest complete.")
