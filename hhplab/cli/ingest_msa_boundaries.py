"""CLI command for ingesting official Census MSA boundary polygons."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.msa.msa_definitions import DEFINITION_VERSION, DELINEATION_FILE_YEAR


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
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Ingest official MSA boundary polygons to curated GeoParquet."""
    import json as json_mod

    import hhplab.naming as naming
    from hhplab.msa.msa_boundaries import ingest_msa_boundaries as ingest_impl
    from hhplab.msa.msa_boundaries import read_msa_boundaries

    output_path = naming.msa_boundaries_path(definition_version)
    if output_path.exists() and not force:
        payload = {
            "status": "error",
            "error": "artifact_exists",
            "existing": str(output_path),
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(
                f"Error: MSA boundary artifact already exists at {output_path}. "
                "Use --force to overwrite.",
                err=True,
            )
        raise typer.Exit(1)

    if not json_output:
        typer.echo(
            "Ingesting official MSA boundary polygons "
            f"(definition={definition_version}, year={tiger_year})..."
        )

    try:
        written_path = ingest_impl(definition_version, tiger_year=tiger_year)
        boundaries = read_msa_boundaries(definition_version)
    except ValueError as exc:
        payload = {"status": "error", "error": "validation_failed", "detail": str(exc)}
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    result = {
        "status": "ok",
        "definition_version": definition_version,
        "geometry_vintage": tiger_year,
        "artifact": str(written_path),
        "msa_count": len(boundaries),
    }
    if json_output:
        typer.echo(json_mod.dumps(result))
        return

    typer.echo(f"  Written: {written_path}")
    typer.echo(f"  MSAs: {len(boundaries)}")
    typer.echo("MSA boundary ingest complete.")
