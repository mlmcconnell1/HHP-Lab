"""CLI command for generating curated metro definition artifacts."""

from __future__ import annotations

from typing import Annotated

import typer

from coclab.metro.definitions import DEFINITION_VERSION


def generate_metro(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Metro definition version to generate.",
        ),
    ] = DEFINITION_VERSION,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing artifacts if they already exist.",
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
    """Generate curated metro definition parquet files.

    Writes metro definitions, CoC membership, and county membership
    artifacts to data/curated/metro/ from the in-code Glynn/Fox constants.

    Examples:

        coclab generate metro

        coclab generate metro --definition-version glynn_fox_v1

        coclab generate metro --force
    """
    import json as json_mod

    import coclab.naming as naming
    from coclab.metro.io import write_metro_artifacts

    # Check for existing artifacts unless --force
    paths_to_write = [
        naming.metro_definitions_path(definition_version),
        naming.metro_coc_membership_path(definition_version),
        naming.metro_county_membership_path(definition_version),
    ]
    existing = [p for p in paths_to_write if p.exists()]
    if existing and not force:
        if json_output:
            typer.echo(json_mod.dumps({
                "status": "error",
                "error": "artifacts_exist",
                "existing": [str(p) for p in existing],
            }))
        else:
            paths_str = "\n".join(f"  - {p}" for p in existing)
            typer.echo(
                f"Error: Metro artifacts already exist:\n{paths_str}\n"
                "Use --force to overwrite.",
                err=True,
            )
        raise typer.Exit(1)

    if not json_output:
        typer.echo(f"Generating metro artifacts (version: {definition_version})...")

    try:
        defs_path, coc_path, county_path = write_metro_artifacts(
            definition_version=definition_version,
        )
    except ValueError as exc:
        if json_output:
            typer.echo(json_mod.dumps({
                "status": "error",
                "error": "validation_failed",
                "detail": str(exc),
            }))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if json_output:
        typer.echo(json_mod.dumps({
            "status": "ok",
            "definition_version": definition_version,
            "artifacts": {
                "definitions": str(defs_path),
                "coc_membership": str(coc_path),
                "county_membership": str(county_path),
            },
        }))
    else:
        typer.echo(f"  Written: {defs_path}")
        typer.echo(f"  Written: {coc_path}")
        typer.echo(f"  Written: {county_path}")
        typer.echo("Metro artifact generation complete.")
