"""CLI command for materializing curated metro boundary polygons."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.metro.metro_definitions import DEFINITION_VERSION


def generate_metro_boundaries(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Metro definition version to materialize.",
        ),
    ] = DEFINITION_VERSION,
    county_vintage: Annotated[
        int,
        typer.Option(
            "--counties",
            "-c",
            help="County geometry vintage used to dissolve metro polygons.",
        ),
    ] = ...,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite an existing metro boundaries artifact.",
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
    """Generate a curated metro boundary artifact from county geometries."""
    import json as json_mod

    import hhplab.naming as naming
    from hhplab.metro.metro_boundaries import generate_metro_boundaries as generate_impl
    from hhplab.metro.metro_boundaries import read_metro_boundaries

    output_path = naming.metro_boundaries_path(definition_version, county_vintage)
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
                f"Error: Metro boundaries artifact already exists at {output_path}. "
                "Use --force to overwrite.",
                err=True,
            )
        raise typer.Exit(1)

    if not json_output:
        typer.echo(
            "Generating metro boundary polygons "
            f"(definition={definition_version}, counties={county_vintage})..."
        )

    try:
        written_path = generate_impl(
            definition_version=definition_version,
            county_vintage=county_vintage,
        )
        boundaries = read_metro_boundaries(
            definition_version=definition_version,
            county_vintage=county_vintage,
        )
    except (FileNotFoundError, ValueError) as exc:
        payload = {"status": "error", "error": "generation_failed", "detail": str(exc)}
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    result = {
        "status": "ok",
        "definition_version": definition_version,
        "county_vintage": county_vintage,
        "artifact": str(written_path),
        "metro_count": len(boundaries),
    }
    if json_output:
        typer.echo(json_mod.dumps(result))
        return

    typer.echo(f"  Written: {written_path}")
    typer.echo(f"  Metros: {len(boundaries)}")
    typer.echo("Metro boundary generation complete.")
