"""CLI command for generating curated metro definition artifacts."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.metro.definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
)


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

        hhplab generate metro

        hhplab generate metro --definition-version glynn_fox_v1

        hhplab generate metro --force
    """
    import json as json_mod

    import hhplab.naming as naming
    from hhplab.metro.io import write_metro_artifacts

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


def generate_metro_universe(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Canonical metro-universe definition version to generate.",
        ),
    ] = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    profile_definition_version: Annotated[
        str,
        typer.Option(
            "--profile-definition-version",
            help="Subset profile version to materialize over the universe.",
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
    """Generate canonical metro-universe and Glynn/Fox subset-profile artifacts."""
    import json as json_mod

    import hhplab.naming as naming
    from hhplab.metro.io import write_metro_universe_artifacts

    paths_to_write = [
        naming.metro_universe_path(definition_version),
        naming.metro_subset_membership_path(
            profile_definition_version,
            definition_version,
        ),
    ]
    existing = [path for path in paths_to_write if path.exists()]
    if existing and not force:
        payload = {
            "status": "error",
            "error": "artifacts_exist",
            "existing": [str(path) for path in existing],
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            paths_str = "\n".join(f"  - {path}" for path in existing)
            typer.echo(
                "Error: Metro-universe artifacts already exist:\n"
                f"{paths_str}\nUse --force to overwrite.",
                err=True,
            )
        raise typer.Exit(1)

    if not json_output:
        typer.echo(
            "Generating canonical metro-universe artifacts "
            f"(definition: {definition_version}, profile: {profile_definition_version})..."
        )

    try:
        universe_path, subset_path = write_metro_universe_artifacts(
            metro_definition_version=definition_version,
            profile_definition_version=profile_definition_version,
        )
    except (FileNotFoundError, ValueError) as exc:
        payload = {
            "status": "error",
            "error": "generation_failed",
            "detail": str(exc),
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if json_output:
        typer.echo(
            json_mod.dumps(
                {
                    "status": "ok",
                    "definition_version": definition_version,
                    "profile_definition_version": profile_definition_version,
                    "artifacts": {
                        "metro_universe": str(universe_path),
                        "subset_membership": str(subset_path),
                    },
                }
            )
        )
        return

    typer.echo(f"  Written: {universe_path}")
    typer.echo(f"  Written: {subset_path}")
    typer.echo("Metro-universe artifact generation complete.")
