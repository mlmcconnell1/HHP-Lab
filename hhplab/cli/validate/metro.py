"""CLI command for validating curated metro artifacts."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
)


def validate_metro(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Metro definition version to validate.",
        ),
    ] = DEFINITION_VERSION,
    county_vintage: Annotated[
        int,
        typer.Option(
            "--counties",
            "-c",
            help="County geometry vintage for the metro boundary artifact.",
        ),
    ] = ...,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Validate curated metro definitions, memberships, and boundary polygons."""
    import json as json_mod

    from hhplab.metro.metro_boundaries import validate_curated_metro_boundaries
    from hhplab.metro.metro_io import validate_curated_metro

    try:
        definition_result = validate_curated_metro(definition_version)
        boundary_result = validate_curated_metro_boundaries(
            definition_version=definition_version,
            county_vintage=county_vintage,
        )
    except FileNotFoundError as exc:
        payload = {
            "status": "error",
            "definition_version": definition_version,
            "county_vintage": county_vintage,
            "errors": [str(exc)],
            "warnings": [],
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    errors = definition_result.errors + boundary_result.errors
    warnings = definition_result.warnings + boundary_result.warnings
    status = "ok" if not errors else "error"

    if json_output:
        typer.echo(
            json_mod.dumps(
                {
                    "status": status,
                    "definition_version": definition_version,
                    "county_vintage": county_vintage,
                    "errors": errors,
                    "warnings": warnings,
                }
            )
        )
        if errors:
            raise typer.Exit(1)
        return

    if errors:
        typer.echo("Metro validation failed:", err=True)
        for error in errors:
            typer.echo(f"  ERROR: {error}", err=True)
        for warning in warnings:
            typer.echo(f"  WARN:  {warning}", err=True)
        raise typer.Exit(1)

    typer.echo(
        f"Metro validation passed for {definition_version} "
        f"with county vintage {county_vintage} ({len(warnings)} warning(s))."
    )
    for warning in warnings:
        typer.echo(f"  WARN:  {warning}")


def validate_metro_universe(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Canonical metro-universe definition version to validate.",
        ),
    ] = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    profile_definition_version: Annotated[
        str,
        typer.Option(
            "--profile-definition-version",
            help="Subset profile version to validate.",
        ),
    ] = DEFINITION_VERSION,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Validate canonical metro-universe and subset-profile artifacts."""
    import json as json_mod

    from hhplab.metro.metro_io import validate_curated_metro_universe

    try:
        result = validate_curated_metro_universe(
            metro_definition_version=definition_version,
            profile_definition_version=profile_definition_version,
        )
    except FileNotFoundError as exc:
        payload = {
            "status": "error",
            "definition_version": definition_version,
            "profile_definition_version": profile_definition_version,
            "errors": [str(exc)],
            "warnings": [],
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    status = "ok" if result.passed else "error"
    if json_output:
        typer.echo(
            json_mod.dumps(
                {
                    "status": status,
                    "definition_version": definition_version,
                    "profile_definition_version": profile_definition_version,
                    "errors": result.errors,
                    "warnings": result.warnings,
                }
            )
        )
        if not result.passed:
            raise typer.Exit(1)
        return

    if not result.passed:
        typer.echo("Metro-universe validation failed:", err=True)
        for error in result.errors:
            typer.echo(f"  ERROR: {error}", err=True)
        for warning in result.warnings:
            typer.echo(f"  WARN:  {warning}", err=True)
        raise typer.Exit(1)

    typer.echo(
        "Metro-universe validation passed for "
        f"{definition_version} with profile {profile_definition_version} "
        f"({len(result.warnings)} warning(s))."
    )
    for warning in result.warnings:
        typer.echo(f"  WARN:  {warning}")
