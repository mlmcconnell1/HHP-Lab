"""CLI command for validating curated MSA artifacts."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.msa.msa_definitions import DEFINITION_VERSION


def validate_msa(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="MSA definition version to validate.",
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
    """Validate curated MSA definitions, membership, and boundary polygons."""
    import json as json_mod

    from hhplab.msa.msa_boundaries import validate_curated_msa_boundaries
    from hhplab.msa.msa_io import validate_curated_msa

    try:
        definitions_result = validate_curated_msa(definition_version)
        boundaries_result = validate_curated_msa_boundaries(definition_version)
    except FileNotFoundError as exc:
        payload = {
            "status": "error",
            "definition_version": definition_version,
            "errors": [str(exc)],
            "warnings": [],
        }
        if json_output:
            typer.echo(json_mod.dumps(payload))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    errors = definitions_result.errors + boundaries_result.errors
    warnings = definitions_result.warnings + boundaries_result.warnings
    status = "ok" if not errors else "error"

    if json_output:
        typer.echo(
            json_mod.dumps(
                {
                    "status": status,
                    "definition_version": definition_version,
                    "errors": errors,
                    "warnings": warnings,
                }
            )
        )
        if errors:
            raise typer.Exit(1)
        return

    if errors:
        typer.echo("MSA validation failed:", err=True)
        for error in errors:
            typer.echo(f"  ERROR: {error}", err=True)
        for warning in warnings:
            typer.echo(f"  WARN:  {warning}", err=True)
        raise typer.Exit(1)

    typer.echo(f"MSA validation passed for {definition_version} ({len(warnings)} warning(s)).")
    for warning in warnings:
        typer.echo(f"  WARN:  {warning}")
