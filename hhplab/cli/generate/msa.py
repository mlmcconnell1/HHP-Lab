"""CLI command for generating curated Census MSA definition artifacts."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.geographies.msa.msa_definitions import DEFINITION_VERSION


def generate_msa(
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="MSA definition version to generate.",
        ),
    ] = DEFINITION_VERSION,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing artifacts if they already exist.",
        ),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Generate curated MSA definition parquet files from the Census workbook."""
    import hhplab.artifacts.naming.naming as naming
    from hhplab.geographies.msa.msa_io import write_msa_artifacts

    paths_to_write = [
        naming.msa_definitions_path(definition_version),
        naming.msa_county_membership_path(definition_version),
    ]
    existing = [path for path in paths_to_write if path.exists()]
    if existing and not force:
        paths_str = "\n".join(f"  - {path}" for path in existing)
        cli_error(
            f"MSA artifacts already exist:\n{paths_str}\nUse --force to overwrite.",
            json_output,
            json_payload={
                "status": "error",
                "error": "artifacts_exist",
                "existing": [str(path) for path in existing],
            },
        )

    if not json_output:
        typer.echo(f"Generating MSA artifacts (version: {definition_version})...")

    try:
        defs_path, county_path = write_msa_artifacts(definition_version=definition_version)
    except ValueError as exc:
        cli_error(
            exc,
            json_output,
            json_payload={"status": "error", "error": "validation_failed", "detail": str(exc)},
        )

    if emit_result(
        {
            "status": "ok",
            "definition_version": definition_version,
            "artifacts": {
                "definitions": str(defs_path),
                "county_membership": str(county_path),
            },
        },
        json_output,
    ):
        return
    typer.echo(f"  Written: {defs_path}")
    typer.echo(f"  Written: {county_path}")
    typer.echo("MSA artifact generation complete.")
