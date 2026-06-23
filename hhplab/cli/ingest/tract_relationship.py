"""CLI command for ingesting Census tract relationship file."""

from typing import Annotated

import typer


def ingest_tract_relationship(
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Download Census tract relationship file (2010↔2020).

    Downloads the Census Bureau's tract-to-tract relationship file that maps
    2010 census tracts to 2020 census tracts. This file is needed to translate
    ACS data from 2010 tract geography to 2020 tract geography.

    The relationship file provides bidirectional area weights:
    - area_2010_to_2020_weight: fraction of 2010 tract mapping to each 2020 tract
    - area_2020_to_2010_weight: fraction of 2020 tract mapping to each 2010 tract

    Example:

        hhplab ingest-tract-relationship
    """
    from hhplab.census.ingest import ingest_tract_relationship as do_ingest

    if force:
        typer.echo("Forcing re-download of tract relationship file...")
    else:
        typer.echo("Checking for tract relationship file...")

    try:
        output_path = do_ingest(force=force)
        typer.echo(f"Tract relationship file: {output_path}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    typer.echo("Done!")
