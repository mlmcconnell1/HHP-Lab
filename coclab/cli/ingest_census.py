"""CLI command for ingesting TIGER geometries."""

from pathlib import Path
from typing import Annotated

import typer

from coclab.naming import county_filename, tract_filename

# Output directory matches the census ingest modules
OUTPUT_DIR = Path("data/curated/census")


def ingest_tiger(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="TIGER vintage year (e.g., 2023).",
        ),
    ] = 2023,
    type_: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="What to download: 'tracts', 'counties', or 'all'.",
        ),
    ] = "all",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Download TIGER census geometries (tracts and/or counties).

    Downloads census tract and county shapefiles from the US Census Bureau's
    TIGER/Line files, reprojects to EPSG:4326, and saves as GeoParquet files.

    Examples:

        coclab ingest tiger --year 2023

        coclab ingest tiger --year 2023 --type tracts

        coclab ingest tiger --year 2023 --type counties --force
    """
    # Validate type option
    valid_types = {"tracts", "counties", "all"}
    if type_ not in valid_types:
        typer.echo(
            f"Error: Invalid type '{type_}'. Must be one of: {', '.join(sorted(valid_types))}",
            err=True,
        )
        raise typer.Exit(1)

    # Determine what to download
    download_tracts = type_ in ("tracts", "all")
    download_counties = type_ in ("counties", "all")

    # Define output paths using canonical naming helpers
    tracts_path = OUTPUT_DIR / tract_filename(year)
    counties_path = OUTPUT_DIR / county_filename(year)

    # Track what was downloaded
    downloaded = []

    # Process tracts
    if download_tracts:
        if tracts_path.exists() and not force:
            typer.echo(f"Tracts file already exists: {tracts_path}")
            typer.echo("Use --force to re-download.")
        else:
            if tracts_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {tracts_path}")
            typer.echo(f"Downloading TIGER tracts for {year}...")
            try:
                from coclab.census.ingest import ingest_tiger_tracts

                output_path = ingest_tiger_tracts(year, show_progress=True)
                typer.echo(f"Saved tracts to: {output_path}")
                downloaded.append(("tracts", output_path))
            except Exception as e:
                typer.echo(f"Error downloading tracts: {e}", err=True)
                if download_counties:
                    typer.echo("Continuing with counties...")
                else:
                    raise typer.Exit(1) from e

    # Process counties
    if download_counties:
        if counties_path.exists() and not force:
            typer.echo(f"Counties file already exists: {counties_path}")
            typer.echo("Use --force to re-download.")
        else:
            if counties_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {counties_path}")
            typer.echo(f"Downloading TIGER counties for {year}...")
            try:
                from coclab.census.ingest import ingest_tiger_counties

                output_path = ingest_tiger_counties(year)
                typer.echo(f"Saved counties to: {output_path}")
                downloaded.append(("counties", output_path))
            except Exception as e:
                typer.echo(f"Error downloading counties: {e}", err=True)
                if not downloaded:
                    raise typer.Exit(1) from e

    # Summary
    if downloaded:
        typer.echo("")
        typer.echo("Census geometry ingestion complete:")
        for name, path in downloaded:
            typer.echo(f"  {name}: {path}")
    elif not (tracts_path.exists() or counties_path.exists()):
        typer.echo("No files were downloaded.")
