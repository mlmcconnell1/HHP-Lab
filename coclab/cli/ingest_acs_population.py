"""CLI command for ingesting ACS tract population data."""

from typing import Annotated

import typer

from coclab.acs.ingest.tract_population import get_output_path


def ingest_acs_population(
    acs: Annotated[
        str,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023').",
        ),
    ],
    tracts: Annotated[
        str,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023').",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
) -> None:
    """Ingest tract-level population data from ACS 5-year estimates.

    Downloads tract population data from the Census Bureau API and saves
    as a Parquet file with provenance metadata. Uses table B01003 (Total Population).

    Examples:

        coclab ingest-acs-population --acs 2019-2023 --tracts 2023

        coclab ingest-acs-population --acs 2019-2023 --tracts 2023 --force
    """
    import pandas as pd

    from coclab.acs.ingest.tract_population import ingest_tract_population

    # Check if cached file exists
    output_path = get_output_path(acs, tracts)
    if output_path.exists() and not force:
        typer.echo(f"Cached file found: {output_path}")
        df = pd.read_parquet(output_path)
        typer.echo(f"Rows: {len(df)}")
        typer.echo("")
        typer.echo("Use --force to re-ingest.")
        return

    typer.echo("Ingesting ACS tract population data...")
    typer.echo(f"  ACS vintage:   {acs}")
    typer.echo(f"  Tract vintage: {tracts}")
    typer.echo("")

    try:
        path = ingest_tract_population(
            acs_vintage=acs,
            tract_vintage=tracts,
            force=force,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    # Load and summarize results
    df = pd.read_parquet(path)

    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("INGEST SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Output file:       {path}")
    typer.echo(f"Total tracts:      {len(df):,}")
    typer.echo(f"Total population:  {df['total_population'].sum():,.0f}")
    typer.echo("")

    # Show state coverage
    df["state_fips"] = df["tract_geoid"].str[:2]
    state_counts = df.groupby("state_fips").size()
    typer.echo(f"States/territories: {len(state_counts)}")
    typer.echo("")

    # Show population stats
    pop = df["total_population"]
    typer.echo("Population per tract:")
    typer.echo(f"  Mean:   {pop.mean():,.0f}")
    typer.echo(f"  Median: {pop.median():,.0f}")
    typer.echo(f"  Min:    {pop.min():,.0f}")
    typer.echo(f"  Max:    {pop.max():,.0f}")
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("Ingest complete!")
