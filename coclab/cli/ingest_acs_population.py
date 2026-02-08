"""CLI command for ingesting ACS tract-level data."""

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
    translate: Annotated[
        bool,
        typer.Option(
            "--translate/--no-translate",
            help="Auto-translate from 2010 to 2020 tract geography if needed.",
        ),
    ] = True,
) -> None:
    """Ingest tract-level ACS 5-year estimates.

    Downloads tract data from the Census Bureau API (tables B01003, B01001,
    B19013, B25064, C17002) and saves as a Parquet file with provenance
    metadata.

    Variables fetched: total population, adult population (derived 18+),
    median household income, median gross rent, poverty universe, poverty
    counts (below 50% and 50-99%), and margin of error for total population.

    For ACS vintages 2010-2019 (which use 2010 census tract geography),
    the --translate option (enabled by default) will automatically convert
    GEOIDs to 2020 census tract geography using the tract relationship file.

    Examples:

        coclab ingest acs --acs 2019-2023 --tracts 2023

        coclab ingest acs --acs 2015-2019 --tracts 2023

        coclab ingest acs --acs 2015-2019 --tracts 2023 --no-translate
    """
    import pandas as pd

    from coclab.acs.ingest.tract_population import ingest_tract_data
    from coclab.acs.translate import (
        get_source_tract_vintage,
        needs_translation,
        translate_acs_to_target_vintage,
    )
    from coclab.acs.variables import ACS_TABLES
    from coclab.census.ingest.tract_relationship import TractRelationshipNotFoundError

    # Check if cached file exists
    output_path = get_output_path(acs, tracts)
    if output_path.exists() and not force:
        typer.echo(f"Cached file found: {output_path}")
        df = pd.read_parquet(output_path)
        typer.echo(f"Rows: {len(df)}")
        typer.echo("")
        typer.echo("Use --force to re-ingest.")
        return

    # Check if translation is needed
    source_tract_vintage = get_source_tract_vintage(acs)
    translation_needed = needs_translation(acs, tracts)

    typer.echo("Ingesting ACS tract data...")
    typer.echo(f"  ACS vintage:     {acs}")
    typer.echo(f"  Tables:          {', '.join(ACS_TABLES)}")
    typer.echo(f"  Source tracts:   {source_tract_vintage} (Census API geography)")
    typer.echo(f"  Target tracts:   {tracts}")
    if translation_needed and translate:
        typer.echo(f"  Translation:     {source_tract_vintage} → 2020 (auto)")
    elif translation_needed and not translate:
        typer.echo("  Translation:     disabled (--no-translate)")
    else:
        typer.echo("  Translation:     not needed")
    typer.echo("")

    # Check if translation is needed but relationship file is missing
    if translation_needed and translate:
        try:
            from coclab.census.ingest.tract_relationship import get_tract_relationship_path

            get_tract_relationship_path()
        except TractRelationshipNotFoundError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    try:
        path = ingest_tract_data(
            acs_vintage=acs,
            tract_vintage=tracts,
            force=force,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    # Load and summarize results
    df = pd.read_parquet(path)

    # Validate required columns exist
    required_columns = ["tract_geoid", "total_population", "adult_population"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        typer.echo(
            f"Error: Output file is missing required columns: {missing_columns}. "
            f"Available columns: {list(df.columns)}",
            err=True,
        )
        raise typer.Exit(1)

    # Apply translation if needed
    translation_stats = None
    if translation_needed and translate:
        typer.echo("Translating tract geography...")
        try:
            df, translation_stats = translate_acs_to_target_vintage(
                df,
                acs_vintage=acs,
                target_tract_vintage=tracts,
            )

            # Update tract_vintage in the data to reflect translation
            df["tract_vintage"] = tracts

            # Save translated data back (overwrite)
            from datetime import UTC, datetime

            from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

            provenance = ProvenanceBlock(
                acs_vintage=acs,
                tract_vintage=tracts,
                extra={
                    "dataset": "acs_tract_data",
                    "tables": ACS_TABLES,
                    "source_tract_vintage": str(source_tract_vintage),
                    "translated": True,
                    "translation_match_rate": translation_stats.match_rate,
                    "translation_population_delta_pct": translation_stats.population_delta_pct,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "row_count": len(df),
                },
            )
            write_parquet_with_provenance(df, path, provenance)
            typer.echo(f"Saved translated data to {path}")
        except TractRelationshipNotFoundError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("INGEST SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Output file:       {path}")
    typer.echo(f"Total tracts:      {len(df):,}")
    typer.echo(f"Total population:  {df['total_population'].sum():,.0f}")
    if "adult_population" in df.columns:
        typer.echo(f"Adult population:  {df['adult_population'].sum():,.0f}")
    if "median_household_income" in df.columns:
        typer.echo(f"Median income:     ${df['median_household_income'].median():,.0f}")
    if "median_gross_rent" in df.columns:
        typer.echo(f"Median rent:       ${df['median_gross_rent'].median():,.0f}")

    # Show translation stats if applicable
    if translation_stats:
        typer.echo("")
        typer.echo("TRANSLATION STATS")
        typer.echo(f"  Source tracts:   {translation_stats.input_tracts:,}")
        typer.echo(f"  Output tracts:   {translation_stats.output_tracts:,}")
        typer.echo(f"  Match rate:      {translation_stats.match_rate:.1%}")
        typer.echo(f"  Pop delta:       {translation_stats.population_delta_pct:+.2f}%")
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
