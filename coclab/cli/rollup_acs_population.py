"""CLI command for building CoC population rollup from ACS tract data."""

from typing import Annotated, Literal

import typer


def rollup_acs_population(
    boundary: Annotated[
        str,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025').",
        ),
    ],
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
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area' or 'population_mass'.",
        ),
    ] = "area",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Rebuild even if cached file exists.",
        ),
    ] = False,
) -> None:
    """Build CoC population rollup by aggregating tract population data.

    Aggregates tract-level population to CoC level using area-weighted
    aggregation with the tract-to-CoC crosswalk.

    Examples:

        coclab rollup-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area

        coclab rollup-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting population_mass --force
    """
    import pandas as pd

    from coclab.acs.rollup import (
        build_coc_population_rollup,
        get_crosswalk_path,
        get_output_path,
        get_tract_population_path,
    )

    # Validate weighting option
    if weighting not in ("area", "population_mass"):
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. "
            "Use 'area' or 'population_mass'.",
            err=True,
        )
        raise typer.Exit(1)

    # Cast to Literal type for type checking
    weighting_literal: Literal["area", "population_mass"] = (
        "area" if weighting == "area" else "population_mass"
    )

    # Check if cached file exists
    output_path = get_output_path(boundary, acs, tracts, weighting)
    if output_path.exists() and not force:
        typer.echo(f"Cached file found: {output_path}")
        df = pd.read_parquet(output_path)
        typer.echo(f"CoCs: {len(df)}")
        typer.echo("")
        typer.echo("Use --force to rebuild.")
        return

    # Show input file paths
    tract_pop_path = get_tract_population_path(acs, tracts)
    xwalk_path = get_crosswalk_path(boundary, tracts)

    typer.echo("Building CoC population rollup...")
    typer.echo(f"  Boundary vintage: {boundary}")
    typer.echo(f"  ACS vintage:      {acs}")
    typer.echo(f"  Tract vintage:    {tracts}")
    typer.echo(f"  Weighting:        {weighting}")
    typer.echo("")
    typer.echo("Input files:")
    typer.echo(f"  Tract population: {tract_pop_path}")
    typer.echo(f"  Crosswalk:        {xwalk_path}")
    typer.echo("")

    try:
        path = build_coc_population_rollup(
            boundary_vintage=boundary,
            acs_vintage=acs,
            tract_vintage=tracts,
            weighting=weighting_literal,
            force=force,
        )
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    # Load and summarize results
    df = pd.read_parquet(path)

    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("ROLLUP SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Output file:       {path}")
    typer.echo(f"Total CoCs:        {len(df)}")
    typer.echo(f"Total population:  {df['coc_population'].sum():,.0f}")
    typer.echo("")

    # Population statistics
    pop = df["coc_population"]
    typer.echo("Population per CoC:")
    typer.echo(f"  Mean:   {pop.mean():,.0f}")
    typer.echo(f"  Median: {pop.median():,.0f}")
    typer.echo(f"  Min:    {pop.min():,.0f}")
    typer.echo(f"  Max:    {pop.max():,.0f}")
    typer.echo("")

    # Coverage statistics
    coverage = df["coverage_ratio"]
    low_coverage = (coverage < 0.95).sum()
    high_coverage = (coverage > 1.01).sum()
    typer.echo("Coverage ratio:")
    typer.echo(f"  Mean:              {coverage.mean():.3f}")
    typer.echo(f"  Min:               {coverage.min():.3f}")
    typer.echo(f"  Max:               {coverage.max():.3f}")
    typer.echo(f"  CoCs < 95%:        {low_coverage}")
    typer.echo(f"  CoCs > 101%:       {high_coverage}")
    typer.echo("")

    # Tract count statistics
    tract_counts = df["tract_count"]
    typer.echo("Tracts per CoC:")
    typer.echo(f"  Mean:   {tract_counts.mean():.0f}")
    typer.echo(f"  Median: {tract_counts.median():.0f}")
    typer.echo(f"  Min:    {tract_counts.min()}")
    typer.echo(f"  Max:    {tract_counts.max()}")
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("Rollup complete!")
