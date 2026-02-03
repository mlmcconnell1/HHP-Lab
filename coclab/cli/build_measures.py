"""CLI command for building CoC-level ACS measures."""

from pathlib import Path
from typing import Annotated, Literal

import typer

from coclab.builds import build_curated_dir, require_build_dir, resolve_build_dir
from coclab.measures.acs import build_coc_measures
from coclab.registry.registry import latest_vintage

DEFAULT_XWALK_DIR = Path("data/curated/xwalks")
DEFAULT_OUTPUT_DIR = Path("data/curated/measures")


def build_measures(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        str,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023').",
        ),
    ] = "2018-2022",
    tracts: Annotated[
        int | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage for crosswalk. Defaults to same as ACS year.",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area' or 'population'.",
        ),
    ] = "area",
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            help="Named build directory for outputs and build-local artifacts.",
        ),
    ] = None,
    xwalk_dir: Annotated[
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = DEFAULT_XWALK_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for measure files.",
        ),
    ] = DEFAULT_OUTPUT_DIR,
) -> None:
    """Build CoC-level demographic measures from ACS 5-year estimates.

    Fetches tract-level data from the Census API and aggregates to CoC
    level using tract crosswalks. Produces a measures file containing:

    - total_population: Total population (B01003)
    - adult_population: Population 18+ (derived from B01001)
    - median_household_income: Median household income (B19013)
    - median_gross_rent: Median gross rent (B25064)
    - population_below_poverty: Population below 100% poverty (C17002)
    - poverty_universe: Population for whom poverty is determined (C17002)
    - coverage_ratio: Fraction of CoC area with valid tract data

    Examples:

        coclab build measures --boundary 2025 --acs 2019-2023

        coclab build measures --boundary 2025 --acs 2019-2023 --weighting population

        coclab build measures --build demo --boundary 2025 --acs 2019-2023
    """
    # Validate weighting option
    if weighting not in ("area", "population"):
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. Use 'area' or 'population'.",
            err=True,
        )
        raise typer.Exit(1)

    # Cast to Literal type for type checking
    weighting_literal: Literal["area", "population"] = (
        "area" if weighting == "area" else "population"
    )

    # Resolve tract vintage (default to ACS end year)
    if tracts is not None:
        tract_vintage = tracts
    elif "-" in acs:
        # Extract end year from range like "2019-2023"
        tract_vintage = int(acs.split("-")[1])
    else:
        tract_vintage = int(acs)

    # Resolve boundary vintage from registry
    if boundary is None:
        boundary = latest_vintage()
        if boundary is None:
            typer.echo(
                "Error: No boundary vintages found in registry. Run 'coclab ingest' first.",
                err=True,
            )
            raise typer.Exit(1)
        typer.echo(f"Using latest boundary vintage: {boundary}")

    # Find crosswalk file (try new naming, then legacy)
    from coclab.naming import tract_xwalk_filename

    if build is not None:
        try:
            build_dir = require_build_dir(build)
        except FileNotFoundError:
            build_path = resolve_build_dir(build)
            typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
            typer.echo("Run: coclab build create --name <build>", err=True)
            raise typer.Exit(2)

        build_curated = build_curated_dir(build_dir)
        if xwalk_dir == DEFAULT_XWALK_DIR:
            xwalk_dir = build_curated / "xwalks"
        if output_dir == DEFAULT_OUTPUT_DIR:
            output_dir = build_curated / "measures"

    xwalk_path = xwalk_dir / tract_xwalk_filename(boundary, tract_vintage)
    legacy_xwalk_path = xwalk_dir / f"coc_tract_xwalk__{boundary}__{tract_vintage}.parquet"

    if not xwalk_path.exists():
        # Try legacy path
        if legacy_xwalk_path.exists():
            xwalk_path = legacy_xwalk_path
        else:
            typer.echo(
                f"Error: Crosswalk file not found: {xwalk_path}. "
                f"Run 'coclab build xwalks --boundary {boundary} --tracts {tract_vintage}' first.",
                err=True,
            )
            raise typer.Exit(1)

    typer.echo("Building CoC measures:")
    typer.echo(f"  Boundary vintage: {boundary}")
    typer.echo(f"  ACS vintage: {acs} (5-year estimates)")
    typer.echo(f"  Crosswalk: {xwalk_path}")
    typer.echo(f"  Weighting: {weighting}")
    typer.echo("")

    try:
        measures = build_coc_measures(
            boundary_vintage=boundary,
            acs_vintage=acs,
            crosswalk_path=xwalk_path,
            weighting=weighting_literal,
            output_dir=output_dir,
            show_progress=True,
        )
    except Exception as e:
        typer.echo(f"Error building measures: {e}", err=True)
        raise typer.Exit(1) from e

    # Print summary statistics
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("MEASURE SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Total CoCs: {len(measures)}")
    typer.echo("")

    if "total_population" in measures.columns:
        pop = measures["total_population"]
        typer.echo("Total Population:")
        typer.echo(f"  Sum:    {pop.sum():,.0f}")
        typer.echo(f"  Mean:   {pop.mean():,.0f}")
        typer.echo(f"  Median: {pop.median():,.0f}")
        typer.echo("")

    if "median_household_income" in measures.columns:
        income = measures["median_household_income"].dropna()
        typer.echo("Median Household Income:")
        typer.echo(f"  Mean:   ${income.mean():,.0f}")
        typer.echo(f"  Median: ${income.median():,.0f}")
        typer.echo(f"  Min:    ${income.min():,.0f}")
        typer.echo(f"  Max:    ${income.max():,.0f}")
        typer.echo("")

    if "coverage_ratio" in measures.columns:
        coverage = measures["coverage_ratio"]
        low_coverage = (coverage < 0.95).sum()
        typer.echo("Coverage:")
        typer.echo(f"  Mean coverage ratio: {coverage.mean():.3f}")
        typer.echo(f"  CoCs with <95% coverage: {low_coverage}")
        typer.echo("")

    typer.echo("=" * 60)
    typer.echo("Measure generation complete!")
