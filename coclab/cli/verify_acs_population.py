"""CLI command for one-shot ACS population verification (ingest + rollup + crosscheck)."""

from typing import Annotated, Literal

import typer


def verify_acs_population(
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
            help="Re-ingest and rebuild even if cached files exist.",
        ),
    ] = False,
    warn_pct: Annotated[
        float,
        typer.Option(
            "--warn-pct",
            help="Warning threshold for percent delta (default: 0.01 = 1%).",
        ),
    ] = 0.01,
    error_pct: Annotated[
        float,
        typer.Option(
            "--error-pct",
            help="Error threshold for percent delta (default: 0.05 = 5%).",
        ),
    ] = 0.05,
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Minimum coverage ratio threshold (default: 0.95).",
        ),
    ] = 0.95,
) -> None:
    """One-shot ACS population verification: ingest, rollup, and crosscheck.

    Runs the complete ACS population verification pipeline:
    1. Ingest tract population from ACS (if not cached or --force)
    2. Build CoC population rollup (if not cached or --force)
    3. Cross-check rollup vs existing CoC measures

    Exit codes:
        0 - No errors found (warnings may exist)
        2 - Errors found (threshold exceeded)

    Examples:

        coclab verify-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area

        coclab verify-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area --force

        coclab verify-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area --warn-pct 0.02
    """
    import pandas as pd

    from coclab.acs.crosscheck import print_crosscheck_report, run_crosscheck
    from coclab.acs.ingest.tract_population import (
        get_output_path as get_tract_pop_path,
    )
    from coclab.acs.ingest.tract_population import (
        ingest_tract_population,
    )
    from coclab.acs.rollup import (
        build_coc_population_rollup,
    )
    from coclab.acs.rollup import (
        get_output_path as get_rollup_output_path,
    )

    # Validate weighting option
    if weighting not in ("area", "population_mass"):
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. Use 'area' or 'population_mass'.",
            err=True,
        )
        raise typer.Exit(1)

    # Cast to Literal type for type checking
    weighting_literal: Literal["area", "population_mass"] = (
        "area" if weighting == "area" else "population_mass"
    )

    typer.echo("=" * 70)
    typer.echo("ACS Population Verification Pipeline")
    typer.echo("=" * 70)
    typer.echo(f"  Boundary vintage: {boundary}")
    typer.echo(f"  ACS vintage:      {acs}")
    typer.echo(f"  Tract vintage:    {tracts}")
    typer.echo(f"  Weighting:        {weighting}")
    typer.echo(f"  Force rebuild:    {force}")
    typer.echo("")
    typer.echo("Thresholds:")
    typer.echo(f"  Warning pct:   {warn_pct:.1%}")
    typer.echo(f"  Error pct:     {error_pct:.1%}")
    typer.echo(f"  Min coverage:  {min_coverage:.2f}")
    typer.echo("=" * 70)
    typer.echo("")

    # Step 1: Ingest tract population
    typer.echo("STEP 1: Ingest tract population")
    typer.echo("-" * 40)
    tract_pop_path = get_tract_pop_path(acs, tracts)
    if tract_pop_path.exists() and not force:
        typer.echo(f"Using cached file: {tract_pop_path}")
        tract_df = pd.read_parquet(tract_pop_path)
        typer.echo(f"Rows: {len(tract_df):,}")
    else:
        typer.echo("Ingesting tract population from Census API...")
        try:
            tract_pop_path = ingest_tract_population(
                acs_vintage=acs,
                tract_vintage=tracts,
                force=force,
            )
            tract_df = pd.read_parquet(tract_pop_path)
            typer.echo(f"Output: {tract_pop_path}")
            typer.echo(f"Rows: {len(tract_df):,}")
        except Exception as e:
            typer.echo(f"Error in ingest: {e}", err=True)
            raise typer.Exit(1) from e
    typer.echo("")

    # Step 2: Build CoC population rollup
    typer.echo("STEP 2: Build CoC population rollup")
    typer.echo("-" * 40)
    rollup_path = get_rollup_output_path(boundary, acs, tracts, weighting)
    if rollup_path.exists() and not force:
        typer.echo(f"Using cached file: {rollup_path}")
        rollup_df = pd.read_parquet(rollup_path)
        typer.echo(f"CoCs: {len(rollup_df)}")
    else:
        typer.echo("Building rollup...")
        try:
            rollup_path = build_coc_population_rollup(
                boundary_vintage=boundary,
                acs_vintage=acs,
                tract_vintage=tracts,
                weighting=weighting_literal,
                force=force,
            )
            rollup_df = pd.read_parquet(rollup_path)
            typer.echo(f"Output: {rollup_path}")
            typer.echo(f"CoCs: {len(rollup_df)}")
        except FileNotFoundError as e:
            typer.echo(f"Error in rollup: {e}", err=True)
            raise typer.Exit(1) from e
        except Exception as e:
            typer.echo(f"Error in rollup: {e}", err=True)
            raise typer.Exit(1) from e
    typer.echo("")

    # Step 3: Cross-check against measures
    typer.echo("STEP 3: Cross-check vs CoC measures")
    typer.echo("-" * 40)

    try:
        result = run_crosscheck(
            boundary_vintage=boundary,
            acs_vintage=acs,
            tract_vintage=tracts,
            weighting=weighting,
            warn_pct=warn_pct,
            error_pct=error_pct,
            min_coverage=min_coverage,
            save_report=True,
        )
    except FileNotFoundError as e:
        typer.echo(f"Error in crosscheck: {e}", err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"Error in crosscheck: {e}", err=True)
        raise typer.Exit(1) from e

    # Print the crosscheck report and get exit code
    exit_code = print_crosscheck_report(result)

    raise typer.Exit(exit_code)
