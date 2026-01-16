"""CLI command for cross-checking ACS population rollup vs CoC measures."""

from typing import Annotated

import typer


def crosscheck_acs_population(
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
    """Cross-check population rollup against existing CoC measures.

    Compares the CoC population estimates from the rollup engine against
    existing CoC measures to identify discrepancies that may indicate
    data quality issues or crosswalk problems.

    Exit codes:
        0 - No errors found (warnings may exist)
        2 - Errors found (threshold exceeded)

    Examples:

        coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area

        coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 \\
            --tracts 2023 --weighting area --warn-pct 0.02
    """
    from coclab.acs.crosscheck import (
        get_measures_path,
        get_rollup_path,
        print_crosscheck_report,
        run_crosscheck,
    )

    # Validate weighting option
    if weighting not in ("area", "population_mass"):
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. Use 'area' or 'population_mass'.",
            err=True,
        )
        raise typer.Exit(1)

    # Show input file paths
    rollup_path = get_rollup_path(boundary, acs, tracts, weighting)
    measures_path = get_measures_path(boundary, acs)

    typer.echo("Running ACS population crosscheck...")
    typer.echo(f"  Boundary vintage: {boundary}")
    typer.echo(f"  ACS vintage:      {acs}")
    typer.echo(f"  Tract vintage:    {tracts}")
    typer.echo(f"  Weighting:        {weighting}")
    typer.echo("")
    typer.echo("Thresholds:")
    typer.echo(f"  Warning pct:   {warn_pct:.1%}")
    typer.echo(f"  Error pct:     {error_pct:.1%}")
    typer.echo(f"  Min coverage:  {min_coverage:.2f}")
    typer.echo("")
    typer.echo("Input files:")
    typer.echo(f"  Rollup:   {rollup_path}")
    typer.echo(f"  Measures: {measures_path}")

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
        typer.echo(f"\nError: {e}", err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"\nError: {e}", err=True)
        raise typer.Exit(1) from e

    # Print the crosscheck report and get exit code
    exit_code = print_crosscheck_report(result)

    raise typer.Exit(exit_code)
