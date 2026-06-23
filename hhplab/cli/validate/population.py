"""CLI command for validating population totals from crosswalk aggregation."""

from pathlib import Path
from typing import Annotated

import typer

from hhplab.paths import curated_dir
from hhplab.xwalks.diagnostics import PopulationValidationResult, validate_population_crosswalk


def _run_population_validation(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        str | None,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        str | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023'). Defaults to ACS year.",
        ),
    ] = None,
    xwalk_dir: Annotated[
        Path | None,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = None,
    acs_dir: Annotated[
        Path | None,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = None,
    by_state: Annotated[
        bool,
        typer.Option(
            "--by-state",
            "-s",
            help="Show detailed state-level comparison.",
        ),
    ] = False,
    warn_threshold: Annotated[
        float,
        typer.Option(
            "--warn-threshold",
            "-w",
            help="Warning threshold for CoC/ACS ratio deviation from 1.0 (default: 0.05 = 5%).",
        ),
    ] = 0.05,
) -> None:
    """Validate population totals from crosswalk aggregation against ACS national totals.

    Validates that CoC-aggregated population approximately equals the national
    ACS total, helping identify crosswalk coverage issues, double-counting,
    or data quality problems.

    The check computes:
    - Sum of tract populations weighted by area_share across all CoCs
    - Compares against the raw national ACS tract total
    - Identifies tracts with potential overlap (area_share sum > 1)
    - Identifies tracts with partial coverage (area_share sum < 1)
    - Shows state-level discrepancies

    Examples:

        hhplab validate population

        hhplab validate population --boundary 2025 --acs 2019-2023

        hhplab validate population --by-state

        hhplab validate population --warn-threshold 0.10
    """
    if xwalk_dir is None:
        xwalk_dir = curated_dir("xwalks")
    if acs_dir is None:
        acs_dir = curated_dir("acs")

    # Find crosswalk file
    if boundary is None:
        # Find the latest boundary vintage
        xwalk_files = sorted(xwalk_dir.glob("xwalk__B*xT*.parquet"), reverse=True)
        if not xwalk_files:
            typer.echo(f"Error: No crosswalk files found in {xwalk_dir}", err=True)
            raise typer.Exit(1)
        xwalk_path = xwalk_files[0]
        # Extract boundary vintage from filename
        boundary = xwalk_path.stem.split("__")[1].split("x")[0].replace("B", "")
        tracts_from_file = xwalk_path.stem.split("xT")[1] if "xT" in xwalk_path.stem else None
    else:
        # Find matching crosswalk
        if tracts:
            pattern = f"xwalk__B{boundary}xT{tracts}.parquet"
        else:
            pattern = f"xwalk__B{boundary}xT*.parquet"
        matches = list(xwalk_dir.glob(pattern))
        if not matches:
            typer.echo(f"Error: No crosswalk found matching {pattern}", err=True)
            raise typer.Exit(1)
        xwalk_path = matches[0]
        tracts_from_file = xwalk_path.stem.split("xT")[1] if "xT" in xwalk_path.stem else None

    # Determine tract vintage
    tract_vintage = tracts or tracts_from_file or "2023"

    # Find ACS tract population file
    if acs is None:
        # Find the latest ACS file (new naming: acs5_tracts__A{year}xT{tract}.parquet)
        acs_files = sorted(acs_dir.glob("acs5_tracts__A*xT*.parquet"), reverse=True)
        if not acs_files:
            typer.echo(f"Error: No ACS tract files found in {acs_dir}", err=True)
            raise typer.Exit(1)
        acs_path = acs_files[0]
        # Parse ACS vintage from filename: acs5_tracts__A2023xT2023 → "2019-2023"
        notation = acs_path.stem.split("__")[1]  # "A2023xT2023"
        acs_end_year = notation.split("x")[0][1:]  # "2023"
        acs_vintage = f"{int(acs_end_year) - 4}-{acs_end_year}"
    else:
        # Find matching ACS file by vintage end year
        acs_end_year = acs.split("-")[-1] if "-" in acs else acs
        pattern = f"acs5_tracts__A{acs_end_year}xT*.parquet"
        matches = list(acs_dir.glob(pattern))
        if not matches:
            typer.echo(f"Error: No ACS file found matching {pattern}", err=True)
            raise typer.Exit(1)
        acs_path = matches[0]
        acs_vintage = acs

    typer.echo("=" * 70)
    typer.echo("POPULATION CROSSWALK VALIDATION")
    typer.echo("=" * 70)
    typer.echo("")
    typer.echo("Configuration:")
    typer.echo(f"  Boundary vintage: {boundary}")
    typer.echo(f"  ACS vintage:      {acs_vintage}")
    typer.echo(f"  Tract vintage:    {tract_vintage}")
    typer.echo(f"  Crosswalk:        {xwalk_path.name}")
    typer.echo(f"  ACS file:         {acs_path.name}")
    typer.echo("")

    # Load data
    typer.echo("Loading data...")
    import pandas as pd

    xwalk = pd.read_parquet(xwalk_path)
    tract_pop = pd.read_parquet(acs_path)
    result = validate_population_crosswalk(
        xwalk,
        tract_pop,
        warn_threshold=warn_threshold,
        include_state=by_state,
    )

    _render_population_validation_result(
        result,
        by_state=by_state,
        warn_threshold=warn_threshold,
    )

    # Exit with appropriate code
    if not result.within_threshold:
        typer.echo(
            f"\nWARNING: CoC/National ratio ({result.ratio:.4f}) deviates more than "
            f"{warn_threshold:.0%} from 1.0"
        )
        raise typer.Exit(1)


def _render_population_validation_result(
    result: PopulationValidationResult,
    *,
    by_state: bool,
    warn_threshold: float,
) -> None:
    """Render a population validation result for the CLI."""
    typer.echo("")
    typer.echo(f"1. NATIONAL TOTAL (sum of all tracts): {result.national_total:,.0f}")

    # Crosswalk statistics
    typer.echo("")
    typer.echo("2. CROSSWALK STATISTICS:")
    typer.echo(f"   Total tract-CoC relationships: {result.relationship_count:,}")
    typer.echo(f"   Unique tracts in crosswalk:    {result.unique_crosswalk_tracts:,}")
    typer.echo(f"   Unique tracts in ACS:          {result.unique_population_tracts:,}")
    typer.echo(f"   Unique CoCs:                   {result.unique_geographies:,}")

    typer.echo(f"   Tracts in ACS but not crosswalk: {result.missing_tract_count:,}")
    typer.echo(f"   Tracts in crosswalk but not ACS: {result.extra_tract_count:,}")

    if result.missing_tract_count:
        typer.echo(
            f"   Population in uncovered tracts:  {result.missing_population:,.0f} "
            f"({100 * result.missing_population / result.national_total:.2f}%)"
        )

    # Results
    typer.echo("")
    typer.echo(f"3. COC-AGGREGATED TOTAL: {result.total_coc_population:,.0f}")
    typer.echo(f"   Difference from national: {result.diff:+,.0f}")
    typer.echo(f"   Ratio (CoC/National):     {result.ratio:.4f}")

    # Check status
    if result.within_threshold:
        typer.echo(f"   Status: OK (within {warn_threshold:.0%} threshold)")
    else:
        typer.echo(
            f"   Status: WARNING - deviation exceeds {warn_threshold:.0%} threshold",
        )

    typer.echo("")
    typer.echo("4. AREA_SHARE VALIDATION:")
    typer.echo(
        "   Tracts with sum > 1.01 (potential overlap): "
        f"{result.area_share.overcounted_count:,}"
    )
    typer.echo(
        "   Tracts with sum < 0.99 (partial coverage):  "
        f"{result.area_share.undercounted_count:,}"
    )
    typer.echo(
        f"   Tracts with sum ≈ 1.0:                       "
        f"{result.area_share.balanced_count:,}"
    )

    if result.area_share.overcounted_samples:
        typer.echo("")
        typer.echo("   Sample overcounted tracts:")
        for geoid, area_share_sum in result.area_share.overcounted_samples:
            typer.echo(f"     {geoid}: area_share sum = {area_share_sum:.4f}")

    # State-level comparison
    if by_state:
        typer.echo("")
        typer.echo("5. STATE-LEVEL COMPARISON:")

        # States with issues
        problem_states = sorted(
            (state for state in result.state_comparison if state.ratio < 1 - warn_threshold),
            key=lambda state: state.ratio,
        )

        if problem_states:
            typer.echo("")
            typer.echo(f"   States below {1 - warn_threshold:.0%} coverage:")
            typer.echo(f"   {'State':<8} {'ACS Total':>15} {'CoC Total':>15} {'Ratio':>8}")
            typer.echo("   " + "-" * 50)
            for state in problem_states:
                typer.echo(
                    f"   {state.state:<8} {state.acs_total:>15,.0f} "
                    f"{state.coc_total:>15,.0f} {state.ratio:>8.3f}"
                )

        # Full state table
        typer.echo("")
        typer.echo("   All states:")
        typer.echo(f"   {'State':<8} {'ACS Total':>15} {'CoC Total':>15} {'Ratio':>8}")
        typer.echo("   " + "-" * 50)
        for state in result.state_comparison:
            marker = " " if 1 - warn_threshold <= state.ratio <= 1 + warn_threshold else "*"
            typer.echo(
                f"   {state.state:<8} {state.acs_total:>15,.0f} "
                f"{state.coc_total:>15,.0f} {state.ratio:>7.3f}{marker}"
            )

    typer.echo("")
    typer.echo("=" * 70)


def validate_population(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        str | None,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        str | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023'). Defaults to ACS year.",
        ),
    ] = None,
    xwalk_dir: Annotated[
        Path | None,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = None,
    acs_dir: Annotated[
        Path | None,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = None,
    by_state: Annotated[
        bool,
        typer.Option(
            "--by-state",
            "-s",
            help="Show detailed state-level comparison.",
        ),
    ] = False,
    warn_threshold: Annotated[
        float,
        typer.Option(
            "--warn-threshold",
            "-w",
            help="Warning threshold for CoC/ACS ratio deviation from 1.0 (default: 0.05 = 5%).",
        ),
    ] = 0.05,
) -> None:
    """Validate population totals from crosswalk aggregation."""
    if xwalk_dir is None:
        xwalk_dir = curated_dir("xwalks")
    if acs_dir is None:
        acs_dir = curated_dir("acs")

    _run_population_validation(
        boundary=boundary,
        acs=acs,
        tracts=tracts,
        xwalk_dir=xwalk_dir,
        acs_dir=acs_dir,
        by_state=by_state,
        warn_threshold=warn_threshold,
    )

