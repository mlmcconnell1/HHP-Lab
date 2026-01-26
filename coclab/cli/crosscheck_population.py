"""CLI command for validating population totals from crosswalk aggregation."""

from pathlib import Path
from typing import Annotated

import typer


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
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    acs_dir: Annotated[
        Path,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = Path("data/curated/acs"),
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

        coclab validate population

        coclab validate population --boundary 2025 --acs 2019-2023

        coclab validate population --by-state

        coclab validate population --warn-threshold 0.10
    """
    import pandas as pd

    # Find crosswalk file
    if boundary is None:
        # Find the latest boundary vintage
        xwalk_files = sorted(xwalk_dir.glob("xwalk__B*xT*.parquet"), reverse=True)
        if not xwalk_files:
            typer.echo("Error: No crosswalk files found in {xwalk_dir}", err=True)
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
        # Find the latest ACS file
        acs_files = sorted(acs_dir.glob("tract_population__*__*.parquet"), reverse=True)
        if not acs_files:
            typer.echo(f"Error: No ACS tract files found in {acs_dir}", err=True)
            raise typer.Exit(1)
        acs_path = acs_files[0]
        acs_vintage = acs_path.stem.split("__")[1]
    else:
        # Find matching ACS file
        pattern = f"tract_population__{acs}__*.parquet"
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
    xwalk = pd.read_parquet(xwalk_path)
    tract_pop = pd.read_parquet(acs_path)

    # National total
    national_total = tract_pop["total_population"].sum()
    typer.echo("")
    typer.echo(f"1. NATIONAL TOTAL (sum of all tracts): {national_total:,.0f}")

    # Crosswalk statistics
    typer.echo("")
    typer.echo("2. CROSSWALK STATISTICS:")
    typer.echo(f"   Total tract-CoC relationships: {len(xwalk):,}")
    typer.echo(f"   Unique tracts in crosswalk:    {xwalk['tract_geoid'].nunique():,}")
    typer.echo(f"   Unique tracts in ACS:          {tract_pop['tract_geoid'].nunique():,}")
    typer.echo(f"   Unique CoCs:                   {xwalk['coc_id'].nunique():,}")

    # Check coverage
    acs_tracts = set(tract_pop["tract_geoid"])
    xwalk_tracts = set(xwalk["tract_geoid"])
    missing_tracts = acs_tracts - xwalk_tracts
    extra_tracts = xwalk_tracts - acs_tracts

    typer.echo(f"   Tracts in ACS but not crosswalk: {len(missing_tracts):,}")
    typer.echo(f"   Tracts in crosswalk but not ACS: {len(extra_tracts):,}")

    if missing_tracts:
        missing_pop = tract_pop[tract_pop["tract_geoid"].isin(missing_tracts)][
            "total_population"
        ].sum()
        typer.echo(
            f"   Population in uncovered tracts:  {missing_pop:,.0f} "
            f"({100 * missing_pop / national_total:.2f}%)"
        )

    # Join and calculate CoC-level totals
    merged = xwalk.merge(
        tract_pop[["tract_geoid", "total_population"]],
        on="tract_geoid",
        how="left",
    )
    merged["weighted_pop"] = (
        merged["total_population"].fillna(0) * merged["area_share"].fillna(0)
    )
    coc_totals = merged.groupby("coc_id")["weighted_pop"].sum()
    total_coc_pop = coc_totals.sum()

    # Results
    typer.echo("")
    typer.echo(f"3. COC-AGGREGATED TOTAL: {total_coc_pop:,.0f}")
    diff = total_coc_pop - national_total
    ratio = total_coc_pop / national_total
    typer.echo(f"   Difference from national: {diff:+,.0f}")
    typer.echo(f"   Ratio (CoC/National):     {ratio:.4f}")

    # Check status
    if abs(1 - ratio) <= warn_threshold:
        typer.echo(f"   Status: OK (within {warn_threshold:.0%} threshold)")
    else:
        typer.echo(
            f"   Status: WARNING - deviation exceeds {warn_threshold:.0%} threshold",
        )

    # Area share validation
    tract_area_sums = merged.groupby("tract_geoid")["area_share"].sum()
    overcounted = tract_area_sums[tract_area_sums > 1.01]
    undercounted = tract_area_sums[tract_area_sums < 0.99]

    typer.echo("")
    typer.echo("4. AREA_SHARE VALIDATION:")
    typer.echo(f"   Tracts with sum > 1.01 (potential overlap): {len(overcounted):,}")
    typer.echo(f"   Tracts with sum < 0.99 (partial coverage):  {len(undercounted):,}")
    typer.echo(
        f"   Tracts with sum ≈ 1.0:                       "
        f"{len(tract_area_sums) - len(overcounted) - len(undercounted):,}"
    )

    if len(overcounted) > 0:
        typer.echo("")
        typer.echo("   Sample overcounted tracts:")
        for geoid in list(overcounted.head(5).index):
            typer.echo(f"     {geoid}: area_share sum = {tract_area_sums[geoid]:.4f}")

    # State-level comparison
    if by_state:
        typer.echo("")
        typer.echo("5. STATE-LEVEL COMPARISON:")

        tract_pop_with_state = tract_pop.copy()
        tract_pop_with_state["state"] = tract_pop_with_state["tract_geoid"].str[:2]
        state_acs = tract_pop_with_state.groupby("state")["total_population"].sum()

        merged["state"] = merged["tract_geoid"].str[:2]
        state_coc = merged.groupby("state")["weighted_pop"].sum()

        comparison = pd.DataFrame({"acs_total": state_acs, "coc_total": state_coc}).fillna(0)
        comparison["diff"] = comparison["coc_total"] - comparison["acs_total"]
        comparison["ratio"] = comparison["coc_total"] / comparison["acs_total"]
        comparison["ratio"] = comparison["ratio"].fillna(0)

        # States with issues
        problem_states = comparison[
            (comparison["ratio"] < 1 - warn_threshold) | (comparison["ratio"].isna())
        ].sort_values("ratio")

        if len(problem_states) > 0:
            typer.echo("")
            typer.echo(f"   States below {1 - warn_threshold:.0%} coverage:")
            typer.echo(f"   {'State':<8} {'ACS Total':>15} {'CoC Total':>15} {'Ratio':>8}")
            typer.echo("   " + "-" * 50)
            for state, row in problem_states.iterrows():
                typer.echo(
                    f"   {state:<8} {row['acs_total']:>15,.0f} "
                    f"{row['coc_total']:>15,.0f} {row['ratio']:>8.3f}"
                )

        # Full state table
        typer.echo("")
        typer.echo("   All states:")
        typer.echo(f"   {'State':<8} {'ACS Total':>15} {'CoC Total':>15} {'Ratio':>8}")
        typer.echo("   " + "-" * 50)
        for state, row in comparison.sort_index().iterrows():
            marker = " " if 1 - warn_threshold <= row["ratio"] <= 1 + warn_threshold else "*"
            typer.echo(
                f"   {state:<8} {row['acs_total']:>15,.0f} "
                f"{row['coc_total']:>15,.0f} {row['ratio']:>7.3f}{marker}"
            )

    typer.echo("")
    typer.echo("=" * 70)

    # Exit with appropriate code
    if abs(1 - ratio) > warn_threshold:
        typer.echo(
            f"\nWARNING: CoC/National ratio ({ratio:.4f}) deviates more than "
            f"{warn_threshold:.0%} from 1.0"
        )
        raise typer.Exit(1)


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
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    acs_dir: Annotated[
        Path,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = Path("data/curated/acs"),
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
    _run_population_validation(
        boundary=boundary,
        acs=acs,
        tracts=tracts,
        xwalk_dir=xwalk_dir,
        acs_dir=acs_dir,
        by_state=by_state,
        warn_threshold=warn_threshold,
    )


def crosscheck_population(
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
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    acs_dir: Annotated[
        Path,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = Path("data/curated/acs"),
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
    """Deprecated: use validate population."""
    typer.echo(
        "Warning: 'coclab crosscheck-population' is deprecated; "
        "use 'coclab validate population' instead.",
        err=True,
    )
    validate_population(
        boundary=boundary,
        acs=acs,
        tracts=tracts,
        xwalk_dir=xwalk_dir,
        acs_dir=acs_dir,
        by_state=by_state,
        warn_threshold=warn_threshold,
    )
