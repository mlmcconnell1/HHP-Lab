"""CLI command for validating PIT counts between two vintage releases."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.pit.pit_registry import get_pit_vintage_path, list_pit_vintages
from hhplab.pit.vintage_compare import (
    PIT_DELTA_COLUMNS,
    PitVintageComparisonResult,
    compare_pit_vintages,
)


def _run_pit_vintages_validation(
    vintage1: Annotated[
        str,
        typer.Option(
            "--vintage1",
            "-v1",
            help="First (older) PIT vintage to compare.",
        ),
    ],
    vintage2: Annotated[
        str,
        typer.Option(
            "--vintage2",
            "-v2",
            help="Second (newer) PIT vintage to compare.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save detailed comparison to CSV.",
        ),
    ] = None,
    show_unchanged: Annotated[
        bool,
        typer.Option(
            "--show-unchanged",
            help="Also show CoC-years with no changes.",
        ),
    ] = False,
    year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="Filter to a specific PIT year (e.g., 2020).",
        ),
    ] = None,
) -> None:
    """Validate PIT counts between two vintage releases.

    Compares the total, sheltered, and unsheltered counts for years
    that appear in both vintage files. This helps identify when HUD
    has revised historical PIT data between releases.

    Examples:

        hhplab validate pit-vintages --vintage1 2023 --vintage2 2024

        hhplab validate pit-vintages -v1 2023 -v2 2024 --year 2020

        hhplab validate pit-vintages -v1 2023 -v2 2024 -o comparison.csv
    """
    # Look up vintage paths in registry
    vintages = list_pit_vintages()
    vintage_map = {str(v.vintage): v for v in vintages}

    # Validate vintage1 exists
    if vintage1 not in vintage_map:
        available = [str(v.vintage) for v in vintages] if vintages else []
        typer.echo(
            f"Error: Vintage '{vintage1}' not found in registry. Available: {available}",
            err=True,
        )
        typer.echo(
            "\nHint: Run 'hhplab ingest pit-vintage --vintage <year>' to ingest a vintage.",
            err=True,
        )
        raise typer.Exit(1)

    # Validate vintage2 exists
    if vintage2 not in vintage_map:
        available = [str(v.vintage) for v in vintages] if vintages else []
        typer.echo(
            f"Error: Vintage '{vintage2}' not found in registry. Available: {available}",
            err=True,
        )
        typer.echo(
            "\nHint: Run 'hhplab ingest pit-vintage --vintage <year>' to ingest a vintage.",
            err=True,
        )
        raise typer.Exit(1)

    # Get file paths
    path1 = get_pit_vintage_path(int(vintage1))
    path2 = get_pit_vintage_path(int(vintage2))

    if path1 is None or not path1.exists():
        typer.echo(f"Error: Vintage file not found for {vintage1}", err=True)
        raise typer.Exit(1)

    if path2 is None or not path2.exists():
        typer.echo(f"Error: Vintage file not found for {vintage2}", err=True)
        raise typer.Exit(1)

    # Load vintage files
    typer.echo(f"Loading vintage {vintage1}...")
    try:
        df1 = pd.read_parquet(path1)
    except Exception as e:
        typer.echo(f"Error reading {path1}: {e}", err=True)
        raise typer.Exit(1) from e

    typer.echo(f"Loading vintage {vintage2}...")
    try:
        df2 = pd.read_parquet(path2)
    except Exception as e:
        typer.echo(f"Error reading {path2}: {e}", err=True)
        raise typer.Exit(1) from e

    try:
        result = compare_pit_vintages(df1, df2, year=year)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    common_years = result.common_years
    typer.echo(f"Comparing {len(common_years)} common years: {common_years[0]}-{common_years[-1]}")
    typer.echo("")

    _render_pit_vintage_comparison(
        result,
        vintage1=vintage1,
        vintage2=vintage2,
        output=output,
        show_unchanged=show_unchanged,
    )

    # Exit with code 1 if there are changes (useful for CI/CD)
    if result.has_differences:
        raise typer.Exit(1)
    raise typer.Exit(0)


def _render_pit_vintage_comparison(
    result: PitVintageComparisonResult,
    *,
    vintage1: str,
    vintage2: str,
    output: Path | None,
    show_unchanged: bool,
) -> None:
    added = result.records_with_status("added")
    removed = result.records_with_status("removed")
    changed = result.records_with_status("changed")
    unchanged = result.records_with_status("unchanged")

    # Display summary
    typer.echo("=" * 70)
    typer.echo(f"PIT VINTAGE COMPARISON: {vintage1} -> {vintage2}")
    typer.echo("=" * 70)
    typer.echo("")
    typer.echo(f"Vintage {vintage1}: {result.vintage1_record_count} CoC-year records")
    typer.echo(f"Vintage {vintage2}: {result.vintage2_record_count} CoC-year records")
    typer.echo(
        f"Years compared: {len(result.common_years)} "
        f"({result.common_years[0]}-{result.common_years[-1]})"
    )
    typer.echo("")
    typer.echo("-" * 40)
    typer.echo("SUMMARY")
    typer.echo("-" * 40)
    typer.echo(f"  Added:     {result.added_count:>6} CoC-years")
    typer.echo(f"  Removed:   {result.removed_count:>6} CoC-years")
    typer.echo(f"  Changed:   {result.changed_count:>6} CoC-years")
    typer.echo(f"  Unchanged: {result.unchanged_count:>6} CoC-years")
    typer.echo("")

    # Compare tab totals (all-CoC totals) by year
    typer.echo("-" * 70)
    typer.echo("TAB TOTALS BY YEAR (all CoCs summed)")
    typer.echo("-" * 70)

    # Display header
    typer.echo(
        f"  {'Year':<6} {'Total v1':>10} {'Total v2':>10} {'Delta':>8}  "
        f"{'Shelt v1':>10} {'Shelt v2':>10} {'Delta':>8}  "
        f"{'Unshelt v1':>10} {'Unshelt v2':>10} {'Delta':>8}"
    )
    typer.echo("  " + "-" * 106)

    any_tab_differences = False
    for pit_year in sorted(result.tab_totals.index):
        row = result.tab_totals.loc[pit_year]
        total_v1 = int(row["pit_total_v1"]) if not pd.isna(row["pit_total_v1"]) else 0
        total_v2 = int(row["pit_total_v2"]) if not pd.isna(row["pit_total_v2"]) else 0
        total_delta = int(row["total_delta"]) if not pd.isna(row["total_delta"]) else 0

        shelt_v1 = int(row["pit_sheltered_v1"]) if not pd.isna(row["pit_sheltered_v1"]) else 0
        shelt_v2 = int(row["pit_sheltered_v2"]) if not pd.isna(row["pit_sheltered_v2"]) else 0
        shelt_delta = int(row["sheltered_delta"]) if not pd.isna(row["sheltered_delta"]) else 0

        unshelt_v1 = int(row["pit_unsheltered_v1"]) if not pd.isna(row["pit_unsheltered_v1"]) else 0
        unshelt_v2 = int(row["pit_unsheltered_v2"]) if not pd.isna(row["pit_unsheltered_v2"]) else 0
        unshelt_delta = (
            int(row["unsheltered_delta"]) if not pd.isna(row["unsheltered_delta"]) else 0
        )

        # Format deltas with sign
        total_delta_str = f"{total_delta:+d}" if total_delta != 0 else "0"
        shelt_delta_str = f"{shelt_delta:+d}" if shelt_delta != 0 else "0"
        unshelt_delta_str = f"{unshelt_delta:+d}" if unshelt_delta != 0 else "0"

        # Mark rows with differences
        row_has_differences = any(row[column] != 0 for column in PIT_DELTA_COLUMNS)
        marker = " *" if row_has_differences else "  "
        if row_has_differences:
            any_tab_differences = True

        typer.echo(
            f"{marker}{pit_year:<6} {total_v1:>10,} {total_v2:>10,} {total_delta_str:>8}  "
            f"{shelt_v1:>10,} {shelt_v2:>10,} {shelt_delta_str:>8}  "
            f"{unshelt_v1:>10,} {unshelt_v2:>10,} {unshelt_delta_str:>8}"
        )

    typer.echo("")
    if any_tab_differences:
        typer.echo("  * = tab totals differ between vintages")
    else:
        typer.echo("  All tab totals match between vintages.")
    typer.echo("")

    # Show changed records with details
    if len(changed) > 0:
        typer.echo("-" * 70)
        typer.echo(f"CHANGED ({len(changed)}):")
        typer.echo("-" * 70)

        # Group by year for cleaner output
        changed_sorted = changed.sort_values(["pit_year", "coc_id"])

        # Show up to 20 changes in detail
        display_limit = 20
        for i, (_, row) in enumerate(changed_sorted.iterrows()):
            if i >= display_limit:
                typer.echo(f"  ... and {len(changed) - display_limit} more changes")
                break

            changes = []
            if row["total_delta"] != 0 and not pd.isna(row["total_delta"]):
                sign = "+" if row["total_delta"] > 0 else ""
                changes.append(f"total: {sign}{int(row['total_delta'])}")
            if row["sheltered_delta"] != 0 and not pd.isna(row["sheltered_delta"]):
                sign = "+" if row["sheltered_delta"] > 0 else ""
                changes.append(f"sheltered: {sign}{int(row['sheltered_delta'])}")
            if row["unsheltered_delta"] != 0 and not pd.isna(row["unsheltered_delta"]):
                sign = "+" if row["unsheltered_delta"] > 0 else ""
                changes.append(f"unsheltered: {sign}{int(row['unsheltered_delta'])}")

            changes_str = ", ".join(changes) if changes else "metadata only"
            typer.echo(f"  {int(row['pit_year'])} {row['coc_id']}: {changes_str}")
        typer.echo("")

    # Show aggregate statistics for changes
    if len(changed) > 0:
        typer.echo("-" * 40)
        typer.echo("CHANGE STATISTICS")
        typer.echo("-" * 40)

        total_delta_sum = changed["total_delta"].sum()
        sheltered_delta_sum = changed["sheltered_delta"].sum()
        unsheltered_delta_sum = changed["unsheltered_delta"].sum()

        typer.echo(f"  Net total change:       {total_delta_sum:+,.0f}")
        typer.echo(f"  Net sheltered change:   {sheltered_delta_sum:+,.0f}")
        typer.echo(f"  Net unsheltered change: {unsheltered_delta_sum:+,.0f}")
        typer.echo("")

        # Show years with most changes
        changes_by_year = changed.groupby("pit_year").size().sort_values(ascending=False)
        typer.echo("  Changes by year:")
        for pit_year, count in changes_by_year.head(5).items():
            typer.echo(f"    {int(pit_year)}: {count} CoCs changed")
        typer.echo("")

    # Show added records
    if len(added) > 0:
        typer.echo("-" * 40)
        typer.echo(f"ADDED ({len(added)}):")
        typer.echo("-" * 40)
        for i, (_, row) in enumerate(added.sort_values(["pit_year", "coc_id"]).iterrows()):
            if i >= 10:
                typer.echo(f"  ... and {len(added) - 10} more")
                break
            typer.echo(f"  + {int(row['pit_year'])} {row['coc_id']}")
        typer.echo("")

    # Show removed records
    if len(removed) > 0:
        typer.echo("-" * 40)
        typer.echo(f"REMOVED ({len(removed)}):")
        typer.echo("-" * 40)
        for i, (_, row) in enumerate(removed.sort_values(["pit_year", "coc_id"]).iterrows()):
            if i >= 10:
                typer.echo(f"  ... and {len(removed) - 10} more")
                break
            typer.echo(f"  - {int(row['pit_year'])} {row['coc_id']}")
        typer.echo("")

    # Show unchanged (if requested)
    if show_unchanged and len(unchanged) > 0:
        typer.echo("-" * 40)
        typer.echo(f"UNCHANGED ({len(unchanged)}):")
        typer.echo("-" * 40)
        for i, (_, row) in enumerate(unchanged.sort_values(["pit_year", "coc_id"]).iterrows()):
            if i >= 10:
                typer.echo(f"  ... and {len(unchanged) - 10} more")
                break
            typer.echo(f"    {int(row['pit_year'])} {row['coc_id']}")
        typer.echo("")

    typer.echo("=" * 70)

    # Save to CSV if output path specified
    if output:
        # Prepare output DataFrame
        output_df = result.csv_frame(vintage1, vintage2)

        output.parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(output, index=False)
        typer.echo(f"\nComparison saved to: {output}")


def validate_pit_vintages(
    vintage1: Annotated[
        str,
        typer.Option(
            "--vintage1",
            "-v1",
            help="First (older) PIT vintage to compare.",
        ),
    ],
    vintage2: Annotated[
        str,
        typer.Option(
            "--vintage2",
            "-v2",
            help="Second (newer) PIT vintage to compare.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save detailed comparison to CSV.",
        ),
    ] = None,
    show_unchanged: Annotated[
        bool,
        typer.Option(
            "--show-unchanged",
            help="Also show CoC-years with no changes.",
        ),
    ] = False,
    year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="Filter to a specific PIT year (e.g., 2020).",
        ),
    ] = None,
) -> None:
    """Validate PIT counts between two vintage releases."""
    _run_pit_vintages_validation(
        vintage1=vintage1,
        vintage2=vintage2,
        output=output,
        show_unchanged=show_unchanged,
        year=year,
    )
