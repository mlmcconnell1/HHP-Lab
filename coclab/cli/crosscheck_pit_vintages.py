"""CLI command for cross-checking PIT counts between two vintage releases."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from coclab.pit.registry import get_pit_vintage_path, list_pit_vintages


def crosscheck_pit_vintages(
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
    """Compare PIT counts between two vintage releases.

    Compares the total, sheltered, and unsheltered counts for years
    that appear in both vintage files. This helps identify when HUD
    has revised historical PIT data between releases.

    Examples:

        coclab crosscheck-pit-vintages --vintage1 2023 --vintage2 2024

        coclab crosscheck-pit-vintages -v1 2023 -v2 2024 --year 2020

        coclab crosscheck-pit-vintages -v1 2023 -v2 2024 -o comparison.csv
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
            "\nHint: Run 'coclab ingest-pit-vintage --vintage <year>' to ingest a vintage.",
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
            "\nHint: Run 'coclab ingest-pit-vintage --vintage <year>' to ingest a vintage.",
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

    # Validate required columns
    required_cols = {"pit_year", "coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"}
    for df, _vintage, path in [(df1, vintage1, path1), (df2, vintage2, path2)]:
        missing = required_cols - set(df.columns)
        if missing:
            typer.echo(
                f"Error: Vintage file {path} missing required columns: {missing}",
                err=True,
            )
            raise typer.Exit(1)

    # Find common years
    years1 = set(df1["pit_year"].unique())
    years2 = set(df2["pit_year"].unique())
    common_years = sorted(years1 & years2)

    if not common_years:
        typer.echo(
            f"Error: No common years between vintages. "
            f"Vintage {vintage1} has years {sorted(years1)}, "
            f"vintage {vintage2} has years {sorted(years2)}",
            err=True,
        )
        raise typer.Exit(1)

    # Filter to specific year if requested
    if year is not None:
        if year not in common_years:
            typer.echo(
                f"Error: Year {year} not found in both vintages. Common years: {common_years}",
                err=True,
            )
            raise typer.Exit(1)
        common_years = [year]

    typer.echo(f"Comparing {len(common_years)} common years: {common_years[0]}-{common_years[-1]}")
    typer.echo("")

    # Filter to common years
    df1_filtered = df1[df1["pit_year"].isin(common_years)].copy()
    df2_filtered = df2[df2["pit_year"].isin(common_years)].copy()

    # Create comparison keys
    df1_filtered["key"] = df1_filtered["pit_year"].astype(str) + "_" + df1_filtered["coc_id"]
    df2_filtered["key"] = df2_filtered["pit_year"].astype(str) + "_" + df2_filtered["coc_id"]

    # Merge on key
    merged = pd.merge(
        df1_filtered[["key", "pit_total", "pit_sheltered", "pit_unsheltered"]],
        df2_filtered[["key", "pit_total", "pit_sheltered", "pit_unsheltered"]],
        on="key",
        how="outer",
        suffixes=("_v1", "_v2"),
    )

    # Extract pit_year and coc_id from key (format: "YEAR_COC_ID")
    merged["pit_year"] = merged["key"].str.split("_").str[0].astype(int)
    merged["coc_id"] = merged["key"].str.split("_", n=1).str[1]

    # Calculate differences
    merged["total_delta"] = merged["pit_total_v2"] - merged["pit_total_v1"]
    merged["sheltered_delta"] = merged["pit_sheltered_v2"] - merged["pit_sheltered_v1"]
    merged["unsheltered_delta"] = merged["pit_unsheltered_v2"] - merged["pit_unsheltered_v1"]

    # Classify changes
    def classify_change(row: pd.Series) -> str:
        if pd.isna(row["pit_total_v1"]):
            return "added"
        if pd.isna(row["pit_total_v2"]):
            return "removed"
        if row["total_delta"] != 0 or row["sheltered_delta"] != 0 or row["unsheltered_delta"] != 0:
            # Check for NaN deltas (when one side has null sheltered/unsheltered)
            total_changed = row["total_delta"] != 0 if not pd.isna(row["total_delta"]) else False
            sheltered_changed = (
                row["sheltered_delta"] != 0 if not pd.isna(row["sheltered_delta"]) else False
            )
            unsheltered_changed = (
                row["unsheltered_delta"] != 0 if not pd.isna(row["unsheltered_delta"]) else False
            )
            if total_changed or sheltered_changed or unsheltered_changed:
                return "changed"
        return "unchanged"

    merged["status"] = merged.apply(classify_change, axis=1)

    # Summary statistics
    added = merged[merged["status"] == "added"]
    removed = merged[merged["status"] == "removed"]
    changed = merged[merged["status"] == "changed"]
    unchanged = merged[merged["status"] == "unchanged"]

    # Display summary
    typer.echo("=" * 70)
    typer.echo(f"PIT VINTAGE COMPARISON: {vintage1} -> {vintage2}")
    typer.echo("=" * 70)
    typer.echo("")
    typer.echo(f"Vintage {vintage1}: {len(df1_filtered)} CoC-year records")
    typer.echo(f"Vintage {vintage2}: {len(df2_filtered)} CoC-year records")
    typer.echo(f"Years compared: {len(common_years)} ({common_years[0]}-{common_years[-1]})")
    typer.echo("")
    typer.echo("-" * 40)
    typer.echo("SUMMARY")
    typer.echo("-" * 40)
    typer.echo(f"  Added:     {len(added):>6} CoC-years")
    typer.echo(f"  Removed:   {len(removed):>6} CoC-years")
    typer.echo(f"  Changed:   {len(changed):>6} CoC-years")
    typer.echo(f"  Unchanged: {len(unchanged):>6} CoC-years")
    typer.echo("")

    # Compare tab totals (all-CoC totals) by year
    typer.echo("-" * 70)
    typer.echo("TAB TOTALS BY YEAR (all CoCs summed)")
    typer.echo("-" * 70)

    # Calculate totals by year for each vintage
    totals_v1 = df1_filtered.groupby("pit_year")[
        ["pit_total", "pit_sheltered", "pit_unsheltered"]
    ].sum()
    totals_v2 = df2_filtered.groupby("pit_year")[
        ["pit_total", "pit_sheltered", "pit_unsheltered"]
    ].sum()

    # Merge and calculate deltas
    totals_compare = totals_v1.join(totals_v2, lsuffix="_v1", rsuffix="_v2", how="outer")
    totals_compare["total_delta"] = totals_compare["pit_total_v2"] - totals_compare["pit_total_v1"]
    totals_compare["sheltered_delta"] = (
        totals_compare["pit_sheltered_v2"] - totals_compare["pit_sheltered_v1"]
    )
    totals_compare["unsheltered_delta"] = (
        totals_compare["pit_unsheltered_v2"] - totals_compare["pit_unsheltered_v1"]
    )

    # Display header
    typer.echo(
        f"  {'Year':<6} {'Total v1':>10} {'Total v2':>10} {'Delta':>8}  "
        f"{'Shelt v1':>10} {'Shelt v2':>10} {'Delta':>8}  "
        f"{'Unshelt v1':>10} {'Unshelt v2':>10} {'Delta':>8}"
    )
    typer.echo("  " + "-" * 106)

    any_tab_differences = False
    for pit_year in sorted(totals_compare.index):
        row = totals_compare.loc[pit_year]
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
        marker = " *" if (total_delta != 0 or shelt_delta != 0 or unshelt_delta != 0) else "  "
        if total_delta != 0 or shelt_delta != 0 or unshelt_delta != 0:
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
        output_df = merged[
            [
                "pit_year",
                "coc_id",
                "status",
                "pit_total_v1",
                "pit_total_v2",
                "total_delta",
                "pit_sheltered_v1",
                "pit_sheltered_v2",
                "sheltered_delta",
                "pit_unsheltered_v1",
                "pit_unsheltered_v2",
                "unsheltered_delta",
            ]
        ].copy()

        output_df.insert(0, "vintage1", vintage1)
        output_df.insert(1, "vintage2", vintage2)
        output_df = output_df.sort_values(["pit_year", "coc_id"])

        output.parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(output, index=False)
        typer.echo(f"\nComparison saved to: {output}")

    # Exit with code 1 if there are changes (useful for CI/CD)
    if len(changed) > 0 or len(added) > 0 or len(removed) > 0:
        raise typer.Exit(0)  # Changes found but not an error
    raise typer.Exit(0)
