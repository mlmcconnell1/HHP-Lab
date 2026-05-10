"""CLI command for comparing CoC boundaries between two vintages."""

from pathlib import Path
from typing import Annotated

import geopandas as gpd
import typer

from hhplab.registry.boundary_diff import compare_boundary_records
from hhplab.registry.boundary_registry import list_boundaries


def compare_vintages(
    vintage1: Annotated[
        str,
        typer.Option(
            "--vintage1",
            "-v1",
            help="First (older) boundary vintage to compare.",
        ),
    ],
    vintage2: Annotated[
        str,
        typer.Option(
            "--vintage2",
            "-v2",
            help="Second (newer) boundary vintage to compare.",
        ),
    ],
    show_unchanged: Annotated[
        bool,
        typer.Option(
            "--show-unchanged",
            help="Also list CoCs with no changes.",
        ),
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save diff results to CSV.",
        ),
    ] = None,
) -> None:
    """Compare CoC boundaries between two vintages.

    Compares boundaries by coc_id and geom_hash to identify:
    - Added: CoCs in v2 but not v1
    - Removed: CoCs in v1 but not v2
    - Changed: CoCs in both but with different geom_hash
    - Unchanged: CoCs in both with same geom_hash

    Examples:

        hhplab show vintage-diffs --vintage1 2024 --vintage2 2025

        hhplab show vintage-diffs -v1 2024 -v2 2025 --show-unchanged

        hhplab show vintage-diffs -v1 2024 -v2 2025 -o diff_report.csv
    """
    # Look up vintage paths in registry
    vintages = list_boundaries()
    vintage_map = {v.boundary_vintage: v for v in vintages}

    # Validate vintage1 exists
    if vintage1 not in vintage_map:
        available = [v.boundary_vintage for v in vintages] if vintages else []
        typer.echo(
            f"Error: Vintage '{vintage1}' not found in registry. Available: {available}",
            err=True,
        )
        raise typer.Exit(1)

    # Validate vintage2 exists
    if vintage2 not in vintage_map:
        available = [v.boundary_vintage for v in vintages] if vintages else []
        typer.echo(
            f"Error: Vintage '{vintage2}' not found in registry. Available: {available}",
            err=True,
        )
        raise typer.Exit(1)

    # Get file paths
    path1 = Path(vintage_map[vintage1].path)
    path2 = Path(vintage_map[vintage2].path)

    # Load boundary files
    typer.echo(f"Loading vintage {vintage1}...")
    if not path1.exists():
        typer.echo(f"Error: Boundary file not found: {path1}", err=True)
        raise typer.Exit(1)

    try:
        gdf1 = gpd.read_parquet(path1)
    except Exception as e:
        typer.echo(f"Error reading {path1}: {e}", err=True)
        raise typer.Exit(1) from e

    typer.echo(f"Loading vintage {vintage2}...")
    if not path2.exists():
        typer.echo(f"Error: Boundary file not found: {path2}", err=True)
        raise typer.Exit(1)

    try:
        gdf2 = gpd.read_parquet(path2)
    except Exception as e:
        typer.echo(f"Error reading {path2}: {e}", err=True)
        raise typer.Exit(1) from e

    try:
        diff = compare_boundary_records(gdf1, gdf2)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    # Display summary
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo(f"COMPARISON: {vintage1} -> {vintage2}")
    typer.echo("=" * 60)
    typer.echo("")
    typer.echo(f"Vintage {vintage1}: {diff.v1_count} CoCs")
    typer.echo(f"Vintage {vintage2}: {diff.v2_count} CoCs")
    typer.echo("")
    typer.echo("-" * 40)
    typer.echo("SUMMARY")
    typer.echo("-" * 40)
    typer.echo(f"  Added:     {len(diff.added_ids):>4}")
    typer.echo(f"  Removed:   {len(diff.removed_ids):>4}")
    typer.echo(f"  Changed:   {len(diff.changed_ids):>4}")
    typer.echo(f"  Unchanged: {len(diff.unchanged_ids):>4}")
    typer.echo("")

    # Display added CoCs
    if diff.added_ids:
        typer.echo("-" * 40)
        typer.echo(f"ADDED ({len(diff.added_ids)}):")
        typer.echo("-" * 40)
        for coc_id in diff.added_ids:
            typer.echo(f"  + {coc_id}")
        typer.echo("")

    # Display removed CoCs
    if diff.removed_ids:
        typer.echo("-" * 40)
        typer.echo(f"REMOVED ({len(diff.removed_ids)}):")
        typer.echo("-" * 40)
        for coc_id in diff.removed_ids:
            typer.echo(f"  - {coc_id}")
        typer.echo("")

    # Display changed CoCs
    if diff.changed_ids:
        typer.echo("-" * 40)
        typer.echo(f"CHANGED ({len(diff.changed_ids)}):")
        typer.echo("-" * 40)
        for coc_id in diff.changed_ids:
            typer.echo(f"  ~ {coc_id}")
        typer.echo("")

    # Display unchanged CoCs (if requested)
    if show_unchanged and diff.unchanged_ids:
        typer.echo("-" * 40)
        typer.echo(f"UNCHANGED ({len(diff.unchanged_ids)}):")
        typer.echo("-" * 40)
        for coc_id in diff.unchanged_ids:
            typer.echo(f"    {coc_id}")
        typer.echo("")

    typer.echo("=" * 60)

    # Save to CSV if output path specified
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        diff.to_frame(vintage1, vintage2).to_csv(output, index=False)
        typer.echo(f"\nDiff saved to: {output}")
