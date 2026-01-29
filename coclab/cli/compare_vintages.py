"""CLI command for comparing CoC boundaries between two vintages."""

from pathlib import Path
from typing import Annotated

import geopandas as gpd
import pandas as pd
import typer

from coclab.registry.registry import list_boundaries


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

        coclab show vintage-diffs --vintage1 2024 --vintage2 2025

        coclab show vintage-diffs -v1 2024 -v2 2025 --show-unchanged

        coclab show vintage-diffs -v1 2024 -v2 2025 -o diff_report.csv
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

    # Validate required columns
    required_cols = {"coc_id", "geom_hash"}
    for gdf, _vintage, path in [(gdf1, vintage1, path1), (gdf2, vintage2, path2)]:
        missing = required_cols - set(gdf.columns)
        if missing:
            typer.echo(
                f"Error: Boundary file {path} missing required columns: {missing}",
                err=True,
            )
            raise typer.Exit(1)

    # Create lookup dictionaries: coc_id -> geom_hash
    v1_hashes = dict(zip(gdf1["coc_id"], gdf1["geom_hash"], strict=True))
    v2_hashes = dict(zip(gdf2["coc_id"], gdf2["geom_hash"], strict=True))

    v1_ids = set(v1_hashes.keys())
    v2_ids = set(v2_hashes.keys())

    # Compute differences
    added_ids = sorted(v2_ids - v1_ids)
    removed_ids = sorted(v1_ids - v2_ids)
    common_ids = v1_ids & v2_ids

    changed_ids = sorted(coc_id for coc_id in common_ids if v1_hashes[coc_id] != v2_hashes[coc_id])
    unchanged_ids = sorted(
        coc_id for coc_id in common_ids if v1_hashes[coc_id] == v2_hashes[coc_id]
    )

    # Display summary
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo(f"COMPARISON: {vintage1} -> {vintage2}")
    typer.echo("=" * 60)
    typer.echo("")
    typer.echo(f"Vintage {vintage1}: {len(v1_ids)} CoCs")
    typer.echo(f"Vintage {vintage2}: {len(v2_ids)} CoCs")
    typer.echo("")
    typer.echo("-" * 40)
    typer.echo("SUMMARY")
    typer.echo("-" * 40)
    typer.echo(f"  Added:     {len(added_ids):>4}")
    typer.echo(f"  Removed:   {len(removed_ids):>4}")
    typer.echo(f"  Changed:   {len(changed_ids):>4}")
    typer.echo(f"  Unchanged: {len(unchanged_ids):>4}")
    typer.echo("")

    # Display added CoCs
    if added_ids:
        typer.echo("-" * 40)
        typer.echo(f"ADDED ({len(added_ids)}):")
        typer.echo("-" * 40)
        for coc_id in added_ids:
            typer.echo(f"  + {coc_id}")
        typer.echo("")

    # Display removed CoCs
    if removed_ids:
        typer.echo("-" * 40)
        typer.echo(f"REMOVED ({len(removed_ids)}):")
        typer.echo("-" * 40)
        for coc_id in removed_ids:
            typer.echo(f"  - {coc_id}")
        typer.echo("")

    # Display changed CoCs
    if changed_ids:
        typer.echo("-" * 40)
        typer.echo(f"CHANGED ({len(changed_ids)}):")
        typer.echo("-" * 40)
        for coc_id in changed_ids:
            typer.echo(f"  ~ {coc_id}")
        typer.echo("")

    # Display unchanged CoCs (if requested)
    if show_unchanged and unchanged_ids:
        typer.echo("-" * 40)
        typer.echo(f"UNCHANGED ({len(unchanged_ids)}):")
        typer.echo("-" * 40)
        for coc_id in unchanged_ids:
            typer.echo(f"    {coc_id}")
        typer.echo("")

    typer.echo("=" * 60)

    # Save to CSV if output path specified
    if output:
        # Build diff DataFrame
        diff_records = []

        for coc_id in added_ids:
            diff_records.append(
                {
                    "coc_id": coc_id,
                    "status": "added",
                    "geom_hash_v1": None,
                    "geom_hash_v2": v2_hashes[coc_id],
                }
            )

        for coc_id in removed_ids:
            diff_records.append(
                {
                    "coc_id": coc_id,
                    "status": "removed",
                    "geom_hash_v1": v1_hashes[coc_id],
                    "geom_hash_v2": None,
                }
            )

        for coc_id in changed_ids:
            diff_records.append(
                {
                    "coc_id": coc_id,
                    "status": "changed",
                    "geom_hash_v1": v1_hashes[coc_id],
                    "geom_hash_v2": v2_hashes[coc_id],
                }
            )

        for coc_id in unchanged_ids:
            diff_records.append(
                {
                    "coc_id": coc_id,
                    "status": "unchanged",
                    "geom_hash_v1": v1_hashes[coc_id],
                    "geom_hash_v2": v2_hashes[coc_id],
                }
            )

        diff_df = pd.DataFrame(diff_records)
        diff_df = diff_df.sort_values("coc_id")

        # Add metadata columns
        diff_df.insert(0, "vintage1", vintage1)
        diff_df.insert(1, "vintage2", vintage2)

        # Save CSV
        output.parent.mkdir(parents=True, exist_ok=True)
        diff_df.to_csv(output, index=False)
        typer.echo(f"\nDiff saved to: {output}")
