"""CLI command for listing available measure files."""

from datetime import datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def parse_measures_filename(filename: str) -> tuple[str, str] | None:
    """Parse boundary_vintage and acs_vintage from measures filename.

    Expected formats (new temporal shorthand):
        measures__A{acs}@B{boundary}.parquet
        measures__A{acs}@B{boundary}xT{tract}.parquet

    Legacy format:
        coc_measures__{boundary}__{acs}.parquet

    Returns
    -------
    tuple[str, str] | None
        (boundary_vintage, acs_vintage) or None if filename doesn't match pattern.
    """
    import re

    # New format: measures__A2023@B2025.parquet or measures__A2023@B2025xT2023.parquet
    if filename.startswith("measures__"):
        # Pattern: measures__A{acs}@B{boundary}[xT{tract}].parquet
        new_pattern = r"^measures__A(\d{4})@B(\d{4})(?:xT\d{4})?\.parquet$"
        match = re.match(new_pattern, filename)
        if match:
            acs_vintage = match.group(1)
            boundary_vintage = match.group(2)
            return boundary_vintage, acs_vintage

    # Legacy format: coc_measures__{boundary}__{acs}.parquet
    if not filename.startswith("coc_measures__") or not filename.endswith(".parquet"):
        return None

    # Remove prefix and suffix
    stem = filename[len("coc_measures__") : -len(".parquet")]

    # Split by double underscore
    parts = stem.split("__")
    if len(parts) != 2:
        return None

    return parts[0], parts[1]


def list_measures(
    dir: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for measure files.",
        ),
    ] = Path("data/curated/measures"),
) -> None:
    """List available CoC measure files.

    Scans the measures directory for parquet files and displays
    information about each measure file including boundary vintage,
    ACS vintage, number of CoCs, weighting method, file size,
    and modification time.

    Examples:

        coclab list-measures

        coclab list-measures --dir /path/to/measures
    """
    measures_dir = Path(dir)

    if not measures_dir.exists():
        typer.echo(f"Directory not found: {measures_dir}")
        return

    # Find all parquet files matching the measures pattern (new and legacy)
    measure_files = []
    # New format: measures__A*@B*.parquet
    for filepath in measures_dir.glob("measures__A*.parquet"):
        parsed = parse_measures_filename(filepath.name)
        if parsed:
            boundary_vintage, acs_vintage = parsed
            measure_files.append((filepath, boundary_vintage, acs_vintage))
    # Legacy format: coc_measures__*.parquet
    for filepath in measures_dir.glob("coc_measures__*.parquet"):
        parsed = parse_measures_filename(filepath.name)
        if parsed:
            boundary_vintage, acs_vintage = parsed
            measure_files.append((filepath, boundary_vintage, acs_vintage))

    if not measure_files:
        typer.echo(f"No measure files found in: {measures_dir}")
        return

    # Sort by boundary vintage, then ACS vintage
    measure_files.sort(key=lambda x: (x[1], x[2]))

    # Collect metadata for each file
    rows = []
    for filepath, boundary_vintage, acs_vintage in measure_files:
        # Get file stats
        stat = filepath.stat()
        file_size = format_file_size(stat.st_size)
        modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")

        # Read parquet metadata
        try:
            df = pd.read_parquet(filepath)
            row_count = len(df)

            # Try to get weighting method from data
            if "weighting_method" in df.columns:
                weighting = df["weighting_method"].iloc[0] if len(df) > 0 else "unknown"
            else:
                weighting = "unknown"
        except Exception:
            row_count = "error"
            weighting = "error"

        rows.append(
            {
                "boundary_vintage": boundary_vintage,
                "acs_vintage": acs_vintage,
                "cocs": row_count,
                "weighting": weighting,
                "size": file_size,
                "modified": modified,
            }
        )

    # Display table header
    typer.echo("Available CoC Measure Files:\n")
    typer.echo(
        f"{'Boundary Vintage':<18} {'ACS Vintage':<13} {'CoCs':<8} "
        f"{'Weighting':<12} {'Size':<10} {'Modified'}"
    )
    typer.echo("-" * 85)

    # Display rows
    for row in rows:
        cocs_str = str(row["cocs"]) if isinstance(row["cocs"], int) else row["cocs"]
        typer.echo(
            f"{row['boundary_vintage']:<18} {row['acs_vintage']:<13} {cocs_str:<8} "
            f"{row['weighting']:<12} {row['size']:<10} {row['modified']}"
        )

    typer.echo("")
    typer.echo(f"Total: {len(rows)} measure file(s)")
