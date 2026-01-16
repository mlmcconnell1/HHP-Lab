"""CLI command for listing available census geometry files."""

import re
from datetime import datetime
from pathlib import Path
from typing import Annotated

import pyarrow.parquet as pq
import typer


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _parse_census_filename(filename: str) -> dict | None:
    """Parse census geometry filename to extract type and year.

    Expected formats (new temporal shorthand):
        tracts__T{year}.parquet
        counties__C{year}.parquet

    Also supports legacy formats:
        tracts__{year}.parquet
        counties__{year}.parquet
    """
    # New temporal shorthand format: tracts__T2023.parquet, counties__C2023.parquet
    new_pattern = r"^(tracts)__T(\d{4})\.parquet$|^(counties)__C(\d{4})\.parquet$"
    match = re.match(new_pattern, filename)
    if match:
        if match.group(1):  # tracts match
            return {"type": "tracts", "year": int(match.group(2))}
        else:  # counties match
            return {"type": "counties", "year": int(match.group(4))}

    # Legacy format: tracts__2023.parquet, counties__2023.parquet
    legacy_pattern = r"^(tracts|counties)__(\d{4})\.parquet$"
    match = re.match(legacy_pattern, filename)
    if match:
        return {
            "type": match.group(1),
            "year": int(match.group(2)),
        }
    return None


def _get_parquet_row_count(filepath: Path) -> int:
    """Get the row count from a parquet file without loading all data."""
    parquet_file = pq.ParquetFile(filepath)
    return parquet_file.metadata.num_rows


def list_census(
    census_type: Annotated[
        str | None,
        typer.Option(
            "--type",
            "-t",
            help="Filter by census type: 'counties' or 'tracts'.",
        ),
    ] = None,
    directory: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for census files.",
        ),
    ] = Path("data/curated/census"),
) -> None:
    """List available census geometry files.

    Scans the census directory and displays information about each
    available TIGER census geometry file, including type, year, row count,
    and file metadata.

    Examples:

        coclab list-census

        coclab list-census --type tracts

        coclab list-census --type counties

        coclab list-census --dir /path/to/census
    """
    # Validate type option
    valid_types = ("tracts", "counties")
    if census_type is not None and census_type not in valid_types:
        typer.echo(
            f"Error: Invalid type '{census_type}'. Must be one of: {', '.join(valid_types)}",
            err=True,
        )
        raise typer.Exit(1)

    # Check if directory exists
    if not directory.exists():
        typer.echo(f"Directory not found: {directory}")
        typer.echo("No census files available.")
        return

    if not directory.is_dir():
        typer.echo(f"Error: '{directory}' is not a directory.", err=True)
        raise typer.Exit(1)

    # Scan for parquet files
    parquet_files = list(directory.glob("*.parquet"))

    if not parquet_files:
        typer.echo(f"No census files found in: {directory}")
        return

    # Parse files and collect metadata
    census_files = []
    for filepath in parquet_files:
        filename = filepath.name

        # Try to parse as census geometry file
        parsed = _parse_census_filename(filename)

        if parsed is None:
            # Skip unrecognized files
            continue

        # Apply type filter
        if census_type is not None and parsed["type"] != census_type:
            continue

        # Get file metadata
        stat = filepath.stat()
        file_size = stat.st_size
        modified_time = datetime.fromtimestamp(stat.st_mtime)

        # Get row count from parquet metadata
        try:
            row_count = _get_parquet_row_count(filepath)
        except Exception:
            row_count = -1

        census_files.append(
            {
                "type": parsed["type"],
                "year": parsed["year"],
                "rows": row_count,
                "size": file_size,
                "modified": modified_time,
                "path": filepath,
            }
        )

    if not census_files:
        if census_type is not None:
            typer.echo(f"No {census_type} files found in: {directory}")
        else:
            typer.echo(f"No census files found in: {directory}")
        return

    # Sort by type, then year
    census_files.sort(key=lambda x: (x["type"], x["year"]))

    # Display table header
    typer.echo("\nAvailable census geometry files:\n")
    typer.echo(f"{'Type':<12} {'Year':<8} {'Rows':>12} {'Size':>12} {'Modified'}")
    typer.echo("-" * 65)

    # Display each census file
    for census in census_files:
        row_str = f"{census['rows']:,}" if census["rows"] >= 0 else "?"
        size_str = _format_size(census["size"])
        modified_str = census["modified"].strftime("%Y-%m-%d %H:%M")

        typer.echo(
            f"{census['type']:<12} {census['year']:<8} {row_str:>12} {size_str:>12} {modified_str}"
        )

    typer.echo(f"\nTotal: {len(census_files)} census file(s)")
