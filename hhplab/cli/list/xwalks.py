"""CLI command for listing available crosswalk files."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Annotated

import pyarrow.parquet as pq
import typer

from hhplab.paths import curated_dir


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _parse_tract_filename(filename: str) -> dict | None:
    """Parse tract crosswalk filename to extract vintages.

    Expected formats:
        New: xwalk__B{boundary}xT{tracts}.parquet
        Legacy: coc_tract_xwalk__{boundary}__{tracts}.parquet
    """
    # New format: xwalk__B2025xT2023.parquet
    new_pattern = r"^xwalk__B(\d{4})xT(\d{4})\.parquet$"
    match = re.match(new_pattern, filename)
    if match:
        return {
            "type": "tract",
            "boundary_vintage": match.group(1),
            "census_vintage": match.group(2),
        }

    # Legacy format: coc_tract_xwalk__{boundary}__{tracts}.parquet
    legacy_pattern = r"^coc_tract_xwalk__(.+?)__(.+?)\.parquet$"
    match = re.match(legacy_pattern, filename)
    if match:
        return {
            "type": "tract",
            "boundary_vintage": match.group(1),
            "census_vintage": match.group(2),
        }
    return None


def _parse_county_filename(filename: str) -> dict | None:
    """Parse county crosswalk filename to extract vintages.

    Expected formats:
        New: xwalk__B{boundary}xC{county}.parquet
        Legacy: coc_county_xwalk__{boundary}.parquet
    """
    # New format: xwalk__B2025xC2023.parquet
    new_pattern = r"^xwalk__B(\d{4})xC(\d{4})\.parquet$"
    match = re.match(new_pattern, filename)
    if match:
        return {
            "type": "county",
            "boundary_vintage": match.group(1),
            "census_vintage": match.group(2),
        }

    # Legacy format: coc_county_xwalk__{boundary}.parquet
    legacy_pattern = r"^coc_county_xwalk__(.+?)\.parquet$"
    match = re.match(legacy_pattern, filename)
    if match:
        return {
            "type": "county",
            "boundary_vintage": match.group(1),
            "census_vintage": "-",
        }
    return None


def _parse_msa_filename(filename: str) -> dict | None:
    """Parse CoC-to-MSA crosswalk filename to extract versions."""
    population_pattern = (
        r"^msa_coc_xwalk__N(\d{4})@B(\d{4})xM(\w+)xC(\d{4})xK(\d{4})"
        r"__basis-block_population\.parquet$"
    )
    population_match = re.match(population_pattern, filename)
    if population_match:
        return {
            "type": "msa",
            "boundary_vintage": population_match.group(2),
            "definition_version": population_match.group(3),
            "census_vintage": population_match.group(4),
            "block_vintage": population_match.group(5),
            "decennial_vintage": population_match.group(1),
            "allocation_basis": "block_population",
        }
    pattern = r"^msa_coc_xwalk__B(\d{4})xM(\w+)xC(\d{4})\.parquet$"
    match = re.match(pattern, filename)
    if match:
        return {
            "type": "msa",
            "boundary_vintage": match.group(1),
            "definition_version": match.group(2),
            "census_vintage": match.group(3),
            "allocation_basis": "area",
        }
    return None


def _get_parquet_row_count(filepath: Path) -> int:
    """Get the row count from a parquet file without loading all data."""
    parquet_file = pq.ParquetFile(filepath)
    return parquet_file.metadata.num_rows


def list_xwalks(
    xwalk_type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Filter by crosswalk type: 'tract', 'county', 'msa', or 'all'.",
        ),
    ] = "all",
    directory: Annotated[
        Path | None,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for crosswalk files.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """List available crosswalk files.

    Scans the crosswalk directory and displays information about each
    available crosswalk file, including type, vintages, row count, and
    file metadata.

    Examples:

        hhplab list xwalks

        hhplab list xwalks --type tract

        hhplab list xwalks --dir /path/to/xwalks
    """
    if directory is None:
        directory = curated_dir("xwalks")

    # Validate type option
    valid_types = ("all", "tract", "county", "msa")
    if xwalk_type not in valid_types:
        typer.echo(
            f"Error: Invalid type '{xwalk_type}'. Must be one of: {', '.join(valid_types)}",
            err=True,
        )
        raise typer.Exit(1)

    # Check if directory exists
    if not directory.exists():
        if json_output:
            typer.echo(json.dumps({"status": "ok", "count": 0, "crosswalks": []}, indent=2))
        else:
            typer.echo(f"Directory not found: {directory}")
            typer.echo("No crosswalk files available.")
        return

    if not directory.is_dir():
        typer.echo(f"Error: '{directory}' is not a directory.", err=True)
        raise typer.Exit(1)

    # Scan for parquet files
    parquet_files = list(directory.glob("*.parquet"))

    if not parquet_files:
        if json_output:
            typer.echo(json.dumps({"status": "ok", "count": 0, "crosswalks": []}, indent=2))
        else:
            typer.echo(f"No crosswalk files found in: {directory}")
        return

    # Parse files and collect metadata
    crosswalks = []
    for filepath in parquet_files:
        filename = filepath.name

        # Try to parse as tract or county crosswalk
        parsed = _parse_tract_filename(filename)
        if parsed is None:
            parsed = _parse_county_filename(filename)
        if parsed is None:
            parsed = _parse_msa_filename(filename)

        if parsed is None:
            # Skip unrecognized files
            continue

        # Apply type filter
        if xwalk_type != "all" and parsed["type"] != xwalk_type:
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

        crosswalks.append(
            {
                "type": parsed["type"],
                "boundary_vintage": parsed["boundary_vintage"],
                "census_vintage": parsed["census_vintage"],
                "definition_version": parsed.get("definition_version"),
                "rows": row_count,
                "size": file_size,
                "modified": modified_time,
                "path": filepath,
            }
        )

    if not crosswalks:
        if json_output:
            typer.echo(json.dumps({"status": "ok", "count": 0, "crosswalks": []}, indent=2))
        else:
            if xwalk_type != "all":
                typer.echo(f"No {xwalk_type} crosswalk files found in: {directory}")
            else:
                typer.echo(f"No crosswalk files found in: {directory}")
        return

    # Sort by type, then boundary vintage, then census vintage
    crosswalks.sort(key=lambda x: (x["type"], x["boundary_vintage"], x["census_vintage"]))

    if json_output:
        json_items = [
            {
                "type": c["type"],
                "boundary_vintage": c["boundary_vintage"],
                "census_vintage": c["census_vintage"],
                "definition_version": c.get("definition_version"),
                "rows": c["rows"],
                "bytes": c["size"],
                "modified_at": c["modified"].isoformat(),
                "path": str(c["path"]),
            }
            for c in crosswalks
        ]
        typer.echo(json.dumps({
            "status": "ok",
            "count": len(json_items),
            "crosswalks": json_items,
        }, indent=2))
        return

    # Display table header
    typer.echo("\nAvailable crosswalk files:\n")
    typer.echo(
        f"{'Type':<8} {'Boundary':<20} {'Census':<12} {'Rows':>10} {'Size':>10} {'Modified'}"
    )
    typer.echo("-" * 85)

    # Display each crosswalk
    for xwalk in crosswalks:
        row_str = f"{xwalk['rows']:,}" if xwalk["rows"] >= 0 else "?"
        size_str = _format_size(xwalk["size"])
        modified_str = xwalk["modified"].strftime("%Y-%m-%d %H:%M")

        typer.echo(
            f"{xwalk['type']:<8} {xwalk['boundary_vintage']:<20} "
            f"{xwalk['census_vintage']:<12} {row_str:>10} {size_str:>10} {modified_str}"
        )

    typer.echo(f"\nTotal: {len(crosswalks)} crosswalk file(s)")
