"""CLI command for displaying CoC-level ACS measures."""

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer


def _find_latest_measures_file(measures_dir: Path) -> Path | None:
    """Find the most recent measures parquet file.

    Returns the path to the most recently modified measures file,
    or None if no files are found.

    Supports both new temporal shorthand naming (measures__A*.parquet)
    and legacy naming (coc_measures__*.parquet).
    """
    # Collect files from both naming patterns
    files = []
    files.extend(measures_dir.glob("measures__A*.parquet"))  # New pattern
    files.extend(measures_dir.glob("coc_measures__*.parquet"))  # Legacy pattern

    if not files:
        return None
    # Sort by modification time, most recent first
    return max(files, key=lambda p: p.stat().st_mtime)


def _parse_measures_filename(filename: str) -> tuple[str | None, str | None]:
    """Parse boundary and acs vintage from measures filename.

    Supports both new temporal shorthand naming (measures__A{acs}@B{boundary}.parquet)
    and legacy naming (coc_measures__{boundary}__{acs}.parquet).

    Returns:
        Tuple of (boundary_vintage, acs_vintage) or (None, None) if parse fails.
    """
    import re

    # New format: measures__A2023@B2025.parquet or measures__A2023@B2025xT2023.parquet
    if filename.startswith("measures__"):
        new_pattern = r"^measures__A(\d{4})@B(\d{4})(?:xT\d{4})?\.parquet$"
        match = re.match(new_pattern, filename)
        if match:
            acs_vintage = match.group(1)
            boundary_vintage = match.group(2)
            return boundary_vintage, acs_vintage

    # Legacy format: coc_measures__{boundary}__{acs}.parquet
    stem = filename.replace(".parquet", "")
    parts = stem.split("__")
    if len(parts) >= 3 and parts[0] == "coc_measures":
        return parts[1], parts[2]

    return None, None


def _find_measures_file(
    measures_dir: Path,
    boundary: str | None = None,
    acs: str | None = None,
) -> Path | None:
    """Find a measures file matching the specified criteria.

    Supports both new temporal shorthand naming (measures__A*.parquet)
    and legacy naming (coc_measures__*.parquet).

    Args:
        measures_dir: Directory containing measures files
        boundary: Boundary vintage to match (or None for any)
        acs: ACS vintage to match (or None for any)

    Returns:
        Path to the matching file, or None if not found.
    """
    # Collect files from both naming patterns
    files = []
    files.extend(measures_dir.glob("measures__A*.parquet"))  # New pattern
    files.extend(measures_dir.glob("coc_measures__*.parquet"))  # Legacy pattern

    if not files:
        return None

    # Filter by criteria
    matching = []
    for f in files:
        f_boundary, f_acs = _parse_measures_filename(f.name)
        if boundary is not None and f_boundary != boundary:
            continue
        if acs is not None and f_acs != str(acs):
            continue
        matching.append(f)

    if not matching:
        return None

    # Return most recent if multiple matches
    return max(matching, key=lambda p: p.stat().st_mtime)


def _format_number(value: float | None, prefix: str = "", suffix: str = "") -> str:
    """Format a number for display with optional prefix/suffix."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"{prefix}{value:,.0f}{suffix}"


def _format_currency(value: float | None) -> str:
    """Format a currency value for display."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"


def _format_ratio(value: float | None) -> str:
    """Format a ratio as a percentage for display."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.1%}"


def show_measures(
    coc: Annotated[
        str,
        typer.Option(
            "--coc",
            "-c",
            help="CoC identifier (e.g., 'CO-500')",
        ),
    ],
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="Boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        int | None,
        typer.Option(
            "--acs",
            "-a",
            help="ACS vintage year (e.g., 2022). Uses latest if not specified.",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format: 'table' (default), 'json', or 'csv'.",
        ),
    ] = "table",
    measures_dir: Annotated[
        Path,
        typer.Option(
            "--measures-dir",
            help="Directory containing measure files.",
        ),
    ] = Path("data/curated/measures"),
) -> None:
    """Display computed measures for a specific CoC.

    Shows demographic and economic measures aggregated from ACS data,
    including population, income, rent, and poverty statistics.

    Examples:

        coclab show measures --coc CO-500

        coclab show measures --coc CO-500 --boundary 2025 --acs 2022

        coclab show measures --coc NY-600 --format json
    """
    # Validate output format
    if output_format not in ("table", "json", "csv"):
        typer.echo(
            f"Error: Invalid format '{output_format}'. Use 'table', 'json', or 'csv'.",
            err=True,
        )
        raise typer.Exit(1)

    # Check if measures directory exists
    if not measures_dir.exists():
        typer.echo(
            f"Error: Measures directory not found: {measures_dir}. "
            "Run 'coclab build-measures' first.",
            err=True,
        )
        raise typer.Exit(1)

    # Find the measures file
    acs_str = str(acs) if acs is not None else None
    measures_path = _find_measures_file(measures_dir, boundary, acs_str)

    if measures_path is None:
        # Provide a helpful error message
        if boundary is not None and acs is not None:
            typer.echo(
                f"Error: No measures file found for boundary '{boundary}' and ACS year '{acs}'.",
                err=True,
            )
        elif boundary is not None:
            typer.echo(
                f"Error: No measures file found for boundary '{boundary}'.",
                err=True,
            )
        elif acs is not None:
            typer.echo(
                f"Error: No measures file found for ACS year '{acs}'.",
                err=True,
            )
        else:
            typer.echo(
                "Error: No measures files found. Run 'coclab build-measures' first.",
                err=True,
            )
        raise typer.Exit(1)

    # Load the measures data
    try:
        df = pd.read_parquet(measures_path)
    except Exception as e:
        typer.echo(f"Error reading measures file: {e}", err=True)
        raise typer.Exit(1) from e

    # Determine the CoC ID column
    coc_col = None
    for possible_col in ["coc_number", "coc_id", "COC_NUMBER", "COC_ID"]:
        if possible_col in df.columns:
            coc_col = possible_col
            break

    if coc_col is None:
        typer.echo(
            "Error: Could not find CoC identifier column in measures file.",
            err=True,
        )
        raise typer.Exit(1)

    # Filter to the specified CoC
    coc_upper = coc.upper()
    coc_data = df[df[coc_col].str.upper() == coc_upper]

    if coc_data.empty:
        typer.echo(
            f"Error: CoC '{coc}' not found in measures file.",
            err=True,
        )
        # Suggest similar CoCs if possible
        available_cocs = df[coc_col].unique()
        state_prefix = coc_upper[:2] if len(coc_upper) >= 2 else None
        if state_prefix:
            similar = [c for c in available_cocs if c.upper().startswith(state_prefix)]
            if similar:
                typer.echo(
                    f"Available CoCs in {state_prefix}: {', '.join(sorted(similar)[:5])}", err=True
                )
        raise typer.Exit(1)

    # Get the single row
    row = coc_data.iloc[0]

    # Parse file info for display
    file_boundary, file_acs = _parse_measures_filename(measures_path.name)

    # Define the measures to display
    measure_fields = [
        ("total_population", "Total Population", lambda v: _format_number(v)),
        ("adult_population", "Adult Population", lambda v: _format_number(v)),
        ("population_below_poverty", "Population Below Poverty", lambda v: _format_number(v)),
        ("median_household_income", "Median Household Income", lambda v: _format_currency(v)),
        ("median_gross_rent", "Median Gross Rent", lambda v: _format_currency(v)),
        ("coverage_ratio", "Coverage Ratio", lambda v: _format_ratio(v)),
        ("weighting_method", "Weighting Method", lambda v: str(v) if v else "N/A"),
    ]

    # Build output data
    output_data = {
        "coc_id": row[coc_col],
        "boundary_vintage": file_boundary,
        "acs_vintage": file_acs,
    }

    for field, _label, _formatter in measure_fields:
        if field in row.index:
            value = row[field]
            # Handle NaN values
            if pd.isna(value):
                output_data[field] = None
            else:
                output_data[field] = value
        else:
            output_data[field] = None

    # Output based on format
    if output_format == "json":
        # Convert any non-JSON-serializable values
        json_data = {}
        for k, v in output_data.items():
            if pd.isna(v) if hasattr(v, "__iter__") is False else False:
                json_data[k] = None
            elif isinstance(v, (int, float)) and pd.isna(v):
                json_data[k] = None
            else:
                json_data[k] = v
        typer.echo(json.dumps(json_data, indent=2))

    elif output_format == "csv":
        # Output as CSV row with header
        headers = list(output_data.keys())
        values = [str(v) if v is not None else "" for v in output_data.values()]
        typer.echo(",".join(headers))
        typer.echo(",".join(values))

    else:
        # Table format (default)
        typer.echo("")
        typer.echo("=" * 50)
        typer.echo(f"CoC MEASURES: {row[coc_col]}")
        typer.echo("=" * 50)
        typer.echo("")
        typer.echo(f"  Boundary Vintage:  {file_boundary or 'Unknown'}")
        typer.echo(f"  ACS Vintage:       {file_acs or 'Unknown'}")
        typer.echo("")
        typer.echo("-" * 50)
        typer.echo("  DEMOGRAPHIC & ECONOMIC MEASURES")
        typer.echo("-" * 50)
        typer.echo("")

        for field, label, formatter in measure_fields:
            if field in row.index:
                value = row[field]
                formatted = formatter(value)
                typer.echo(f"  {label + ':':<30} {formatted}")

        typer.echo("")
        typer.echo("=" * 50)
