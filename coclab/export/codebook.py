"""Codebook generation utilities for export bundles.

This module provides functions to generate schema documentation and variable
descriptions for CoC Lab export bundles, including schema.md and variables.csv.
"""

from pathlib import Path

import pyarrow.parquet as pq

# Known variable descriptions for CoC panel columns
# Format: name -> (type, source, description)
VARIABLE_DESCRIPTIONS: dict[str, tuple[str, str, str]] = {
    "coc_id": ("string", "panel", "CoC identifier (e.g., CO-500)"),
    "year": ("int", "panel", "Panel year"),
    "pit_total": ("int", "pit", "Total homeless count from PIT"),
    "pit_sheltered": ("int", "pit", "Sheltered count (nullable)"),
    "pit_unsheltered": ("int", "pit", "Unsheltered count (nullable)"),
    "boundary_vintage_used": ("string", "panel", "CoC boundary version applied"),
    "acs_vintage_used": ("string", "panel", "ACS estimate version applied"),
    "total_population": ("float", "acs", "Weighted population estimate"),
    "median_household_income": ("float", "acs", "Population-weighted median income"),
    "median_gross_rent": ("float", "acs", "Population-weighted median rent"),
    "coverage_ratio": ("float", "panel", "Fraction of CoC area with data"),
    "zori_coc": ("float", "zori", "CoC-level ZORI rent value"),
    "zori_coverage_ratio": ("float", "zori", "Fraction of CoC covered by ZORI"),
    "zori_is_eligible": ("bool", "zori", "Meets ZORI coverage threshold"),
    "rent_to_income": ("float", "derived", "zori_coc / (median_household_income / 12)"),
    # PIT extended fields
    "pit_chronic": ("int", "pit", "Chronically homeless count (nullable)"),
    "pit_veterans": ("int", "pit", "Veteran homeless count (nullable)"),
    "pit_youth": ("int", "pit", "Youth homeless count (nullable)"),
    "pit_families": ("int", "pit", "Family homeless count (nullable)"),
    # ACS extended fields
    "poverty_rate": ("float", "acs", "Poverty rate estimate"),
    "unemployment_rate": ("float", "acs", "Unemployment rate estimate"),
    "vacancy_rate": ("float", "acs", "Housing vacancy rate estimate"),
    "median_home_value": ("float", "acs", "Median home value estimate"),
    "pct_renter_occupied": ("float", "acs", "Percentage of renter-occupied housing"),
    # Geometry fields
    "geometry": ("geometry", "boundary", "CoC boundary polygon"),
    "area_sqkm": ("float", "boundary", "CoC area in square kilometers"),
    # Additional derived metrics
    "pit_rate_per_10k": ("float", "derived", "PIT count per 10,000 population"),
    "affordability_index": ("float", "derived", "Rent affordability index"),
}

# Key columns that should be highlighted in schema documentation
KEY_COLUMNS = [
    "coc_id",
    "year",
    "pit_total",
    "total_population",
    "zori_is_eligible",
    "rent_to_income",
]


def _pyarrow_type_to_str(pa_type) -> str:
    """Convert PyArrow type to human-readable string."""
    type_str = str(pa_type)

    # Simplify common types
    type_mapping = {
        "int64": "int",
        "int32": "int",
        "int16": "int",
        "int8": "int",
        "uint64": "int",
        "uint32": "int",
        "float64": "float",
        "float32": "float",
        "double": "float",
        "string": "string",
        "large_string": "string",
        "bool": "bool",
        "boolean": "bool",
        "date32[day]": "date",
        "date64[ms]": "date",
        "timestamp[us]": "datetime",
        "timestamp[ns]": "datetime",
        "timestamp[ms]": "datetime",
    }

    return type_mapping.get(type_str, type_str)


def generate_schema_md(panel_path: Path) -> str:
    """Generate schema.md content describing the panel schema.

    Args:
        panel_path: Path to the panel parquet file

    Returns:
        Markdown content for schema.md

    Raises:
        FileNotFoundError: If panel file doesn't exist
        OSError: If panel file cannot be read
    """
    if not panel_path.exists():
        raise FileNotFoundError(f"Panel file not found: {panel_path}")

    schema = pq.read_schema(panel_path)

    lines: list[str] = []

    # Header
    lines.append("# Panel Schema")
    lines.append("")
    lines.append(
        "This document describes the schema for the CoC panel dataset included "
        "in this export bundle."
    )
    lines.append("")

    # File info
    lines.append("## File Information")
    lines.append("")
    lines.append(f"- **Source file:** `{panel_path.name}`")
    lines.append(f"- **Total columns:** {len(schema)}")
    lines.append("")

    # Key columns section
    key_cols_present = [col for col in KEY_COLUMNS if col in schema.names]
    if key_cols_present:
        lines.append("## Key Columns")
        lines.append("")
        lines.append("The following columns are primary identifiers or key metrics for analysis:")
        lines.append("")
        for col in key_cols_present:
            desc = VARIABLE_DESCRIPTIONS.get(col, (None, None, "No description"))[2]
            lines.append(f"- **`{col}`**: {desc}")
        lines.append("")

    # Full column table
    lines.append("## Column Reference")
    lines.append("")
    lines.append("| Column | Type | Source | Description |")
    lines.append("|--------|------|--------|-------------|")

    nullable_cols: list[str] = []

    for field in schema:
        name = field.name
        dtype = _pyarrow_type_to_str(field.type)

        # Check if nullable from metadata or naming convention
        is_nullable = field.nullable

        # Get description from known variables or generate placeholder
        if name in VARIABLE_DESCRIPTIONS:
            _, source, desc = VARIABLE_DESCRIPTIONS[name]
        else:
            source = "unknown"
            desc = "No description available"

        # Mark nullable fields
        if is_nullable and dtype not in ("geometry",):
            nullable_cols.append(name)

        # Add key indicator
        if name in KEY_COLUMNS:
            name_display = f"**{name}**"
        else:
            name_display = name

        lines.append(f"| {name_display} | {dtype} | {source} | {desc} |")

    lines.append("")

    # Notes section
    lines.append("## Notes")
    lines.append("")

    if nullable_cols:
        lines.append("### Nullable Fields")
        lines.append("")
        lines.append(
            "The following fields may contain null/missing values. Handle "
            "appropriately in analysis:"
        )
        lines.append("")
        for col in nullable_cols:
            lines.append(f"- `{col}`")
        lines.append("")

    lines.append("### Data Sources")
    lines.append("")
    lines.append("- **panel**: Core panel identifiers and structure")
    lines.append("- **pit**: HUD Point-in-Time homeless counts")
    lines.append("- **acs**: American Community Survey estimates")
    lines.append("- **zori**: Zillow Observed Rent Index")
    lines.append("- **boundary**: CoC boundary geometry data")
    lines.append("- **derived**: Computed from other fields")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        "*This schema documentation was auto-generated by CoC Lab. "
        "See MANIFEST.json for complete provenance metadata.*"
    )
    lines.append("")

    return "\n".join(lines)


def generate_variables_csv(panel_path: Path) -> str:
    """Generate variables.csv with columns: name, type, source, description.

    Args:
        panel_path: Path to the panel parquet file

    Returns:
        CSV content as string

    Raises:
        FileNotFoundError: If panel file doesn't exist
        OSError: If panel file cannot be read
    """
    if not panel_path.exists():
        raise FileNotFoundError(f"Panel file not found: {panel_path}")

    schema = pq.read_schema(panel_path)

    lines: list[str] = []

    # CSV header
    lines.append("name,type,source,description")

    for field in schema:
        name = field.name
        dtype = _pyarrow_type_to_str(field.type)

        # Get info from known variables or generate defaults
        if name in VARIABLE_DESCRIPTIONS:
            known_type, source, desc = VARIABLE_DESCRIPTIONS[name]
            # Use actual schema type but fall back to known type if needed
            if dtype == "unknown":
                dtype = known_type
        else:
            source = "unknown"
            desc = "No description available"

        # Escape commas and quotes in description for CSV
        desc_escaped = desc.replace('"', '""')
        if "," in desc_escaped or '"' in desc_escaped:
            desc_escaped = f'"{desc_escaped}"'

        lines.append(f"{name},{dtype},{source},{desc_escaped}")

    return "\n".join(lines)


def write_codebook(bundle_root: Path, panel_path: Path) -> list[Path]:
    """Write codebook files to bundle.

    Creates codebook/schema.md and codebook/variables.csv in the bundle
    directory.

    Args:
        bundle_root: Root directory of export bundle
        panel_path: Path to the panel parquet file

    Returns:
        List of created file paths

    Raises:
        FileNotFoundError: If panel file doesn't exist
        OSError: If files cannot be written
    """
    # Create codebook directory
    codebook_dir = bundle_root / "codebook"
    codebook_dir.mkdir(parents=True, exist_ok=True)

    created_files: list[Path] = []

    # Generate and write schema.md
    schema_content = generate_schema_md(panel_path)
    schema_path = codebook_dir / "schema.md"
    schema_path.write_text(schema_content, encoding="utf-8")
    created_files.append(schema_path)

    # Generate and write variables.csv
    variables_content = generate_variables_csv(panel_path)
    variables_path = codebook_dir / "variables.csv"
    variables_path.write_text(variables_content, encoding="utf-8")
    created_files.append(variables_path)

    return created_files
