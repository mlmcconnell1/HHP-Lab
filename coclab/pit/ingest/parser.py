"""PIT data parsing and canonicalization.

This module provides functions for parsing PIT (Point-in-Time) count data
from various file formats (CSV, Excel) into a canonical schema.

Canonical PIT Schema
--------------------
- pit_year (int): Calendar year of PIT count
- coc_id (str): Normalized CoC ID (ST-NNN format)
- pit_total (int): Total persons experiencing homelessness
- pit_sheltered (int, nullable): Sheltered count
- pit_unsheltered (int, nullable): Unsheltered count
- data_source (str): Source identifier (e.g., 'hud_exchange')
- source_ref (str): URL or dataset identifier
- ingested_at (datetime, UTC): Timestamp of ingestion
- notes (str, nullable): Data quirks or caveats

Implementation Notes
--------------------
This module is part of WP-3B: PIT Parsing & Canonicalization for Phase 3.
Key requirements:
- Handle missing or merged CoCs explicitly (no silent fixes)
- Support CSV and Excel file formats
- Normalize CoC identifiers to ST-NNN format
- Flag data quality issues in notes field
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Standard column names in canonical schema
CANONICAL_COLUMNS = [
    "pit_year",
    "coc_id",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "data_source",
    "source_ref",
    "ingested_at",
    "notes",
]

# Common column name mappings from HUD Exchange files
COLUMN_MAPPINGS = {
    # CoC ID columns
    "coc_code": "coc_id",
    "coc code": "coc_id",
    "coc number": "coc_id",
    "coc_number": "coc_id",
    "cocnumber": "coc_id",
    # Total homeless columns
    "overall homeless": "pit_total",
    "overall homeless, 2024": "pit_total",
    "overall homeless, 2023": "pit_total",
    "overall homeless, 2022": "pit_total",
    "total homeless": "pit_total",
    # Sheltered columns
    "sheltered total homeless": "pit_sheltered",
    "sheltered homeless": "pit_sheltered",
    "sheltered total": "pit_sheltered",
    # Unsheltered columns
    "unsheltered homeless": "pit_unsheltered",
    "unsheltered total homeless": "pit_unsheltered",
    "unsheltered": "pit_unsheltered",
}

# US State and Territory codes for CoC ID validation
US_STATE_CODES = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    # US Territories
    "AS", "DC", "GU", "MP", "PR", "VI",
})


class PITParseError(Exception):
    """Exception raised when PIT data parsing fails."""

    pass


class InvalidCoCIdError(ValueError):
    """Exception raised when a CoC ID cannot be normalized."""

    pass


def normalize_coc_id(raw_id: str, *, validate_state: bool = True) -> str:
    """Normalize a CoC identifier to standard ST-NNN format.

    Handles various input formats commonly seen in PIT data files:
    - "CO-500" -> "CO-500" (already normalized)
    - "co-500" -> "CO-500" (lowercase)
    - "CO500" -> "CO-500" (missing hyphen)
    - "CO 500" -> "CO-500" (space instead of hyphen)
    - "CO_500" -> "CO-500" (underscore)
    - "CO-5" -> "CO-005" (short number, padded)
    - " CO-500 " -> "CO-500" (whitespace trimmed)
    - "MO-604a" -> "MO-604" (letter suffix stripped, e.g., cross-state CoCs)

    Note: Strings longer than 7 characters (after trimming) are rejected early
    to skip footnotes and other non-CoC text in data files.

    Parameters
    ----------
    raw_id : str
        Raw CoC identifier string.
    validate_state : bool, optional
        If True (default), validate that the state code is a valid US state/territory.

    Returns
    -------
    str
        Normalized CoC ID in ST-NNN format (e.g., "CO-500", "CA-600").

    Raises
    ------
    InvalidCoCIdError
        If the ID cannot be parsed into a valid format.

    Examples
    --------
    >>> normalize_coc_id("CO-500")
    'CO-500'
    >>> normalize_coc_id("co-500")
    'CO-500'
    >>> normalize_coc_id("CO500")
    'CO-500'
    >>> normalize_coc_id(" CO-500 ")
    'CO-500'
    >>> normalize_coc_id("CO-5")
    'CO-005'
    >>> normalize_coc_id("MO-604a")
    'MO-604'
    """
    if pd.isna(raw_id) or not raw_id:
        raise InvalidCoCIdError("CoC ID cannot be empty or null")

    # Clean whitespace and convert to uppercase
    cleaned = str(raw_id).strip().upper()

    if not cleaned:
        raise InvalidCoCIdError("CoC ID cannot be empty or null")

    # Valid CoC IDs are at most 7 chars (e.g., "ST-NNNx" with letter suffix)
    # Skip longer strings early - they're likely footnotes or other text
    if len(cleaned) > 7:
        raise InvalidCoCIdError(
            f"CoC ID too long ({len(cleaned)} chars): {raw_id[:50]!r}..."
            if len(raw_id) > 50 else f"CoC ID too long ({len(cleaned)} chars): {raw_id!r}"
        )

    # Pattern: two letters, optional separator (dash/space/underscore), 1-3 digits,
    # optional letter suffix (e.g., "a" in MO-604a for cross-state CoCs)
    # Supports various formats: CO-500, CO500, CO 500, CO_500, CO-5, CO-05, MO-604a
    match = re.match(r"^([A-Z]{2})[-\s_]*(\d{1,3})([A-Z])?$", cleaned)
    if match:
        state, number, suffix = match.groups()

        # Validate state code if requested
        if validate_state and state not in US_STATE_CODES:
            raise InvalidCoCIdError(
                f"Invalid state code '{state}' in CoC ID: {raw_id!r}. "
                f"Must be a valid US state or territory code."
            )

        # Zero-pad number to 3 digits
        normalized = f"{state}-{int(number):03d}"

        # Log when letter suffix is stripped (e.g., MO-604a -> MO-604)
        if suffix:
            logger.info(
                f"Mapping CoC ID '{raw_id}' -> '{normalized}' "
                f"(stripped '{suffix}' suffix, cross-state CoC)"
            )

        return normalized

    raise InvalidCoCIdError(
        f"Cannot normalize CoC ID: {raw_id!r}. "
        f"Expected format like 'ST-NNN' (e.g., 'CO-500')"
    )


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name from candidates."""
    df_cols_lower = {c.lower().strip(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in df_cols_lower:
            return df_cols_lower[candidate.lower()]
    return None


def _read_file(
    file_path: Path, sheet_name: str | int | None = None, year: int | None = None
) -> pd.DataFrame:
    """Read a PIT data file (CSV or Excel).

    Parameters
    ----------
    file_path : Path
        Path to the data file.
    sheet_name : str or int, optional
        Specific sheet to read.
    year : int, optional
        If provided, prefer a sheet named after this year (e.g., "2024").
        This is needed for new HUD User .xlsb files which have year-named sheets.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)
    elif suffix in (".xlsx", ".xls", ".xlsb"):
        # HUD files often have multiple sheets
        # .xlsb is Excel Binary format (used by HUD User since 2024)
        engine = "pyxlsb" if suffix == ".xlsb" else None

        if sheet_name is not None:
            return pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)

        try:
            xl = pd.ExcelFile(file_path, engine=engine)
            sheet_names = xl.sheet_names

            # For new HUD User format, look for year-named sheet first
            if year is not None:
                year_str = str(year)
                if year_str in sheet_names:
                    logger.info(f"Using year sheet: {year_str}")
                    return pd.read_excel(file_path, sheet_name=year_str, engine=engine)

            # Look for PIT-specific sheets (but avoid template/chart sheets)
            for name in sheet_names:
                name_lower = name.lower()
                if ("pit" in name_lower or "count" in name_lower) and \
                   "template" not in name_lower and "chart" not in name_lower:
                    logger.info(f"Using sheet: {name}")
                    return pd.read_excel(file_path, sheet_name=name, engine=engine)

            # Fall back to first sheet
            logger.info(f"Using first sheet: {sheet_names[0]}")
            return pd.read_excel(file_path, sheet_name=0, engine=engine)

        except Exception as e:
            logger.warning(f"Error reading Excel file: {e}")
            return pd.read_excel(file_path, sheet_name=0, engine=engine)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def parse_pit_file(
    file_path: Path,
    year: int,
    source: str = "hud_exchange",
    source_ref: str | None = None,
    sheet_name: str | int | None = None,
) -> pd.DataFrame:
    """Parse a PIT data file into canonical schema.

    Parameters
    ----------
    file_path : Path
        Path to the PIT data file (CSV or Excel).
    year : int
        The PIT count year to extract (files often contain multiple years).
    source : str
        Data source identifier.
    source_ref : str, optional
        URL or reference for the source data.
    sheet_name : str or int, optional
        Specific sheet to read from Excel files.

    Returns
    -------
    pd.DataFrame
        DataFrame in canonical schema with columns:
        pit_year, coc_id, pit_total, pit_sheltered, pit_unsheltered,
        data_source, source_ref, ingested_at, notes

    Raises
    ------
    ValueError
        If required columns cannot be found or data is invalid.
    """
    logger.info(f"Parsing PIT file: {file_path} for year {year}")

    df = _read_file(file_path, sheet_name, year=year)
    logger.info(f"Read {len(df)} rows, columns: {list(df.columns)}")

    # Normalize column names for matching
    df.columns = [str(c).strip() for c in df.columns]

    # Find CoC ID column
    coc_col = _find_column(
        df, ["coc_code", "coc code", "coc number", "coc_number", "cocnumber", "coc"]
    )
    if not coc_col:
        raise ValueError(f"Cannot find CoC ID column. Available: {list(df.columns)}")

    # Look for year-specific columns or filter by year column
    year_str = str(year)

    # Check if there's a year column to filter on
    year_col = _find_column(df, ["year", "pit_year", "count_year"])

    # Find homeless count columns
    # Try year-specific columns first (e.g., "Overall Homeless, 2024")
    total_candidates = [
        f"overall homeless, {year}",
        f"overall homeless {year}",
        f"total homeless, {year}",
        f"total homeless {year}",
        "overall homeless",
        "total homeless",
        "pit_total",
    ]
    total_col = _find_column(df, total_candidates)

    sheltered_candidates = [
        f"sheltered total homeless, {year}",
        f"sheltered homeless, {year}",
        f"sheltered total, {year}",
        "sheltered total homeless",
        "sheltered homeless",
        "pit_sheltered",
    ]
    sheltered_col = _find_column(df, sheltered_candidates)

    unsheltered_candidates = [
        f"unsheltered homeless, {year}",
        f"unsheltered total homeless, {year}",
        f"unsheltered, {year}",
        "unsheltered homeless",
        "unsheltered total homeless",
        "pit_unsheltered",
    ]
    unsheltered_col = _find_column(df, unsheltered_candidates)

    if not total_col:
        raise ValueError(
            f"Cannot find total homeless column for year {year}. "
            f"Available: {list(df.columns)}"
        )

    # Filter by year if year column exists
    if year_col:
        df = df[df[year_col] == year].copy()
        logger.info(f"Filtered to {len(df)} rows for year {year}")

    # Build result DataFrame
    result_rows = []
    ingested_at = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        try:
            coc_id = normalize_coc_id(row[coc_col])
        except ValueError as e:
            logger.warning(f"Skipping row with invalid CoC ID: {e}")
            continue

        pit_total = row[total_col]
        if pd.isna(pit_total):
            logger.warning(f"Skipping {coc_id}: missing total count")
            continue

        try:
            pit_total = int(pit_total)
        except (ValueError, TypeError):
            logger.warning(f"Skipping {coc_id}: invalid total count {pit_total!r}")
            continue

        pit_sheltered = None
        if sheltered_col and not pd.isna(row.get(sheltered_col)):
            try:
                pit_sheltered = int(row[sheltered_col])
            except (ValueError, TypeError):
                pass

        pit_unsheltered = None
        if unsheltered_col and not pd.isna(row.get(unsheltered_col)):
            try:
                pit_unsheltered = int(row[unsheltered_col])
            except (ValueError, TypeError):
                pass

        result_rows.append(
            {
                "pit_year": year,
                "coc_id": coc_id,
                "pit_total": pit_total,
                "pit_sheltered": pit_sheltered,
                "pit_unsheltered": pit_unsheltered,
                "data_source": source,
                "source_ref": source_ref or str(file_path),
                "ingested_at": ingested_at,
                "notes": None,
            }
        )

    result = pd.DataFrame(result_rows)

    # Check for duplicates
    duplicates = result[result.duplicated(subset=["coc_id"], keep=False)]
    if len(duplicates) > 0:
        dup_cocs = duplicates["coc_id"].unique()
        logger.warning(f"Found duplicate CoC IDs: {list(dup_cocs)}")
        # Keep first occurrence
        result = result.drop_duplicates(subset=["coc_id"], keep="first")

    logger.info(f"Parsed {len(result)} CoC records for year {year}")
    return result


def write_pit_parquet(
    df: pd.DataFrame,
    output_path: Path | str,
    *,
    compression: str = "snappy",
) -> Path:
    """Write parsed PIT data to Parquet format with provenance metadata.

    Embeds provenance metadata in the Parquet file following the
    coclab.provenance conventions.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame in canonical PIT schema.
    output_path : Path or str
        Output file path.
    compression : str, optional
        Parquet compression codec (default: snappy).

    Returns
    -------
    Path
        Path to the written file.

    Raises
    ------
    ValueError
        If required columns are missing from the DataFrame.
    """
    from coclab.provenance import PROVENANCE_KEY, ProvenanceBlock

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Validate required columns
    required_cols = {"pit_year", "coc_id", "pit_total", "data_source", "source_ref", "ingested_at"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Ensure proper dtypes
    df = df.copy()
    df["pit_year"] = df["pit_year"].astype(int)
    df["pit_total"] = df["pit_total"].astype(int)
    df["coc_id"] = df["coc_id"].astype(str)
    df["data_source"] = df["data_source"].astype(str)
    df["source_ref"] = df["source_ref"].astype(str)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)

    # Handle nullable integer columns
    for col in ["pit_sheltered", "pit_unsheltered"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")  # Nullable integer

    # Create provenance metadata
    pit_year = int(df["pit_year"].iloc[0]) if len(df) > 0 else None
    provenance = ProvenanceBlock(
        extra={
            "pit_year": pit_year,
            "row_count": len(df),
            "data_source": df["data_source"].iloc[0] if len(df) > 0 else None,
            "source_ref": df["source_ref"].iloc[0] if len(df) > 0 else None,
        }
    )

    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Add provenance metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
    }
    table = table.replace_schema_metadata(new_meta)

    # Write file
    pq.write_table(table, output_path, compression=compression)
    logger.info(f"Wrote PIT data to {output_path} ({len(df)} records)")
    return output_path


def get_canonical_output_path(year: int, base_dir: Path | str | None = None) -> Path:
    """Get the canonical output path for a PIT year.

    Parameters
    ----------
    year : int
        PIT survey year.
    base_dir : Path or str, optional
        Base directory for curated PIT data.
        Defaults to 'data/curated/pit'.

    Returns
    -------
    Path
        Path like 'data/curated/pit/pit_counts__2024.parquet'.
    """
    if base_dir is None:
        base_dir = Path("data/curated/pit")
    else:
        base_dir = Path(base_dir)
    return base_dir / f"pit_counts__{year}.parquet"
