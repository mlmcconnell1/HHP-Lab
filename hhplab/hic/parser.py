"""HUD Housing Inventory Count parsing and curated output helpers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.naming import hic_filename
from hhplab.paths import curated_dir
from hhplab.pit.ingest.parser import normalize_coc_id
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import HIC_CANONICAL_COLUMNS

CANONICAL_COLUMNS: list[str] = HIC_CANONICAL_COLUMNS

HIC_DATA_SOURCE = "HUD Housing Inventory Count"

_SUPPORTED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".xlsb"}

_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "hic_year": ("year", "hic_year", "inventory_year"),
    "coc_id": ("coc_id", "cocid", "coc_id_hud", "coc_id_hud_num", "coc_id_number"),
    "coc_name": ("coc_name", "coc", "continuum_of_care", "cocname"),
    "state": ("state", "coc_state", "cocstate"),
    "total_beds": ("total_beds", "total_bed_inventory", "bed_total", "beds_total"),
    "total_units": ("total_units", "total_unit_inventory", "unit_total", "units_total"),
}

_BED_COLUMN_RE = re.compile(r"(?:^|_)beds?(?:_|$)|(?:^|_)bed_inventory(?:_|$)")
_UNIT_COLUMN_RE = re.compile(r"(?:^|_)units?(?:_|$)|(?:^|_)unit_inventory(?:_|$)")
_EXCLUDED_SUM_COMPONENT_RE = re.compile(
    r"hmis|utilization|occupied|available|seasonal|overflow|voucher|pit|persons?"
)
_YEAR_TOKEN_RE = re.compile(r"(?:^|_)(20\d{2}|19\d{2})(?:_|$)")


class HICParseError(ValueError):
    """Raised when a HIC source file cannot be parsed into canonical rows."""


@dataclass
class HICParseResult:
    """Parsed HIC data and source-file statistics."""

    df: pd.DataFrame
    rows_read: int
    rows_skipped: int
    raw_sha256: str


def normalize_column_name(name: object) -> str:
    """Normalize raw HUD HIC header text to lower snake_case."""
    normalized = str(name).strip().lower()
    normalized = normalized.replace("\\", "_")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _read_hic_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    try:
        if suffix == ".csv":
            return pd.read_csv(file_path, dtype=str)
        if suffix in {".xls", ".xlsx"}:
            return pd.read_excel(file_path, dtype=str)
        if suffix == ".xlsb":
            return pd.read_excel(file_path, dtype=str, engine="pyxlsb")
    except pd.errors.EmptyDataError as exc:
        raise HICParseError(f"HIC source file is empty: {file_path}") from exc
    raise HICParseError(
        f"Unsupported HIC file extension '{file_path.suffix}'. "
        "Use a CSV, XLS, XLSX, or XLSB file."
    )


def _resolve_column(columns: set[str], canonical: str) -> str | None:
    for candidate in _COLUMN_ALIASES[canonical]:
        if candidate in columns:
            return candidate
    return None


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    cleaned = (
        df[column]
        .astype("string")
        .str.replace(",", "", regex=False)
        .str.replace(r"^\s*$", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def _column_years(column: str) -> set[int]:
    return {int(match) for match in _YEAR_TOKEN_RE.findall(column)}


def _candidate_sum_columns(
    columns: list[str],
    pattern: re.Pattern[str],
    *,
    year: int | None = None,
) -> list[str]:
    return [
        column
        for column in columns
        if pattern.search(column) and not _EXCLUDED_SUM_COMPONENT_RE.search(column)
        and (year is None or year in _column_years(column))
    ]


def _coerce_count_column(
    df: pd.DataFrame,
    columns: list[str],
    canonical: str,
    pattern: re.Pattern[str],
    *,
    year: int,
) -> pd.Series:
    direct = _resolve_column(set(columns), canonical)
    if direct is not None:
        return _numeric_series(df, direct)

    year_columns = _candidate_sum_columns(columns, pattern, year=year)
    if year_columns:
        total_year_columns = [column for column in year_columns if "total" in column]
        component_columns = total_year_columns or year_columns
    elif any(_column_years(column) for column in _candidate_sum_columns(columns, pattern)):
        component_columns = []
    else:
        component_columns = _candidate_sum_columns(columns, pattern)
    if not component_columns:
        raise HICParseError(
            f"Missing HIC {canonical} column. Expected one of "
            f"{', '.join(_COLUMN_ALIASES[canonical])} or component columns matching "
            f"{pattern.pattern!r}."
        )
    total = pd.Series(0, index=df.index, dtype="float64")
    for column in component_columns:
        total = total + _numeric_series(df, column)
    return total


def parse_hic_file(
    file_path: Path | str,
    *,
    year: int,
    source: str = HIC_DATA_SOURCE,
    source_ref: str | None = None,
) -> HICParseResult:
    """Parse a HUD HIC CSV or Excel file into canonical CoC-year summary rows."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"HIC source file not found: {path}")

    raw_bytes = path.read_bytes()
    raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    raw = _read_hic_file(path)
    rows_read = len(raw)
    if raw.empty:
        raise HICParseError(f"HIC source file is empty: {path}")

    df = raw.rename(columns={column: normalize_column_name(column) for column in raw.columns})
    columns = list(df.columns)
    column_set = set(columns)

    coc_id_col = _resolve_column(column_set, "coc_id")
    if coc_id_col is None:
        raise HICParseError("Missing HIC CoC ID column. Expected Coc ID, Coc\\ID, or coc_id.")

    year_col = _resolve_column(column_set, "hic_year")
    coc_name_col = _resolve_column(column_set, "coc_name")
    state_col = _resolve_column(column_set, "state")

    parsed = pd.DataFrame(index=df.index)
    parsed["hic_year"] = (
        pd.to_numeric(df[year_col], errors="coerce").fillna(year).astype("int64")
        if year_col is not None
        else year
    )
    parsed["coc_id"] = df[coc_id_col].map(lambda value: _normalize_hic_coc_id(value))
    parsed["coc_name"] = df[coc_name_col].astype("string").str.strip() if coc_name_col else pd.NA
    parsed["state"] = df[state_col].astype("string").str.strip().str.upper() if state_col else pd.NA
    parsed["total_beds"] = _coerce_count_column(
        df,
        columns,
        "total_beds",
        _BED_COLUMN_RE,
        year=year,
    )
    parsed["total_units"] = _coerce_count_column(
        df,
        columns,
        "total_units",
        _UNIT_COLUMN_RE,
        year=year,
    )

    valid = parsed["coc_id"].notna()
    parsed = parsed.loc[valid].copy()
    rows_skipped = rows_read - len(parsed)
    if parsed.empty:
        raise HICParseError("No valid HIC records remained after CoC ID normalization.")

    parsed = parsed[parsed["hic_year"] == int(year)].copy()
    if parsed.empty:
        raise HICParseError(f"No HIC records found for year {year}.")

    grouped = (
        parsed.groupby(["hic_year", "coc_id"], as_index=False, dropna=False)
        .agg(
            coc_name=("coc_name", _first_non_null),
            state=("state", _first_non_null),
            total_beds=("total_beds", "sum"),
            total_units=("total_units", "sum"),
        )
        .sort_values(["hic_year", "coc_id"])
        .reset_index(drop=True)
    )
    grouped["total_beds"] = grouped["total_beds"].round().astype("Int64")
    grouped["total_units"] = grouped["total_units"].round().astype("Int64")
    grouped["data_source"] = source
    grouped["source_ref"] = source_ref or str(path)
    grouped["ingested_at"] = datetime.now(UTC)
    grouped["notes"] = pd.NA

    return HICParseResult(
        df=grouped[CANONICAL_COLUMNS],
        rows_read=rows_read,
        rows_skipped=rows_skipped,
        raw_sha256=raw_sha256,
    )


def _normalize_hic_coc_id(value: object) -> str | None:
    if pd.isna(value):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return normalize_coc_id(raw)
    except ValueError:
        return None


def _first_non_null(values: pd.Series) -> str | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    value = str(non_null.iloc[0]).strip()
    return value or None


def write_hic_parquet(
    df: pd.DataFrame,
    output_path: Path | str,
    *,
    raw_path: Path | str,
    raw_sha256: str,
    source_ref: str,
    rows_read: int,
    rows_skipped: int,
) -> Path:
    """Write canonical HIC rows with embedded provenance metadata."""
    missing = set(CANONICAL_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required HIC columns: {sorted(missing)}")

    normalized = df[CANONICAL_COLUMNS].copy()
    normalized["hic_year"] = normalized["hic_year"].astype("int64")
    normalized["total_beds"] = normalized["total_beds"].astype("Int64")
    normalized["total_units"] = normalized["total_units"].astype("Int64")
    normalized["ingested_at"] = pd.to_datetime(normalized["ingested_at"], utc=True)

    years = sorted(int(year) for year in normalized["hic_year"].dropna().unique())
    provenance = ProvenanceBlock(
        extra={
            "dataset": "hic",
            "source": HIC_DATA_SOURCE,
            "source_ref": source_ref,
            "raw_path": str(raw_path),
            "raw_sha256": raw_sha256,
            "years_included": years,
            "hic_year": years[0] if len(years) == 1 else None,
            "row_count": len(normalized),
            "rows_read": rows_read,
            "rows_skipped": rows_skipped,
        }
    )
    return write_parquet_with_provenance(normalized, output_path, provenance)


def get_canonical_output_path(year: int, base_dir: Path | str | None = None) -> Path:
    """Return the canonical curated HIC parquet path for a year."""
    output_dir = Path(base_dir) if base_dir is not None else curated_dir("hic")
    return output_dir / hic_filename(year)
