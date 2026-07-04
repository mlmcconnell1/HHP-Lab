"""Inspection helpers for built panel Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.schema.measures import resolve_panel_measure_entry


class PanelInspectError(ValueError):
    """Raised when a panel inspection request is invalid."""


def _read_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise PanelInspectError(f"Panel parquet not found: {path}")
    return pd.read_parquet(path)


def _semantics(column: str, *, panel_columns: list[str]) -> dict[str, Any]:
    entry = resolve_panel_measure_entry(column, panel_columns=panel_columns)
    if entry is None:
        return {}
    return {
        "definition": entry.definition,
        "units": entry.units,
        "source_provider": entry.source_provider,
        "source_product": entry.source_product,
        "native_geometry": entry.native_geometry,
        "role_hint": entry.role_hint,
    }


def _geo_column(df: pd.DataFrame) -> str | None:
    for column in ("geo_id", "coc_id", "metro_id", "msa_id"):
        if column in df.columns:
            return column
    return None


def _measure_columns(df: pd.DataFrame, requested: list[str] | None) -> list[str]:
    if requested:
        missing = [column for column in requested if column not in df.columns]
        if missing:
            raise PanelInspectError(
                f"Requested panel columns are missing: {missing}. "
                f"Available columns: {sorted(df.columns.tolist())}"
            )
        return requested
    return [
        column
        for column in df.columns
        if pd.api.types.is_numeric_dtype(df[column]) and column != "year"
    ]


def describe_panel_file(path: Path, *, columns: list[str] | None = None) -> dict[str, Any]:
    """Return summary statistics, missingness, and coverage for a panel file."""
    df = _read_panel(path)
    measures = _measure_columns(df, columns)
    geo_col = _geo_column(df)
    summary: list[dict[str, Any]] = []
    for column in measures:
        series = pd.to_numeric(df[column], errors="coerce")
        non_null = series.dropna()
        row = {
            "column": column,
            "n": int(non_null.shape[0]),
            "missing": int(series.isna().sum()),
            "missing_rate": float(series.isna().mean()) if len(series) else 0.0,
            "mean": float(non_null.mean()) if not non_null.empty else None,
            "min": float(non_null.min()) if not non_null.empty else None,
            "median": float(non_null.median()) if not non_null.empty else None,
            "max": float(non_null.max()) if not non_null.empty else None,
        }
        row.update(_semantics(column, panel_columns=df.columns.tolist()))
        summary.append(row)

    missingness_by_year: list[dict[str, Any]] = []
    if "year" in df.columns:
        for year, group in df.groupby("year", dropna=False):
            record: dict[str, Any] = {"year": int(year) if pd.notna(year) else None}
            for column in measures:
                record[column] = float(group[column].isna().mean())
            missingness_by_year.append(record)

    missingness_by_geography: list[dict[str, Any]] = []
    if geo_col is not None:
        for geo_id, group in df.groupby(geo_col, dropna=False):
            record = {geo_col: None if pd.isna(geo_id) else str(geo_id)}
            for column in measures:
                record[column] = float(group[column].isna().mean())
            missingness_by_geography.append(record)

    return {
        "status": "ok",
        "panel_path": str(path),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "geo_column": geo_col,
        "geography_count": int(df[geo_col].nunique()) if geo_col is not None else None,
        "year_min": int(df["year"].min()) if "year" in df.columns and not df.empty else None,
        "year_max": int(df["year"].max()) if "year" in df.columns and not df.empty else None,
        "measure_count": len(measures),
        "measures": summary,
        "missingness_by_year": missingness_by_year,
        "missingness_by_geography": missingness_by_geography,
    }


def query_panel_file(
    path: Path,
    *,
    columns: list[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return filtered panel records for ad-hoc agent inspection."""
    df = _read_panel(path)
    if where:
        try:
            df = df.query(where)
        except Exception as exc:
            raise PanelInspectError(f"Invalid panel query expression '{where}': {exc}") from exc
    if columns:
        missing = [column for column in columns if column not in df.columns]
        if missing:
            raise PanelInspectError(
                f"Requested panel query columns are missing: {missing}. "
                f"Available columns: {sorted(df.columns.tolist())}"
            )
        df = df.loc[:, columns]
    if limit is not None:
        df = df.head(limit)
    return {
        "status": "ok",
        "panel_path": str(path),
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "records": df.to_dict(orient="records"),
    }
