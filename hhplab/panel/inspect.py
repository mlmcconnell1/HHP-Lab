"""Inspection helpers for built panel Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.schema.measures import resolve_panel_measure_entry
from hhplab.storage.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)


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


def _year_coverage(df: pd.DataFrame) -> dict[str, Any]:
    if "year" not in df.columns or df.empty:
        return {
            "observed_years": [],
            "expected_years": [],
            "missing_years": [],
            "observed_year_count": 0,
            "expected_year_count": 0,
            "missing_year_count": 0,
            "has_internal_gaps": False,
        }
    observed_years = sorted(
        int(year) for year in pd.to_numeric(df["year"], errors="coerce").dropna().unique()
    )
    if not observed_years:
        expected_years: list[int] = []
    else:
        expected_years = list(range(observed_years[0], observed_years[-1] + 1))
    missing_years = [year for year in expected_years if year not in set(observed_years)]
    return {
        "observed_years": observed_years,
        "expected_years": expected_years,
        "missing_years": missing_years,
        "observed_year_count": len(observed_years),
        "expected_year_count": len(expected_years),
        "missing_year_count": len(missing_years),
        "has_internal_gaps": bool(missing_years),
    }


def _geo_year_missingness(
    df: pd.DataFrame,
    *,
    geo_col: str | None,
    measures: list[str],
    expected_years: list[int],
) -> list[dict[str, Any]]:
    if geo_col is None or "year" not in df.columns or not expected_years:
        return []

    geographies = sorted(str(geo_id) for geo_id in df[geo_col].dropna().unique())
    records: list[dict[str, Any]] = []
    for geo_id in geographies:
        geo_df = df[df[geo_col].astype(str) == geo_id]
        for year in expected_years:
            group = geo_df[geo_df["year"] == year]
            row_present = not group.empty
            record: dict[str, Any] = {
                geo_col: geo_id,
                "year": year,
                "row_present": row_present,
                "row_count": int(len(group)),
            }
            missing_measure_count = 0
            for column in measures:
                missing_rate = 1.0 if group.empty else float(group[column].isna().mean())
                record[column] = missing_rate
                if missing_rate >= 1.0:
                    missing_measure_count += 1
            record["measure_count"] = len(measures)
            record["missing_measure_count"] = missing_measure_count
            record["observed_measure_count"] = len(measures) - missing_measure_count
            records.append(record)
    return records


def _aggregate_missingness(
    records: list[dict[str, Any]],
    *,
    group_column: str,
    measures: list[str],
) -> list[dict[str, Any]]:
    if not records:
        return []
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(record[group_column], []).append(record)

    output: list[dict[str, Any]] = []
    for group_value in sorted(grouped):
        group_records = grouped[group_value]
        row: dict[str, Any] = {group_column: group_value}
        for column in measures:
            row[column] = float(
                sum(float(record[column]) for record in group_records) / len(group_records)
            )
        output.append(row)
    return output


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

    year_coverage = _year_coverage(df)
    missingness_by_geo_year = _geo_year_missingness(
        df,
        geo_col=geo_col,
        measures=measures,
        expected_years=year_coverage["expected_years"],
    )
    missingness_by_year = _aggregate_missingness(
        missingness_by_geo_year,
        group_column="year",
        measures=measures,
    )
    missingness_by_geography = (
        _aggregate_missingness(
            missingness_by_geo_year,
            group_column=geo_col,
            measures=measures,
        )
        if geo_col is not None
        else []
    )

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
        "year_coverage": year_coverage,
        "missingness_by_geo_year": missingness_by_geo_year,
        "missingness_by_year": missingness_by_year,
        "missingness_by_geography": missingness_by_geography,
    }


def query_panel_file(
    path: Path,
    *,
    columns: list[str] | None = None,
    where: str | None = None,
    sort: str | None = None,
    descending: bool = False,
    top: int | None = None,
    limit: int | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Return filtered panel records for ad-hoc agent inspection."""
    df = _read_panel(path)
    input_row_count = int(len(df))
    if where:
        try:
            df = df.query(where)
        except Exception as exc:
            raise PanelInspectError(f"Invalid panel query expression '{where}': {exc}") from exc
    filtered_row_count = int(len(df))
    if sort is not None:
        if sort not in df.columns:
            raise PanelInspectError(
                f"Requested panel sort column is missing: {sort}. "
                f"Available columns: {sorted(df.columns.tolist())}"
            )
        df = df.sort_values(by=sort, ascending=not descending, kind="mergesort")
    if top is not None:
        df = df.head(top)
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
    if output_path is not None:
        source_provenance = read_provenance(path)
        provenance_kwargs: dict[str, Any] = {}
        if source_provenance is not None:
            provenance_kwargs = {
                "boundary_vintage": source_provenance.boundary_vintage,
                "tract_vintage": source_provenance.tract_vintage,
                "county_vintage": source_provenance.county_vintage,
                "acs_vintage": source_provenance.acs_vintage,
                "notation": source_provenance.notation,
                "weighting": source_provenance.weighting,
                "geo_type": source_provenance.geo_type,
                "definition_version": source_provenance.definition_version,
            }
        provenance = ProvenanceBlock(
            **provenance_kwargs,
            extra={
                "dataset_type": "panel_query",
                "input_panel": str(path),
                "input_provenance": (
                    source_provenance.to_dict() if source_provenance is not None else None
                ),
                "parameters": {
                    "where": where,
                    "columns": columns,
                    "sort": sort,
                    "descending": descending,
                    "top": top,
                    "limit": limit,
                },
                "input_row_count": input_row_count,
                "filtered_row_count": filtered_row_count,
                "output_row_count": int(len(df)),
                "output_columns": list(df.columns),
            },
        )
        write_parquet_with_provenance(df, output_path, provenance)
    return {
        "status": "ok",
        "panel_path": str(path),
        "output_path": str(output_path) if output_path is not None else None,
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "records": df.to_dict(orient="records"),
    }
