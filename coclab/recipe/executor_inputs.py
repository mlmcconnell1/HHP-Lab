"""Dataset input preparation: column resolution, year filtering, temporal filters.

Houses the helpers that turn a raw dataset DataFrame into the year-
sliced, column-validated form the resample step expects.  Also owns
``_load_support_dataset_for_year``, which loads weighted-transform
support datasets and registers them as consumed assets.  All helpers
are re-exported from ``coclab.recipe.executor`` so existing call sites
in the resample step (and the test file's direct imports) keep working.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from coclab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    _classify_path,
)
from coclab.recipe.manifest import AssetRecord
from coclab.recipe.planner import (
    ResampleTask,
    _resolve_dataset_year,
)
from coclab.recipe.probes import (
    probe_acs5_tract_translation_provenance,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_year_column,
)
from coclab.recipe.recipe_schema import (
    TemporalFilter,
    expand_year_spec,
)


def _resolve_year_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str | None:
    """Resolve year column: use declared value, or auto-detect, or None."""
    result = probe_year_column(list(df.columns), declared)
    if not result.ok:
        raise ExecutorError(result.message)
    return result.detail["year_column"] if result.detail else None


def _resolve_geo_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str:
    """Resolve geo-ID column: use declared value or auto-detect."""
    result = probe_geo_column(list(df.columns), declared)
    if not result.ok:
        raise ExecutorError(result.message)
    return result.detail["geo_column"]


def _filter_to_year(
    df: pd.DataFrame,
    year_col: str,
    year: int,
) -> pd.DataFrame:
    """Filter a DataFrame to a requested year, tolerating string year values."""
    series = df[year_col]
    if pd.api.types.is_numeric_dtype(series):
        mask = series == year
    else:
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            mask = coerced == year
        else:
            mask = series.astype(str) == str(year)
    return df[mask].copy()


def _validate_columns(
    df: pd.DataFrame,
    measures: list[str],
    dataset_id: str,
    year: int,
) -> None:
    """Validate that all required measure columns exist."""
    result = probe_measures(list(df.columns), measures, dataset_id)
    if not result.ok:
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: {result.message}"
        )


def _validate_input_dataset_provenance(
    *,
    ctx: ExecutionContext,
    dataset_id: str,
    effective_geometry,
    input_path: str,
) -> None:
    """Reject known-stale translated ACS tract caches before reading them."""
    ds = ctx.recipe.datasets.get(dataset_id)
    if ds is None:
        return

    input_file = ctx.project_root / input_path
    result = probe_acs5_tract_translation_provenance(
        dataset_id=dataset_id,
        dataset_spec=ds,
        effective_geometry=effective_geometry,
        path=input_file,
        path_label=input_path,
    )
    if result.ok:
        return

    remediation_command = (
        result.detail.get("remediation_command")
        if result.detail is not None
        else None
    )
    remediation = (
        f" Rebuild it with '{remediation_command}'."
        if remediation_command is not None
        else ""
    )
    raise ExecutorError(
        f"{result.message}{remediation}"
    )


def _reject_implicit_static_broadcast(
    *,
    ctx: ExecutionContext,
    task: ResampleTask,
    year_column: str | None,
) -> str | None:
    """Return an error when a multi-year build would silently broadcast data."""
    if year_column is not None:
        return None

    universe_years = expand_year_spec(ctx.recipe.universe)
    if len(universe_years) <= 1:
        return None

    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None:
        return None

    if task.dataset_id in ctx._distinct_paths_cache:
        distinct_paths = ctx._distinct_paths_cache[task.dataset_id]
    else:
        distinct_paths: int | None = None
        if ds.file_set is not None:
            resolved_paths = {
                _resolve_dataset_year(task.dataset_id, year, ctx.recipe).path
                for year in universe_years
            }
            distinct_paths = len(resolved_paths)
        ctx._distinct_paths_cache[task.dataset_id] = distinct_paths

    result = probe_static_broadcast(
        ds,
        task.dataset_id,
        year_column_found=year_column is not None,
        universe_year_count=len(universe_years),
        distinct_paths=distinct_paths,
    )
    if not result.ok:
        return (
            f"Dataset '{task.dataset_id}' year {task.year}: {result.message}"
        )
    return None


def _apply_temporal_filter(
    df: pd.DataFrame,
    filt: TemporalFilter,
    year: int,
    dataset_id: str,
    year_column: str | None = None,
) -> pd.DataFrame:
    """Apply a temporal filter to a DataFrame before resampling."""
    col = filt.column
    if col not in df.columns:
        raise ExecutorError(
            f"Temporal filter for '{dataset_id}': column '{col}' not found. "
            f"Available: {sorted(df.columns)}"
        )
    if filt.method == "point_in_time":
        series = df[col]
        if hasattr(series, "dt") and hasattr(series.dt, "month"):
            # Datetime column: filter by month and derive year column
            filtered = df[series.dt.month == filt.month].copy()
            if filtered.empty:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}' year {year}: "
                    f"no rows where {col}.month=={filt.month}."
                )
            if "year" not in filtered.columns:
                filtered["year"] = filtered[col].dt.year
            return filtered.drop(columns=[col])
        else:
            filtered = df[series == filt.month]
            if filtered.empty:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}' year {year}: "
                    f"no rows where {col}=={filt.month}."
                )
            return filtered.drop(columns=[col])
    elif filt.method in ("calendar_mean", "calendar_median"):
        # Group by identifier columns and preserve year keys when present.
        numeric = set(df.select_dtypes("number").columns)
        group_cols = [c for c in df.columns if c != col and c not in numeric]
        if year_column is not None:
            if year_column not in df.columns:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}': declared year column "
                    f"'{year_column}' not found. Available: {sorted(df.columns)}"
                )
            if year_column != col and year_column not in group_cols:
                group_cols.append(year_column)
        elif "year" in df.columns and "year" != col and "year" not in group_cols:
            group_cols.append("year")
        measure_cols = [
            c for c in df.select_dtypes("number").columns
            if c != col and c not in group_cols
        ]
        if not group_cols:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': no grouping columns found "
                f"after excluding temporal column '{col}'."
            )
        agg_func = "mean" if filt.method == "calendar_mean" else "median"
        return df.groupby(group_cols, as_index=False)[measure_cols].agg(agg_func)
    elif filt.method == "interpolate_to_month":
        # Linear interpolation between adjacent annual observations to estimate
        # a value at a target month.  E.g. PEP July→January: for year Y,
        # jan(Y) = 0.5 * jul(Y-1) + 0.5 * jul(Y).
        if filt.month is None:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': "
                "interpolate_to_month requires 'month'."
            )
        target_month = filt.month
        series = df[col]
        if not (hasattr(series, "dt") and hasattr(series.dt, "month")):
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                f"requires a datetime column '{col}'."
            )
        source_months = series.dt.month.unique()
        if len(source_months) != 1:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                f"expects a single source month but found {sorted(source_months)}."
            )
        source_month = int(source_months[0])
        if source_month == target_month:
            # No interpolation needed — fall through to point_in_time semantics.
            if "year" not in df.columns:
                df = df.copy()
                df["year"] = df[col].dt.year
            return df.drop(columns=[col])

        # Determine the interpolation fraction and direction.
        # months_forward = distance (in months) from the prior source observation
        # to the target month.
        months_forward = (target_month - source_month) % 12
        fraction = months_forward / 12.0

        # Identify grouping and measure columns.
        yr_col = year_column if year_column else (
            "year" if "year" in df.columns else None
        )
        if yr_col is None or yr_col not in df.columns:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                "requires a year column. Set year_column on the dataset spec."
            )
        numeric_cols = set(df.select_dtypes("number").columns)
        geo_cols = [
            c for c in df.columns
            if c != col and c != yr_col and c not in numeric_cols
        ]
        measure_cols = [
            c for c in df.select_dtypes("number").columns
            if c != yr_col
        ]

        # Build a current-year and previous-year copy for the merge.
        keep = geo_cols + [yr_col] + measure_cols
        df_curr = df[keep].copy()

        # Coerce year column to int for arithmetic (handles string-typed years).
        original_yr_dtype = df_curr[yr_col].dtype
        df_curr[yr_col] = pd.to_numeric(df_curr[yr_col], errors="coerce").astype("Int64")

        if target_month < source_month:
            # Target is before source in the calendar year.
            # January(Y) sits between Jul(Y-1) and Jul(Y).
            df_prev = df_curr.copy()
            df_prev[yr_col] = df_prev[yr_col] + 1  # align Y-1 row to year Y
        else:
            # Target is after source in the calendar year.
            # e.g. Jul(Y) sits between Jan(Y) and Jan(Y+1).
            # prev = source data at year Y, curr = source data from year Y+1
            # shifted down so it merges on year Y.
            df_prev = df_curr.copy()
            df_curr = df_curr.copy()
            df_curr[yr_col] = df_curr[yr_col] - 1

        merged = df_curr.merge(
            df_prev,
            on=geo_cols + [yr_col],
            suffixes=("", "_prev"),
            how="left",
        )

        for mcol in measure_cols:
            curr_vals = merged[mcol]
            prev_col = f"{mcol}_prev"
            if prev_col in merged.columns:
                prev_vals = merged[prev_col]
                merged[mcol] = np.where(
                    prev_vals.notna(),
                    (1 - fraction) * prev_vals + fraction * curr_vals,
                    curr_vals,
                )
        # Drop the _prev helper columns.
        prev_drop = [c for c in merged.columns if c.endswith("_prev")]
        result = merged.drop(columns=prev_drop)

        # Restore original year column dtype (e.g. string).
        if pd.api.types.is_object_dtype(original_yr_dtype):
            result[yr_col] = result[yr_col].astype(str)
        elif original_yr_dtype != result[yr_col].dtype:
            result[yr_col] = result[yr_col].astype(original_yr_dtype)

        return result
    else:
        raise ExecutorError(f"Unknown temporal filter method '{filt.method}'.")


def _load_support_dataset_for_year(
    *,
    ctx: ExecutionContext,
    dataset_id: str,
    year: int,
) -> pd.DataFrame:
    """Load an auxiliary dataset and filter it to the requested year."""
    resolved = _resolve_dataset_year(dataset_id, year, ctx.recipe)
    if resolved.path is None:
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: no input path resolved."
        )

    input_file = ctx.project_root / resolved.path
    if not input_file.exists():
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: input file not found at "
            f"{resolved.path}"
        )

    _validate_input_dataset_provenance(
        ctx=ctx,
        dataset_id=dataset_id,
        effective_geometry=resolved.effective_geometry,
        input_path=resolved.path,
    )
    df = ctx.cache.read_parquet(input_file)
    identity = ctx.cache.file_identity(input_file)
    root, rel_path = _classify_path(input_file, ctx)
    ctx.consumed_assets.append(AssetRecord(
        role="dataset",
        path=rel_path,
        sha256=identity.sha256,
        size=identity.size,
        root=root,
        dataset_id=dataset_id,
    ))

    ds = ctx.recipe.datasets[dataset_id]
    filt = ctx.recipe.filters.get(dataset_id)
    if filt is not None and isinstance(filt, TemporalFilter):
        df = _apply_temporal_filter(
            df,
            filt,
            year,
            dataset_id,
            year_column=ds.year_column,
        )

    year_col = _resolve_year_column(df, ds.year_column)
    if year_col is not None:
        df = _filter_to_year(df, year_col, year)
        if df.empty:
            raise ExecutorError(
                f"Dataset '{dataset_id}' year {year}: no rows after filtering "
                f"{year_col}=={year}."
            )

    return df
