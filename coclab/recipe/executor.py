"""Recipe execution orchestrator.

Given a validated RecipeV1 and pipeline id, resolves the execution plan
via the planner and executes materialize → resample → join tasks in
deterministic order.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from coclab.naming import (
    county_xwalk_path,
    panel_filename,
    tract_xwalk_path,
)
from coclab.recipe.cache import RecipeCache
from coclab.recipe.manifest import AssetRecord, RecipeManifest, write_manifest
from coclab.recipe.planner import (
    ExecutionPlan,
    JoinTask,
    MaterializeTask,
    PlannerError,
    ResampleTask,
    _resolve_dataset_year,
    resolve_plan,
)
from coclab.recipe.recipe_schema import (
    GeometryRef,
    RecipeV1,
    TemporalFilter,
    expand_year_spec,
)


class ExecutorError(Exception):
    """Raised when recipe execution fails at runtime."""


@dataclass
class StepResult:
    """Outcome of a single execution step."""

    step_kind: str
    detail: str
    success: bool
    error: str | None = None


@dataclass
class PipelineResult:
    """Aggregate outcome for one pipeline execution."""

    pipeline_id: str
    steps: list[StepResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(s.success for s in self.steps)

    @property
    def error_count(self) -> int:
        return sum(1 for s in self.steps if not s.success)


@dataclass
class ExecutionContext:
    """Shared mutable state across step executions within a pipeline."""

    project_root: Path
    recipe: RecipeV1
    # transform_id → resolved file path on disk
    transform_paths: dict[str, Path] = field(default_factory=dict)
    # (dataset_id, year) → resampled DataFrame
    intermediates: dict[tuple[str, int], pd.DataFrame] = field(
        default_factory=dict,
    )
    # Asset cache for avoiding redundant reads
    cache: RecipeCache = field(default_factory=RecipeCache)
    # Assets consumed during execution (for provenance manifest)
    consumed_assets: list[AssetRecord] = field(default_factory=list)
    # Suppress progress output (for --json mode)
    quiet: bool = False


def _echo(ctx: ExecutionContext, message: str) -> None:
    """Print progress message unless quiet mode is active."""
    if not ctx.quiet:
        typer.echo(message)


# ---------------------------------------------------------------------------
# Transform artifact resolution
# ---------------------------------------------------------------------------


def _resolve_transform_path(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> Path:
    """Map a transform spec to its expected crosswalk file path.

    Uses the transform's ``from_`` / ``to`` geometry refs to determine
    the canonical crosswalk filename via the naming module.

    Raises ExecutorError if the geometry pair is not recognised.
    """
    transform = None
    for t in recipe.transforms:
        if t.id == transform_id:
            transform = t
            break
    if transform is None:
        raise ExecutorError(
            f"Transform '{transform_id}' referenced in materialize step "
            f"but not found in recipe transforms."
        )

    from_ = transform.from_
    to = transform.to

    # Determine which geometry is the CoC boundary and which is the
    # base geography so we can build the right crosswalk filename.
    coc_ref, base_ref = _identify_coc_and_base(from_, to)
    if coc_ref is None:
        raise ExecutorError(
            f"Transform '{transform_id}' connects "
            f"{from_.type}@{from_.vintage} → {to.type}@{to.vintage}: "
            f"cannot resolve crosswalk path (no 'coc' geometry in pair)."
        )

    boundary_vintage = str(coc_ref.vintage) if coc_ref.vintage else "latest"
    base_vintage: str | int = base_ref.vintage if base_ref.vintage else "latest"

    if base_ref.type == "tract":
        return project_root / tract_xwalk_path(boundary_vintage, base_vintage)
    elif base_ref.type == "county":
        return project_root / county_xwalk_path(boundary_vintage, base_vintage)
    else:
        raise ExecutorError(
            f"Transform '{transform_id}': unsupported geometry pair "
            f"{from_.type} → {to.type}. Only tract↔coc and county↔coc "
            f"crosswalks are currently supported."
        )


def _identify_coc_and_base(
    from_: GeometryRef, to: GeometryRef,
) -> tuple[GeometryRef | None, GeometryRef]:
    """Identify which end of a transform is the CoC boundary."""
    if to.type == "coc":
        return to, from_
    if from_.type == "coc":
        return from_, to
    return None, from_


# ---------------------------------------------------------------------------
# Resample helpers
# ---------------------------------------------------------------------------

# Maps geometry types to the column name used as join key in crosswalks.
_XWALK_JOIN_KEYS: dict[str, str] = {
    "tract": "tract_geoid",
    "county": "county_fips",
}

# Auto-detect candidates for geo-ID and year columns.
_GEO_CANDIDATES: list[str] = [
    "geo_id", "GEOID", "geoid", "coc_id", "metro_id", "tract_geoid", "county_fips",
]

# Columns in crosswalks that are NOT the target geography identifier.
_XWALK_NON_GEO_COLS: set[str] = {
    "tract_geoid", "county_fips", "area_share", "pop_share",
    "intersection_area", "tract_area", "county_area", "coc_area", "geo_area",
    "boundary_vintage", "tract_vintage", "definition_version",
}


def _detect_xwalk_target_col(
    xwalk: pd.DataFrame,
    source_key: str,
) -> str:
    """Detect the target geography column in a crosswalk.

    Finds the geo-ID column by looking for known candidates that aren't
    the source join key.  Falls back to ``"coc_id"`` if detection fails.
    """
    candidates = [
        c for c in xwalk.columns
        if c not in _XWALK_NON_GEO_COLS and c != source_key
        and c in {"coc_id", "metro_id", "geo_id"}
    ]
    if len(candidates) == 1:
        return candidates[0]
    # Prefer more specific names
    for c in ("coc_id", "metro_id", "geo_id"):
        if c in candidates:
            return c
    return "coc_id"  # legacy default
_YEAR_CANDIDATES: list[str] = ["year", "pit_year"]


def _resolve_year_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str | None:
    """Resolve year column: use declared value, or auto-detect, or None."""
    if declared is not None:
        if declared not in df.columns:
            raise ExecutorError(
                f"Declared year_column '{declared}' not found in dataset. "
                f"Available: {sorted(df.columns)}"
            )
        return declared
    matches = [c for c in _YEAR_CANDIDATES if c in df.columns]
    if len(matches) > 1:
        raise ExecutorError(
            f"Ambiguous year column: found {matches}. "
            f"Declare year_column in the dataset spec to resolve."
        )
    return matches[0] if matches else None


def _resolve_geo_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str:
    """Resolve geo-ID column: use declared value or auto-detect."""
    if declared is not None:
        if declared not in df.columns:
            raise ExecutorError(
                f"Declared geo_column '{declared}' not found in dataset. "
                f"Available: {sorted(df.columns)}"
            )
        return declared
    matches = [c for c in _GEO_CANDIDATES if c in df.columns]
    if len(matches) == 0:
        raise ExecutorError(
            f"Cannot find geo-ID column in dataset. "
            f"Expected one of {_GEO_CANDIDATES}, "
            f"got columns: {sorted(df.columns)}"
        )
    if len(matches) > 1:
        raise ExecutorError(
            f"Ambiguous geo-ID column: found {matches}. "
            f"Declare geo_column in the dataset spec to resolve."
        )
    return matches[0]


def _validate_columns(
    df: pd.DataFrame,
    measures: list[str],
    dataset_id: str,
    year: int,
) -> None:
    """Validate that all required measure columns exist."""
    missing = [m for m in measures if m not in df.columns]
    if missing:
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: missing measure columns "
            f"{missing}. Available: {sorted(df.columns)}"
        )


def _reject_implicit_static_broadcast(
    *,
    ctx: ExecutionContext,
    task: ResampleTask,
    year_column: str | None,
) -> str | None:
    """Return an error when a multi-year build would silently broadcast data.

    A dataset without a year column will be reused for every requested year.
    That is sometimes intentional for static covariates, but it is unsafe as
    the default because it can hide recipe mistakes. Callers may opt in with
    ``params.broadcast_static: true`` on the dataset spec.
    """
    if year_column is not None:
        return None

    universe_years = expand_year_spec(ctx.recipe.universe)
    if len(universe_years) <= 1:
        return None

    ds = ctx.recipe.datasets[task.dataset_id]
    if bool(ds.params.get("broadcast_static", False)):
        return None

    if ds.file_set is not None:
        resolved_paths = {
            _resolve_dataset_year(task.dataset_id, year, ctx.recipe).path
            for year in universe_years
        }
        if len(resolved_paths) > 1:
            return None

    return (
        f"Dataset '{task.dataset_id}' year {task.year}: no year column found "
        f"after preprocessing, but recipe universe spans {len(universe_years)} "
        "years. Reusing the same dataset for every year would broadcast a "
        "static snapshot across time. Add a year_column, switch to file_set "
        "for year-specific files, or set params.broadcast_static=true if this "
        "broadcast is intentional."
    )


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
        filtered = df[df[col] == filt.month]
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
    else:
        raise ExecutorError(f"Unknown temporal filter method '{filt.method}'.")


def _resample_identity(
    df: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Identity resample: passthrough with column standardisation."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    cols = [geo_col] + task.measures
    if "year" in df.columns and "year" not in cols:
        cols.insert(1, "year")

    result = df[cols].copy()
    if geo_col != "geo_id":
        result = result.rename(columns={geo_col: "geo_id"})
    return result


def _resample_aggregate(
    df: pd.DataFrame,
    xwalk: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Aggregate resample: many-to-few via crosswalk weights."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    # Determine join key in crosswalk based on effective geometry type
    geo_type = task.effective_geometry.type
    xwalk_key = _XWALK_JOIN_KEYS.get(geo_type)
    if xwalk_key is None or xwalk_key not in xwalk.columns:
        raise ExecutorError(
            f"Resample aggregate for '{task.dataset_id}' year {task.year}: "
            f"crosswalk does not have expected join key '{xwalk_key}' "
            f"for geometry type '{geo_type}'. "
            f"Crosswalk columns: {sorted(xwalk.columns)}"
        )

    if "area_share" not in xwalk.columns:
        raise ExecutorError(
            "Crosswalk missing weight column 'area_share'. "
            f"Available: {sorted(xwalk.columns)}"
        )
    has_pop_share = "pop_share" in xwalk.columns and xwalk["pop_share"].notna().any()

    # Detect the target geography column in the crosswalk
    target_col = _detect_xwalk_target_col(xwalk, xwalk_key)

    # Merge dataset with crosswalk
    xwalk_cols = [target_col, xwalk_key, "area_share"]
    if "pop_share" in xwalk.columns:
        xwalk_cols.append("pop_share")
    merged = df.merge(
        xwalk[xwalk_cols],
        left_on=geo_col,
        right_on=xwalk_key,
        how="inner",
    )

    if merged.empty:
        raise ExecutorError(
            f"Resample aggregate for '{task.dataset_id}' year {task.year}: "
            f"zero rows after joining dataset ({geo_col}) with crosswalk "
            f"({xwalk_key}). Check that geo-ID formats match."
        )

    # Resolve per-measure aggregation methods
    measure_aggs: dict[str, str] = {}
    if task.measure_aggregations:
        measure_aggs = task.measure_aggregations
    else:
        default_agg = task.aggregation or "sum"
        measure_aggs = {m: default_agg for m in task.measures}

    # Convert measures to float64 to handle nullable Pandas types (Int64, Float64)
    for m in task.measures:
        before_notna = merged[m].notna().sum()
        merged[m] = pd.to_numeric(merged[m], errors="coerce").astype("float64")
        lost = before_notna - merged[m].notna().sum()
        if lost > 0:
            warnings.warn(
                f"Dataset '{task.dataset_id}' year {task.year}: "
                f"{lost} non-numeric value(s) in measure '{m}' coerced to NaN.",
                stacklevel=2,
            )
    merged["area_share"] = merged["area_share"].astype("float64")
    if "pop_share" in merged.columns:
        merged["pop_share"] = pd.to_numeric(merged["pop_share"], errors="coerce").astype("float64")

    # Group measures by aggregation method for efficient dispatch
    agg_groups: dict[str, list[str]] = {}
    for m, agg in measure_aggs.items():
        agg_groups.setdefault(agg, []).append(m)

    # Aggregate per geography unit, per method
    result_parts: list[pd.DataFrame] = []
    for agg, measures in agg_groups.items():
        if agg == "sum":
            part = merged.copy()
            for m in measures:
                part[m] = part[m] * part["area_share"]
            result_parts.append(part.groupby(target_col)[measures].sum().reset_index())
        elif agg == "mean":
            result_parts.append(merged.groupby(target_col)[measures].mean().reset_index())
        elif agg == "weighted_mean":
            weight_col = "pop_share" if has_pop_share else "area_share"
            rows = []
            for geo_id, group in merged.groupby(target_col):
                w = group[weight_col].values
                w_sum = w.sum()
                row: dict[str, object] = {target_col: geo_id}
                for m in measures:
                    vals = group[m].values
                    mask = ~np.isnan(vals)
                    if w_sum > 0 and mask.any():
                        row[m] = (vals[mask] * w[mask]).sum() / w[mask].sum()
                    else:
                        row[m] = None
                rows.append(row)
            result_parts.append(pd.DataFrame(rows))
        else:
            raise ExecutorError(
                f"Unsupported aggregation method '{agg}' for "
                f"dataset '{task.dataset_id}'."
            )

    # Merge all result parts on target column
    result = result_parts[0]
    for part in result_parts[1:]:
        result = result.merge(part, on=target_col, how="outer")

    if target_col != "geo_id":
        result = result.rename(columns={target_col: "geo_id"})
    result["year"] = task.year
    return result


def _resample_allocate(
    df: pd.DataFrame,
    xwalk: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Allocate resample: few-to-many via crosswalk weights."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    # For allocate, the target geometry is finer-grained.
    # The crosswalk connects target (fine) <-> source (coarse).
    target_type = task.to_geometry.type
    target_key = _XWALK_JOIN_KEYS.get(target_type)
    if target_key is None or target_key not in xwalk.columns:
        raise ExecutorError(
            f"Resample allocate for '{task.dataset_id}' year {task.year}: "
            f"crosswalk does not have target key '{target_key}' "
            f"for target geometry '{target_type}'. "
            f"Crosswalk columns: {sorted(xwalk.columns)}"
        )

    weight_col = "area_share"
    if weight_col not in xwalk.columns:
        raise ExecutorError(
            f"Crosswalk missing weight column '{weight_col}'. "
            f"Available: {sorted(xwalk.columns)}"
        )

    # Detect the source geography column in the crosswalk
    source_col = _detect_xwalk_target_col(xwalk, target_key)

    # Merge dataset (coarse) with crosswalk
    merged = df.merge(
        xwalk[[source_col, target_key, weight_col]],
        left_on=geo_col,
        right_on=source_col,
        how="inner",
    )

    if merged.empty:
        raise ExecutorError(
            f"Resample allocate for '{task.dataset_id}' year {task.year}: "
            f"zero rows after join."
        )

    # Allocate: distribute source value by weight
    for m in task.measures:
        merged[m] = merged[m] * merged[weight_col]

    result = merged[[target_key] + task.measures].copy()
    result = result.rename(columns={target_key: "geo_id"})
    result["year"] = task.year
    return result


# ---------------------------------------------------------------------------
# Step executors
# ---------------------------------------------------------------------------


def _execute_materialize(
    task: MaterializeTask,
    ctx: ExecutionContext,
) -> StepResult:
    """Resolve or verify transform artifacts on disk.

    For each transform_id, maps the transform spec to a canonical
    crosswalk file path and checks whether it exists.  Stores resolved
    paths in ``ctx.transform_paths`` for subsequent resample steps.
    """
    detail = f"materialize transforms: {task.transform_ids}"
    _echo(ctx, f"  [materialize] {detail}")

    for tid in task.transform_ids:
        try:
            path = _resolve_transform_path(
                tid, ctx.recipe, ctx.project_root,
            )
        except ExecutorError as exc:
            return StepResult(
                step_kind="materialize",
                detail=detail,
                success=False,
                error=str(exc),
            )

        if path.exists():
            _echo(ctx, f"    reuse: {path.relative_to(ctx.project_root)}")
            ctx.transform_paths[tid] = path
            identity = ctx.cache.file_identity(path)
            ctx.consumed_assets.append(AssetRecord(
                role="crosswalk",
                path=str(path.relative_to(ctx.project_root)),
                sha256=identity.sha256,
                size=identity.size,
                transform_id=tid,
            ))
        else:
            return StepResult(
                step_kind="materialize",
                detail=detail,
                success=False,
                error=(
                    f"Transform '{tid}' artifact not found at "
                    f"{path.relative_to(ctx.project_root)}. "
                    f"Run 'coclab generate xwalks' to build it first."
                ),
            )

    return StepResult(step_kind="materialize", detail=detail, success=True)


def _execute_resample(
    task: ResampleTask,
    ctx: ExecutionContext,
) -> StepResult:
    """Execute a resample step (identity/aggregate/allocate).

    Loads the input dataset, applies the resampling method, validates
    required columns, and stores the result in ``ctx.intermediates``.
    """
    detail = (
        f"resample {task.dataset_id} year={task.year} "
        f"method={task.method}"
    )
    if task.transform_id:
        detail += f" via={task.transform_id}"
    _echo(ctx, f"  [resample] {detail}")

    # Load input dataset
    if task.input_path is None:
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=(
                f"Dataset '{task.dataset_id}' year {task.year}: "
                f"no input path resolved by planner."
            ),
        )

    input_file = ctx.project_root / task.input_path
    if not input_file.exists():
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=(
                f"Dataset '{task.dataset_id}' year {task.year}: "
                f"input file not found at {task.input_path}"
            ),
        )

    try:
        df = ctx.cache.read_parquet(input_file)
    except (FileNotFoundError, OSError, pa.ArrowInvalid) as exc:
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=(
                f"Dataset '{task.dataset_id}' year {task.year}: "
                f"failed to read {task.input_path}: {exc}"
            ),
        )

    # Record consumed asset (deduplicated by path later in manifest)
    identity = ctx.cache.file_identity(input_file)
    ctx.consumed_assets.append(AssetRecord(
        role="dataset",
        path=str(input_file.relative_to(ctx.project_root)),
        sha256=identity.sha256,
        size=identity.size,
        dataset_id=task.dataset_id,
    ))

    # Apply temporal filter if declared for this dataset
    filt = ctx.recipe.filters.get(task.dataset_id)
    if filt is not None and isinstance(filt, TemporalFilter):
        try:
            df = _apply_temporal_filter(
                df,
                filt,
                task.year,
                task.dataset_id,
                year_column=task.year_column,
            )
        except ExecutorError as exc:
            return StepResult(
                step_kind="resample",
                detail=detail,
                success=False,
                error=str(exc),
            )

    # Filter to the target year if the dataset has a year column
    year_col = _resolve_year_column(df, task.year_column)
    static_broadcast_error = _reject_implicit_static_broadcast(
        ctx=ctx,
        task=task,
        year_column=year_col,
    )
    if static_broadcast_error is not None:
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=static_broadcast_error,
        )
    if year_col is not None:
        df = df[df[year_col] == task.year].copy()
        if df.empty:
            return StepResult(
                step_kind="resample",
                detail=detail,
                success=False,
                error=(
                    f"Dataset '{task.dataset_id}' year {task.year}: "
                    f"no rows after filtering {year_col}=={task.year}."
                ),
            )
        if year_col != "year":
            df = df.rename(columns={year_col: "year"})

    try:
        if task.method == "identity":
            result_df = _resample_identity(df, task)
        elif task.method in ("aggregate", "allocate"):
            # Load crosswalk
            if task.transform_id is None:
                return StepResult(
                    step_kind="resample",
                    detail=detail,
                    success=False,
                    error=(
                        f"Dataset '{task.dataset_id}' year {task.year}: "
                        f"method={task.method} requires a transform but "
                        f"none was resolved."
                    ),
                )
            xwalk_path = ctx.transform_paths.get(task.transform_id)
            if xwalk_path is None:
                return StepResult(
                    step_kind="resample",
                    detail=detail,
                    success=False,
                    error=(
                        f"Transform '{task.transform_id}' not materialized. "
                        f"Ensure a materialize step runs first."
                    ),
                )
            try:
                xwalk = ctx.cache.read_parquet(xwalk_path)
            except (FileNotFoundError, OSError, pa.ArrowInvalid) as exc:
                return StepResult(
                    step_kind="resample",
                    detail=detail,
                    success=False,
                    error=(
                        f"Dataset '{task.dataset_id}' year {task.year}: "
                        f"failed to read crosswalk "
                        f"'{task.transform_id}': {exc}"
                    ),
                )

            if task.method == "aggregate":
                result_df = _resample_aggregate(df, xwalk, task)
            else:
                result_df = _resample_allocate(df, xwalk, task)
        else:
            return StepResult(
                step_kind="resample",
                detail=detail,
                success=False,
                error=f"Unknown resample method '{task.method}'.",
            )
    except ExecutorError as exc:
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=str(exc),
        )

    # Store intermediate
    ctx.intermediates[(task.dataset_id, task.year)] = result_df
    return StepResult(step_kind="resample", detail=detail, success=True)


def _execute_join(
    task: JoinTask,
    ctx: ExecutionContext,
) -> StepResult:
    """Execute a join step (merge resampled datasets for a year).

    Looks up intermediates for each dataset at the given year and
    performs an outer join on the specified keys.  Stores the result
    back in ``ctx.intermediates`` keyed as ``("__joined__", year)``.
    """
    detail = (
        f"join datasets={task.datasets} year={task.year} "
        f"on={task.join_on}"
    )
    _echo(ctx, f"  [join] {detail}")

    frames: list[pd.DataFrame] = []
    for ds_id in task.datasets:
        key = (ds_id, task.year)
        if key not in ctx.intermediates:
            return StepResult(
                step_kind="join",
                detail=detail,
                success=False,
                error=(
                    f"Intermediate for dataset '{ds_id}' year {task.year} "
                    f"not found. Ensure a resample step produces it first."
                ),
            )
        frames.append(ctx.intermediates[key])

    if not frames:
        return StepResult(
            step_kind="join",
            detail=detail,
            success=False,
            error="No datasets to join.",
        )

    # Use the available join keys that exist in all frames
    join_keys = [k for k in task.join_on if all(k in f.columns for f in frames)]
    if not join_keys:
        return StepResult(
            step_kind="join",
            detail=detail,
            success=False,
            error=(
                f"None of the join keys {task.join_on} are present "
                f"in all intermediate datasets."
            ),
        )

    # Progressive outer join
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=join_keys, how="outer")

    ctx.intermediates[("__joined__", task.year)] = merged
    return StepResult(step_kind="join", detail=detail, success=True)


# ---------------------------------------------------------------------------
# Output persistence
# ---------------------------------------------------------------------------


def _deduplicate_assets(
    assets: list[AssetRecord],
) -> list[AssetRecord]:
    """Deduplicate asset records by (role, path)."""
    seen: set[tuple[str, str]] = set()
    result: list[AssetRecord] = []
    for a in assets:
        key = (a.role, a.path)
        if key not in seen:
            seen.add(key)
            result.append(a)
    return result


def _build_provenance(
    recipe: RecipeV1,
    pipeline_id: str,
    ctx: ExecutionContext,
) -> dict[str, object]:
    """Build provenance metadata for the output artifact."""
    deduped = _deduplicate_assets(ctx.consumed_assets)
    return {
        "recipe_name": recipe.name,
        "recipe_version": recipe.version,
        "pipeline_id": pipeline_id,
        "datasets": {
            ds_id: {
                "provider": ds.provider,
                "product": ds.product,
                "version": ds.version,
                "path": ds.path,
            }
            for ds_id, ds in recipe.datasets.items()
        },
        "transforms": {
            tid: str(path.relative_to(ctx.project_root))
            for tid, path in ctx.transform_paths.items()
        },
        "consumed_assets": [
            {
                "role": a.role,
                "path": a.path,
                "sha256": a.sha256,
                "size": a.size,
                "dataset_id": a.dataset_id,
                "transform_id": a.transform_id,
            }
            for a in deduped
        ],
    }


def _build_manifest(
    recipe: RecipeV1,
    pipeline_id: str,
    ctx: ExecutionContext,
    output_path: str | None = None,
) -> RecipeManifest:
    """Build a full provenance manifest for the execution."""
    return RecipeManifest(
        recipe_name=recipe.name,
        recipe_version=recipe.version,
        pipeline_id=pipeline_id,
        assets=_deduplicate_assets(ctx.consumed_assets),
        datasets={
            ds_id: {
                "provider": ds.provider,
                "product": ds.product,
                "version": ds.version,
                "path": ds.path,
            }
            for ds_id, ds in recipe.datasets.items()
        },
        transforms={
            tid: str(path.relative_to(ctx.project_root))
            for tid, path in ctx.transform_paths.items()
        },
        output_path=output_path,
    )


def _persist_outputs(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Collect joined intermediates and write panel output.

    Concatenates all ``("__joined__", year)`` intermediates into a
    single DataFrame, writes it to the canonical panel path, and
    attaches provenance metadata.
    """
    # Find the pipeline target to determine output geometry
    pipeline = None
    for p in ctx.recipe.pipelines:
        if p.id == plan.pipeline_id:
            pipeline = p
            break
    if pipeline is None:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=f"Pipeline '{plan.pipeline_id}' not found in recipe.",
        )

    target = None
    for t in ctx.recipe.targets:
        if t.id == pipeline.target:
            target = t
            break
    if target is None:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=f"Target '{pipeline.target}' not found in recipe.",
        )

    # Collect joined DataFrames
    universe_years = expand_year_spec(ctx.recipe.universe)
    frames: list[pd.DataFrame] = []
    for year in universe_years:
        key = ("__joined__", year)
        if key in ctx.intermediates:
            frames.append(ctx.intermediates[key])

    if not frames:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error="No joined outputs to persist.",
        )

    panel = pd.concat(frames, ignore_index=True)

    # Determine output path using canonical naming
    boundary_vintage = str(target.geometry.vintage or "unknown")
    start_year = min(universe_years)
    end_year = max(universe_years)
    output_dir = ctx.project_root / "data" / "curated" / "panel"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / panel_filename(start_year, end_year, boundary_vintage)

    # Run conformance checks on the assembled panel
    from coclab.panel.conformance import PanelRequest, run_conformance

    panel_request = PanelRequest(
        start_year=start_year,
        end_year=end_year,
    )
    conformance_report = run_conformance(panel, panel_request)
    if not ctx.quiet:
        import sys

        print(conformance_report.summary(), file=sys.stderr)

    # Build provenance and write with metadata
    output_rel = str(output_file.relative_to(ctx.project_root))
    provenance = _build_provenance(ctx.recipe, plan.pipeline_id, ctx)
    provenance["conformance"] = conformance_report.to_dict()
    table = pa.Table.from_pandas(panel)
    metadata = table.schema.metadata or {}
    metadata[b"coclab_provenance"] = json.dumps(provenance).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, output_file)

    # Write manifest sidecar JSON
    manifest = _build_manifest(
        ctx.recipe, plan.pipeline_id, ctx, output_path=output_rel,
    )
    manifest_file = output_file.with_suffix(".manifest.json")
    write_manifest(manifest, manifest_file)

    detail = (
        f"persist panel: {len(frames)} year(s), "
        f"{len(panel)} rows → "
        f"{output_file.relative_to(ctx.project_root)}"
    )
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist", detail=detail, success=True)


def _persist_diagnostics(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Generate and persist diagnostics for the assembled panel.

    Runs the panel diagnostics report and writes a JSON sidecar file
    alongside the panel output.  The diagnostics file uses the same
    base name as the panel with a ``__diagnostics.json`` suffix.
    """
    from coclab.panel.diagnostics import generate_diagnostics_report

    # Re-collect the panel from intermediates (same logic as _persist_outputs)
    universe_years = expand_year_spec(ctx.recipe.universe)
    frames: list[pd.DataFrame] = []
    for year in universe_years:
        key = ("__joined__", year)
        if key in ctx.intermediates:
            frames.append(ctx.intermediates[key])

    if not frames:
        return StepResult(
            step_kind="persist_diagnostics",
            detail="persist diagnostics",
            success=False,
            error="No joined outputs available for diagnostics.",
        )

    panel = pd.concat(frames, ignore_index=True)

    # Resolve output path: same directory as panel, same stem + __diagnostics.json
    pipeline = next(
        (p for p in ctx.recipe.pipelines if p.id == plan.pipeline_id), None
    )
    target = None
    if pipeline is not None:
        target = next(
            (t for t in ctx.recipe.targets if t.id == pipeline.target), None
        )
    boundary_vintage = str(
        target.geometry.vintage if target is not None else "unknown"
    )
    start_year = min(universe_years)
    end_year = max(universe_years)
    output_dir = ctx.project_root / "data" / "curated" / "panel"
    output_dir.mkdir(parents=True, exist_ok=True)
    panel_stem = panel_filename(start_year, end_year, boundary_vintage).replace(
        ".parquet", ""
    )
    diagnostics_file = output_dir / f"{panel_stem}__diagnostics.json"

    # Generate diagnostics
    report = generate_diagnostics_report(panel)

    # Write as JSON
    diagnostics_dict = report.to_dict()
    diagnostics_file.write_text(json.dumps(diagnostics_dict, indent=2, default=str) + "\n")

    detail = (
        f"persist diagnostics: "
        f"{diagnostics_file.relative_to(ctx.project_root)}"
    )
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist_diagnostics", detail=detail, success=True)


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


def _execute_plan(
    plan: ExecutionPlan,
    recipe: RecipeV1,
    project_root: Path,
    *,
    cache: RecipeCache | None = None,
    quiet: bool = False,
) -> PipelineResult:
    """Execute all tasks in a resolved plan in deterministic order.

    Order: materialize → resample → join → persist.
    Stops on first failure and reports context.
    """
    ctx = ExecutionContext(
        project_root=project_root,
        recipe=recipe,
        cache=cache or RecipeCache(),
        quiet=quiet,
    )
    result = PipelineResult(pipeline_id=plan.pipeline_id)

    # Phase 1: materialize
    for task in plan.materialize_tasks:
        step = _execute_materialize(task, ctx)
        result.steps.append(step)
        if not step.success:
            return result

    # Phase 2: resample
    for task in plan.resample_tasks:
        step = _execute_resample(task, ctx)
        result.steps.append(step)
        if not step.success:
            return result

    # Phase 3: join
    for task in plan.join_tasks:
        step = _execute_join(task, ctx)
        result.steps.append(step)
        if not step.success:
            return result

    # Phase 4: persist outputs (only if there are join tasks AND
    # the target declares "panel" in its outputs list).
    if plan.join_tasks:
        # Resolve target to check declared outputs
        pipeline = next(
            (p for p in ctx.recipe.pipelines if p.id == plan.pipeline_id),
            None,
        )
        target = None
        if pipeline is not None:
            target = next(
                (t for t in ctx.recipe.targets if t.id == pipeline.target),
                None,
            )
        declared_outputs = target.outputs if target is not None else ["panel"]

        if "panel" in declared_outputs:
            step = _persist_outputs(plan, ctx)
            result.steps.append(step)
            if not step.success:
                return result

        if "diagnostics" in declared_outputs:
            step = _persist_diagnostics(plan, ctx)
            result.steps.append(step)
            if not step.success:
                return result

        unsupported = [
            o for o in declared_outputs if o not in ("panel", "diagnostics")
        ]
        if unsupported:
            import warnings as _w
            _w.warn(
                f"Pipeline '{plan.pipeline_id}': target outputs "
                f"{unsupported} are declared but not yet implemented. "
                f"Only 'panel' and 'diagnostics' outputs are currently supported.",
                stacklevel=2,
            )

    return result


def execute_recipe(
    recipe: RecipeV1,
    project_root: Path | None = None,
    *,
    cache: RecipeCache | None = None,
    quiet: bool = False,
) -> list[PipelineResult]:
    """Execute all pipelines in a recipe.

    Parameters
    ----------
    recipe : RecipeV1
        A validated recipe.
    project_root : Path | None
        Project root for resolving dataset paths.  Defaults to cwd.
    cache : RecipeCache | None
        Asset cache.  Pass ``RecipeCache(enabled=False)`` to disable
        caching.  Defaults to an enabled cache.
    quiet : bool
        Suppress progress output (for JSON mode).

    Returns
    -------
    list[PipelineResult]
        One result per pipeline.

    Raises
    ------
    ExecutorError
        If a planner or runtime error occurs.  The message includes
        pipeline and step context for actionable diagnostics.
    """
    if project_root is None:
        project_root = Path.cwd()
    if cache is None:
        cache = RecipeCache()

    results: list[PipelineResult] = []

    def _log(msg: str) -> None:
        if not quiet:
            typer.echo(msg)

    for pipeline in recipe.pipelines:
        _log(f"\nExecuting pipeline '{pipeline.id}'...")

        # Resolve the execution plan
        try:
            plan = resolve_plan(recipe, pipeline.id)
        except PlannerError as exc:
            raise ExecutorError(
                f"Pipeline '{pipeline.id}': planning failed: {exc}"
            ) from exc

        task_count = (
            len(plan.materialize_tasks)
            + len(plan.resample_tasks)
            + len(plan.join_tasks)
        )
        _log(f"  Resolved {task_count} tasks")

        # Execute the plan
        result = _execute_plan(
            plan, recipe, project_root, cache=cache, quiet=quiet,
        )
        results.append(result)

        if result.success:
            _log(
                f"  Pipeline '{pipeline.id}' completed: "
                f"{len(result.steps)} steps OK"
            )
        else:
            failed = next(s for s in result.steps if not s.success)
            raise ExecutorError(
                f"Pipeline '{pipeline.id}': step failed: "
                f"[{failed.step_kind}] {failed.detail}"
                + (f" — {failed.error}" if failed.error else "")
            )

    return results
