"""Recipe execution orchestrator.

Given a validated RecipeV1 and pipeline id, resolves the execution plan
via the planner and executes materialize → resample → join tasks in
deterministic order.
"""

from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from coclab.config import StorageConfig, load_config
from coclab.geo.ct_planning_regions import (
    CT_LEGACY_COUNTY_VINTAGE,
    CT_PLANNING_REGION_VINTAGE,
    CtPlanningRegionCrosswalk,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
    translate_weights_planning_to_legacy,
)
from coclab.naming import (
    county_path,
    county_xwalk_path,
    geo_panel_filename,
    tract_path,
    tract_xwalk_path,
)
from coclab.panel.finalize import (
    finalize_panel,
)
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.recipe.cache import RecipeCache
from coclab.recipe.manifest import (
    ROOT_ASSET_STORE,
    ROOT_OUTPUT,
    AssetRecord,
    RecipeManifest,
    write_manifest,
)
from coclab.recipe.planner import (
    ExecutionPlan,
    JoinTask,
    MaterializeTask,
    PlannerError,
    ResampleTask,
    _resolve_dataset_year,
    resolve_plan,
)
from coclab.recipe.probes import (
    get_weighted_transform_requirements,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_year_column,
)
from coclab.recipe.recipe_schema import (
    CohortSelector,
    GeometryRef,
    PanelPolicy,
    RecipeV1,
    TemporalFilter,
    expand_year_spec,
)


class ExecutorError(Exception):
    """Raised when recipe execution fails at runtime.

    Attributes
    ----------
    partial_results : list[PipelineResult]
        Results collected before (and including) the failure.  When
        ``execute_recipe`` encounters pipeline errors it continues
        through all remaining pipelines so callers can inspect what
        succeeded and what failed.
    """

    partial_results: list[PipelineResult]

    def __init__(self, message: str, *, partial_results: list[PipelineResult] | None = None):
        super().__init__(message)
        self.partial_results = partial_results or []


@dataclass
class StepResult:
    """Outcome of a single execution step."""

    step_kind: str
    detail: str
    success: bool
    error: str | None = None
    notes: list[str] = field(default_factory=list)


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
    # Cached CT county bridge overlays keyed by (legacy_vintage, planning_vintage)
    ct_county_alignment_cache: dict[tuple[int, int], CtPlanningRegionCrosswalk] = field(
        default_factory=dict,
    )
    # Assets consumed during execution (for provenance manifest)
    consumed_assets: list[AssetRecord] = field(default_factory=list)
    # Storage roots (asset store, output) — resolved from config precedence
    storage_config: StorageConfig | None = None
    # Suppress progress output (for --json mode)
    quiet: bool = False
    # Cache: dataset_id → number of distinct resolved paths (for broadcast check)
    _distinct_paths_cache: dict[str, int | None] = field(
        default_factory=dict,
    )


def _classify_path(
    file_path: Path,
    ctx: ExecutionContext,
) -> tuple[str | None, str]:
    """Classify a file path to its logical root and compute the relative path.

    Returns ``(root, relative_path)`` where *root* is ``"asset_store"``,
    ``"output"``, or ``None`` (fallback to project-relative).
    """
    cfg = ctx.storage_config or load_config(project_root=ctx.project_root)
    resolved = file_path.resolve()

    # Check output root first (it may be nested inside asset store)
    try:
        rel = resolved.relative_to(cfg.output_root.resolve())
        return ROOT_OUTPUT, str(rel)
    except ValueError:
        pass

    try:
        rel = resolved.relative_to(cfg.asset_store_root.resolve())
        return ROOT_ASSET_STORE, str(rel)
    except ValueError:
        pass

    # Fallback: project-relative
    try:
        return None, str(resolved.relative_to(ctx.project_root.resolve()))
    except ValueError:
        return None, str(file_path)


def _echo(ctx: ExecutionContext, message: str) -> None:
    """Print progress message unless quiet mode is active."""
    if not ctx.quiet:
        typer.echo(message)


def _record_step_note(
    ctx: ExecutionContext,
    step_notes: list[str] | None,
    message: str,
) -> None:
    """Attach a human- and machine-visible note to the current step."""
    if step_notes is None or message in step_notes:
        return
    step_notes.append(message)
    _echo(ctx, f"    note: {message}")


def _get_transform(recipe: RecipeV1, transform_id: str):
    """Return a transform by id or raise an ExecutorError."""
    for transform in recipe.transforms:
        if transform.id == transform_id:
            return transform
    raise ExecutorError(
        f"Transform '{transform_id}' referenced in materialize step "
        "but not found in recipe transforms."
    )


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
    transform = _get_transform(recipe, transform_id)

    from_ = transform.from_
    to = transform.to

    metro_ref, base_ref = _identify_metro_and_base(from_, to)
    if metro_ref is not None:
        return _generated_metro_transform_path(
            transform_id,
            metro_ref=metro_ref,
            base_ref=base_ref,
            project_root=project_root,
        )

    # Determine which geometry is the CoC boundary and which is the
    # base geography so we can build the right crosswalk filename.
    coc_ref, base_ref = _identify_coc_and_base(from_, to)
    if coc_ref is None:
        raise ExecutorError(
            f"Transform '{transform_id}' connects "
            f"{from_.type}@{from_.vintage} → {to.type}@{to.vintage}: "
            f"cannot resolve crosswalk path (no 'coc' geometry in pair)."
        )

    if coc_ref.vintage is None:
        raise ExecutorError(
            f"Transform '{transform_id}': CoC geometry has no vintage. "
            f"Cannot resolve crosswalk path without a concrete boundary vintage. "
            f"Set vintage on the 'coc' geometry ref (e.g., vintage: 2025)."
        )
    if base_ref.vintage is None:
        raise ExecutorError(
            f"Transform '{transform_id}': {base_ref.type} geometry has no vintage. "
            f"Cannot resolve crosswalk path without a concrete {base_ref.type} vintage. "
            f"Set vintage on the '{base_ref.type}' geometry ref."
        )
    boundary_vintage = str(coc_ref.vintage)
    base_vintage: str | int = base_ref.vintage

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


def _identify_metro_and_base(
    from_: GeometryRef, to: GeometryRef,
) -> tuple[GeometryRef | None, GeometryRef]:
    """Identify which end of a transform is the metro geometry."""
    if to.type == "metro":
        return to, from_
    if from_.type == "metro":
        return from_, to
    return None, from_


def _generated_metro_transform_path(
    transform_id: str,
    *,
    metro_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> Path:
    """Return the recipe-cache path for a generated metro transform."""
    definition = metro_ref.source or "unknown_definition"
    base_suffix = base_ref.type
    if base_ref.vintage is not None:
        base_suffix = f"{base_suffix}_{base_ref.vintage}"
    filename = f"{transform_id}__{base_suffix}__{definition}.parquet"
    return project_root / _RECIPE_TRANSFORM_DIR / filename


def _resolve_metro_transform_df(
    *,
    metro_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> pd.DataFrame:
    """Build a metro crosswalk DataFrame from curated membership artifacts."""
    if not metro_ref.source:
        raise ExecutorError(
            "Metro transforms require geometry.source to identify the "
            "definition version (for example 'glynn_fox_v1')."
        )

    from coclab.metro.io import (
        read_metro_coc_membership,
        read_metro_county_membership,
    )

    data_root = project_root / "data"
    definition_version = metro_ref.source

    if base_ref.type == "coc":
        xwalk = read_metro_coc_membership(
            definition_version=definition_version,
            base_dir=data_root,
        )
        xwalk["area_share"] = 1.0
        return xwalk[["metro_id", "coc_id", "area_share", "definition_version"]]

    if base_ref.type == "county":
        xwalk = read_metro_county_membership(
            definition_version=definition_version,
            base_dir=data_root,
        )
        xwalk["area_share"] = 1.0
        return xwalk[["metro_id", "county_fips", "area_share", "definition_version"]]

    if base_ref.type == "tract":
        if base_ref.vintage is None:
            raise ExecutorError(
                "Metro tract transforms require a tract vintage so the "
                "executor can load the tract geometry artifact."
            )
        county_membership = read_metro_county_membership(
            definition_version=definition_version,
            base_dir=data_root,
        )
        tracts = pd.read_parquet(tract_path(base_ref.vintage, data_root))
        tract_col: str | None = None
        for candidate in ("tract_geoid", "GEOID", "geoid"):
            if candidate in tracts.columns:
                tract_col = candidate
                break
        if tract_col is None:
            raise ExecutorError(
                "Tract geometry artifact is missing a tract identifier column. "
                f"Expected one of tract_geoid/GEOID/geoid. "
                f"Available columns: {sorted(tracts.columns)}"
            )
        tract_index = tracts[[tract_col]].copy()
        tract_index["tract_geoid"] = tract_index[tract_col].astype(str)
        tract_index["county_fips"] = tract_index["tract_geoid"].str[:5]
        xwalk = county_membership.merge(tract_index, on="county_fips", how="inner")
        xwalk["area_share"] = 1.0
        return xwalk[["metro_id", "tract_geoid", "area_share", "definition_version"]]

    raise ExecutorError(
        f"Metro transforms currently support tract, county, or coc bases; "
        f"got '{base_ref.type}'."
    )


def _materialize_generated_metro_transform(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> Path:
    """Generate and persist a metro transform artifact for recipe execution."""
    transform = _get_transform(recipe, transform_id)

    metro_ref, base_ref = _identify_metro_and_base(transform.from_, transform.to)
    if metro_ref is None:
        raise ExecutorError(
            f"Transform '{transform_id}' does not target metro geometry."
        )

    output_path = _generated_metro_transform_path(
        transform_id,
        metro_ref=metro_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    if output_path.exists():
        return output_path

    xwalk = _resolve_metro_transform_df(
        metro_ref=metro_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=metro_ref.source,
        tract_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "tract" and base_ref.vintage is not None
            else None
        ),
        county_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "county" and base_ref.vintage is not None
            else None
        ),
        extra={
            "dataset_type": "recipe_transform",
            "transform_id": transform_id,
            "from_type": transform.from_.type,
            "to_type": transform.to.type,
        },
    )
    write_parquet_with_provenance(xwalk, output_path, provenance)
    return output_path


# ---------------------------------------------------------------------------
# Resample helpers
# ---------------------------------------------------------------------------

# Maps geometry types to the column name used as join key in crosswalks.
_XWALK_JOIN_KEYS: dict[str, str] = {
    "tract": "tract_geoid",
    "county": "county_fips",
    "coc": "coc_id",
    "metro": "metro_id",
}

# Auto-detect candidates for geo-ID and year columns.

# Columns in crosswalks that are NOT the target geography identifier.
_XWALK_NON_GEO_COLS: set[str] = {
    "tract_geoid", "county_fips", "area_share", "pop_share",
    "intersection_area", "tract_area", "county_area", "coc_area", "geo_area",
    "boundary_vintage", "tract_vintage", "definition_version",
}

_RECIPE_TRANSFORM_DIR = Path(".recipe_cache") / "transforms"


def _detect_xwalk_target_col(
    xwalk: pd.DataFrame,
    source_key: str,
) -> str:
    """Detect the target geography column in a crosswalk.

    Finds the geo-ID column by looking for known candidates that aren't
    the source join key.  Raises ``ExecutorError`` when no candidate is
    found, rather than silently returning a default that may not exist.
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
    raise ExecutorError(
        f"Cannot detect target geography column in crosswalk. "
        f"Columns: {list(xwalk.columns)}, source_key: {source_key!r}. "
        f"Expected one of: coc_id, metro_id, geo_id."
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


def _needs_ct_planning_to_legacy_alignment(
    *,
    xwalk: pd.DataFrame,
    source_values: pd.Series,
    source_key: str,
) -> bool:
    """Return True when CT planning-region inputs need a legacy-county bridge."""
    if source_key != "county_fips" or source_key not in xwalk.columns:
        return False

    xwalk_has_ct_legacy = xwalk[source_key].dropna().astype(str).map(
        is_ct_legacy_county_fips,
    ).any()
    source_has_ct_planning = source_values.dropna().astype(str).map(
        is_ct_planning_region_fips,
    ).any()
    return bool(xwalk_has_ct_legacy and source_has_ct_planning)


def _load_ct_county_alignment_crosswalk(
    *,
    ctx: ExecutionContext,
    legacy_vintage: int,
) -> CtPlanningRegionCrosswalk:
    """Load and cache the CT planning-region→legacy county bridge."""
    cache_key = (legacy_vintage, CT_PLANNING_REGION_VINTAGE)
    cached = ctx.ct_county_alignment_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        crosswalk = build_ct_county_planning_region_crosswalk(
            legacy_county_vintage=legacy_vintage,
            planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
        )
    except (FileNotFoundError, ValueError) as exc:
        legacy_path = county_path(legacy_vintage)
        planning_path = county_path(CT_PLANNING_REGION_VINTAGE)
        raise ExecutorError(
            "Connecticut county alignment is required because the recipe "
            "crosswalk uses legacy county FIPS while the dataset uses "
            "planning-region FIPS. Failed to build the authoritative CT "
            f"county bridge from {legacy_path} and {planning_path}: {exc}"
        ) from exc

    ctx.ct_county_alignment_cache[cache_key] = crosswalk
    return crosswalk


def _translate_ct_planning_values_to_legacy(
    *,
    df: pd.DataFrame,
    geo_col: str,
    value_columns: list[str],
    crosswalk: CtPlanningRegionCrosswalk,
    year_value: int | None = None,
) -> pd.DataFrame:
    """Translate CT planning-region values to legacy counties column by column."""
    translated_parts: list[pd.DataFrame] = []
    for value_col in value_columns:
        translated = translate_weights_planning_to_legacy(
            df[[geo_col, value_col]].rename(
                columns={geo_col: "county_fips", value_col: "weight_value"},
            ),
            crosswalk,
        ).rename(columns={"county_fips": geo_col, "weight_value": value_col})
        translated_parts.append(translated)

    result = translated_parts[0]
    for part in translated_parts[1:]:
        result = result.merge(part, on=geo_col, how="outer")
    if year_value is not None:
        result["year"] = year_value
    return result


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
        if original_yr_dtype == object:
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


def _attach_dynamic_pop_share(
    *,
    xwalk: pd.DataFrame,
    task: ResampleTask,
    ctx: ExecutionContext,
    step_notes: list[str] | None = None,
) -> pd.DataFrame:
    """Populate pop_share from a transform's declared population source."""
    if task.transform_id is None:
        return xwalk
    if "pop_share" in xwalk.columns and xwalk["pop_share"].notna().any():
        return xwalk

    transform = _get_transform(ctx.recipe, task.transform_id)
    reqs = get_weighted_transform_requirements(transform)
    if reqs is None:
        return xwalk
    population_source, population_field = reqs

    source_key = _XWALK_JOIN_KEYS.get(task.effective_geometry.type)
    if source_key is None or source_key not in xwalk.columns:
        raise ExecutorError(
            f"Transform '{task.transform_id}' cannot derive pop_share for "
            f"geometry type '{task.effective_geometry.type}'."
        )

    weights_df = _load_support_dataset_for_year(
        ctx=ctx,
        dataset_id=population_source,
        year=task.year,
    )
    weights_geo_col = _resolve_geo_column(
        weights_df,
        ctx.recipe.datasets[population_source].geo_column,
    )
    if population_field not in weights_df.columns:
        raise ExecutorError(
            f"Dataset '{population_source}' year {task.year}: missing "
            f"population field '{population_field}'. "
            f"Available: {sorted(weights_df.columns)}"
        )

    target_col = _detect_xwalk_target_col(xwalk, source_key)
    weights = weights_df[[weights_geo_col, population_field]].copy()
    if _needs_ct_planning_to_legacy_alignment(
        xwalk=xwalk,
        source_values=weights[weights_geo_col],
        source_key=source_key,
    ):
        legacy_vintage = (
            int(task.effective_geometry.vintage)
            if task.effective_geometry.vintage is not None
            else CT_LEGACY_COUNTY_VINTAGE
        )
        ct_bridge = _load_ct_county_alignment_crosswalk(
            ctx=ctx,
            legacy_vintage=legacy_vintage,
        )
        _record_step_note(
            ctx,
            step_notes,
            "Connecticut special-case alignment applied: translated "
            f"planning-region population_source '{population_source}' to "
            "legacy counties before deriving pop_share.",
        )
        weights = _translate_ct_planning_values_to_legacy(
            df=weights,
            geo_col=weights_geo_col,
            value_columns=[population_field],
            crosswalk=ct_bridge,
            year_value=task.year if "year" in weights_df.columns else None,
        )
    weights = weights.rename(columns={weights_geo_col: source_key})
    weights[population_field] = pd.to_numeric(
        weights[population_field],
        errors="coerce",
    )

    enriched = xwalk.merge(weights, on=source_key, how="left")
    group_sum = enriched.groupby(target_col)[population_field].transform("sum")
    enriched["pop_share"] = np.where(
        group_sum > 0,
        enriched[population_field] / group_sum,
        np.nan,
    )
    return enriched.drop(columns=[population_field])


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
    measure_aggs: dict[str, str] = task.measure_aggregations or {
        m: "sum" for m in task.measures
    }

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
        generation_error: str | None = None
        transform = None
        for candidate in ctx.recipe.transforms:
            if candidate.id == tid:
                transform = candidate
                break
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

        metro_ref, _base_ref = (
            _identify_metro_and_base(transform.from_, transform.to)
            if transform is not None
            else (None, None)
        )
        if metro_ref is not None and not path.exists():
            try:
                path = _materialize_generated_metro_transform(
                    tid,
                    ctx.recipe,
                    ctx.project_root,
                )
            except ExecutorError as exc:
                generation_error = str(exc)

        if path.exists():
            _echo(ctx, f"    reuse: {path.relative_to(ctx.project_root)}")
            ctx.transform_paths[tid] = path
            identity = ctx.cache.file_identity(path)
            xwalk_root, xwalk_rel = _classify_path(path, ctx)
            ctx.consumed_assets.append(AssetRecord(
                role="crosswalk",
                path=xwalk_rel,
                sha256=identity.sha256,
                size=identity.size,
                root=xwalk_root,
                transform_id=tid,
            ))
        else:
            return StepResult(
                step_kind="materialize",
                detail=detail,
                success=False,
                error=(
                    generation_error
                    if generation_error is not None
                    else (
                        f"Transform '{tid}' artifact not found at "
                        f"{path.relative_to(ctx.project_root)}. "
                        "Run 'coclab generate xwalks' to build CoC crosswalks "
                        "first, or ensure the metro definition artifacts exist "
                        "for metro transforms."
                    )
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
    step_notes: list[str] = []
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
    ds_root, ds_rel = _classify_path(input_file, ctx)
    ctx.consumed_assets.append(AssetRecord(
        role="dataset",
        path=ds_rel,
        sha256=identity.sha256,
        size=identity.size,
        root=ds_root,
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
        df = _filter_to_year(df, year_col, task.year)
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
        df["year"] = task.year

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
                xwalk = _attach_dynamic_pop_share(
                    xwalk=xwalk,
                    task=task,
                    ctx=ctx,
                    step_notes=step_notes,
                )
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
            except ExecutorError as exc:
                return StepResult(
                    step_kind="resample",
                    detail=detail,
                    success=False,
                    error=str(exc),
                )

            if task.method == "aggregate":
                if task.effective_geometry.type == "county":
                    measure_aggs = task.measure_aggregations or {
                        measure: "sum" for measure in task.measures
                    }
                    non_sum_measures = [
                        measure
                        for measure in task.measures
                        if measure_aggs.get(measure, "sum") != "sum"
                    ]
                    geo_col = _resolve_geo_column(df, task.geo_column)
                    if _needs_ct_planning_to_legacy_alignment(
                        xwalk=xwalk,
                        source_values=df[geo_col],
                        source_key="county_fips",
                    ):
                        if non_sum_measures:
                            raise ExecutorError(
                                f"Resample aggregate for '{task.dataset_id}' "
                                f"year {task.year}: Connecticut planning-region "
                                "county inputs require legacy-county alignment, "
                                "but the current recipe uses non-sum measures "
                                f"{non_sum_measures}. Add an explicit CT "
                                "compatibility mapping for those measures."
                            )
                        legacy_vintage = (
                            int(task.effective_geometry.vintage)
                            if task.effective_geometry.vintage is not None
                            else CT_LEGACY_COUNTY_VINTAGE
                        )
                        ct_bridge = _load_ct_county_alignment_crosswalk(
                            ctx=ctx,
                            legacy_vintage=legacy_vintage,
                        )
                        _record_step_note(
                            ctx,
                            step_notes,
                            "Connecticut special-case alignment applied: "
                            f"translated planning-region dataset "
                            f"'{task.dataset_id}' to legacy counties before "
                            "aggregate resampling.",
                        )
                        df = _translate_ct_planning_values_to_legacy(
                            df=df[[geo_col, *task.measures]].copy(),
                            geo_col=geo_col,
                            value_columns=task.measures,
                            crosswalk=ct_bridge,
                            year_value=task.year if "year" in df.columns else None,
                        )
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
            notes=step_notes,
        )

    # Store intermediate
    ctx.intermediates[(task.dataset_id, task.year)] = result_df
    return StepResult(
        step_kind="resample",
        detail=detail,
        success=True,
        notes=step_notes,
    )


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
    """Build provenance metadata for the output artifact.

    Derives from :func:`_build_manifest` so that both the Parquet-embedded
    provenance and the sidecar JSON share a single code path for asset
    deduplication and dataset/transform extraction.
    """
    manifest = _build_manifest(recipe, pipeline_id, ctx)
    d = manifest.to_dict()
    # Rename for backward-compatible Parquet metadata key
    d["consumed_assets"] = d.pop("assets")
    # Remove manifest-only fields not needed in Parquet provenance
    d.pop("executed_at", None)
    d.pop("output_path", None)
    return d


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
            tid: _classify_path(path, ctx)[1]
            for tid, path in ctx.transform_paths.items()
        },
        output_path=output_path,
    )


def _target_geometry_metadata(
    target_geometry: GeometryRef,
) -> tuple[str, str | None, str | None]:
    """Return (geo_type, boundary_vintage, definition_version) for a target."""
    geo_type = target_geometry.type
    boundary_vintage = (
        str(target_geometry.vintage)
        if target_geometry.vintage is not None
        else None
    )
    definition_version = target_geometry.source if geo_type == "metro" else None
    return geo_type, boundary_vintage, definition_version


def _resolve_pipeline_target(
    recipe: RecipeV1,
    pipeline_id: str,
):
    """Return the pipeline and target referenced by *pipeline_id*."""
    pipeline = next((p for p in recipe.pipelines if p.id == pipeline_id), None)
    if pipeline is None:
        raise ExecutorError(f"Pipeline '{pipeline_id}' not found in recipe.")

    target = next((t for t in recipe.targets if t.id == pipeline.target), None)
    if target is None:
        raise ExecutorError(f"Target '{pipeline.target}' not found in recipe.")

    return pipeline, target


def _recipe_output_dirname(recipe_name: str) -> str:
    """Return a deterministic directory name for a recipe output namespace."""
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", recipe_name.strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-.")
    return normalized or "recipe"


def _resolve_panel_output_file(
    recipe: RecipeV1,
    pipeline_id: str,
    project_root: Path,
    storage_config: StorageConfig | None = None,
) -> Path:
    """Return the canonical panel parquet path for a pipeline."""
    _, target = _resolve_pipeline_target(recipe, pipeline_id)
    target_geo_type, boundary_vintage, definition_version = _target_geometry_metadata(
        target.geometry,
    )

    if target_geo_type == "metro" and definition_version is None:
        raise ExecutorError(
            "Metro recipe targets must set geometry.source to the "
            "metro definition version so panel outputs can be named."
        )

    universe_years = expand_year_spec(recipe.universe)
    start_year = min(universe_years)
    end_year = max(universe_years)

    cfg = storage_config or load_config(project_root=project_root)
    recipe_dir = _recipe_output_dirname(recipe.name)
    return (
        cfg.output_root / recipe_dir / geo_panel_filename(
            start_year,
            end_year,
            geo_type=target_geo_type,
            boundary_vintage=boundary_vintage if target_geo_type == "coc" else None,
            definition_version=definition_version,
        )
    )


def resolve_pipeline_artifacts(
    recipe: RecipeV1,
    pipeline_id: str,
    *,
    project_root: Path | None = None,
    storage_config: StorageConfig | None = None,
) -> dict[str, str]:
    """Return canonical output paths for a pipeline's declared outputs.

    Paths are relative to *project_root* when the output falls within the
    project tree, otherwise absolute.
    """
    if project_root is None:
        project_root = Path.cwd()

    _, target = _resolve_pipeline_target(recipe, pipeline_id)
    panel_file = _resolve_panel_output_file(
        recipe, pipeline_id, project_root, storage_config=storage_config,
    )
    artifacts: dict[str, str] = {}

    def _display_path(p: Path) -> str:
        try:
            return str(p.relative_to(project_root))
        except ValueError:
            return str(p)

    if "panel" in target.outputs:
        artifacts["panel_path"] = _display_path(panel_file)
        artifacts["manifest_path"] = _display_path(
            panel_file.with_suffix(".manifest.json"),
        )

    if "diagnostics" in target.outputs:
        diagnostics_file = panel_file.with_name(
            f"{panel_file.stem}__diagnostics.json",
        )
        artifacts["diagnostics_path"] = _display_path(diagnostics_file)

    return artifacts


def _apply_cohort_selector(
    panel: pd.DataFrame,
    cohort: CohortSelector,
    geo_id_col: str = "geo_id",
    year_col: str = "year",
) -> pd.DataFrame:
    """Filter panel to a ranked subset of geographies.

    Ranks geographies by ``cohort.rank_by`` at ``cohort.reference_year``,
    then keeps only the selected geo_ids across all years.
    """
    ref = panel[panel[year_col] == cohort.reference_year]
    if ref.empty:
        raise ExecutorError(
            f"Cohort selector reference_year {cohort.reference_year} "
            f"produced no rows in the panel."
        )
    if cohort.rank_by not in ref.columns:
        raise ExecutorError(
            f"Cohort selector rank_by column '{cohort.rank_by}' "
            f"not found in panel columns: {sorted(panel.columns.tolist())}"
        )

    ranked = ref[[geo_id_col, cohort.rank_by]].dropna(subset=[cohort.rank_by])
    ranked = ranked.sort_values(cohort.rank_by, ascending=False)

    if cohort.method == "top_n":
        selected = ranked.head(cohort.n)[geo_id_col]
    elif cohort.method == "bottom_n":
        selected = ranked.tail(cohort.n)[geo_id_col]
    elif cohort.method == "percentile":
        threshold_value = ranked[cohort.rank_by].quantile(cohort.threshold)
        selected = ranked[ranked[cohort.rank_by] >= threshold_value][geo_id_col]
    else:
        raise ExecutorError(f"Unknown cohort method: {cohort.method}")

    return panel[panel[geo_id_col].isin(selected)].reset_index(drop=True)


def _canonicalize_panel_for_target(
    panel: pd.DataFrame,
    target_geometry: GeometryRef,
) -> pd.DataFrame:
    """Add target-geometry metadata columns expected by downstream tools."""
    result = panel.copy()
    geo_type, boundary_vintage, definition_version = _target_geometry_metadata(
        target_geometry
    )
    if "geo_id" in result.columns:
        result["geo_type"] = geo_type
        if geo_type == "coc" and "coc_id" not in result.columns:
            result["coc_id"] = result["geo_id"]
        if geo_type == "metro":
            if "metro_id" not in result.columns:
                result["metro_id"] = result["geo_id"]
            if "metro_name" not in result.columns or result["metro_name"].isna().any():
                from coclab.metro.definitions import metro_name_for_id

                result["metro_name"] = result["metro_id"].map(metro_name_for_id)
            if (
                definition_version is not None
                and "definition_version_used" not in result.columns
            ):
                result["definition_version_used"] = definition_version
        if (
            geo_type == "coc"
            and boundary_vintage is not None
            and "boundary_vintage_used" not in result.columns
        ):
            result["boundary_vintage_used"] = boundary_vintage
    return result


@dataclass
class _AssembledPanel:
    """Result of assembling a panel from joined intermediates."""

    panel: pd.DataFrame
    frames: list[pd.DataFrame]
    target: object  # TargetSpec
    target_geo_type: str
    boundary_vintage: str | None
    definition_version: str | None
    zori_provenance: object | None = None  # ZoriProvenance, when ZORI policy active


def _resolve_panel_aliases(target) -> dict[str, str]:
    """Return column aliases for a target from its panel_policy.

    Aliases are opt-in: only applied when the target's ``panel_policy``
    declares explicit ``column_aliases``.  The preferred recipe aliases
    are available as ``RECIPE_COLUMN_ALIASES`` for recipes that want
    the new naming convention (coclab-t9rp).
    """
    policy: PanelPolicy | None = getattr(target, "panel_policy", None)
    if policy is not None and policy.column_aliases:
        return dict(policy.column_aliases)
    return {}


def _assemble_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
    *,
    step_kind: str = "persist",
) -> _AssembledPanel | StepResult:
    """Collect joined intermediates, canonicalize, and apply cohort selector.

    Returns an :class:`_AssembledPanel` on success or a failed
    :class:`StepResult` on error.  Shared by ``_persist_outputs`` and
    ``_persist_diagnostics`` to avoid duplicating panel assembly logic.
    """
    try:
        _, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    universe_years = expand_year_spec(ctx.recipe.universe)
    frames: list[pd.DataFrame] = []
    for year in universe_years:
        key = ("__joined__", year)
        if key in ctx.intermediates:
            frames.append(ctx.intermediates[key])

    if not frames:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error="No joined outputs available.",
        )

    panel = pd.concat(frames, ignore_index=True)
    panel = _canonicalize_panel_for_target(panel, target.geometry)

    target_geo_type, boundary_vintage, definition_version = _target_geometry_metadata(
        target.geometry,
    )

    # Resolve panel policy for source label and ZORI inclusion.
    policy: PanelPolicy | None = getattr(target, "panel_policy", None)
    source_label = policy.source_label if policy else None
    include_zori = policy is not None and policy.zori is not None
    aliases = _resolve_panel_aliases(target)
    extra_columns: list[str] | None = None
    zori_provenance = None

    # -----------------------------------------------------------------
    # ZORI eligibility, rent_to_income, and provenance (coclab-gude.2)
    # -----------------------------------------------------------------
    # Canonicalize recipe-native ZORI measure → canonical panel column.
    # Recipe aggregation (county→target) produces a column named "zori"
    # (the recipe measure name); the eligibility logic expects "zori_coc".
    if include_zori and "zori" in panel.columns and "zori_coc" not in panel.columns:
        panel = panel.rename(columns={"zori": "zori_coc"})

    if include_zori and "zori_coc" in panel.columns:
        from coclab.panel.zori_eligibility import (
            ZoriProvenance,
            add_provenance_columns,
            apply_zori_eligibility,
            compute_rent_to_income,
        )

        zori_policy = policy.zori  # type: ignore[union-attr]

        # Detect rent alignment from resampled data (column injected by
        # the ZORI resample step when the source has a "method" column).
        rent_alignment = "pit_january"
        if "method" in panel.columns:
            methods = panel["method"].dropna().unique()
            if len(methods) == 1:
                rent_alignment = str(methods[0])

        panel = apply_zori_eligibility(
            panel,
            min_coverage=zori_policy.min_coverage,
        )
        panel = compute_rent_to_income(panel)

        zori_provenance = ZoriProvenance(
            rent_alignment=rent_alignment,
            zori_min_coverage=zori_policy.min_coverage,
        )
        panel = add_provenance_columns(panel, zori_provenance)

        # Drop temporary columns that leak from resample intermediates.
        for _tmp in ("method", "geo_count"):
            if _tmp in panel.columns:
                panel = panel.drop(columns=[_tmp])

        if "zori_max_geo_contribution" in panel.columns:
            extra_columns = ["zori_max_geo_contribution"]

    # -----------------------------------------------------------------
    # ACS 1-year provenance columns (coclab-gude.3)
    # -----------------------------------------------------------------
    if (
        target_geo_type == "metro"
        and policy is not None
        and policy.acs1 is not None
        and policy.acs1.include
    ):
        has_acs1_data = (
            "unemployment_rate_acs1" in panel.columns
            and panel["unemployment_rate_acs1"].notna().any()
        )
        if has_acs1_data:
            # The recipe pipeline normalises every dataset's year_column to
            # the universe year during resample (acs1_vintage → year).  The
            # panel "year" therefore IS the resolved ACS1 vintage, not a PIT
            # year that needs a lag offset.
            panel["acs1_vintage_used"] = panel["year"].astype(str)
            panel["acs_products_used"] = "acs5,acs1"
            # Null out vintage for rows where ACS1 data is missing.
            acs1_missing = panel["unemployment_rate_acs1"].isna()
            if acs1_missing.any():
                panel.loc[acs1_missing, "acs1_vintage_used"] = pd.NA
        else:
            panel["acs1_vintage_used"] = pd.NA
            panel["acs_products_used"] = "acs5"
            if "unemployment_rate_acs1" not in panel.columns:
                panel["unemployment_rate_acs1"] = np.nan

    # -----------------------------------------------------------------
    # BLS LAUS metro provenance columns
    # -----------------------------------------------------------------
    if (
        target_geo_type == "metro"
        and policy is not None
        and policy.laus is not None
        and policy.laus.include
    ):
        has_laus_data = (
            "unemployment_rate" in panel.columns
            and panel["unemployment_rate"].notna().any()
        )
        if has_laus_data:
            # LAUS is year-aligned: each panel row's year is the LAUS reference year.
            panel["laus_vintage_used"] = panel["year"].astype(str)
            laus_missing = panel["unemployment_rate"].isna()
            if laus_missing.any():
                panel.loc[laus_missing, "laus_vintage_used"] = pd.NA
        else:
            panel["laus_vintage_used"] = pd.NA
            for col in ["labor_force", "employed", "unemployed", "unemployment_rate"]:
                if col not in panel.columns:
                    panel[col] = np.nan

    # Shared finalization: boundary detection, column ordering, dtypes,
    # source labeling, and column aliases.
    panel = finalize_panel(
        panel,
        geo_type=target_geo_type,
        include_zori=include_zori,
        source_label=source_label,
        column_aliases=aliases,
        extra_columns=extra_columns,
    )

    if target.cohort is not None:
        pre_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        panel = _apply_cohort_selector(panel, target.cohort)
        post_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        _echo(
            ctx,
            f"  [cohort] {target.cohort.method} rank_by={target.cohort.rank_by} "
            f"ref_year={target.cohort.reference_year}: "
            f"{pre_count} → {post_count} geographies",
        )

    return _AssembledPanel(
        panel=panel,
        frames=frames,
        target=target,
        target_geo_type=target_geo_type,
        boundary_vintage=boundary_vintage,
        definition_version=definition_version,
        zori_provenance=zori_provenance,
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
    assembled = _assemble_panel(plan, ctx, step_kind="persist")
    if isinstance(assembled, StepResult):
        return assembled

    panel = assembled.panel
    frames = assembled.frames
    target_geo_type = assembled.target_geo_type
    boundary_vintage = assembled.boundary_vintage
    definition_version = assembled.definition_version

    universe_years = expand_year_spec(ctx.recipe.universe)
    start_year = min(universe_years)
    end_year = max(universe_years)

    try:
        output_file = _resolve_panel_output_file(
            ctx.recipe, plan.pipeline_id, ctx.project_root,
            storage_config=ctx.storage_config,
        )
    except ExecutorError as exc:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=str(exc),
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Detect output filename collision from a prior pipeline in this run.
    if output_file.exists() and output_file in getattr(ctx, "_written_outputs", set()):
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=(
                f"Output collision: pipeline '{plan.pipeline_id}' resolves to "
                f"'{output_file}' which was "
                f"already written by another pipeline in this recipe. "
                f"Namespace targets or use distinct geometry vintages."
            ),
        )

    # Run conformance checks on the assembled panel
    from coclab.panel.conformance import (
        ACS_MEASURE_COLUMNS,
        PanelRequest,
        run_conformance,
    )

    # Derive measure_columns from recipe datasets so non-ACS schemas
    # (e.g. PEP) get correct conformance checking (coclab-d0qm).
    recipe_products = {ds.product for ds in ctx.recipe.datasets.values()}
    if recipe_products & {"acs", "acs5"}:
        measure_columns: list[str] | None = None  # ACS default
    else:
        # Non-ACS schema: check whichever known measures are in the panel.
        known = set(ACS_MEASURE_COLUMNS) | {"population"}
        measure_columns = [c for c in panel.columns if c in known] or None

    # Resolve panel policy for ACS1 and ZORI conformance awareness.
    _, persist_target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
    persist_policy: PanelPolicy | None = getattr(persist_target, "panel_policy", None)

    # Translate measure_columns through any active column aliases so that
    # conformance checks look for the renamed names in the finalized panel.
    _panel_aliases = _resolve_panel_aliases(persist_target)
    if _panel_aliases:
        base_cols = ACS_MEASURE_COLUMNS if measure_columns is None else measure_columns
        measure_columns = [_panel_aliases.get(c, c) for c in base_cols]

    # ACS1-aware conformance (coclab-gude.3): include acs1 product when
    # the panel policy requests it and the column is present.
    acs_products = ["acs5"]
    if (
        persist_policy is not None
        and persist_policy.acs1 is not None
        and persist_policy.acs1.include
        and "unemployment_rate_acs1" in panel.columns
    ):
        acs_products = ["acs5", "acs1"]

    # LAUS-aware conformance: set include_laus when the policy requests it.
    include_laus = (
        persist_policy is not None
        and persist_policy.laus is not None
        and persist_policy.laus.include
    )

    # ZORI-aware conformance (coclab-gude.2).
    include_zori = persist_policy is not None and persist_policy.zori is not None

    panel_request = PanelRequest(
        start_year=start_year,
        end_year=end_year,
        geo_type=target_geo_type,
        measure_columns=measure_columns,
        acs_products=acs_products,
        include_zori=include_zori,
        include_laus=include_laus,
    )
    conformance_report = run_conformance(panel, panel_request)
    if not ctx.quiet:
        import sys

        print(conformance_report.summary(), file=sys.stderr)

    # Build provenance and write with metadata
    try:
        output_rel = str(output_file.relative_to(ctx.project_root))
    except ValueError:
        output_rel = str(output_file)
    provenance = _build_provenance(ctx.recipe, plan.pipeline_id, ctx)
    provenance["target_geometry"] = {
        "type": target_geo_type,
        **(
            {"vintage": boundary_vintage}
            if target_geo_type == "coc" and boundary_vintage is not None
            else {}
        ),
        **(
            {"source": definition_version}
            if target_geo_type == "metro" and definition_version is not None
            else {}
        ),
    }
    provenance["conformance"] = conformance_report.to_dict()

    # Embed ZORI provenance and summary (coclab-gude.2).
    if assembled.zori_provenance is not None:
        provenance["zori"] = assembled.zori_provenance.to_dict()
        from coclab.panel.zori_eligibility import summarize_zori_eligibility

        zori_summary = summarize_zori_eligibility(panel)
        if zori_summary.get("zori_integrated"):
            provenance["zori_summary"] = zori_summary

    table = pa.Table.from_pandas(panel)
    metadata = table.schema.metadata or {}
    metadata[b"coclab_provenance"] = json.dumps(provenance).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, output_file)

    # Track written outputs for collision detection across pipelines.
    if not hasattr(ctx, "_written_outputs"):
        ctx._written_outputs = set()  # type: ignore[attr-defined]
    ctx._written_outputs.add(output_file)  # type: ignore[attr-defined]

    # Write manifest sidecar JSON
    manifest = _build_manifest(
        ctx.recipe, plan.pipeline_id, ctx, output_path=output_rel,
    )
    manifest_file = output_file.with_suffix(".manifest.json")
    write_manifest(manifest, manifest_file)

    detail = (
        f"persist panel: {len(frames)} year(s), "
        f"{len(panel)} rows → "
        f"{output_rel}"
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

    assembled = _assemble_panel(plan, ctx, step_kind="persist_diagnostics")
    if isinstance(assembled, StepResult):
        return assembled

    panel = assembled.panel

    try:
        panel_file = _resolve_panel_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )
    except ExecutorError as exc:
        return StepResult(
            step_kind="persist_diagnostics",
            detail="persist_diagnostics",
            success=False,
            error=str(exc),
        )
    diagnostics_file = panel_file.with_name(
        f"{panel_file.stem}__diagnostics.json",
    )
    diagnostics_file.parent.mkdir(parents=True, exist_ok=True)

    # Generate diagnostics
    report = generate_diagnostics_report(panel)

    # Write as JSON
    diagnostics_dict = report.to_dict()
    diagnostics_file.write_text(json.dumps(diagnostics_dict, indent=2, default=str) + "\n")

    try:
        diag_display = str(diagnostics_file.relative_to(ctx.project_root))
    except ValueError:
        diag_display = str(diagnostics_file)
    detail = f"persist diagnostics: {diag_display}"
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
    storage_config: StorageConfig | None = None,
) -> PipelineResult:
    """Execute all tasks in a resolved plan in deterministic order.

    Order: materialize → resample → join → persist.
    Stops on first failure and reports context.
    """
    ctx = ExecutionContext(
        project_root=project_root,
        recipe=recipe,
        cache=cache or RecipeCache(),
        storage_config=storage_config,
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

    return result


def execute_recipe(
    recipe: RecipeV1,
    project_root: Path | None = None,
    *,
    cache: RecipeCache | None = None,
    quiet: bool = False,
    storage_config: StorageConfig | None = None,
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
    storage_config : StorageConfig | None
        Storage root configuration.  Defaults to ``load_config()``.

    Returns
    -------
    list[PipelineResult]
        One result per pipeline.

    Raises
    ------
    ExecutorError
        If a planner or runtime error occurs.  The message includes
        pipeline and step context for actionable diagnostics.  The
        exception's ``partial_results`` attribute contains results for
        all pipelines (including the failed ones), so callers can
        inspect what succeeded.
    """
    if project_root is None:
        project_root = Path.cwd()
    if cache is None:
        cache = RecipeCache()

    results: list[PipelineResult] = []
    errors: list[str] = []

    def _log(msg: str) -> None:
        if not quiet:
            typer.echo(msg)

    for pipeline in recipe.pipelines:
        _log(f"\nExecuting pipeline '{pipeline.id}'...")

        # Resolve the execution plan
        try:
            plan = resolve_plan(recipe, pipeline.id)
        except PlannerError as exc:
            msg = f"Pipeline '{pipeline.id}': planning failed: {exc}"
            errors.append(msg)
            _log(f"  {msg}")
            # Record a failed result so partial_results is complete
            results.append(PipelineResult(
                pipeline_id=pipeline.id,
                steps=[StepResult(
                    step_kind="plan",
                    detail=f"planning failed: {exc}",
                    success=False,
                    error=str(exc),
                )],
            ))
            continue

        task_count = (
            len(plan.materialize_tasks)
            + len(plan.resample_tasks)
            + len(plan.join_tasks)
        )
        _log(f"  Resolved {task_count} tasks")

        # Execute the plan
        result = _execute_plan(
            plan, recipe, project_root,
            cache=cache, quiet=quiet, storage_config=storage_config,
        )
        results.append(result)

        if result.success:
            _log(
                f"  Pipeline '{pipeline.id}' completed: "
                f"{len(result.steps)} steps OK"
            )
        else:
            failed = next(s for s in result.steps if not s.success)
            msg = (
                f"Pipeline '{pipeline.id}': step failed: "
                f"[{failed.step_kind}] {failed.detail}"
                + (f" — {failed.error}" if failed.error else "")
            )
            errors.append(msg)

    if errors:
        raise ExecutorError(
            "; ".join(errors),
            partial_results=results,
        )

    return results
