"""Recipe execution orchestrator.

Given a validated RecipeV1 and pipeline id, resolves the execution plan
via the planner and executes materialize → resample → join tasks in
deterministic order.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import typer

from hhplab.config import StorageConfig
from hhplab.geo.ct_planning_regions import CT_LEGACY_COUNTY_VINTAGE
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    PipelineResult,
    StepResult,
    _classify_path,
    _echo,
    _get_transform,
    _record_step_note,
)
from hhplab.recipe.executor_ct_alignment import (
    _load_ct_county_alignment_crosswalk,
    _needs_ct_planning_to_legacy_alignment,
    _translate_ct_planning_values_to_legacy,
)
from hhplab.recipe.executor_inputs import (
    _apply_temporal_filter,
    _filter_to_year,
    _load_support_dataset_for_year,
    _reject_implicit_static_broadcast,
    _resolve_geo_column,
    _resolve_year_column,
    _validate_columns,
    _validate_input_dataset_provenance,
)
from hhplab.recipe.executor_manifest import (
    _build_manifest,
    _build_provenance,
    _deduplicate_assets,
    _recipe_output_dirname,
    _resolve_map_output_file,
    _resolve_panel_output_file,
    _resolve_pipeline_target,
    _target_geometry_metadata,
    resolve_pipeline_artifacts,
)
from hhplab.recipe.executor_panel import (
    AssembledPanel as _AssembledPanel,  # noqa: F401  (public re-export)
)
from hhplab.recipe.executor_panel import (
    apply_cohort_selector as _apply_cohort_selector,  # noqa: F401  (public re-export)
)
from hhplab.recipe.executor_panel import (
    assemble_panel as _assemble_panel,  # noqa: F401  (public re-export)
)
from hhplab.recipe.executor_panel import (
    canonicalize_panel_for_target as _canonicalize_panel_for_target,  # noqa: F401
)
from hhplab.recipe.executor_panel import (
    resolve_panel_aliases as _resolve_panel_aliases,  # noqa: F401
)
from hhplab.recipe.executor_persistence import (
    persist_diagnostics as _persist_diagnostics,
)
from hhplab.recipe.executor_persistence import (
    persist_outputs as _persist_outputs,
)
from hhplab.recipe.executor_resample import (
    _XWALK_JOIN_KEYS,
    _XWALK_NON_GEO_COLS,
    _attach_dynamic_pop_share,
    _detect_xwalk_target_col,
    _resample_aggregate,
    _resample_allocate,
    _resample_identity,
)
from hhplab.recipe.executor_transforms import (
    _generated_metro_transform_path,
    _identify_coc_and_base,
    _identify_metro_and_base,
    _identify_msa_and_base,
    _materialize_generated_metro_transform,
    _materialize_generated_msa_transform,
    _resolve_metro_transform_df,
    _resolve_transform_path,
)
from hhplab.recipe.manifest import (
    AssetRecord,
)
from hhplab.recipe.planner import (
    ExecutionPlan,
    JoinTask,
    MaterializeTask,
    PlannerError,
    ResampleTask,
    resolve_plan,
)
from hhplab.recipe.recipe_schema import (
    RecipeV1,
    TemporalFilter,
)

# Re-export the core primitives so ``from hhplab.recipe.executor import
# ExecutorError`` (and similar imports used by tests, the CLI, and third-
# party callers) keep resolving.  The canonical home is ``executor_core``;
# importing from there avoids the partial-initialization cycle that used
# to hit direct submodule imports (coclab-l6be).
__all__ = [
    "ExecutionContext",
    "ExecutorError",
    "PipelineResult",
    "StepResult",
    "_classify_path",
    "_echo",
    "_get_transform",
    "_record_step_note",
    # Submodule re-exports preserved for backwards compatibility.
    "_XWALK_JOIN_KEYS",
    "_XWALK_NON_GEO_COLS",
    "_AssembledPanel",
    "_apply_cohort_selector",
    "_apply_temporal_filter",
    "_assemble_panel",
    "_attach_dynamic_pop_share",
    "_build_manifest",
    "_build_provenance",
    "_canonicalize_panel_for_target",
    "_deduplicate_assets",
    "_detect_xwalk_target_col",
    "_filter_to_year",
    "_generated_metro_transform_path",
    "_identify_coc_and_base",
    "_identify_metro_and_base",
    "_identify_msa_and_base",
    "_load_ct_county_alignment_crosswalk",
    "_load_support_dataset_for_year",
    "_materialize_generated_metro_transform",
    "_materialize_generated_msa_transform",
    "_needs_ct_planning_to_legacy_alignment",
    "_resolve_map_output_file",
    "_persist_diagnostics",
    "_persist_outputs",
    "_recipe_output_dirname",
    "_reject_implicit_static_broadcast",
    "_resample_aggregate",
    "_resample_allocate",
    "_resample_identity",
    "_resolve_geo_column",
    "_resolve_metro_transform_df",
    "_resolve_panel_aliases",
    "_resolve_panel_output_file",
    "_resolve_pipeline_target",
    "_resolve_transform_path",
    "_resolve_year_column",
    "_target_geometry_metadata",
    "_translate_ct_planning_values_to_legacy",
    "_validate_columns",
    # Orchestration entry points defined in this module.
    "execute_recipe",
    "resolve_pipeline_artifacts",
]


# ---------------------------------------------------------------------------
# Step executors
# ---------------------------------------------------------------------------

_ACS_PATH_VINTAGE_RE = re.compile(r"__A(\d{4})")


def _normalize_acs5_vintage(value: object) -> str:
    """Normalize an ACS5 vintage value to its end-year string."""
    text = str(value).strip()
    if not text:
        raise ExecutorError("Empty ACS5 vintage value cannot be normalized.")
    if "-" in text:
        end = text.split("-")[-1]
        if end.isdigit() and len(end) == 4:
            return end
    if text.isdigit() and len(text) == 4:
        return text
    match = re.search(r"(\d{4})$", text)
    if match is not None:
        return match.group(1)
    raise ExecutorError(
        f"Could not normalize ACS5 vintage value {value!r} to a 4-digit end year."
    )


def _single_string_value(values: pd.Series, label: str) -> str | None:
    """Return a single distinct non-null string value, or None when absent."""
    distinct = values.dropna().astype(str).unique().tolist()
    if not distinct:
        return None
    if len(distinct) > 1:
        raise ExecutorError(
            f"{label} has multiple distinct values in one dataset-year slice: "
            f"{sorted(distinct)}."
        )
    return str(distinct[0])


def _fallback_dataset_year_value(
    *,
    df: pd.DataFrame,
    task: ResampleTask,
) -> str | None:
    """Return a single year-like value from the active dataset slice."""
    year_column = _resolve_year_column(df, task.year_column)
    if year_column is None or year_column not in df.columns:
        if "year" not in df.columns:
            return str(task.year)
        year_column = "year"
    return _single_string_value(
        df[year_column],
        f"Dataset '{task.dataset_id}' year {task.year}: {year_column}",
    )


def _record_dataset_year_metadata(
    *,
    task: ResampleTask,
    ctx: ExecutionContext,
    df: pd.DataFrame,
) -> None:
    """Capture dataset/year provenance needed during panel assembly."""
    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None:
        return

    metadata: dict[str, str] = {}

    if ds.provider == "census" and ds.product in {"acs", "acs5"}:
        acs5_vintage = None
        if "acs_vintage" in df.columns:
            acs5_vintage = _single_string_value(
                df["acs_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs_vintage",
            )
            if acs5_vintage is not None:
                acs5_vintage = _normalize_acs5_vintage(acs5_vintage)
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs5_vintage = match.group(1)
        if acs5_vintage is None:
            fallback_year = _fallback_dataset_year_value(df=df, task=task)
            if fallback_year is not None:
                acs5_vintage = _normalize_acs5_vintage(fallback_year)
        if acs5_vintage is not None:
            metadata["acs5_vintage_used"] = acs5_vintage

    if ds.provider == "census" and ds.product == "acs1":
        acs1_vintage = None
        if "acs1_vintage" in df.columns:
            acs1_vintage = _single_string_value(
                df["acs1_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs1_vintage",
            )
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs1_vintage = match.group(1)
        if acs1_vintage is None:
            acs1_vintage = _fallback_dataset_year_value(df=df, task=task)
        if acs1_vintage is not None:
            metadata["acs1_vintage_used"] = acs1_vintage

    if metadata:
        ctx.dataset_year_metadata[(task.dataset_id, task.year)] = metadata


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
        msa_ref, _msa_base_ref = (
            _identify_msa_and_base(transform.from_, transform.to)
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
        elif msa_ref is not None and not path.exists():
            try:
                path = _materialize_generated_msa_transform(
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
                        "Run 'hhplab generate xwalks' to build CoC crosswalks "
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
        _validate_input_dataset_provenance(
            ctx=ctx,
            dataset_id=task.dataset_id,
            effective_geometry=task.effective_geometry,
            input_path=task.input_path,
        )
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
    except ExecutorError as exc:
        return StepResult(
            step_kind="resample",
            detail=detail,
            success=False,
            error=str(exc),
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
        filtered_df = _filter_to_year(df, year_col, task.year)
        if filtered_df.empty:
            ds = ctx.recipe.datasets.get(task.dataset_id)
            unique_year_values: list[str] = []
            if (
                ds is not None
                and ds.provider == "census"
                and ds.product == "acs1"
                and year_col == "acs1_vintage"
            ):
                source_years = df[year_col].dropna()
                if not source_years.empty:
                    numeric_years = pd.to_numeric(source_years, errors="coerce")
                    if numeric_years.notna().all():
                        unique_year_values = [
                            str(int(value))
                            for value in sorted(numeric_years.astype("int64").unique())
                        ]
                    else:
                        unique_year_values = sorted(source_years.astype(str).unique().tolist())
            if len(unique_year_values) == 1:
                resolved_vintage = unique_year_values[0]
                df = df.copy()
                _record_step_note(
                    ctx,
                    step_notes,
                    f"Dataset '{task.dataset_id}' year {task.year}: using lagged "
                    f"ACS1 vintage {resolved_vintage} from the resolved input file.",
                )
            else:
                return StepResult(
                    step_kind="resample",
                    detail=detail,
                    success=False,
                    error=(
                        f"Dataset '{task.dataset_id}' year {task.year}: "
                        f"no rows after filtering {year_col}=={task.year}."
                    ),
                )
        else:
            df = filtered_df
        df = df.copy()
        df["year"] = task.year

    _record_dataset_year_metadata(task=task, ctx=ctx, df=df)

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

# Panel assembly lives in ``executor_panel`` and the persistence path
# (parquet writes, manifest sidecar, diagnostics JSON) lives in
# ``executor_persistence``.  Both are imported at the top of this module
# and re-exported under their original underscored names for backwards
# compatibility with tests and CLI callers.


def _persist_map_output(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Render a recipe-native HTML map artifact."""
    output_file = _resolve_map_output_file(
        ctx.recipe,
        plan.pipeline_id,
        ctx.project_root,
        storage_config=ctx.storage_config,
    )
    _pipeline, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
    try:
        from hhplab.viz.map_folium import render_recipe_map

        rendered = render_recipe_map(
            target,
            project_root=ctx.project_root,
            out_html=output_file,
        )
    except (FileNotFoundError, ValueError) as exc:
        return StepResult(
            step_kind="persist_map",
            detail=f"persist map: {output_file}",
            success=False,
            error=str(exc),
        )

    try:
        display = str(rendered.relative_to(ctx.project_root))
    except ValueError:
        display = str(rendered)

    return StepResult(
        step_kind="persist_map",
        detail=f"persist map: {display}",
        success=True,
    )


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

    # Phase 4: persist declared outputs.
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

    if plan.join_tasks:
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

    if "map" in declared_outputs:
        step = _persist_map_output(plan, ctx)
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
