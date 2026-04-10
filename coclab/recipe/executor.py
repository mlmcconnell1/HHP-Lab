"""Recipe execution orchestrator.

Given a validated RecipeV1 and pipeline id, resolves the execution plan
via the planner and executes materialize → resample → join tasks in
deterministic order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer

from coclab.config import StorageConfig
from coclab.geo.ct_planning_regions import CT_LEGACY_COUNTY_VINTAGE
from coclab.panel.finalize import (
    finalize_panel,
)
from coclab.recipe.cache import RecipeCache
from coclab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    PipelineResult,
    StepResult,
    _classify_path,
    _echo,
    _get_transform,
    _record_step_note,
)
from coclab.recipe.executor_ct_alignment import (
    _load_ct_county_alignment_crosswalk,
    _needs_ct_planning_to_legacy_alignment,
    _translate_ct_planning_values_to_legacy,
)
from coclab.recipe.executor_inputs import (
    _apply_temporal_filter,
    _filter_to_year,
    _load_support_dataset_for_year,
    _reject_implicit_static_broadcast,
    _resolve_geo_column,
    _resolve_year_column,
    _validate_columns,
)
from coclab.recipe.executor_manifest import (
    _build_manifest,
    _build_provenance,
    _deduplicate_assets,
    _recipe_output_dirname,
    _resolve_panel_output_file,
    _resolve_pipeline_target,
    _target_geometry_metadata,
    resolve_pipeline_artifacts,
)
from coclab.recipe.executor_panel import (
    canonicalize_panel_for_target as _canonicalize_panel_for_target,
)
from coclab.recipe.executor_panel import (
    resolve_panel_aliases as _resolve_panel_aliases,
)
from coclab.recipe.executor_resample import (
    _XWALK_JOIN_KEYS,
    _XWALK_NON_GEO_COLS,
    _attach_dynamic_pop_share,
    _detect_xwalk_target_col,
    _resample_aggregate,
    _resample_allocate,
    _resample_identity,
)
from coclab.recipe.executor_transforms import (
    _generated_metro_transform_path,
    _identify_coc_and_base,
    _identify_metro_and_base,
    _materialize_generated_metro_transform,
    _resolve_metro_transform_df,
    _resolve_transform_path,
)
from coclab.recipe.manifest import (
    AssetRecord,
    write_manifest,
)
from coclab.recipe.planner import (
    ExecutionPlan,
    JoinTask,
    MaterializeTask,
    PlannerError,
    ResampleTask,
    resolve_plan,
)
from coclab.recipe.recipe_schema import (
    CohortSelector,
    PanelPolicy,
    RecipeV1,
    TemporalFilter,
    expand_year_spec,
)

# Re-export the core primitives so ``from coclab.recipe.executor import
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
    "_apply_temporal_filter",
    "_attach_dynamic_pop_share",
    "_build_manifest",
    "_canonicalize_panel_for_target",
    "_build_provenance",
    "_deduplicate_assets",
    "_detect_xwalk_target_col",
    "_filter_to_year",
    "_generated_metro_transform_path",
    "_identify_coc_and_base",
    "_identify_metro_and_base",
    "_load_ct_county_alignment_crosswalk",
    "_load_support_dataset_for_year",
    "_materialize_generated_metro_transform",
    "_needs_ct_planning_to_legacy_alignment",
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

# Manifest, provenance, and output-path helpers live in ``executor_manifest``
# and are re-exported from this module's top-level import block so legacy
# callers (CLI command, manual docs, tests that import _recipe_output_dirname)
# keep working unchanged.


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
        LAUS_MEASURE_COLUMNS,
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

    # LAUS-aware conformance: determine include_laus before alias translation
    # so that LAUS columns are included in the alias-translated measure_columns
    # list (coclab-xt72).
    include_laus = (
        persist_policy is not None
        and persist_policy.laus is not None
        and persist_policy.laus.include
    )

    # Translate measure_columns through any active column aliases so that
    # conformance checks look for the renamed names in the finalized panel.
    # When include_laus is True, LAUS columns are appended to base_cols before
    # translation so they are not silently dropped by the early-return path in
    # _effective_measure_columns (coclab-xt72).
    _panel_aliases = _resolve_panel_aliases(persist_target)
    if _panel_aliases:
        base_cols = list(ACS_MEASURE_COLUMNS if measure_columns is None else measure_columns)
        if include_laus:
            base_cols += [c for c in LAUS_MEASURE_COLUMNS if c not in base_cols]
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
