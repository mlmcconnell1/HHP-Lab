"""Population-specific recipe execution policy.

This module owns normalization, lineage, collision handling, and the special
PEP decennial tract-mediated aggregation path. The executor orchestrator calls
these helpers without embedding source-specific population semantics.
"""

from __future__ import annotations

import pandas as pd

from hhplab.recipe.executor.core import ExecutionContext, ExecutorError, _get_transform
from hhplab.recipe.executor.inputs import _resolve_geo_column, _validate_columns
from hhplab.recipe.executor.resample import (
    _XWALK_JOIN_KEYS,
    _detect_xwalk_target_col,
)
from hhplab.recipe.planner import ResampleTask
from hhplab.schema.columns import TOTAL_POPULATION
from hhplab.schema.lineage import (
    PopulationLineage,
    PopulationMethod,
    PopulationSource,
    normalize_population_measure,
    population_lineage_columns,
)


def normalize_recipe_population_measure(
    result_df: pd.DataFrame,
    *,
    task: ResampleTask,
    ctx: ExecutionContext,
) -> pd.DataFrame:
    """Normalize PEP CoC population outputs to canonical total_population."""
    ds = ctx.recipe.datasets.get(task.dataset_id)
    if (
        ds is None
        or ds.provider != "census"
        or ds.product != "pep"
        or task.to_geometry.type != "coc"
        or "population" not in result_df.columns
        or "total_population" in result_df.columns
    ):
        return result_df

    method = (
        PopulationMethod.POPULATION_CROSSWALK
        if (task.weight_column or "") == "pop_share"
        else PopulationMethod.AREA_CROSSWALK
    )
    return normalize_population_measure(
        result_df,
        source_column="population",
        lineage=PopulationLineage(
            source=PopulationSource.PEP,
            source_year=task.year,
            method=method,
            crosswalk_id=task.transform_id,
            crosswalk_geometry=(
                f"{task.effective_geometry.type}_to_{task.to_geometry.type}"
                if task.transform_id is not None
                else None
            ),
            crosswalk_vintage=task.effective_geometry.vintage,
        ),
    )


def _population_source_token_for_dataset(dataset_id: str, ctx: ExecutionContext) -> str:
    ds = ctx.recipe.datasets.get(dataset_id)
    if ds is None:
        return dataset_id
    if ds.provider == "census" and ds.product in {"acs", "acs5"}:
        return "acs5"
    if ds.provider == "census" and ds.product == "pep":
        return "pep"
    return dataset_id


def rename_conflicting_population_columns(
    frames: list[pd.DataFrame],
    dataset_ids: list[str],
    ctx: ExecutionContext,
) -> list[pd.DataFrame]:
    """Preserve multiple population estimates as source-specific columns."""
    population_frame_indexes = [
        index for index, frame in enumerate(frames) if TOTAL_POPULATION in frame.columns
    ]
    if len(population_frame_indexes) <= 1:
        return frames

    tokens = [_population_source_token_for_dataset(dataset_id, ctx) for dataset_id in dataset_ids]
    if len(tokens) != len(set(tokens)):
        tokens = list(dataset_ids)

    lineage_cols = population_lineage_columns()
    renamed: list[pd.DataFrame] = []
    for index, frame in enumerate(frames):
        if index not in population_frame_indexes:
            renamed.append(frame)
            continue

        token = tokens[index]
        rename_map = {TOTAL_POPULATION: f"{TOTAL_POPULATION}_{token}"}
        for lineage_col in lineage_cols:
            if lineage_col in frame.columns:
                rename_map[lineage_col] = (
                    f"{TOTAL_POPULATION}_{token}{lineage_col.removeprefix(TOTAL_POPULATION)}"
                )
        renamed.append(frame.rename(columns=rename_map))
    return renamed


def is_pep_decennial_tract_mediated_population_task(
    task: ResampleTask,
    ctx: ExecutionContext,
) -> bool:
    """Return whether a task needs PEP baseline-scaling aggregation semantics."""
    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None or ds.provider != "census" or ds.product != "pep":
        return False
    if task.method != "aggregate" or task.transform_id is None:
        return False
    if task.weight_column != "population_weight":
        return False
    if "population" not in task.measures:
        return False
    transform = _get_transform(ctx.recipe, task.transform_id)
    weighting = getattr(transform.spec, "weighting", None)
    return (
        weighting is not None
        and weighting.scheme == "tract_mediated"
        and weighting.denominator_source == "decennial"
    )


def resample_pep_decennial_tract_mediated_population(
    source_df: pd.DataFrame,
    xwalk: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Use PEP's baseline-scaling semantics for decennial tract-mediated weights."""
    from hhplab.pep.pep_aggregate import aggregate_pep_counties

    geo_col = _resolve_geo_column(source_df, task.geo_column)
    _validate_columns(source_df, ["population"], task.dataset_id, task.year)
    if "year" not in source_df.columns:
        raise ExecutorError(
            f"Dataset '{task.dataset_id}' requires a year column for "
            "PEP decennial tract-mediated population aggregation."
        )
    xwalk_key = _XWALK_JOIN_KEYS.get(task.effective_geometry.type)
    if xwalk_key is None or xwalk_key not in xwalk.columns:
        raise ExecutorError(
            f"PEP decennial tract-mediated transform '{task.transform_id}' "
            f"requires crosswalk join key '{xwalk_key}'."
        )
    target_col = _detect_xwalk_target_col(xwalk, xwalk_key)
    pep_df = (
        source_df[[geo_col, "year", "population"]]
        .rename(columns={geo_col: "county_fips"})
        .copy()
    )
    result = aggregate_pep_counties(
        pep_df,
        xwalk,
        geo_id_col=target_col,
        weighting="population_weight",
        boundary_vintage=(
            str(task.to_geometry.vintage) if task.to_geometry.vintage is not None else None
        ),
        county_vintage=(
            str(task.effective_geometry.vintage)
            if task.effective_geometry.vintage is not None
            else None
        ),
    )
    result = result[result["year"] == task.year].reset_index(drop=True)
    if target_col != "geo_id":
        result = result.rename(columns={target_col: "geo_id"})
    return normalize_population_measure(
        result,
        source_column="population",
        lineage=PopulationLineage(
            source=PopulationSource.PEP,
            source_year=task.year,
            method=PopulationMethod.TRACT_MEDIATED_CROSSWALK,
            crosswalk_id=task.transform_id,
            crosswalk_geometry=f"{task.effective_geometry.type}_to_{task.to_geometry.type}",
            crosswalk_vintage=task.effective_geometry.vintage,
        ),
    )
