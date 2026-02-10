"""Resolution planner: deterministic expansion of recipe → execution plan.

Given a validated RecipeV1, the planner resolves each (dataset, year) pair to:
  - input path
  - effective native geometry (type + vintage)
  - chosen transform (for via:auto)
and emits structured ResampleTask / JoinTask objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from coclab.recipe.recipe_schema import (
    CrosswalkTransform,
    FileSetSpec,
    GeometryRef,
    JoinStep,
    MaterializeStep,
    RecipeV1,
    ResampleStep,
    RollupTransform,
    expand_year_spec,
)


class PlannerError(Exception):
    """Raised when the planner cannot resolve a recipe."""


# ---------------------------------------------------------------------------
# Plan data structures
# ---------------------------------------------------------------------------

@dataclass
class ResolvedDatasetYear:
    """Resolution result for a single (dataset_id, year)."""
    dataset_id: str
    year: int
    path: Optional[str]
    effective_geometry: GeometryRef


@dataclass
class ResampleTask:
    """A single resample operation for one dataset-year."""
    dataset_id: str
    year: int
    input_path: Optional[str]
    effective_geometry: GeometryRef
    method: str
    transform_id: Optional[str]
    to_geometry: GeometryRef
    measures: list[str]
    aggregation: Optional[str] = None


@dataclass
class JoinTask:
    """A join operation merging resampled datasets for a year."""
    datasets: list[str]
    join_on: list[str]
    year: int


@dataclass
class MaterializeTask:
    """Ensure specified transforms are materialized."""
    transform_ids: list[str]


@dataclass
class ExecutionPlan:
    """The full resolved execution plan for a recipe pipeline."""
    pipeline_id: str
    materialize_tasks: list[MaterializeTask] = field(default_factory=list)
    resample_tasks: list[ResampleTask] = field(default_factory=list)
    join_tasks: list[JoinTask] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dataset-year resolution
# ---------------------------------------------------------------------------

def _resolve_dataset_year(
    dataset_id: str,
    year: int,
    recipe: RecipeV1,
) -> ResolvedDatasetYear:
    """Resolve the path and effective geometry for a dataset at a given year."""
    ds = recipe.datasets[dataset_id]

    if ds.file_set is not None:
        return _resolve_via_file_set(dataset_id, year, ds.file_set, ds.native_geometry)

    # No file_set — check year coverage if declared, then use static path
    if ds.years is not None:
        covered = expand_year_spec(ds.years)
        if year not in covered:
            raise PlannerError(
                f"Dataset '{dataset_id}' declares year coverage "
                f"{ds.years.range or ds.years.years} but year {year} is not covered."
            )

    return ResolvedDatasetYear(
        dataset_id=dataset_id,
        year=year,
        path=ds.path,
        effective_geometry=ds.native_geometry,
    )


def _resolve_via_file_set(
    dataset_id: str,
    year: int,
    file_set: FileSetSpec,
    native_geometry: GeometryRef,
) -> ResolvedDatasetYear:
    """Resolve path + effective geometry from file_set segments."""
    matching_segments = []
    for seg in file_set.segments:
        seg_years = expand_year_spec(seg.years)
        if year in seg_years:
            matching_segments.append(seg)

    if len(matching_segments) == 0:
        raise PlannerError(
            f"Dataset '{dataset_id}' has no file_set segment covering year {year}."
        )
    if len(matching_segments) > 1:
        raise PlannerError(
            f"Dataset '{dataset_id}' has multiple file_set segments covering "
            f"year {year} (should not happen after validation)."
        )

    seg = matching_segments[0]
    path = seg.overrides.get(year) or file_set.path_template.format(year=year)

    return ResolvedDatasetYear(
        dataset_id=dataset_id,
        year=year,
        path=path,
        effective_geometry=seg.geometry,
    )


# ---------------------------------------------------------------------------
# Transform auto-selection
# ---------------------------------------------------------------------------

def _geometry_matches(a: GeometryRef, b: GeometryRef) -> bool:
    """Check if two geometry refs match on type and vintage."""
    if a.type != b.type:
        return False
    if a.vintage is not None and b.vintage is not None:
        return a.vintage == b.vintage
    return True


def _resolve_auto_transform(
    dataset_id: str,
    year: int,
    effective_geometry: GeometryRef,
    to_geometry: GeometryRef,
    method: str,
    recipe: RecipeV1,
) -> str:
    """Select a compatible transform for via:auto.

    For aggregate: the transform's `from` should match to_geometry (the target)
    and the transform's `to` should match the dataset's effective native geometry.

    For allocate: the transform's `from` should match to_geometry and the
    transform's `to` should match the effective native geometry.

    In both cases, we look for a crosswalk/rollup whose endpoints connect
    to_geometry <-> effective_geometry.
    """
    candidates: list[str] = []

    for t in recipe.transforms:
        # Check from→to_geometry and to→effective_geometry
        if _geometry_matches(t.from_, to_geometry) and _geometry_matches(t.to, effective_geometry):
            candidates.append(t.id)
        # Also check the reverse direction
        elif _geometry_matches(t.from_, effective_geometry) and _geometry_matches(t.to, to_geometry):
            candidates.append(t.id)

    if len(candidates) == 0:
        available = [
            f"{t.id} ({t.from_.type}@{t.from_.vintage} -> {t.to.type}@{t.to.vintage})"
            for t in recipe.transforms
        ]
        raise PlannerError(
            f"Resample step for dataset '{dataset_id}' year {year} has effective "
            f"geometry {effective_geometry.type}@{effective_geometry.vintage} but no "
            f"compatible transform found for to_geometry "
            f"{to_geometry.type}@{to_geometry.vintage}. "
            f"Available transforms: {available}"
        )

    if len(candidates) > 1:
        raise PlannerError(
            f"Resample step for dataset '{dataset_id}' year {year} has multiple "
            f"compatible transforms: {candidates}. Specify via explicitly."
        )

    return candidates[0]


# ---------------------------------------------------------------------------
# Plan resolution
# ---------------------------------------------------------------------------

def resolve_plan(recipe: RecipeV1, pipeline_id: str) -> ExecutionPlan:
    """Resolve an execution plan for one pipeline in the recipe.

    Parameters
    ----------
    recipe : RecipeV1
        Validated recipe.
    pipeline_id : str
        Which pipeline to resolve.

    Returns
    -------
    ExecutionPlan
        Structured plan with materialize, resample, and join tasks.
    """
    pipeline = None
    for p in recipe.pipelines:
        if p.id == pipeline_id:
            pipeline = p
            break
    if pipeline is None:
        raise PlannerError(f"Pipeline '{pipeline_id}' not found in recipe.")

    universe_years = expand_year_spec(recipe.universe)
    plan = ExecutionPlan(pipeline_id=pipeline_id)

    for step in pipeline.steps:
        if isinstance(step, MaterializeStep):
            plan.materialize_tasks.append(
                MaterializeTask(transform_ids=list(step.transforms))
            )

        elif isinstance(step, ResampleStep):
            for year in universe_years:
                resolved = _resolve_dataset_year(step.dataset, year, recipe)

                transform_id: str | None = None
                if step.method != "identity":
                    if step.via == "auto":
                        transform_id = _resolve_auto_transform(
                            dataset_id=step.dataset,
                            year=year,
                            effective_geometry=resolved.effective_geometry,
                            to_geometry=step.to_geometry,
                            method=step.method,
                            recipe=recipe,
                        )
                    else:
                        transform_id = step.via

                plan.resample_tasks.append(
                    ResampleTask(
                        dataset_id=step.dataset,
                        year=year,
                        input_path=resolved.path,
                        effective_geometry=resolved.effective_geometry,
                        method=step.method,
                        transform_id=transform_id,
                        to_geometry=step.to_geometry,
                        measures=list(step.measures),
                        aggregation=step.aggregation,
                    )
                )

        elif isinstance(step, JoinStep):
            for year in universe_years:
                plan.join_tasks.append(
                    JoinTask(
                        datasets=list(step.datasets),
                        join_on=list(step.join_on),
                        year=year,
                    )
                )

    return plan
