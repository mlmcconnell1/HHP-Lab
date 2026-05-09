"""Resolution planner: deterministic expansion of recipe → execution plan.

Given a validated RecipeV1, the planner resolves each (dataset, year) pair to:
  - input path
  - effective native geometry (type + vintage)
  - chosen transform (for via:auto)
and emits structured ResampleTask / JoinTask objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from hhplab.recipe.recipe_schema import (
    FileSetSpec,
    JoinStep,
    MaterializeStep,
    RecipeV1,
    ResampleStep,
    SmallAreaEstimateStep,
)
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec


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
    path: str | None
    effective_geometry: GeometryRef


@dataclass
class ResampleTask:
    """A single resample operation for one dataset-year."""
    dataset_id: str
    year: int
    input_path: str | None
    effective_geometry: GeometryRef
    method: str
    transform_id: str | None
    to_geometry: GeometryRef
    measures: list[str]
    measure_aggregations: dict[str, str] | None = None
    year_column: str | None = None
    geo_column: str | None = None
    weighting_variety: str | None = None
    weight_column: str | None = None
    weighting_variety_count: int = 1


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
class SmallAreaEstimateTask:
    """Resolved ACS1/ACS5 small-area estimation task for one analysis year."""

    output_dataset: str
    year: int
    source_dataset: str
    support_dataset: str
    source_path: str | None
    support_path: str | None
    source_geometry: GeometryRef
    support_geometry: GeometryRef
    target_geometry: GeometryRef
    terminal_acs5_vintage: str
    tract_vintage: str
    allocation_method: str
    denominators: dict[str, str]
    measure_families: list[str]
    derived_outputs: dict[str, list[str]]
    diagnostics: dict[str, bool]


def _geometry_to_dict(g: GeometryRef) -> dict:
    d: dict = {"type": g.type}
    if g.vintage is not None:
        d["vintage"] = g.vintage
    if g.source is not None:
        d["source"] = g.source
    if g.subset_profile is not None:
        d["subset_profile"] = g.subset_profile
    if g.subset_profile_definition_version is not None:
        d["subset_profile_definition_version"] = g.subset_profile_definition_version
    return d


@dataclass
class ExecutionPlan:
    """The full resolved execution plan for a recipe pipeline."""
    pipeline_id: str
    materialize_tasks: list[MaterializeTask] = field(default_factory=list)
    resample_tasks: list[ResampleTask] = field(default_factory=list)
    small_area_estimate_tasks: list[SmallAreaEstimateTask] = field(default_factory=list)
    join_tasks: list[JoinTask] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize the plan to a JSON-safe dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "materialize_tasks": [
                {"transform_ids": t.transform_ids}
                for t in self.materialize_tasks
            ],
            "resample_tasks": [
                {
                    "dataset_id": t.dataset_id,
                    "year": t.year,
                    "input_path": t.input_path,
                    "effective_geometry": _geometry_to_dict(
                        t.effective_geometry,
                    ),
                    "method": t.method,
                    "transform_id": t.transform_id,
                    "to_geometry": _geometry_to_dict(t.to_geometry),
                    "measures": t.measures,
                    "measure_aggregations": t.measure_aggregations,
                    "weighting_variety": t.weighting_variety,
                    "weight_column": t.weight_column,
                    "weighting_variety_count": t.weighting_variety_count,
                }
                for t in self.resample_tasks
            ],
            "small_area_estimate_tasks": [
                {
                    "output_dataset": t.output_dataset,
                    "year": t.year,
                    "source_dataset": t.source_dataset,
                    "support_dataset": t.support_dataset,
                    "source_path": t.source_path,
                    "support_path": t.support_path,
                    "source_geometry": _geometry_to_dict(t.source_geometry),
                    "support_geometry": _geometry_to_dict(t.support_geometry),
                    "target_geometry": _geometry_to_dict(t.target_geometry),
                    "terminal_acs5_vintage": t.terminal_acs5_vintage,
                    "tract_vintage": t.tract_vintage,
                    "allocation_method": t.allocation_method,
                    "denominators": dict(t.denominators),
                    "measure_families": list(t.measure_families),
                    "derived_outputs": {
                        family: list(outputs)
                        for family, outputs in t.derived_outputs.items()
                    },
                    "diagnostics": dict(t.diagnostics),
                }
                for t in self.small_area_estimate_tasks
            ],
            "join_tasks": [
                {
                    "datasets": t.datasets,
                    "join_on": t.join_on,
                    "year": t.year,
                }
                for t in self.join_tasks
            ],
            "task_count": (
                len(self.materialize_tasks)
                + len(self.resample_tasks)
                + len(self.small_area_estimate_tasks)
                + len(self.join_tasks)
            ),
        }


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
    if year in seg.overrides:
        path = seg.overrides[year]
    else:
        render_ctx: dict[str, object] = {"year": year}
        render_ctx.update(seg.constants)
        render_ctx.update({k: year + offset for k, offset in seg.year_offsets.items()})
        try:
            path = file_set.path_template.format(**render_ctx)
        except KeyError as exc:
            missing = exc.args[0]
            raise PlannerError(
                f"Dataset '{dataset_id}' file_set path_template references "
                f"missing variable '{missing}' for year {year}. "
                f"Available variables: {sorted(render_ctx.keys())}"
            ) from exc

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
    """Check if two geometry refs match on type, vintage, and source.

    Every populated identity field must match exactly. A None field only
    matches another None value — it is *not* treated as a wildcard.
    """
    if a.type != b.type:
        return False
    return (
        a.vintage == b.vintage
        and a.source == b.source
        and a.subset_profile == b.subset_profile
        and a.subset_profile_definition_version == b.subset_profile_definition_version
    )


def _resolve_auto_transform(
    dataset_id: str,
    year: int,
    effective_geometry: GeometryRef,
    to_geometry: GeometryRef,
    recipe: RecipeV1,
    method: str = "",
) -> str:
    """Select a compatible transform for via:auto.

    Looks for a crosswalk/rollup whose endpoints connect
    to_geometry <-> effective_geometry, constrained by the resample
    method's direction requirements.

    When ``method`` is specified, the transform must connect the two
    geometry endpoints.  Both from/to directions are accepted because
    crosswalk data is symmetric (contains columns for both geometry
    types regardless of the from/to naming convention).
    """
    candidates: list[str] = []

    for t in recipe.transforms:
        # Direction A: transform from→to_geometry, to→effective_geometry
        # (crosswalk goes target → source, used by allocate)
        fwd = (
            _geometry_matches(t.from_, to_geometry)
            and _geometry_matches(t.to, effective_geometry)
        )
        # Direction B: transform from→effective_geometry, to→to_geometry
        # (crosswalk goes source → target, used by aggregate)
        rev = (
            _geometry_matches(t.from_, effective_geometry)
            and _geometry_matches(t.to, to_geometry)
        )

        if fwd or rev:
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


_TRACT_MEDIATED_WEIGHT_COLUMNS: dict[str, str] = {
    "area": "area_weight",
    "population": "population_weight",
    "households": "household_weight",
    "renter_households": "renter_household_weight",
}


def _transform_weighting_varieties(
    recipe: RecipeV1,
    transform_id: str | None,
) -> tuple[str | None, ...]:
    """Return the resample task variants implied by a transform."""
    if transform_id is None:
        return (None,)
    transform = next((t for t in recipe.transforms if t.id == transform_id), None)
    if transform is None or transform.type != "crosswalk":
        return (None,)
    weighting = transform.spec.weighting
    if weighting.scheme != "tract_mediated":
        return (None,)
    return tuple(weighting.resolved_varieties)


def _weight_column_for_variety(variety: str | None) -> str | None:
    if variety is None:
        return None
    return _TRACT_MEDIATED_WEIGHT_COLUMNS[variety]


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
                            recipe=recipe,
                            method=step.method,
                        )
                    else:
                        transform_id = step.via

                weighting_varieties = _transform_weighting_varieties(
                    recipe,
                    transform_id,
                )
                for weighting_variety in weighting_varieties:
                    ds = recipe.datasets[step.dataset]
                    measure_aggs = {
                        name: cfg.aggregation
                        for name, cfg in step.measures.items()
                    }
                    plan.resample_tasks.append(
                        ResampleTask(
                            dataset_id=step.dataset,
                            year=year,
                            input_path=resolved.path,
                            effective_geometry=resolved.effective_geometry,
                            method=step.method,
                            transform_id=transform_id,
                            to_geometry=step.to_geometry,
                            measures=step.measure_names,
                            measure_aggregations=measure_aggs,
                            year_column=ds.year_column,
                            geo_column=ds.geo_column,
                            weighting_variety=weighting_variety,
                            weight_column=_weight_column_for_variety(weighting_variety),
                            weighting_variety_count=len(weighting_varieties),
                        )
                    )

        elif isinstance(step, SmallAreaEstimateStep):
            for year in universe_years:
                source = _resolve_dataset_year(step.source_dataset, year, recipe)
                support = _resolve_dataset_year(step.support_dataset, year, recipe)
                plan.small_area_estimate_tasks.append(
                    SmallAreaEstimateTask(
                        output_dataset=step.output_dataset,
                        year=year,
                        source_dataset=step.source_dataset,
                        support_dataset=step.support_dataset,
                        source_path=source.path,
                        support_path=support.path,
                        source_geometry=source.effective_geometry,
                        support_geometry=support.effective_geometry,
                        target_geometry=step.target_geometry,
                        terminal_acs5_vintage=str(step.terminal_acs5_vintage),
                        tract_vintage=str(step.tract_vintage),
                        allocation_method=step.allocation_method,
                        denominators=dict(step.denominators),
                        measure_families=[str(family) for family in step.measures],
                        derived_outputs={
                            str(family): list(config.outputs)
                            for family, config in step.measures.items()
                        },
                        diagnostics=step.diagnostics.model_dump(),
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
