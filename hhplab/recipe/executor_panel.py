"""Panel assembly from recipe execution intermediates.

Owns the pure transformation from per-year joined intermediates onto a
fully-canonicalized panel DataFrame: year-frame gathering, target
metadata stamping, ZORI/ACS1/LAUS panel policy application, shared
``finalize_panel`` shaping, and the cohort selector.  No parquet, no
JSON, no manifest, no conformance — those all live in
``executor_persistence``.

This module is one leg of the executor panel/persistence split tracked
in coclab-anb0; the step-by-step extraction plan lives in
``background/executor_panel_split_design.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from hhplab.panel.finalize import (
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    finalize_panel,
)
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    StepResult,
    _echo,
)
from hhplab.recipe.executor_manifest import (
    _resolve_pipeline_target,
    _target_geometry_metadata,
)
from hhplab.recipe.executor_panel_policies import (
    DEFAULT_APPLIERS,
    PanelPolicyApplier,
    PolicyApplication,
)
from hhplab.recipe.planner import ExecutionPlan
from hhplab.recipe.recipe_schema import (
    CohortSelector,
    PanelPolicy,
)
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec
from hhplab.schema.columns import POPULATION_DENSITY_COLUMN, TOTAL_POPULATION
from hhplab.schema.lineage import (
    PopulationMethod,
    PopulationSource,
    population_lineage_columns,
)


def canonicalize_panel_for_target(
    panel: pd.DataFrame,
    target_geometry: GeometryRef,
) -> pd.DataFrame:
    """Add target-geometry metadata columns expected by downstream tools."""
    result = panel.copy()
    (
        geo_type,
        boundary_vintage,
        definition_version,
        _profile_definition_version,
    ) = _target_geometry_metadata(target_geometry)
    if "geo_id" in result.columns:
        result["geo_type"] = geo_type
        if geo_type == "coc" and "coc_id" not in result.columns:
            result["coc_id"] = result["geo_id"]
        if geo_type == "metro":
            if "metro_id" not in result.columns:
                result["metro_id"] = result["geo_id"]
            if "metro_name" not in result.columns or result["metro_name"].isna().any():
                from hhplab.metro.metro_definitions import metro_name_for_id

                result["metro_name"] = result["metro_id"].map(metro_name_for_id)
            if definition_version is not None and "definition_version_used" not in result.columns:
                result["definition_version_used"] = definition_version
        if geo_type == "msa":
            if "msa_id" not in result.columns:
                result["msa_id"] = result["geo_id"]
            if definition_version is not None and "definition_version_used" not in result.columns:
                result["definition_version_used"] = definition_version
        if (
            geo_type == "coc"
            and boundary_vintage is not None
            and "boundary_vintage_used" not in result.columns
        ):
            result["boundary_vintage_used"] = boundary_vintage
    return result


def resolve_panel_aliases(target) -> dict[str, str]:
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


def apply_cohort_selector(
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
            f"Cohort selector reference_year {cohort.reference_year} produced no rows in the panel."
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
class AssembledPanel:
    """Result of assembling a panel from joined intermediates.

    ``policy_artifacts`` is keyed by applier name (e.g. ``"zori"``) so
    ``executor_persistence`` can reach back into a specific applier's
    result — today only the ZORI applier produces a provenance object.
    The ``zori_provenance`` property preserves the attribute-style
    access used by the legacy persistence path.
    """

    panel: pd.DataFrame
    frames: list[pd.DataFrame]
    target: object  # TargetSpec
    target_geo_type: str
    boundary_vintage: str | None
    definition_version: str | None
    policy_artifacts: dict[str, PolicyApplication] = field(default_factory=dict)

    @property
    def zori_provenance(self) -> object | None:
        """Backward-compatible accessor used by executor_persistence."""
        app = self.policy_artifacts.get("zori")
        return app.provenance if app is not None else None


_RECIPE_COC_COLUMN_ORDER: list[str] = [
    "coc_id",
    "coc_name",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "total_population_source",
    "total_population_source_year",
    "total_population_method",
    "total_population_crosswalk_id",
    "total_population_crosswalk_geometry",
    "total_population_crosswalk_vintage",
    "population_density_per_sq_km",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

_RECIPE_METRO_COLUMN_ORDER: list[str] = [
    "metro_id",
    "metro_name",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "profile",
    "profile_definition_version",
    "profile_metro_id",
    "profile_metro_name",
    "profile_rank",
    "acs5_vintage_used",
    "acs1_vintage_used",
    "tract_vintage_used",
    "laus_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "total_population_source",
    "total_population_source_year",
    "total_population_method",
    "total_population_crosswalk_id",
    "total_population_crosswalk_geometry",
    "total_population_crosswalk_vintage",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "unemployment_rate_acs1",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

_RECIPE_MSA_COLUMN_ORDER: list[str] = [
    "msa_id",
    "msa_name",
    "cbsa_code",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "total_population_source",
    "total_population_source_year",
    "total_population_method",
    "total_population_crosswalk_id",
    "total_population_crosswalk_geometry",
    "total_population_crosswalk_vintage",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "coverage_ratio",
    "boundary_changed",
    "source",
]


def _recipe_column_order(
    *,
    geo_type: str,
    include_zori: bool,
    extra_columns: list[str] | None,
) -> list[str]:
    """Return the preferred recipe output column order."""
    if geo_type == "metro":
        columns = list(_RECIPE_METRO_COLUMN_ORDER)
    elif geo_type == "msa":
        columns = list(_RECIPE_MSA_COLUMN_ORDER)
    else:
        columns = list(_RECIPE_COC_COLUMN_ORDER)
    if include_zori:
        columns += ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS
    if extra_columns:
        for col in extra_columns:
            if col not in columns:
                columns.append(col)
    return columns


def _add_recipe_coc_population_density(
    panel: pd.DataFrame,
    *,
    project_root,
) -> pd.DataFrame:
    """Derive CoC population density for recipe-built panels."""
    if panel.empty:
        return panel
    if TOTAL_POPULATION not in panel.columns:
        if POPULATION_DENSITY_COLUMN in panel.columns:
            raise ExecutorError(
                "Cannot derive population_density_per_sq_km because no canonical "
                "total_population column is available. Add a population measure or "
                "set target.panel_policy.canonical_population_source when multiple "
                "population sources are present."
            )
        return panel

    from hhplab.panel.assemble import _add_coc_population_density

    return _add_coc_population_density(
        panel,
        boundaries_dir=project_root / "data" / "curated" / "coc_boundaries",
    )


_POPULATION_LINEAGE_SUFFIXES = tuple(
    column.removeprefix(TOTAL_POPULATION) for column in population_lineage_columns()
)
_POPULATION_LINEAGE_NAMES = {
    suffix.lstrip("_") for suffix in _POPULATION_LINEAGE_SUFFIXES
}
_KNOWN_POPULATION_SOURCE_TOKENS = {source.value for source in PopulationSource}


def _source_specific_population_columns(panel: pd.DataFrame) -> dict[str, str]:
    """Return source token to source-specific total_population column."""
    candidates: dict[str, str] = {}
    prefix = f"{TOTAL_POPULATION}_"
    for column in panel.columns:
        if not column.startswith(prefix):
            continue
        suffix = column.removeprefix(prefix)
        if suffix in _POPULATION_LINEAGE_NAMES:
            continue
        if any(suffix.endswith(lineage_suffix) for lineage_suffix in _POPULATION_LINEAGE_SUFFIXES):
            continue
        if suffix in _KNOWN_POPULATION_SOURCE_TOKENS:
            candidates[suffix] = column
    return candidates


def _copy_source_specific_population_lineage(
    panel: pd.DataFrame,
    *,
    source_column: str,
) -> pd.DataFrame:
    result = panel.copy()
    for suffix in _POPULATION_LINEAGE_SUFFIXES:
        specific_col = f"{source_column}{suffix}"
        canonical_col = f"{TOTAL_POPULATION}{suffix}"
        if specific_col in result.columns:
            result[canonical_col] = result[specific_col]
    return result


def _fill_missing_population_lineage(
    panel: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Attach canonical population lineage when the selected source lacks it."""
    result = panel.copy()
    source_col, year_col, method_col, xwalk_id_col, xwalk_geom_col, xwalk_vintage_col = (
        population_lineage_columns()
    )
    if source_col not in result.columns or result[source_col].isna().all():
        result[source_col] = source
    if method_col not in result.columns or result[method_col].isna().all():
        result[method_col] = PopulationMethod.NATIVE.value
    if year_col not in result.columns or result[year_col].isna().all():
        if source == PopulationSource.ACS5.value and "acs5_vintage_used" in result.columns:
            result[year_col] = result["acs5_vintage_used"].astype("string")
        elif "year" in result.columns:
            result[year_col] = result["year"].astype("string")
        else:
            result[year_col] = pd.NA
    for col in (xwalk_id_col, xwalk_geom_col, xwalk_vintage_col):
        if col not in result.columns:
            result[col] = pd.NA
    return result


def _resolve_canonical_population(
    panel: pd.DataFrame,
    *,
    policy: PanelPolicy | None,
) -> pd.DataFrame:
    """Promote exactly one population estimate to canonical total_population."""
    source_specific = _source_specific_population_columns(panel)
    has_canonical = TOTAL_POPULATION in panel.columns
    if not has_canonical and not source_specific:
        return panel

    selected_source = policy.canonical_population_source if policy else None
    ambiguous_population_source = len(source_specific) > 1 or (
        has_canonical and source_specific
    )
    if selected_source is None and ambiguous_population_source:
        available_sources = sorted(
            {
                *source_specific,
                *(["acs5"] if has_canonical and "acs5_vintage_used" in panel.columns else []),
            }
        )
        raise ExecutorError(
            "Panel contains multiple population sources "
            f"{available_sources} but no canonical source was selected. "
            "Set target.panel_policy.canonical_population_source to one of "
            f"{available_sources}."
        )

    result = panel.copy()
    if selected_source is not None:
        if selected_source in source_specific:
            selected_col = source_specific[selected_source]
            result[TOTAL_POPULATION] = result[selected_col]
            result = _copy_source_specific_population_lineage(
                result,
                source_column=selected_col,
            )
        elif not has_canonical:
            raise ExecutorError(
                "target.panel_policy.canonical_population_source="
                f"'{selected_source}' did not match any available population "
                f"source. Available sources: {sorted(source_specific)}."
            )
    else:
        selected_source = next(iter(source_specific), PopulationSource.ACS5.value)
        if not has_canonical:
            selected_col = source_specific[selected_source]
            result[TOTAL_POPULATION] = result[selected_col]
            result = _copy_source_specific_population_lineage(
                result,
                source_column=selected_col,
            )
        elif "acs5_vintage_used" in result.columns:
            selected_source = PopulationSource.ACS5.value

    return _fill_missing_population_lineage(result, source=selected_source)


def _add_recipe_coc_names(
    panel: pd.DataFrame,
    *,
    project_root,
) -> pd.DataFrame:
    """Backfill CoC names from the curated boundary artifact."""
    if panel.empty:
        return panel
    if "coc_id" not in panel.columns or "boundary_vintage_used" not in panel.columns:
        return panel

    from hhplab.geo.geo_io import read_geoparquet
    from hhplab.panel.assemble import _resolve_boundary_file

    result = panel.copy()
    boundary_vintages = sorted(
        str(v) for v in result["boundary_vintage_used"].dropna().unique().tolist()
    )
    if not boundary_vintages:
        return result

    boundary_frames: list[pd.DataFrame] = []
    for vintage in boundary_vintages:
        boundary_path = _resolve_boundary_file(
            vintage,
            boundaries_dir=project_root / "data" / "curated" / "coc_boundaries",
        )
        if boundary_path is None:
            continue
        gdf = read_geoparquet(boundary_path)
        if "coc_id" not in gdf.columns or "coc_name" not in gdf.columns:
            continue
        boundary_frames.append(
            pd.DataFrame(
                {
                    "coc_id": gdf["coc_id"].astype(str),
                    "boundary_vintage_used": str(vintage),
                    "coc_name_boundary": gdf["coc_name"].astype(str),
                }
            )
        )

    if not boundary_frames:
        return result

    name_lookup = pd.concat(boundary_frames, ignore_index=True).drop_duplicates(
        subset=["coc_id", "boundary_vintage_used"],
        keep="last",
    )
    result = result.merge(
        name_lookup,
        on=["coc_id", "boundary_vintage_used"],
        how="left",
    )
    if "coc_name" in result.columns:
        result["coc_name"] = result["coc_name"].fillna(result["coc_name_boundary"])
    else:
        result["coc_name"] = result["coc_name_boundary"]
    return result.drop(columns=["coc_name_boundary"])


def _add_recipe_metro_metadata(
    panel: pd.DataFrame,
    *,
    project_root,
    target_geometry: GeometryRef,
) -> pd.DataFrame:
    """Backfill metro names and optional subset-profile provenance."""
    if panel.empty:
        return panel
    if "metro_id" not in panel.columns and "geo_id" not in panel.columns:
        return panel
    if (
        target_geometry.source == target_geometry.resolved_metro_subset_definition_version()
        and target_geometry.subset_profile is None
        and target_geometry.subset_profile_definition_version is None
    ):
        return panel

    from hhplab.metro.metro_io import read_metro_subset_membership, read_metro_universe

    result = panel.copy()
    geo_col = "metro_id" if "metro_id" in result.columns else "geo_id"
    data_root = project_root / "data"
    metro_definition_version = target_geometry.resolved_metro_definition_version()
    if metro_definition_version is None:
        return result

    universe_df = read_metro_universe(
        metro_definition_version,
        base_dir=data_root,
    )[["metro_id", "metro_name"]].drop_duplicates(subset=["metro_id"])
    result = result.merge(
        universe_df.rename(columns={"metro_name": "metro_name_universe"}),
        left_on=geo_col,
        right_on="metro_id",
        how="left",
    )
    if "metro_name" in result.columns:
        result["metro_name"] = result["metro_name"].fillna(result["metro_name_universe"])
    else:
        result["metro_name"] = result["metro_name_universe"]
    if "metro_id_x" in result.columns:
        result = result.rename(columns={"metro_id_x": "metro_id"})
    result = result.drop(
        columns=[col for col in ("metro_id_y", "metro_name_universe") if col in result.columns]
    )

    profile_definition_version = target_geometry.resolved_metro_subset_definition_version()
    if profile_definition_version is None:
        return result

    subset_df = read_metro_subset_membership(
        profile_definition_version=profile_definition_version,
        metro_definition_version=metro_definition_version,
        base_dir=data_root,
    ).copy()
    profile_name = target_geometry.resolved_metro_subset_profile()
    if profile_name is not None and "profile" in subset_df.columns:
        subset_df = subset_df[subset_df["profile"].astype(str) == profile_name].copy()

    subset_cols = [
        "metro_id",
        "profile",
        "profile_definition_version",
        "profile_metro_id",
        "profile_metro_name",
        "profile_rank",
    ]
    result = result.merge(
        subset_df[subset_cols].drop_duplicates(subset=["metro_id"]),
        left_on=geo_col,
        right_on="metro_id",
        how="left",
    )
    if "metro_id_x" in result.columns:
        result = result.rename(columns={"metro_id_x": "metro_id"})
    return result.drop(columns=[col for col in ("metro_id_y",) if col in result.columns])


def _add_recipe_msa_metadata(
    panel: pd.DataFrame,
    *,
    project_root,
    target_geometry: GeometryRef,
) -> pd.DataFrame:
    """Backfill MSA names and CBSA codes from curated definitions."""
    if panel.empty:
        return panel
    if "msa_id" not in panel.columns and "geo_id" not in panel.columns:
        return panel
    if target_geometry.source is None:
        return panel

    from hhplab.msa.msa_io import read_msa_definitions

    result = panel.copy()
    geo_col = "msa_id" if "msa_id" in result.columns else "geo_id"
    definitions = read_msa_definitions(
        target_geometry.source,
        base_dir=project_root / "data",
    )
    required = {"msa_id", "msa_name", "cbsa_code"}
    if not required <= set(definitions.columns):
        return result

    lookup = (
        definitions[["msa_id", "msa_name", "cbsa_code"]]
        .assign(msa_id=lambda df: df["msa_id"].astype(str))
        .drop_duplicates(subset=["msa_id"], keep="last")
        .rename(
            columns={
                "msa_name": "msa_name_definition",
                "cbsa_code": "cbsa_code_definition",
            }
        )
    )
    result = result.merge(lookup, left_on=geo_col, right_on="msa_id", how="left")
    if "msa_id_x" in result.columns:
        result = result.rename(columns={"msa_id_x": "msa_id"})

    if "msa_name" in result.columns:
        result["msa_name"] = result["msa_name"].fillna(result["msa_name_definition"])
    else:
        result["msa_name"] = result["msa_name_definition"]
    if "cbsa_code" in result.columns:
        result["cbsa_code"] = result["cbsa_code"].fillna(result["cbsa_code_definition"])
    else:
        result["cbsa_code"] = result["cbsa_code_definition"]

    return result.drop(
        columns=[
            col
            for col in ("msa_id_y", "msa_name_definition", "cbsa_code_definition")
            if col in result.columns
        ]
    )


def _resolve_single_product_value(
    *,
    values: set[str],
    label: str,
    year: int,
) -> str:
    """Return the single product value for a year or raise on conflicts."""
    if not values:
        raise ExecutorError(
            f"Year {year}: no resolved value found for required {label}. "
            "Use a canonical curated filename or include the source vintage "
            f"column so {label} can be derived."
        )
    if len(values) > 1:
        raise ExecutorError(
            f"Year {year}: multiple distinct {label} values contribute to one panel "
            f"slice: {sorted(values)}. Use a single product vintage per year."
        )
    return next(iter(values))


def _stamp_recipe_acs5_provenance(
    panel: pd.DataFrame,
    *,
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> pd.DataFrame:
    """Annotate recipe-built rows with ACS5 and tract vintages when present."""
    if panel.empty or "year" not in panel.columns:
        return panel

    result = panel.copy()
    join_map = {task.year: task.datasets for task in plan.join_tasks}
    resample_map = {(task.dataset_id, task.year): task for task in plan.resample_tasks}

    for year, datasets in join_map.items():
        acs5_vintages: set[str] = set()
        tract_vintages: set[str] = set()
        saw_acs5 = False

        for dataset_id in datasets:
            ds = ctx.recipe.datasets.get(dataset_id)
            if ds is None or ds.provider != "census" or ds.product not in {"acs", "acs5"}:
                continue
            saw_acs5 = True
            metadata = ctx.dataset_year_metadata.get((dataset_id, year), {})
            acs5_vintage = metadata.get("acs5_vintage_used")
            if acs5_vintage is not None:
                acs5_vintages.add(acs5_vintage)
            task = resample_map.get((dataset_id, year))
            if (
                task is not None
                and task.effective_geometry.type == "tract"
                and task.effective_geometry.vintage is not None
            ):
                tract_vintages.add(str(task.effective_geometry.vintage))

        if not saw_acs5:
            continue

        year_mask = result["year"] == year
        result.loc[year_mask, "acs5_vintage_used"] = _resolve_single_product_value(
            values=acs5_vintages,
            label="acs5_vintage_used",
            year=year,
        )
        if tract_vintages:
            result.loc[year_mask, "tract_vintage_used"] = _resolve_single_product_value(
                values=tract_vintages,
                label="tract_vintage_used",
                year=year,
            )

    return result


def assemble_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
    *,
    step_kind: str = "persist",
    appliers: tuple[PanelPolicyApplier, ...] = DEFAULT_APPLIERS,
) -> AssembledPanel | StepResult:
    """Collect joined intermediates, canonicalize, and apply cohort selector.

    Returns an :class:`AssembledPanel` on success or a failed
    :class:`StepResult` on error.  Shared by ``persist_outputs`` and
    ``persist_diagnostics`` in ``executor_persistence`` to avoid
    duplicating panel assembly logic.
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
    panel = canonicalize_panel_for_target(panel, target.geometry)
    try:
        panel = _stamp_recipe_acs5_provenance(panel, plan=plan, ctx=ctx)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    (
        target_geo_type,
        boundary_vintage,
        definition_version,
        _profile_definition_version,
    ) = _target_geometry_metadata(target.geometry)

    # Resolve panel policy for source label and ZORI inclusion.
    policy: PanelPolicy | None = getattr(target, "panel_policy", None)
    source_label = policy.source_label if policy else None
    include_zori = policy is not None and policy.zori is not None
    aliases = resolve_panel_aliases(target)
    extras: list[str] = []
    policy_artifacts: dict[str, PolicyApplication] = {}

    # Apply each policy branch (ZORI → ACS1 → LAUS) through its strategy
    # object.  ``DEFAULT_APPLIERS`` captures the ordering invariant, so
    # adding a new policy is one applier class plus one tuple entry.
    for applier in appliers:
        if not applier.applies_to(target_geo_type=target_geo_type, policy=policy):
            continue
        application = applier.apply(
            panel,
            policy=policy,  # type: ignore[arg-type]
            target_geo_type=target_geo_type,
        )
        panel = application.panel
        extras.extend(application.extra_columns)
        policy_artifacts[applier.name] = application
        for note in application.notes:
            _echo(ctx, f"  [{applier.name}] {note}")

    try:
        panel = _resolve_canonical_population(panel, policy=policy)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    if target_geo_type == "coc":
        try:
            panel = _add_recipe_coc_names(
                panel,
                project_root=ctx.project_root,
            )
            panel = _add_recipe_coc_population_density(
                panel,
                project_root=ctx.project_root,
            )
        except ExecutorError as exc:
            return StepResult(
                step_kind=step_kind,
                detail=f"{step_kind}",
                success=False,
                error=str(exc),
            )
    elif target_geo_type == "metro":
        panel = _add_recipe_metro_metadata(
            panel,
            project_root=ctx.project_root,
            target_geometry=target.geometry,
        )
    elif target_geo_type == "msa":
        panel = _add_recipe_msa_metadata(
            panel,
            project_root=ctx.project_root,
            target_geometry=target.geometry,
        )

    # Shared finalization: boundary detection, column ordering, dtypes,
    # source labeling, and column aliases.
    panel = finalize_panel(
        panel,
        geo_type=target_geo_type,
        include_zori=include_zori,
        source_label=source_label,
        column_aliases=aliases,
        extra_columns=extras or None,
        canonical_columns=_recipe_column_order(
            geo_type=target_geo_type,
            include_zori=include_zori,
            extra_columns=extras or None,
        ),
        ensure_canonical_columns=False,
    )

    if target.cohort is not None:
        pre_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        panel = apply_cohort_selector(panel, target.cohort)
        post_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        _echo(
            ctx,
            f"  [cohort] {target.cohort.method} rank_by={target.cohort.rank_by} "
            f"ref_year={target.cohort.reference_year}: "
            f"{pre_count} → {post_count} geographies",
        )

    return AssembledPanel(
        panel=panel,
        frames=frames,
        target=target,
        target_geo_type=target_geo_type,
        boundary_vintage=boundary_vintage,
        definition_version=definition_version,
        policy_artifacts=policy_artifacts,
    )
