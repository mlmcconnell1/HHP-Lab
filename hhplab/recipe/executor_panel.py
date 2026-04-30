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
    GeometryRef,
    PanelPolicy,
    expand_year_spec,
)


def canonicalize_panel_for_target(
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
                from hhplab.metro.definitions import metro_name_for_id

                result["metro_name"] = result["metro_id"].map(metro_name_for_id)
            if (
                definition_version is not None
                and "definition_version_used" not in result.columns
            ):
                result["definition_version_used"] = definition_version
        if geo_type == "msa":
            if "msa_id" not in result.columns:
                result["msa_id"] = result["geo_id"]
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
    "acs5_vintage_used",
    "acs1_vintage_used",
    "tract_vintage_used",
    "laus_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
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

    target_geo_type, boundary_vintage, definition_version = _target_geometry_metadata(
        target.geometry,
    )

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
