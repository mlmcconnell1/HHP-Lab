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

import numpy as np
import pandas as pd

from coclab.panel.finalize import finalize_panel
from coclab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    StepResult,
    _echo,
)
from coclab.recipe.executor_manifest import (
    _resolve_pipeline_target,
    _target_geometry_metadata,
)
from coclab.recipe.executor_panel_policies import (
    PolicyApplication,
    ZoriPolicyApplier,
)
from coclab.recipe.planner import ExecutionPlan
from coclab.recipe.recipe_schema import (
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


# Module-level applier registry.  Ordering matters: ZORI renames/cleans
# columns that the ACS1 and LAUS appliers (added in step 6) later inspect.
_ZORI_APPLIER = ZoriPolicyApplier()


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


def assemble_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
    *,
    step_kind: str = "persist",
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

    # -----------------------------------------------------------------
    # ZORI eligibility, rent_to_income, and provenance (coclab-gude.2)
    # -----------------------------------------------------------------
    if _ZORI_APPLIER.applies_to(target_geo_type=target_geo_type, policy=policy):
        application = _ZORI_APPLIER.apply(
            panel,
            policy=policy,  # type: ignore[arg-type]
            target_geo_type=target_geo_type,
        )
        panel = application.panel
        extras.extend(application.extra_columns)
        policy_artifacts[_ZORI_APPLIER.name] = application

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
        extra_columns=extras or None,
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
