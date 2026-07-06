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
from pathlib import Path

import numpy as np
import pandas as pd

import hhplab.naming as naming
from hhplab.config import load_config
from hhplab.msa.coverage import select_primary_msa_for_cocs
from hhplab.msa.crosswalk import read_coc_msa_crosswalk
from hhplab.msa.msa_io import read_msa_county_membership, read_msa_definitions
from hhplab.naming import (
    acs5_tracts_filename,
    decennial_tracts_filename,
    msa_coc_xwalk_path,
    msa_county_membership_path,
    msa_definitions_path,
    tract_xwalk_path,
)
from hhplab.panel.finalize import (
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    finalize_panel,
)
from hhplab.recipe.executor.core import (
    ExecutionContext,
    ExecutorError,
    StepResult,
    _classify_path,
    _echo,
)
from hhplab.recipe.executor.manifest import (
    _resolve_pipeline_target,
    _target_geometry_metadata,
)
from hhplab.recipe.executor.panel_policies import (
    DEFAULT_APPLIERS,
    PanelPolicyApplier,
    PolicyApplication,
)
from hhplab.recipe.manifest import AssetRecord
from hhplab.recipe.planner import ExecutionPlan
from hhplab.recipe.recipe_schema import (
    CohortSelector,
    DerivedMeasureSpec,
    InflationAdjustmentPolicy,
    PanelPolicy,
)
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec
from hhplab.schema.columns import (
    POPULATION_DENSITY_COLUMN,
    TOTAL_POPULATION,
)
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


def _recipe_uses_hic(recipe) -> bool:
    """Whether any recipe dataset declares HUD HIC inputs."""
    return any(
        getattr(ds, "provider", None) == "hud" and getattr(ds, "product", None) == "hic"
        for ds in recipe.datasets.values()
    )


def _normalize_recipe_hic_columns(panel: pd.DataFrame, *, recipe) -> pd.DataFrame:
    """Rename canonical HIC source columns to stable panel measure names."""
    if not _recipe_uses_hic(recipe):
        return panel
    renames = {"total_beds": "hic_total_beds", "total_units": "hic_total_units"}
    active = {
        source: target
        for source, target in renames.items()
        if source in panel.columns and target not in panel.columns
    }
    if not active:
        return panel
    return panel.rename(columns=active)


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
    filtered, _summary = apply_cohort_selector_with_summary(
        panel,
        cohort,
        geo_id_col=geo_id_col,
        year_col=year_col,
    )
    return filtered


def apply_cohort_selector_with_summary(
    panel: pd.DataFrame,
    cohort: CohortSelector,
    geo_id_col: str = "geo_id",
    year_col: str = "year",
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Filter panel to cohort geographies and return provenance details."""
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
    elif cohort.method == "predicate":
        if cohort.operator == "gte":
            mask = ranked[cohort.rank_by] >= cohort.value
        elif cohort.operator == "lte":
            mask = ranked[cohort.rank_by] <= cohort.value
        elif cohort.operator == "gt":
            mask = ranked[cohort.rank_by] > cohort.value
        elif cohort.operator == "lt":
            mask = ranked[cohort.rank_by] < cohort.value
        elif cohort.operator == "eq":
            mask = ranked[cohort.rank_by] == cohort.value
        else:
            raise ExecutorError(f"Unknown cohort predicate operator: {cohort.operator}")
        selected = ranked.loc[mask, geo_id_col]
    else:
        raise ExecutorError(f"Unknown cohort method: {cohort.method}")

    selected_ids = sorted(selected.astype(str).unique().tolist())
    filtered = panel[panel[geo_id_col].astype(str).isin(selected_ids)].reset_index(drop=True)
    summary = {
        "config": cohort.model_dump(mode="json", exclude_none=True),
        "selected_geo_count": len(selected_ids),
        "selected_geo_ids": selected_ids,
        "row_count_before": int(len(panel)),
        "row_count_after": int(len(filtered)),
    }
    return filtered, summary


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
    cohort_summary: dict[str, object] | None = None
    inflation_summary: dict[str, object] | None = None
    derived_measures_summary: dict[str, object] | None = None

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
    "rent_burden_40_plus",
    "rent_burden_50_plus",
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
    urban_fraction_population_col = "coc_total_population"
    population_columns = (
        (TOTAL_POPULATION, urban_fraction_population_col)
        if urban_fraction_population_col in panel.columns
        else (TOTAL_POPULATION,)
    )
    if not any(column in panel.columns for column in population_columns):
        if POPULATION_DENSITY_COLUMN in panel.columns:
            raise ExecutorError(
                "Cannot derive population_density_per_sq_km because no canonical "
                "population column is available. Add total_population, "
                "coc_total_population, or set target.panel_policy."
                "canonical_population_source when multiple population sources are present."
            )
        return panel

    from hhplab.panel.assemble import _add_coc_population_density

    return _add_coc_population_density(
        panel,
        boundaries_dir=project_root / "data" / "curated" / "coc_boundaries",
        population_columns=population_columns,
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


def _project_panel_output(panel: pd.DataFrame, policy: PanelPolicy | None) -> pd.DataFrame:
    """Apply an explicit final panel projection declared by the recipe."""
    if policy is None or policy.output_columns is None:
        return panel
    missing = [column for column in policy.output_columns if column not in panel.columns]
    if missing:
        raise ExecutorError(
            "Target panel_policy.output_columns references missing columns "
            f"{missing}. Available columns: {sorted(panel.columns.tolist())}"
        )
    return panel.loc[:, policy.output_columns].copy()


def _resolve_inflation_cpi_path(
    policy: InflationAdjustmentPolicy,
    ctx: ExecutionContext,
) -> Path:
    if policy.cpi_path is not None:
        return ctx.project_root / policy.cpi_path
    if policy.cpi_dataset is not None:
        ds = ctx.recipe.datasets.get(policy.cpi_dataset)
        if ds is None:
            raise ExecutorError(
                "target.panel_policy.inflation_adjustment references unknown "
                f"cpi_dataset '{policy.cpi_dataset}'."
            )
        if ds.path is None:
            raise ExecutorError(
                "target.panel_policy.inflation_adjustment cpi_dataset "
                f"'{policy.cpi_dataset}' must declare a path to a CPI-U artifact."
            )
        return ctx.project_root / ds.path
    return naming.cpi_u_path(base_dir=ctx.project_root / "data")


def _load_inflation_index(
    policy: InflationAdjustmentPolicy,
    ctx: ExecutionContext,
) -> tuple[pd.Series, Path]:
    cpi_path = _resolve_inflation_cpi_path(policy, ctx)
    if not cpi_path.exists():
        raise ExecutorError(
            "CPI-U artifact not found for target.panel_policy.inflation_adjustment: "
            f"{cpi_path}. Run `hhplab ingest cpi-u --start-year START --end-year END` "
            "or set cpi_path/cpi_dataset to an existing CPI-U parquet file."
        )

    cpi = pd.read_parquet(cpi_path)
    required = [policy.cpi_year_column, policy.cpi_value_column]
    missing = [column for column in required if column not in cpi.columns]
    if missing:
        raise ExecutorError(
            f"CPI-U artifact {cpi_path} is missing required columns {missing}. "
            f"Available columns: {sorted(cpi.columns.tolist())}"
        )

    index = cpi[[policy.cpi_year_column, policy.cpi_value_column]].copy()
    index[policy.cpi_year_column] = pd.to_numeric(index[policy.cpi_year_column], errors="coerce")
    index[policy.cpi_value_column] = pd.to_numeric(index[policy.cpi_value_column], errors="coerce")
    index = index.dropna(subset=[policy.cpi_year_column, policy.cpi_value_column])
    index[policy.cpi_year_column] = index[policy.cpi_year_column].astype(int)
    duplicated = index[index[policy.cpi_year_column].duplicated()][policy.cpi_year_column]
    if not duplicated.empty:
        raise ExecutorError(
            f"CPI-U artifact {cpi_path} contains duplicate year rows: "
            f"{sorted(duplicated.astype(int).unique().tolist())}."
        )
    series = index.set_index(policy.cpi_year_column)[policy.cpi_value_column]
    if policy.base_year not in series.index:
        raise ExecutorError(
            f"CPI-U artifact {cpi_path} does not contain base_year {policy.base_year}. "
            "Ingest a range that includes the requested base year."
        )
    return series, cpi_path


ACS5_INFLATION_YEAR_COLUMNS: frozenset[str] = frozenset(
    {
        "median_household_income",
        "median_gross_rent",
        "msa_income",
        "msa_median_rent",
    }
)


def _inflation_year_column_for(
    column: str,
    *,
    adjustment: InflationAdjustmentPolicy,
    panel: pd.DataFrame,
) -> str:
    override = adjustment.column_year_columns.get(column)
    if override is not None:
        return override
    if column in ACS5_INFLATION_YEAR_COLUMNS and "acs5_vintage_used" in panel.columns:
        return "acs5_vintage_used"
    return adjustment.year_column


def _apply_inflation_adjustment(
    panel: pd.DataFrame,
    *,
    policy: PanelPolicy | None,
    ctx: ExecutionContext,
) -> tuple[pd.DataFrame, dict[str, object] | None]:
    if policy is None or policy.inflation_adjustment is None:
        return panel, None

    adjustment = policy.inflation_adjustment
    column_year_columns = {
        column: _inflation_year_column_for(column, adjustment=adjustment, panel=panel)
        for column in adjustment.columns
    }
    inflation_columns = [*column_year_columns.values(), *adjustment.columns]
    missing = [column for column in inflation_columns if column not in panel.columns]
    if missing:
        raise ExecutorError(
            "target.panel_policy.inflation_adjustment references missing panel "
            f"columns {missing}. Available columns: {sorted(panel.columns.tolist())}"
        )

    cpi, cpi_path = _load_inflation_index(adjustment, ctx)

    result = panel.copy()
    base_cpi = float(cpi.loc[adjustment.base_year])
    factor_year_column: str | None = None
    if adjustment.factor_column is not None:
        distinct_year_columns = set(column_year_columns.values())
        if len(distinct_year_columns) != 1:
            raise ExecutorError(
                "target.panel_policy.inflation_adjustment.factor_column is ambiguous "
                "because adjusted columns use multiple value-year columns. Use separate "
                "policies or omit factor_column."
            )
        [factor_year_column] = list(distinct_year_columns)

    suffix = adjustment.output_suffix.format(base_year=adjustment.base_year)
    output_columns: list[str] = []
    missing_cpi_years_by_column: dict[str, list[int]] = {}
    factor_values: pd.Series | None = None
    for column in adjustment.columns:
        year_column = column_year_columns[column]
        year_values = pd.to_numeric(panel[year_column], errors="coerce")
        missing_year_rows = panel[year_values.isna()]
        if not missing_year_rows.empty:
            raise ExecutorError(
                f"Inflation adjustment year column '{year_column}' for '{column}' "
                f"contains {len(missing_year_rows)} non-numeric row(s)."
            )
        years = year_values.astype(int)
        missing_cpi_years = sorted(set(years.unique().tolist()) - set(cpi.index.astype(int)))
        if missing_cpi_years:
            missing_cpi_years_by_column[column] = missing_cpi_years
            continue
        factors = years.map(lambda year: base_cpi / float(cpi.loc[int(year)]))
        output_column = f"{column}{suffix}"
        if output_column in result.columns and output_column not in adjustment.columns:
            raise ExecutorError(
                "Inflation adjustment output column collision: "
                f"'{output_column}' already exists."
            )
        result[output_column] = pd.to_numeric(result[column], errors="coerce") * factors
        output_columns.append(output_column)
        if factor_year_column == year_column:
            factor_values = factors.astype(float)
    if missing_cpi_years_by_column:
        raise ExecutorError(
            f"CPI-U artifact {cpi_path} is missing panel year/value year(s) "
            "by adjusted column: "
            f"{missing_cpi_years_by_column}. Ingest CPI-U for the full recipe universe "
            "or provide a complete cpi_path."
        )
    if adjustment.factor_column is not None and factor_values is not None:
        result[adjustment.factor_column] = factor_values

    _record_panel_policy_asset(ctx, cpi_path, role="inflation_adjustment_cpi")
    summary = {
        "base_year": adjustment.base_year,
        "columns": list(adjustment.columns),
        "column_year_columns": column_year_columns,
        "output_columns": output_columns,
        "factor_column": adjustment.factor_column,
        "cpi_path": str(cpi_path),
        "cpi_year_column": adjustment.cpi_year_column,
        "cpi_value_column": adjustment.cpi_value_column,
        "base_cpi": base_cpi,
    }
    return result, summary


def _numeric_panel_column(
    panel: pd.DataFrame,
    column: str,
    *,
    policy_path: str,
) -> pd.Series:
    if column not in panel.columns:
        raise ExecutorError(
            f"{policy_path} references missing panel column '{column}'. "
            f"Available columns: {sorted(panel.columns.tolist())}"
        )
    return pd.to_numeric(panel[column], errors="coerce")


def _safe_ratio(numerator: pd.Series, denominator: pd.Series, scale: float) -> pd.Series:
    safe_denominator = denominator.where(denominator != 0)
    return numerator / safe_denominator * scale


def _apply_lagged_derived_measure(
    panel: pd.DataFrame,
    *,
    spec: DerivedMeasureSpec,
    values: pd.Series,
) -> pd.Series:
    missing = [
        column
        for column in [*spec.group_by, spec.order_by]
        if column not in panel.columns
    ]
    if missing:
        raise ExecutorError(
            "target.panel_policy.derived_measures references missing lag/difference "
            f"grouping columns {missing}. Available columns: {sorted(panel.columns.tolist())}"
        )

    work = panel[[*spec.group_by, spec.order_by]].copy()
    work["__value"] = values
    work["__original_index"] = panel.index
    work = work.sort_values([*spec.group_by, spec.order_by, "__original_index"])
    grouped = work.groupby(spec.group_by, dropna=False)
    shift_periods = -spec.periods if spec.type == "lead" else spec.periods
    shifted = grouped["__value"].shift(shift_periods)
    shifted_order = grouped[spec.order_by].shift(shift_periods)
    order_values = pd.to_numeric(work[spec.order_by], errors="coerce")
    paired_order_values = pd.to_numeric(shifted_order, errors="coerce")
    if spec.type == "lead":
        continuous = (paired_order_values - order_values) == spec.periods
    else:
        continuous = (order_values - paired_order_values) == spec.periods
    shifted = shifted.where(continuous)
    if spec.type in {"lag", "lead"}:
        derived = shifted
    else:
        derived = work["__value"] - shifted
    return derived.reindex(work["__original_index"]).sort_index()


def _apply_single_derived_measure(
    panel: pd.DataFrame,
    spec: DerivedMeasureSpec,
) -> pd.Series:
    policy_path = (
        f"target.panel_policy.derived_measures[{spec.output_column}]"
    )
    if spec.type in {"ratio", "per_capita", "per_10k"}:
        assert spec.numerator is not None
        assert spec.denominator is not None
        numerator = _numeric_panel_column(panel, spec.numerator, policy_path=policy_path)
        denominator = _numeric_panel_column(panel, spec.denominator, policy_path=policy_path)
        default_scale = {"ratio": 1.0, "per_capita": 1.0, "per_10k": 10_000.0}[spec.type]
        return _safe_ratio(numerator, denominator, spec.scale or default_scale)

    assert spec.column is not None
    values = _numeric_panel_column(panel, spec.column, policy_path=policy_path)
    if spec.type == "log":
        positive_values = values.where(values > 0)
        if spec.log_base == "10":
            return np.log10(positive_values)
        return np.log(positive_values)
    if spec.type in {"lag", "lead", "difference"}:
        return _apply_lagged_derived_measure(panel, spec=spec, values=values)
    raise ExecutorError(f"Unsupported derived measure type: {spec.type}")


def _apply_derived_measures(
    panel: pd.DataFrame,
    *,
    policy: PanelPolicy | None,
) -> tuple[pd.DataFrame, dict[str, object] | None]:
    if policy is None or not policy.derived_measures:
        return panel, None

    result = panel.copy()
    summaries: list[dict[str, object]] = []
    for spec in policy.derived_measures:
        if spec.output_column in result.columns:
            raise ExecutorError(
                "target.panel_policy.derived_measures output column collision: "
                f"'{spec.output_column}' already exists."
            )
        result[spec.output_column] = _apply_single_derived_measure(result, spec)
        summaries.append(spec.model_dump(mode="json", exclude_none=True))

    return result, {
        "count": len(summaries),
        "measures": summaries,
    }


def _record_panel_policy_asset(
    ctx: ExecutionContext,
    path,
    *,
    role: str,
) -> None:
    identity = ctx.cache.file_identity(path)
    root, rel = _classify_path(path, ctx)
    ctx.consumed_assets.append(
        AssetRecord(
            role=role,
            path=rel,
            sha256=identity.sha256,
            size=identity.size,
            root=root,
        )
    )


def _primary_msa_population_reference_year(primary_policy) -> int:
    if primary_policy.population_source == "decennial":
        return int(primary_policy.decennial_population_vintage)
    return int(
        primary_policy.acs5_population_reference_year
        or primary_policy.acs5_population_vintage
    )


def _primary_msa_population_path(data_root, primary_policy):
    if primary_policy.population_source == "decennial":
        if primary_policy.decennial_population_vintage is None:
            raise ExecutorError(
                "target.panel_policy.primary_msa overlap_basis='population' with "
                "population_source='decennial' requires decennial_population_vintage."
            )
        return (
            data_root
            / "curated"
            / "census"
            / decennial_tracts_filename(
                primary_policy.decennial_population_vintage,
                primary_policy.tract_vintage,
            )
        )
    if primary_policy.acs5_population_vintage is None:
        raise ExecutorError(
            "target.panel_policy.primary_msa overlap_basis='population' with "
            "population_source='acs5' requires acs5_population_vintage."
        )
    return (
        data_root
        / "curated"
        / "acs"
        / acs5_tracts_filename(
            str(primary_policy.acs5_population_vintage),
            primary_policy.tract_vintage,
        )
    )


def _read_primary_msa_population_overlap(
    *,
    ctx: ExecutionContext,
    data_root,
    boundary_vintage: str,
    primary_policy,
    coc_ids: tuple[str, ...],
) -> tuple[pd.DataFrame, list]:
    if primary_policy.tract_vintage is None:
        raise ExecutorError(
            "target.panel_policy.primary_msa overlap_basis='population' requires "
            "tract_vintage."
        )

    membership_file = msa_county_membership_path(primary_policy.definition_version, data_root)
    definitions_file = msa_definitions_path(primary_policy.definition_version, data_root)
    xwalk_file = tract_xwalk_path(boundary_vintage, primary_policy.tract_vintage, data_root)
    population_file = _primary_msa_population_path(data_root, primary_policy)
    artifacts = [xwalk_file, membership_file, definitions_file, population_file]
    missing = [path for path in artifacts if not path.exists()]
    if missing:
        rel_missing = [str(path.relative_to(ctx.project_root)) for path in missing]
        raise ExecutorError(
            "target.panel_policy.primary_msa overlap_basis='population' requires "
            f"population-overlap artifacts. Missing: {rel_missing}."
        )

    membership = read_msa_county_membership(primary_policy.definition_version, base_dir=data_root)
    membership = membership[["msa_id", "county_fips"]].copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)

    xwalk = pd.read_parquet(xwalk_file)
    xwalk = xwalk[xwalk["coc_id"].astype(str).isin(coc_ids)].copy()
    xwalk["coc_id"] = xwalk["coc_id"].astype(str)
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype(str).str.zfill(11)
    xwalk["county_fips"] = xwalk["tract_geoid"].str[:5]
    xwalk["area_share"] = pd.to_numeric(xwalk["area_share"], errors="coerce").fillna(0.0)

    population = pd.read_parquet(population_file)
    tract_col = next(
        (column for column in ("tract_geoid", "GEOID", "geoid") if column in population.columns),
        None,
    )
    if tract_col is None or "total_population" not in population.columns:
        raise ExecutorError(
            "target.panel_policy.primary_msa overlap_basis='population' requires "
            "tract population rows with tract_geoid/GEOID/geoid and total_population."
        )
    population = population.copy()
    if primary_policy.population_source == "acs5" and "year" in population.columns:
        population = population[
            population["year"].astype(str) == str(primary_policy.acs5_population_vintage)
        ].copy()
    population["tract_geoid"] = population[tract_col].astype(str).str.zfill(11)
    population["total_population"] = pd.to_numeric(
        population["total_population"],
        errors="coerce",
    ).fillna(0.0)
    population = population[["tract_geoid", "total_population"]].drop_duplicates("tract_geoid")

    allocated = xwalk.merge(population, on="tract_geoid", how="left")
    allocated["total_population"] = allocated["total_population"].fillna(0.0)
    allocated["allocated_population"] = allocated["total_population"] * allocated["area_share"]
    pair_allocated = allocated.merge(membership, on="county_fips", how="inner")
    if pair_allocated.empty:
        return pd.DataFrame(columns=["coc_id", "msa_id", "overlap_basis"]), artifacts

    pair = (
        pair_allocated.groupby(["coc_id", "msa_id"], as_index=False)["allocated_population"]
        .sum()
        .rename(columns={"allocated_population": "intersection_value"})
    )
    coc_denominator = (
        allocated.groupby("coc_id", as_index=False)["allocated_population"]
        .sum()
        .rename(columns={"allocated_population": "coc_denominator"})
    )
    msa_population = population.copy()
    msa_population["county_fips"] = msa_population["tract_geoid"].str[:5]
    msa_denominator = (
        msa_population.merge(membership, on="county_fips", how="inner")
        .groupby("msa_id", as_index=False)["total_population"]
        .sum()
        .rename(columns={"total_population": "msa_denominator"})
    )
    coverage = pair.merge(coc_denominator, on="coc_id", how="left").merge(
        msa_denominator,
        on="msa_id",
        how="left",
    )
    coverage["overlap_basis"] = "population"
    coverage["coc_contained_in_msa_percent"] = (
        coverage["intersection_value"]
        / coverage["coc_denominator"].where(coverage["coc_denominator"] > 0)
        * 100.0
    )
    coverage["msa_covered_by_coc_percent"] = (
        coverage["intersection_value"]
        / coverage["msa_denominator"].where(coverage["msa_denominator"] > 0)
        * 100.0
    )
    definitions = read_msa_definitions(primary_policy.definition_version, base_dir=data_root)
    if {"msa_id", "msa_name"} <= set(definitions.columns):
        names = definitions[["msa_id", "msa_name"]].copy()
        names["msa_id"] = names["msa_id"].astype(str).str.zfill(5)
        coverage = coverage.merge(
            names.drop_duplicates("msa_id"),
            on="msa_id",
            how="left",
        )
    return coverage, artifacts


def _add_primary_msa_annotations(
    panel: pd.DataFrame,
    *,
    ctx: ExecutionContext,
    policy: PanelPolicy | None,
    boundary_vintage: str | None,
) -> tuple[pd.DataFrame, list[str]]:
    """Apply ``panel_policy.primary_msa`` to a CoC panel."""
    primary_policy = policy.primary_msa if policy is not None else None
    if primary_policy is None:
        return panel, []
    if boundary_vintage is None:
        raise ExecutorError(
            "target.panel_policy.primary_msa requires the CoC target geometry to declare "
            "a boundary vintage."
        )
    if "coc_id" not in panel.columns:
        raise ExecutorError("target.panel_policy.primary_msa requires a panel with 'coc_id'.")

    cfg = ctx.storage_config or load_config(project_root=ctx.project_root)
    data_root = cfg.asset_store_root
    xwalk_file = msa_coc_xwalk_path(
        str(boundary_vintage),
        primary_policy.definition_version,
        primary_policy.county_vintage,
        data_root,
    )
    definitions_file = msa_definitions_path(primary_policy.definition_version, data_root)
    if primary_policy.overlap_basis == "population":
        coc_ids = tuple(panel["coc_id"].dropna().astype(str).unique())
        overlap, consumed_assets = _read_primary_msa_population_overlap(
            ctx=ctx,
            data_root=data_root,
            boundary_vintage=str(boundary_vintage),
            primary_policy=primary_policy,
            coc_ids=coc_ids,
        )
    else:
        missing = [path for path in (xwalk_file, definitions_file) if not path.exists()]
        if missing:
            rel_missing = [str(path.relative_to(ctx.project_root)) for path in missing]
            raise ExecutorError(
                "target.panel_policy.primary_msa requires MSA-CoC overlap artifacts. "
                f"Missing: {rel_missing}. Run `hhplab generate msa-xwalk "
                f"--boundaries {boundary_vintage} --counties {primary_policy.county_vintage}` "
                "and `hhplab generate msa` for the requested definition version."
            )

        overlap = read_coc_msa_crosswalk(
            str(boundary_vintage),
            primary_policy.definition_version,
            str(primary_policy.county_vintage),
            base_dir=data_root,
        )
        definitions = read_msa_definitions(primary_policy.definition_version, base_dir=data_root)
        if {"msa_id", "msa_name"} <= set(definitions.columns):
            overlap = overlap.merge(
                definitions[["msa_id", "msa_name"]].drop_duplicates("msa_id"),
                on="msa_id",
                how="left",
            )
        consumed_assets = [xwalk_file, definitions_file]

    annotations = select_primary_msa_for_cocs(
        overlap,
        coc_ids=tuple(panel["coc_id"].dropna().astype(str).unique()),
        overlap_basis=primary_policy.overlap_basis,
        min_coc_contained_share=primary_policy.min_coc_contained_share,
    )
    columns = primary_policy.output_columns
    annotations = annotations.rename(
        columns={
            "primary_msa_id": columns.msa_id,
            "primary_msa_name": columns.msa_name,
            "primary_msa_population": columns.population,
            "primary_msa_overlap_basis": columns.overlap_basis,
            "primary_msa_coc_contained_percent": columns.contained_share,
        }
    )

    result = panel.merge(annotations, on="coc_id", how="left")
    for asset in consumed_assets:
        _record_panel_policy_asset(ctx, asset, role="primary_msa")
    extras = [
        columns.msa_id,
        columns.msa_name,
        columns.population,
        columns.overlap_basis,
        columns.contained_share,
        "primary_msa_covered_by_coc_percent",
    ]
    return result, extras


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
    panel = _normalize_recipe_hic_columns(panel, recipe=ctx.recipe)
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

    cohort_summary: dict[str, object] | None = None
    inflation_summary: dict[str, object] | None = None
    derived_measures_summary: dict[str, object] | None = None
    if target.cohort is not None:
        pre_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        panel, cohort_summary = apply_cohort_selector_with_summary(panel, target.cohort)
        post_count = panel["geo_id"].nunique() if "geo_id" in panel.columns else len(panel)
        _echo(
            ctx,
            f"  [cohort] {target.cohort.method} rank_by={target.cohort.rank_by} "
            f"ref_year={target.cohort.reference_year}: "
            f"{pre_count} → {post_count} geographies",
        )

    if target_geo_type == "coc":
        try:
            panel, _primary_msa_extras = _add_primary_msa_annotations(
                panel,
                ctx=ctx,
                policy=policy,
                boundary_vintage=boundary_vintage,
            )
        except ExecutorError as exc:
            return StepResult(
                step_kind=step_kind,
                detail=f"{step_kind}",
                success=False,
                error=str(exc),
            )

    try:
        panel, inflation_summary = _apply_inflation_adjustment(panel, policy=policy, ctx=ctx)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    try:
        panel, derived_measures_summary = _apply_derived_measures(panel, policy=policy)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    try:
        panel = _project_panel_output(panel, policy)
    except ExecutorError as exc:
        return StepResult(
            step_kind=step_kind,
            detail=f"{step_kind}",
            success=False,
            error=str(exc),
        )

    return AssembledPanel(
        panel=panel,
        frames=frames,
        target=target,
        target_geo_type=target_geo_type,
        boundary_vintage=boundary_vintage,
        definition_version=definition_version,
        policy_artifacts=policy_artifacts,
        cohort_summary=cohort_summary,
        inflation_summary=inflation_summary,
        derived_measures_summary=derived_measures_summary,
    )
