"""Run tracked robustness checks for composition-rent-population screens.

This script consumes the panel artifacts created by the composition builder
scripts and reproduces the follow-up fixed-effect checks documented in
devdocs/composition_rent_population_findings.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.results.workflows.build_household_size_composition_panel import OUT

FD_INPUTS = {
    "renter_household_share": OUT / "renter_household_share_composition_fd.parquet",
    "local_income": OUT / "local_income_composition_fd.parquet",
    "employment_labor_force": OUT / "employment_labor_force_composition_fd.parquet",
    "income_inequality": OUT / "income_inequality_composition_fd.parquet",
}
LEVEL_INPUTS = {
    "renter_household_share": OUT / "renter_household_share_composition_levels.parquet",
    "rent_levels_bridge": OUT / "renter_household_share_composition_levels.parquet",
    "household_size": OUT / "household_size_composition_levels.parquet",
    "recent_mover_income": OUT / "recent_mover_income_composition_levels.parquet",
    "local_income": OUT / "local_income_composition_levels.parquet",
    "employment_labor_force": OUT / "employment_labor_force_composition_levels.parquet",
    "income_inequality": OUT / "income_inequality_composition_levels.parquet",
}

ROBUSTNESS_PARQUET = OUT / "composition_rent_population_robustness_regressions.parquet"
ROBUSTNESS_CSV = OUT / "composition_rent_population_robustness_regressions.csv"
ROBUSTNESS_SUMMARY = OUT / "composition_rent_population_robustness_summary.json"


@dataclass(frozen=True)
class DerivedColumnSpec:
    name: str
    kind: str
    source_columns: tuple[str, ...]


@dataclass(frozen=True)
class RegressionSpec:
    family: str
    model: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    sample_filter: str
    derived_columns: tuple[DerivedColumnSpec, ...] = ()
    focal_terms: tuple[str, ...] = ()


RENTER_SHARE_INTERACTION_COLUMNS = (
    DerivedColumnSpec(
        name="renter_household_share_c",
        kind="center",
        source_columns=("renter_household_share",),
    ),
    DerivedColumnSpec(
        name="d_log_zori_x_renter_share_c",
        kind="interaction",
        source_columns=("d_log_zori", "renter_household_share_c"),
    ),
)


FD_RENTER_SHARE_SPECS = (
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=("d_renter_household_share",),
        fixed_effects=("year",),
        sample_filter="fd_unsheltered_direct_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_state_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=("d_renter_household_share",),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_unsheltered_direct_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_region_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=("d_renter_household_share",),
        fixed_effects=("region_year",),
        sample_filter="fd_unsheltered_direct_year_gap_1",
        focal_terms=("d_renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_interaction_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=(
            "d_log_zori",
            "d_log_pop",
            "renter_household_share_c",
            "d_log_zori_x_renter_share_c",
        ),
        fixed_effects=("year",),
        sample_filter="fd_unsheltered_interaction_year_gap_1",
        derived_columns=RENTER_SHARE_INTERACTION_COLUMNS,
        focal_terms=("d_log_zori", "renter_household_share_c", "d_log_zori_x_renter_share_c"),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_interaction_state_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=(
            "d_log_zori",
            "d_log_pop",
            "renter_household_share_c",
            "d_log_zori_x_renter_share_c",
        ),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_unsheltered_interaction_year_gap_1",
        derived_columns=RENTER_SHARE_INTERACTION_COLUMNS,
        focal_terms=("d_log_zori", "renter_household_share_c", "d_log_zori_x_renter_share_c"),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_fd_renter_household_share_interaction_region_year_fe",
        outcome="d_log_unshelt_rate",
        predictors=(
            "d_log_zori",
            "d_log_pop",
            "renter_household_share_c",
            "d_log_zori_x_renter_share_c",
        ),
        fixed_effects=("region_year",),
        sample_filter="fd_unsheltered_interaction_year_gap_1",
        derived_columns=RENTER_SHARE_INTERACTION_COLUMNS,
        focal_terms=("d_log_zori", "renter_household_share_c", "d_log_zori_x_renter_share_c"),
    ),
    RegressionSpec(
        family="household_formation",
        model="rent_fd_household_formation_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_total_households_per_panel_person"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_total_households_per_panel_person",),
    ),
    RegressionSpec(
        family="household_formation",
        model="rent_fd_household_formation_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_total_households_per_panel_person"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_total_households_per_panel_person",),
    ),
    RegressionSpec(
        family="household_formation",
        model="rent_fd_household_formation_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_total_households_per_panel_person"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_total_households_per_panel_person",),
    ),
)

FD_LOCAL_INCOME_SPECS = (
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_by_tenure_total_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_by_tenure_total"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_by_tenure_total_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_by_tenure_total"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_by_tenure_total_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_by_tenure_total"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_renter_occupied_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_renter_occupied"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_renter_occupied",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_renter_occupied_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_renter_occupied"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_renter_occupied",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_fd_log_median_household_income_renter_occupied_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_median_household_income_renter_occupied"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_log_median_household_income_renter_occupied",),
    ),
)

EMPLOYMENT_LABOR_FORCE_FD_COLUMNS = (
    "d_log_civilian_labor_force_per_panel_person",
    "d_log_employed_count_per_panel_person",
    "d_labor_force_participation_rate",
    "d_employment_to_population_16_plus",
    "d_unemployment_rate_acs1",
)
EMPLOYMENT_LABOR_FORCE_LEVEL_COLUMNS = (
    "log_civilian_labor_force_per_panel_person",
    "log_employed_count_per_panel_person",
    "labor_force_participation_rate",
    "employment_to_population_16_plus",
    "unemployment_rate_acs1",
)


def _employment_labor_force_fd_specs() -> tuple[RegressionSpec, ...]:
    specs: list[RegressionSpec] = []
    for column in EMPLOYMENT_LABOR_FORCE_FD_COLUMNS:
        base_name = column.removeprefix("d_")
        for suffix, fixed_effect in (
            ("year_fe", "year"),
            ("region_year_fe", "region_year"),
            ("state_year_fe", "primary_state_year"),
        ):
            specs.append(
                RegressionSpec(
                    family="employment_labor_force",
                    model=f"rent_fd_{base_name}_{suffix}",
                    outcome="d_log_zori",
                    predictors=("d_log_pop", column),
                    fixed_effects=(fixed_effect,),
                    sample_filter="fd_year_gap_1",
                    focal_terms=(column,),
                )
            )
    return tuple(specs)


FD_EMPLOYMENT_LABOR_FORCE_SPECS = _employment_labor_force_fd_specs()

FD_INCOME_INEQUALITY_SPECS = (
    RegressionSpec(
        family="income_inequality",
        model="rent_fd_gini_index_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_gini_index"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_gini_index",),
    ),
    RegressionSpec(
        family="income_inequality",
        model="rent_fd_gini_index_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_gini_index"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_gini_index",),
    ),
    RegressionSpec(
        family="income_inequality",
        model="rent_fd_gini_index_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_gini_index"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
        focal_terms=("d_gini_index",),
    ),
)


def _employment_labor_force_level_specs() -> tuple[RegressionSpec, ...]:
    specs: list[RegressionSpec] = []
    for column in EMPLOYMENT_LABOR_FORCE_LEVEL_COLUMNS:
        specs.extend(
            (
                RegressionSpec(
                    family="employment_labor_force",
                    model=f"rent_levels_{column}_msa_year_fe",
                    outcome="log_zori",
                    predictors=("log_pop", column),
                    fixed_effects=("msa_id", "year"),
                    sample_filter="levels_complete_case",
                    focal_terms=(column,),
                ),
                RegressionSpec(
                    family="employment_labor_force",
                    model=f"rent_levels_{column}_msa_region_year_fe",
                    outcome="log_zori",
                    predictors=("log_pop", column),
                    fixed_effects=("msa_id", "region_year"),
                    sample_filter="levels_complete_case",
                    focal_terms=(column,),
                ),
                RegressionSpec(
                    family="employment_labor_force",
                    model=f"rent_levels_{column}_msa_state_year_fe",
                    outcome="log_zori",
                    predictors=("log_pop", column),
                    fixed_effects=("msa_id", "primary_state_year"),
                    sample_filter="levels_complete_case",
                    focal_terms=(column,),
                ),
            )
        )
    return tuple(specs)


LEVEL_FE_SPECS = (
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_state_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_region_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_levels_renter_household_share_msa_year_fe",
        outcome="log_unshelt_rate",
        predictors=("renter_household_share",),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_unsheltered_direct_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_levels_renter_household_share_msa_state_year_fe",
        outcome="log_unshelt_rate",
        predictors=("renter_household_share",),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_unsheltered_direct_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="renter_household_share",
        model="unsheltered_levels_renter_household_share_msa_region_year_fe",
        outcome="log_unshelt_rate",
        predictors=("renter_household_share",),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_unsheltered_direct_complete_case",
        focal_terms=("renter_household_share",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_msa_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori",),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_unsheltered_rent_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_msa_state_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori",),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_unsheltered_rent_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_msa_region_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori",),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_unsheltered_rent_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_log_pop_msa_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori", "log_pop"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_unsheltered_rent_pop_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_log_pop_msa_state_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori", "log_pop"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_unsheltered_rent_pop_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="rent_levels_bridge",
        model="unsheltered_levels_log_zori_log_pop_msa_region_year_fe",
        outcome="log_unshelt_rate",
        predictors=("log_zori", "log_pop"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_unsheltered_rent_pop_complete_case",
        focal_terms=("log_zori",),
    ),
    RegressionSpec(
        family="household_size",
        model="rent_levels_renter_household_size_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "average_household_size_renter_occupied"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("average_household_size_renter_occupied",),
    ),
    RegressionSpec(
        family="recent_mover_income",
        model="rent_levels_moved_diff_state_income_ratio_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "moved_diff_state_income_ratio_total"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("moved_diff_state_income_ratio_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_by_tenure_total_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_by_tenure_total"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_by_tenure_total_msa_region_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_by_tenure_total"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_by_tenure_total_msa_state_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_by_tenure_total"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_by_tenure_total",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_renter_occupied_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_renter_occupied"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_renter_occupied",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_renter_occupied_msa_region_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_renter_occupied"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_renter_occupied",),
    ),
    RegressionSpec(
        family="local_income",
        model="rent_levels_log_median_household_income_renter_occupied_msa_state_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "log_median_household_income_renter_occupied"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_complete_case",
        focal_terms=("log_median_household_income_renter_occupied",),
    ),
    RegressionSpec(
        family="income_inequality",
        model="rent_levels_gini_index_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "gini_index"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
        focal_terms=("gini_index",),
    ),
    RegressionSpec(
        family="income_inequality",
        model="rent_levels_gini_index_msa_region_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "gini_index"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_complete_case",
        focal_terms=("gini_index",),
    ),
    RegressionSpec(
        family="income_inequality",
        model="rent_levels_gini_index_msa_state_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "gini_index"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_complete_case",
        focal_terms=("gini_index",),
    ),
) + _employment_labor_force_level_specs()


def _effect_series(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "primary_state_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "primary_state_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the composition panels from the base panel."
            )
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if effect == "region_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "region_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the composition panels from the base panel."
            )
        regions = sample["primary_state"].map(census_region)
        return regions.astype("string") + "_" + sample["year"].astype("string")
    if effect not in sample.columns:
        raise ValueError(f"Fixed effect column '{effect}' is missing from the regression sample.")
    return sample[effect].astype("string")


def _design_matrix(sample: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    x_parts = [sample[list(spec.predictors)].astype("float64")]
    for effect in spec.fixed_effects:
        dummies = pd.get_dummies(
            _effect_series(sample, effect),
            prefix=effect,
            drop_first=True,
            dtype=float,
        )
        x_parts.append(dummies)
    return sm.add_constant(pd.concat(x_parts, axis=1), has_constant="add")


def _required_columns(spec: RegressionSpec) -> list[str]:
    derived_names = {column.name for column in spec.derived_columns}
    required = [spec.outcome, "msa_id", *spec.fixed_effects]
    required.extend(predictor for predictor in spec.predictors if predictor not in derived_names)
    for column in spec.derived_columns:
        required.extend(source for source in column.source_columns if source not in derived_names)
    return required


def _prepare_sample(frame: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    required = _required_columns(spec)
    for derived_effect in ("primary_state_year", "region_year"):
        if derived_effect in required:
            required.remove(derived_effect)
            required.extend(["primary_state", "year"])
    missing_required = sorted(set(required) - set(frame.columns))
    if missing_required:
        if "primary_state" in missing_required and any(
            effect in spec.fixed_effects for effect in ("primary_state_year", "region_year")
        ):
            raise ValueError(
                f"{'+'.join(spec.fixed_effects)} fixed effects require columns "
                f"{missing_required}. Rebuild the composition panels from the base panel."
            )
        raise ValueError(
            f"Regression spec '{spec.model}' requires missing column(s) {missing_required}."
        )
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        return sample

    for column in spec.derived_columns:
        if column.kind == "center":
            source = column.source_columns[0]
            sample[column.name] = sample[source] - sample[source].mean()
            continue
        if column.kind == "interaction":
            left, right = column.source_columns
            sample[column.name] = sample[left] * sample[right]
            continue
        raise ValueError(f"Unknown derived column kind '{column.kind}' in spec '{spec.model}'.")
    return sample


def fit_spec(frame: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    sample = _prepare_sample(frame, spec)
    if sample.empty:
        return pd.DataFrame()

    x = _design_matrix(sample, spec)
    y = sample[spec.outcome].astype("float64")
    result = sm.OLS(y, x).fit(
        cov_type="cluster",
        cov_kwds={"groups": sample["msa_id"].astype(str)},
    )

    rows: list[dict[str, object]] = []
    for term in spec.predictors:
        rows.append(
            {
                "family": spec.family,
                "model": spec.model,
                "outcome": spec.outcome,
                "term": term,
                "estimate": float(result.params[term]),
                "std_error": float(result.bse[term]),
                "t_stat": float(result.tvalues[term]),
                "p_value": float(result.pvalues[term]),
                "nobs": int(result.nobs),
                "clusters": int(sample["msa_id"].nunique()),
                "r_squared": float(result.rsquared),
                "fixed_effects": "+".join(spec.fixed_effects),
                "sample_filter": spec.sample_filter,
                "focal_term": term in (spec.focal_terms or spec.predictors),
                "std_error_type": "clustered:msa_id",
            }
        )
    return pd.DataFrame(rows)


def load_required_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required composition panel artifact not found: {path}. "
            "Run `uv run hhplab build result composition-rent-population --json` "
            "or the corresponding `python -m hhplab.results.workflows.build_*` "
            "module first."
        )
    return pd.read_parquet(path)


def run_robustness_checks() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    renter_fd = load_required_parquet(FD_INPUTS["renter_household_share"])
    for spec in FD_RENTER_SHARE_SPECS:
        frames.append(fit_spec(renter_fd, spec))

    local_income_fd = load_required_parquet(FD_INPUTS["local_income"])
    for spec in FD_LOCAL_INCOME_SPECS:
        frames.append(fit_spec(local_income_fd, spec))

    employment_labor_force_fd = load_required_parquet(FD_INPUTS["employment_labor_force"])
    for spec in FD_EMPLOYMENT_LABOR_FORCE_SPECS:
        frames.append(fit_spec(employment_labor_force_fd, spec))

    income_inequality_fd = load_required_parquet(FD_INPUTS["income_inequality"])
    for spec in FD_INCOME_INEQUALITY_SPECS:
        frames.append(fit_spec(income_inequality_fd, spec))

    for spec in LEVEL_FE_SPECS:
        levels = load_required_parquet(LEVEL_INPUTS[spec.family])
        frames.append(fit_spec(levels, spec))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def summarize_regressions(regressions: pd.DataFrame) -> dict[str, object]:
    if regressions.empty:
        return {"regression_rows": 0, "models": []}
    focal = regressions[regressions["focal_term"]].copy()
    return {
        "regression_rows": int(len(regressions)),
        "models": sorted(regressions["model"].unique().tolist()),
        "focal_terms": json.loads(focal.to_json(orient="records")),
    }


def write_outputs(regressions: pd.DataFrame) -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    regressions.to_parquet(ROBUSTNESS_PARQUET, index=False)
    regressions.to_csv(ROBUSTNESS_CSV, index=False)
    summary = summarize_regressions(regressions)
    ROBUSTNESS_SUMMARY.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    regressions = run_robustness_checks()
    summary = write_outputs(regressions)
    print(f"regression rows: {len(regressions)} -> {ROBUSTNESS_PARQUET}")
    print(f"csv -> {ROBUSTNESS_CSV}")
    print(f"summary -> {ROBUSTNESS_SUMMARY}")
    if summary["regression_rows"]:
        print(pd.DataFrame(summary["focal_terms"]))


if __name__ == "__main__":
    main()
