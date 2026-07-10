"""Decompose MSA rent-growth R-squared across tracked covariate channels."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows._paths import OUTPUTS_ROOT, write_result_parquet

OUT = OUTPUTS_ROOT / "rent_growth_r2_decomposition"
MODELS_PARQUET = OUT / "rent_growth_r2_decomposition_models.parquet"
COVERAGE_PARQUET = OUT / "rent_growth_r2_decomposition_coverage.parquet"
SUMMARY_JSON = OUT / "rent_growth_r2_decomposition_summary.json"

KEY_COLUMNS = ("msa_id", "year")
OUTCOME = "d_log_zori"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    relative_path: str
    columns: tuple[str, ...]
    build_workflow: str


SOURCES = (
    SourceSpec(
        "renter_household",
        "composition_rent_population/renter_household_share_composition_fd.parquet",
        (
            OUTCOME,
            "d_log_pop",
            "d_renter_household_share",
            "d_log_total_households_per_panel_person",
        ),
        "composition-rent-population",
    ),
    SourceSpec(
        "household_size",
        "composition_rent_population/household_size_composition_fd.parquet",
        ("d_average_household_size_renter_occupied",),
        "composition-rent-population",
    ),
    SourceSpec(
        "mover_income",
        "composition_rent_population/recent_mover_income_composition_fd.parquet",
        ("d_moved_diff_state_income_ratio_total",),
        "composition-rent-population",
    ),
    SourceSpec(
        "local_income",
        "composition_rent_population/local_income_composition_fd.parquet",
        ("d_log_median_household_income_by_tenure_total",),
        "composition-rent-population",
    ),
    SourceSpec(
        "income_inequality",
        "composition_rent_population/income_inequality_composition_fd.parquet",
        ("d_gini_index",),
        "composition-rent-population",
    ),
    SourceSpec(
        "acs_employment",
        "composition_rent_population/employment_labor_force_composition_fd.parquet",
        ("d_log_employed_count_per_panel_person",),
        "composition-rent-population",
    ),
    SourceSpec(
        "housing_cost_burden",
        "composition_rent_population/housing_cost_burden_composition_fd.parquet",
        ("d_acs5_rent_burden_30_plus",),
        "composition-rent-population",
    ),
    SourceSpec(
        "original_noncompositional",
        "noncompositional_rent_population/noncompositional_rent_population_fd.parquet",
        (
            "supply_constraint_bps",
            "d_log_pop_x_supply_constraint_bps",
            "d_seasonal_recreational_vacancy_share",
            "d_work_from_home_share",
        ),
        "noncompositional-rent-population",
    ),
    SourceSpec(
        "subsidized_housing",
        "subsidized_housing_stock/subsidized_housing_stock_fd.parquet",
        (
            "d_log_subsidized_households_per_1000",
            "d_log_housing_choice_vouchers_per_1000",
        ),
        "subsidized-housing-stock",
    ),
    SourceSpec(
        "eviction",
        "eviction_rate_timing/eviction_rate_timing_fd.parquet",
        ("d_log_eviction_rate",),
        "eviction-rate-timing",
    ),
    SourceSpec(
        "irs_migration",
        "irs_migration_pooled/irs_migration_pooled_fd.parquet",
        ("inflow_agi_per_return_k", "outflow_agi_per_return_k"),
        "irs-migration-pooled",
    ),
    SourceSpec(
        "qcew",
        "qcew_labor_market/qcew_labor_market_fd.parquet",
        ("d_log_qcew_annual_avg_emplvl",),
        "qcew-labor-market",
    ),
    SourceSpec(
        "bps_valuation",
        "bps_valuation_rent_channel/bps_valuation_rent_channel_levels.parquet",
        ("d_log_bps_valuation",),
        "bps-valuation-rent-channel",
    ),
)

POPULATION_COLUMNS = ("d_log_pop",)
ORIGINAL_COLUMNS = (
    "d_renter_household_share",
    "d_average_household_size_renter_occupied",
    "d_moved_diff_state_income_ratio_total",
    "supply_constraint_bps",
    "d_log_pop_x_supply_constraint_bps",
    "d_seasonal_recreational_vacancy_share",
    "d_work_from_home_share",
)
ROADMAP_TIER1_COLUMNS = (
    "d_log_total_households_per_panel_person",
    "d_log_median_household_income_by_tenure_total",
    "d_gini_index",
    "d_log_employed_count_per_panel_person",
    "d_log_subsidized_households_per_1000",
    "d_log_housing_choice_vouchers_per_1000",
    "d_acs5_rent_burden_30_plus",
)
ROADMAP_LATER_COLUMNS = (
    "d_log_eviction_rate",
    "inflow_agi_per_return_k",
    "outflow_agi_per_return_k",
    "d_log_qcew_annual_avg_emplvl",
    "d_log_bps_valuation",
)
ALL_PREDICTORS = (
    *POPULATION_COLUMNS,
    *ORIGINAL_COLUMNS,
    *ROADMAP_TIER1_COLUMNS,
    *ROADMAP_LATER_COLUMNS,
)

MODEL_BLOCKS = (
    ("year_fe_only", ()),
    ("population", POPULATION_COLUMNS),
    ("original_covariates", (*POPULATION_COLUMNS, *ORIGINAL_COLUMNS)),
    (
        "plus_roadmap_tier1",
        (*POPULATION_COLUMNS, *ORIGINAL_COLUMNS, *ROADMAP_TIER1_COLUMNS),
    ),
    ("everything_tested", ALL_PREDICTORS),
)


def load_source(spec: SourceSpec, root: Path = OUTPUTS_ROOT) -> pd.DataFrame:
    path = root / spec.relative_path
    if not path.exists():
        raise FileNotFoundError(
            f"Required R-squared decomposition artifact not found: {path}. Run "
            f"`uv run hhplab build result {spec.build_workflow} --json` first."
        )
    frame = pd.read_parquet(path, columns=[*KEY_COLUMNS, *spec.columns])
    missing = sorted(set((*KEY_COLUMNS, *spec.columns)) - set(frame.columns))
    if missing:
        raise ValueError(f"Source '{spec.name}' is missing required columns {missing}.")
    if frame.duplicated(list(KEY_COLUMNS)).any():
        raise ValueError(f"Source '{spec.name}' has duplicate msa_id/year rows.")
    return frame


def merge_sources(frames: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing_sources = [spec.name for spec in SOURCES if spec.name not in frames]
    if missing_sources:
        raise ValueError(f"Missing source frames: {missing_sources}.")
    merged: pd.DataFrame | None = None
    coverage_rows: list[dict[str, object]] = []
    required_so_far: list[str] = []
    for spec in SOURCES:
        frame = frames[spec.name]
        required_so_far.extend(spec.columns)
        merged = (
            frame.copy()
            if merged is None
            else merged.merge(frame, on=list(KEY_COLUMNS), how="inner", validate="one_to_one")
        )
        complete = merged.dropna(subset=list(dict.fromkeys(required_so_far)))
        coverage_rows.append(
            {
                "source": spec.name,
                "merged_rows": int(len(merged)),
                "complete_rows_after_source": int(len(complete)),
                "complete_msas_after_source": int(complete["msa_id"].nunique()),
                "min_year": int(complete["year"].min()) if not complete.empty else None,
                "max_year": int(complete["year"].max()) if not complete.empty else None,
            }
        )
    assert merged is not None
    return merged, pd.DataFrame(coverage_rows)


def common_sample(frame: pd.DataFrame) -> pd.DataFrame:
    required = [OUTCOME, *ALL_PREDICTORS]
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Merged decomposition panel is missing columns {missing}.")
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        raise ValueError("Full covariate intersection sample is empty.")
    return sample


def fit_models(sample: pd.DataFrame) -> pd.DataFrame:
    year_dummies = pd.get_dummies(
        sample["year"].astype("string"), prefix="year", drop_first=True, dtype=float
    )
    rows: list[dict[str, object]] = []
    baseline_r2: float | None = None
    previous_r2: float | None = None
    for order, (name, predictors) in enumerate(MODEL_BLOCKS):
        parts = [sample[list(predictors)].astype("float64")] if predictors else []
        parts.append(year_dummies)
        design = sm.add_constant(pd.concat(parts, axis=1), has_constant="add")
        result = sm.OLS(sample[OUTCOME].astype("float64"), design).fit()
        r_squared = float(result.rsquared)
        if baseline_r2 is None:
            baseline_r2 = r_squared
        rows.append(
            {
                "model_order": order,
                "model": name,
                "predictors": "+".join(predictors),
                "predictor_count": len(predictors),
                "r_squared": r_squared,
                "adjusted_r_squared": float(result.rsquared_adj),
                "delta_vs_year_fe": r_squared - baseline_r2,
                "delta_vs_previous": None if previous_r2 is None else r_squared - previous_r2,
                "unexplained_share": 1.0 - r_squared,
                "nobs": int(result.nobs),
                "msa_count": int(sample["msa_id"].nunique()),
                "min_year": int(sample["year"].min()),
                "max_year": int(sample["year"].max()),
                "fixed_effects": "year",
            }
        )
        previous_r2 = r_squared
    return pd.DataFrame(rows)


def run() -> dict[str, object]:
    frames = {spec.name: load_source(spec) for spec in SOURCES}
    merged, coverage = merge_sources(frames)
    sample = common_sample(merged)
    models = fit_models(sample)
    OUT.mkdir(parents=True, exist_ok=True)
    write_result_parquet(models, MODELS_PARQUET, index=False)
    write_result_parquet(coverage, COVERAGE_PARQUET, index=False)
    final = models.iloc[-1]
    summary = {
        "intersection_rows": int(len(sample)),
        "intersection_msas": int(sample["msa_id"].nunique()),
        "intersection_years": sorted(int(year) for year in sample["year"].unique()),
        "everything_r_squared": float(final["r_squared"]),
        "everything_unexplained_share": float(final["unexplained_share"]),
        "models": json.loads(models.to_json(orient="records")),
        "coverage": json.loads(coverage.to_json(orient="records")),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "outputs": {
            "models_parquet": str(MODELS_PARQUET),
            "coverage_parquet": str(COVERAGE_PARQUET),
            "summary_json": str(SUMMARY_JSON),
        },
    }


def main() -> None:
    print(json.dumps(run(), indent=2))


if __name__ == "__main__":
    main()
