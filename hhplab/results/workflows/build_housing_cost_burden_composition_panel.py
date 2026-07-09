"""Build pooled top-150 housing-cost-burden panels and timing-aware rent screens.

This workflow combines ACS5 tract-derived MSA affordability measures with ACS1
metro-native rent burden. ACS vintages follow the standard lag convention:
ACS vintage end year E is aligned to PIT year E + 1.

The primary modeled question is whether prior-year affordability burden levels
predict subsequent rent growth. Same-year first-difference screens are retained
as secondary descriptive checks and are labeled separately in the regression
output.
"""

from __future__ import annotations

import glob
import json
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.results.workflows._paths import write_result_parquet
from hhplab.results.workflows.build_household_size_composition_panel import (
    ACS1_METRO_GLOB,
    OUT,
    _as_msa_id,
    _primary_state,
    load_pooled_base_panel,
)
from hhplab.results.workflows.build_renter_household_share_composition_panel import (
    MEASURES_GLOB,
    _acs_end_year_from_measures_path,
)

ACS5_SOURCE_COLUMNS = [
    "median_household_income",
    "median_gross_rent",
    "msa_rent_burden",
    "msa_rent_burden_40_plus",
    "msa_rent_burden_50_plus",
    "owner_costs_pct_income_with_mortgage_total",
    "owner_costs_pct_income_with_mortgage_30_to_34_9",
    "owner_costs_pct_income_with_mortgage_35_to_39_9",
    "owner_costs_pct_income_with_mortgage_40_to_49_9",
    "owner_costs_pct_income_with_mortgage_50_plus",
    "owner_costs_pct_income_with_mortgage_not_computed",
    "owner_costs_pct_income_without_mortgage_total",
    "owner_costs_pct_income_without_mortgage_30_to_34_9",
    "owner_costs_pct_income_without_mortgage_35_to_39_9",
    "owner_costs_pct_income_without_mortgage_40_to_49_9",
    "owner_costs_pct_income_without_mortgage_50_plus",
    "owner_costs_pct_income_without_mortgage_not_computed",
]
ACS1_SOURCE_COLUMNS = [
    "median_household_income_by_tenure_total",
    "median_gross_rent",
    "rent_burden_40_plus",
    "rent_burden_50_plus",
]
ACS5_SCREEN_COLUMNS = [
    "acs5_rent_burden_30_plus",
    "acs5_rent_burden_40_plus",
    "acs5_rent_burden_50_plus",
    "acs5_owner_cost_burden_30_plus",
    "acs5_owner_cost_burden_50_plus",
    "acs5_rent_to_income",
]
ACS1_SCREEN_COLUMNS = [
    "acs1_rent_burden_40_plus",
    "acs1_rent_burden_50_plus",
    "acs1_rent_to_income",
]
SCREEN_COLUMNS = [*ACS5_SCREEN_COLUMNS, *ACS1_SCREEN_COLUMNS]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]
    family: str
    fixed_effects: tuple[str, ...]


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce").astype("float64")
    right = pd.to_numeric(denominator, errors="coerce").astype("float64")
    return left / right.where(right > 0)


def _sum_columns(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    return frame[columns].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)


def _census_region_or_na(state: object) -> object:
    try:
        return census_region(state)
    except ValueError:
        return pd.NA


def load_acs5_housing_cost_burden_panel() -> pd.DataFrame:
    files = sorted(glob.glob(MEASURES_GLOB))
    if not files:
        raise FileNotFoundError(f"No MSA ACS5 measure files matched {MEASURES_GLOB}")

    output_columns = ["msa_id", "year", "acs5_vintage_used", *ACS5_SCREEN_COLUMNS]
    frames: list[pd.DataFrame] = []
    for file_name in files:
        acs_end_year = _acs_end_year_from_measures_path(file_name)
        frame = pd.read_parquet(file_name, columns=["msa_id", "acs_vintage", *ACS5_SOURCE_COLUMNS])
        if frame.empty:
            continue
        frame["msa_id"] = _as_msa_id(frame["msa_id"])
        frame["acs5_vintage_used"] = acs_end_year
        frame["year"] = acs_end_year + 1

        frame["acs5_rent_burden_30_plus"] = pd.to_numeric(
            frame["msa_rent_burden"],
            errors="coerce",
        ).astype("float64")
        frame["acs5_rent_burden_40_plus"] = pd.to_numeric(
            frame["msa_rent_burden_40_plus"],
            errors="coerce",
        ).astype("float64")
        frame["acs5_rent_burden_50_plus"] = pd.to_numeric(
            frame["msa_rent_burden_50_plus"],
            errors="coerce",
        ).astype("float64")
        frame["acs5_rent_to_income"] = _safe_ratio(
            frame["median_gross_rent"],
            pd.to_numeric(frame["median_household_income"], errors="coerce") / 12.0,
        )

        with_mortgage_denominator = pd.to_numeric(
            frame["owner_costs_pct_income_with_mortgage_total"], errors="coerce"
        ) - pd.to_numeric(
            frame["owner_costs_pct_income_with_mortgage_not_computed"],
            errors="coerce",
        )
        without_mortgage_denominator = pd.to_numeric(
            frame["owner_costs_pct_income_without_mortgage_total"], errors="coerce"
        ) - pd.to_numeric(
            frame["owner_costs_pct_income_without_mortgage_not_computed"],
            errors="coerce",
        )
        with_mortgage_30_plus = _sum_columns(
            frame,
            [
                "owner_costs_pct_income_with_mortgage_30_to_34_9",
                "owner_costs_pct_income_with_mortgage_35_to_39_9",
                "owner_costs_pct_income_with_mortgage_40_to_49_9",
                "owner_costs_pct_income_with_mortgage_50_plus",
            ],
        )
        without_mortgage_30_plus = _sum_columns(
            frame,
            [
                "owner_costs_pct_income_without_mortgage_30_to_34_9",
                "owner_costs_pct_income_without_mortgage_35_to_39_9",
                "owner_costs_pct_income_without_mortgage_40_to_49_9",
                "owner_costs_pct_income_without_mortgage_50_plus",
            ],
        )
        with_mortgage_50_plus = pd.to_numeric(
            frame["owner_costs_pct_income_with_mortgage_50_plus"],
            errors="coerce",
        )
        without_mortgage_50_plus = pd.to_numeric(
            frame["owner_costs_pct_income_without_mortgage_50_plus"],
            errors="coerce",
        )
        total_owner_denominator = with_mortgage_denominator + without_mortgage_denominator
        frame["acs5_owner_cost_burden_30_plus"] = _safe_ratio(
            with_mortgage_30_plus + without_mortgage_30_plus,
            total_owner_denominator,
        )
        frame["acs5_owner_cost_burden_50_plus"] = _safe_ratio(
            with_mortgage_50_plus + without_mortgage_50_plus,
            total_owner_denominator,
        )
        frames.append(frame[output_columns])

    if not frames:
        return pd.DataFrame(columns=output_columns)
    return pd.concat(frames, ignore_index=True)


def load_acs1_housing_cost_burden_panel() -> pd.DataFrame:
    files = sorted(glob.glob(ACS1_METRO_GLOB))
    if not files:
        raise FileNotFoundError(f"No ACS1 metro files matched {ACS1_METRO_GLOB}")

    output_columns = ["msa_id", "year", "acs1_vintage_used", *ACS1_SCREEN_COLUMNS]
    frames: list[pd.DataFrame] = []
    for file_name in files:
        vintage = int(file_name.split("__A", maxsplit=1)[1].split("@", maxsplit=1)[0])
        frame = pd.read_parquet(
            file_name,
            columns=["metro_id", "acs1_vintage", *ACS1_SOURCE_COLUMNS],
        )
        frame["msa_id"] = _as_msa_id(frame["metro_id"])
        frame["acs1_vintage_used"] = vintage
        frame["year"] = vintage + 1
        frame["acs1_rent_burden_40_plus"] = pd.to_numeric(
            frame["rent_burden_40_plus"],
            errors="coerce",
        ).astype("float64")
        frame["acs1_rent_burden_50_plus"] = pd.to_numeric(
            frame["rent_burden_50_plus"],
            errors="coerce",
        ).astype("float64")
        frame["acs1_rent_to_income"] = _safe_ratio(
            frame["median_gross_rent"],
            pd.to_numeric(frame["median_household_income_by_tenure_total"], errors="coerce") / 12.0,
        )
        frames.append(frame[output_columns])

    return pd.concat(frames, ignore_index=True)


def add_timing_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    if "primary_state" not in levels.columns and "msa_name" in levels.columns:
        levels["primary_state"] = levels["msa_name"].map(_primary_state)
    if "primary_state" in levels.columns:
        levels["region"] = levels["primary_state"].map(_census_region_or_na)
        levels["primary_state_year"] = (
            levels["primary_state"].astype("string")
            + "_"
            + levels["year"].astype("Int64").astype("string")
        )
        levels["region_year"] = (
            levels["region"].astype("string")
            + "_"
            + levels["year"].astype("Int64").astype("string")
        )
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_total_rate"] = grouped["log_total_rate"].diff()
    levels["d_log_shelt_rate"] = grouped["log_shelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in SCREEN_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
        levels[f"{column}_lag1"] = grouped[column].shift(1)
        levels.loc[levels["year_gap"] != 1, f"{column}_lag1"] = pd.NA
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    acs5 = load_acs5_housing_cost_burden_panel()
    acs1 = load_acs1_housing_cost_burden_panel()
    merged = base.merge(acs5, on=["msa_id", "year"], how="left")
    merged = merged.merge(acs1, on=["msa_id", "year"], how="left")
    return add_timing_columns(merged)


def _model_specs() -> Iterable[ModelSpec]:
    rent_fixed_effects = (
        ("year", "year_fe"),
        ("region_year", "region_year_fe"),
        ("primary_state_year", "state_year_fe"),
    )
    for column in SCREEN_COLUMNS:
        diff_column = f"d_{column}"
        lag_column = f"{column}_lag1"
        for fixed_effect, label in rent_fixed_effects:
            yield ModelSpec(
                name=f"rent_lag1_{column}_{label}",
                outcome="d_log_zori",
                predictors=("d_log_pop", lag_column),
                family="lagged_level_channel",
                fixed_effects=(fixed_effect,),
            )
            yield ModelSpec(
                name=f"rent_fd_same_year_{column}_{label}",
                outcome="d_log_zori",
                predictors=("d_log_pop", diff_column),
                family="same_year_screen",
                fixed_effects=(fixed_effect,),
            )
        yield ModelSpec(
            name=f"unsheltered_fd_same_year_{column}",
            outcome="d_log_unshelt_rate",
            predictors=("d_log_zori", "d_log_pop", diff_column),
            family="same_year_unsheltered_screen",
            fixed_effects=("year",),
        )


def fit_clustered_fd_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [spec.outcome, *spec.predictors, *spec.fixed_effects, "msa_id"]
        sample = fd.dropna(subset=required).copy()
        if sample.empty:
            continue

        fe_parts = [
            pd.get_dummies(
                sample[fixed_effect].astype("string"),
                prefix=fixed_effect,
                drop_first=True,
                dtype=float,
            )
            for fixed_effect in spec.fixed_effects
        ]
        x = pd.concat([sample[list(spec.predictors)].astype(float), *fe_parts], axis=1)
        x = sm.add_constant(x, has_constant="add")
        y = sample[spec.outcome].astype(float)
        result = sm.OLS(y, x).fit(
            cov_type="cluster",
            cov_kwds={"groups": sample["msa_id"].astype(str)},
        )

        for term in spec.predictors:
            rows.append(
                {
                    "family": spec.family,
                    "model": spec.name,
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
                    "year_fixed_effects": True,
                    "std_error_type": "clustered:msa_id",
                }
            )
    return pd.DataFrame(rows)


def summarize_panel(levels: pd.DataFrame, fd: pd.DataFrame) -> dict[str, object]:
    main_column = "acs5_rent_burden_30_plus"
    main_lag_column = f"{main_column}_lag1"
    complete_lagged = fd.dropna(subset=["d_log_zori", "d_log_pop", main_lag_column])
    complete_same_year = fd.dropna(
        subset=["d_log_zori", "d_log_unshelt_rate", "d_log_pop", f"d_{main_column}"]
    )
    level_summary = (
        levels[SCREEN_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        complete_same_year[
            [
                "d_log_zori",
                "d_log_unshelt_rate",
                "d_log_pop",
                *[f"d_{column}" for column in SCREEN_COLUMNS],
            ]
        ]
        .corr()
        .round(6)
    )
    sample_sizes = {
        spec.name: int(
            len(fd.dropna(subset=[spec.outcome, *spec.predictors, *spec.fixed_effects, "msa_id"]))
        )
        for spec in _model_specs()
    }
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "primary_model_family": "lagged_level_channel",
        "rent_model_fixed_effects": ["year", "region_year", "primary_state_year"],
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "acs5_vintages_used": [
            int(vintage) for vintage in sorted(levels["acs5_vintage_used"].dropna().unique())
        ],
        "acs1_vintages_used": [
            int(vintage) for vintage in sorted(levels["acs1_vintage_used"].dropna().unique())
        ],
        "lagged_level_rows_for_acs5_rent_burden_30_plus": int(len(complete_lagged)),
        "same_year_complete_fd_rows_for_acs5_rent_burden_30_plus": int(len(complete_same_year)),
        "timing_sample_sizes": sample_sizes,
        "level_housing_cost_burden_summary": json.loads(level_summary.to_json()),
        "levels_missing_affordability_columns": {
            column: int(levels[column].isna().sum()) for column in SCREEN_COLUMNS
        },
        "levels_missing_affordability_lag_columns": {
            f"{column}_lag1": int(levels[f"{column}_lag1"].isna().sum())
            for column in SCREEN_COLUMNS
        },
        "fd_missing_affordability_diff_columns": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum()) for column in SCREEN_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "housing_cost_burden_composition_levels.parquet"
    fd_path = OUT / "housing_cost_burden_composition_fd.parquet"
    regression_path = OUT / "housing_cost_burden_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "housing_cost_burden_composition_fd_regressions.csv"
    summary_path = OUT / "housing_cost_burden_composition_summary.json"

    write_result_parquet(levels, levels_path, index=False)
    write_result_parquet(fd, fd_path, index=False)
    write_result_parquet(regressions, regression_path, index=False)
    regressions.to_csv(regression_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return {
        "summary": summary,
        "regressions": json.loads(regressions.to_json(orient="records")),
        "outputs": {
            "levels_parquet": str(levels_path),
            "fd_parquet": str(fd_path),
            "regressions_parquet": str(regression_path),
            "regressions_csv": str(regression_csv_path),
            "summary_json": str(summary_path),
        },
    }


def main() -> None:
    result = run()
    summary = result["summary"]
    outputs = result["outputs"]
    regressions = pd.DataFrame(result["regressions"])

    print(f"levels rows: {summary['levels_rows']} -> {outputs['levels_parquet']}")
    print(f"fd rows: {summary['fd_rows_year_gap_1']} -> {outputs['fd_parquet']}")
    print(f"regression rows: {len(regressions)} -> {outputs['regressions_parquet']}")
    print(f"summary -> {outputs['summary_json']}")
    if not regressions.empty:
        print(regressions[regressions["term"].str.contains("burden|rent_to_income")])


if __name__ == "__main__":
    main()
