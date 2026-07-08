"""Build non-compositional rent/population mechanism screens.

These screens follow the pooled top-150 MSA first-difference design used by
the composition work. They cover mechanisms that can move rents without a
matching headcount increase:

* housing supply constraints interacting with population growth
* ACS1 seasonal/recreational/occasional vacancy share as a free proxy for
  short-term-rental or vacation-home conversion out of long-term stock
* ACS1 work-from-home share as a direct remote-work demand proxy
* ACS1 bedroom-mix shifts as an available proxy for non-headcount space demand
"""

from __future__ import annotations

import glob
import json
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd
import statsmodels.api as sm
from build_household_size_composition_panel import (
    ACS1_METRO_GLOB,
    OUT,
    _as_msa_id,
    _vintage_from_acs1_path,
    load_pooled_base_panel,
)
from build_supply_iv_panel import BPS_SHORT_WINDOW, DEFAULT_INPUT_PATHS, build_bps_exposures

NONCOMPOSITIONAL_OUT = OUT.parent / "noncompositional_rent_population"

SPACE_DEMAND_COLUMNS = [
    "gross_rent_bedrooms_total",
    "gross_rent_1_bedroom_total",
    "gross_rent_2_bedrooms_total",
    "gross_rent_3plus_bedrooms_total",
]
SPACE_DEMAND_SHARE_COLUMNS = [
    "gross_rent_2plus_bedroom_share",
    "gross_rent_3plus_bedroom_share",
]
STR_PROXY_COLUMNS = [
    "seasonal_recreational_vacancy_share",
]
REMOTE_WORK_PROXY_COLUMNS = [
    "work_from_home_share",
]
SUPPLY_COLUMNS = [
    "supply_constraint_bps",
    "supply_constraint_bps_long",
]
SUPPLY_EXPOSURE_END_YEAR = max(BPS_SHORT_WINDOW)


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]
    family: str


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype("float64") / denominator.astype("float64").where(denominator > 0)


def load_acs1_space_demand_panel(acs1_glob: str = ACS1_METRO_GLOB) -> pd.DataFrame:
    files = sorted(glob.glob(acs1_glob))
    if not files:
        raise FileNotFoundError(f"No ACS1 metro files matched {acs1_glob}")

    frames: list[pd.DataFrame] = []
    for file_name in files:
        vintage = _vintage_from_acs1_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=[
                "metro_id",
                "acs1_vintage",
                *SPACE_DEMAND_COLUMNS,
                *STR_PROXY_COLUMNS,
                *REMOTE_WORK_PROXY_COLUMNS,
            ],
        )
        frame["msa_id"] = _as_msa_id(frame["metro_id"])
        frame["acs1_vintage_used"] = vintage
        frame["year"] = vintage + 1
        frames.append(
            frame[
                [
                    "msa_id",
                    "year",
                    "acs1_vintage_used",
                    *SPACE_DEMAND_COLUMNS,
                    *STR_PROXY_COLUMNS,
                    *REMOTE_WORK_PROXY_COLUMNS,
                ]
            ]
        )

    return pd.concat(frames, ignore_index=True)


def add_space_demand_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.copy()
    two_plus = levels["gross_rent_2_bedrooms_total"] + levels["gross_rent_3plus_bedrooms_total"]
    levels["gross_rent_2plus_bedroom_share"] = _safe_ratio(
        two_plus,
        levels["gross_rent_bedrooms_total"],
    )
    levels["gross_rent_3plus_bedroom_share"] = _safe_ratio(
        levels["gross_rent_3plus_bedrooms_total"],
        levels["gross_rent_bedrooms_total"],
    )
    return levels


def add_first_differences(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in SPACE_DEMAND_SHARE_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    for column in STR_PROXY_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    for column in REMOTE_WORK_PROXY_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    for column in SUPPLY_COLUMNS:
        levels[f"d_log_pop_x_{column}"] = levels["d_log_pop"] * levels[column]
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    cohort = base[["msa_id", "msa_name"]].drop_duplicates("msa_id")
    supply = build_bps_exposures(
        cohort,
        paths=DEFAULT_INPUT_PATHS,
        include_long_bps=True,
    )
    space = add_space_demand_columns(load_acs1_space_demand_panel())
    merged = base.merge(supply[["msa_id", *SUPPLY_COLUMNS]], on="msa_id", how="left")
    merged = merged.merge(space, on=["msa_id", "year"], how="left")
    return add_first_differences(merged)


def _model_specs() -> Iterable[ModelSpec]:
    for column in SUPPLY_COLUMNS:
        yield ModelSpec(
            name=f"rent_fd_population_x_{column}",
            outcome="d_log_zori",
            predictors=("d_log_pop", column, f"d_log_pop_x_{column}"),
            family="supply_constraint",
        )
    for column in STR_PROXY_COLUMNS:
        yield ModelSpec(
            name=f"rent_fd_{column}",
            outcome="d_log_zori",
            predictors=("d_log_pop", f"d_{column}"),
            family="short_term_rental_proxy",
        )
    for column in REMOTE_WORK_PROXY_COLUMNS:
        yield ModelSpec(
            name=f"rent_fd_{column}",
            outcome="d_log_zori",
            predictors=("d_log_pop", f"d_{column}"),
            family="remote_work_proxy",
        )
    for column in SPACE_DEMAND_SHARE_COLUMNS:
        diff_column = f"d_{column}"
        yield ModelSpec(
            name=f"rent_fd_{column}",
            outcome="d_log_zori",
            predictors=("d_log_pop", diff_column),
            family="space_demand_proxy",
        )
        yield ModelSpec(
            name=f"unsheltered_fd_{column}",
            outcome="d_log_unshelt_rate",
            predictors=("d_log_zori", "d_log_pop", diff_column),
            family="space_demand_proxy",
        )


def _validate_supply_model_sample(spec: ModelSpec, sample: pd.DataFrame) -> None:
    if spec.family != "supply_constraint" or sample.empty:
        return

    overlap = sorted(
        int(year)
        for year in sample["year"].dropna().unique()
        if int(year) <= SUPPLY_EXPOSURE_END_YEAR
    )
    if overlap:
        raise ValueError(
            f"{spec.name} supply-constraint sample overlaps the "
            f"{min(BPS_SHORT_WINDOW)}-{SUPPLY_EXPOSURE_END_YEAR} BPS exposure window "
            f"in analysis year(s) {overlap}. Restrict supply-constraint outcomes to "
            f"years after {SUPPLY_EXPOSURE_END_YEAR} so the exposure is pre-sample."
        )


def fit_clustered_fd_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [spec.outcome, *spec.predictors, "year", "msa_id"]
        sample = fd.dropna(subset=required).copy()
        if sample.empty:
            continue
        _validate_supply_model_sample(spec, sample)

        year_fe = pd.get_dummies(
            sample["year"].astype("string"),
            prefix="year",
            drop_first=True,
            dtype=float,
        )
        x = pd.concat([sample[list(spec.predictors)].astype(float), year_fe], axis=1)
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
                    "year_fixed_effects": True,
                    "std_error_type": "clustered:msa_id",
                }
            )
    return pd.DataFrame(rows)


def summarize_panel(levels: pd.DataFrame, fd: pd.DataFrame) -> dict[str, object]:
    supply_complete = fd.dropna(subset=["d_log_zori", "d_log_pop", "supply_constraint_bps"])
    space_complete = fd.dropna(
        subset=[
            "d_log_zori",
            "d_log_unshelt_rate",
            "d_log_pop",
            "d_gross_rent_2plus_bedroom_share",
        ]
    )
    str_complete = fd.dropna(
        subset=[
            "d_log_zori",
            "d_log_pop",
            "d_seasonal_recreational_vacancy_share",
        ]
    )
    remote_work_complete = fd.dropna(
        subset=[
            "d_log_zori",
            "d_log_pop",
            "d_work_from_home_share",
        ]
    )
    space_summary = (
        levels[SPACE_DEMAND_SHARE_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    str_summary = (
        levels[STR_PROXY_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    remote_work_summary = (
        levels[REMOTE_WORK_PROXY_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        space_complete[
            [
                "d_log_zori",
                "d_log_unshelt_rate",
                "d_log_pop",
                *[f"d_{column}" for column in SPACE_DEMAND_SHARE_COLUMNS],
                *[f"d_{column}" for column in STR_PROXY_COLUMNS],
                *[f"d_{column}" for column in REMOTE_WORK_PROXY_COLUMNS],
            ]
        ]
        .corr()
        .round(6)
    )
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "acs1_vintages_used": [
            int(vintage) for vintage in sorted(levels["acs1_vintage_used"].dropna().unique())
        ],
        "complete_fd_rows_for_supply_constraint": int(len(supply_complete)),
        "complete_fd_rows_for_short_term_rental_proxy": int(len(str_complete)),
        "complete_fd_rows_for_remote_work_proxy": int(len(remote_work_complete)),
        "complete_fd_rows_for_space_demand_proxy": int(len(space_complete)),
        "short_term_rental_proxy_level_summary": json.loads(str_summary.to_json()),
        "remote_work_proxy_level_summary": json.loads(remote_work_summary.to_json()),
        "space_demand_level_summary": json.loads(space_summary.to_json()),
        "levels_missing_supply": {
            column: int(levels[column].isna().sum()) for column in SUPPLY_COLUMNS
        },
        "levels_missing_short_term_rental_proxy": {
            column: int(levels[column].isna().sum()) for column in STR_PROXY_COLUMNS
        },
        "levels_missing_remote_work_proxy": {
            column: int(levels[column].isna().sum()) for column in REMOTE_WORK_PROXY_COLUMNS
        },
        "levels_missing_space_demand": {
            column: int(levels[column].isna().sum())
            for column in [*SPACE_DEMAND_COLUMNS, *SPACE_DEMAND_SHARE_COLUMNS]
        },
        "fd_missing_short_term_rental_proxy_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum()) for column in STR_PROXY_COLUMNS
        },
        "fd_missing_remote_work_proxy_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum())
            for column in REMOTE_WORK_PROXY_COLUMNS
        },
        "fd_missing_space_demand_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum())
            for column in SPACE_DEMAND_SHARE_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
        "measure_discovery": {
            "short_term_rental_proxy": {
                "status": "tested_with_acs1_b25004_proxy",
                "registry_tables": ["B25004"],
                "note": (
                    "No commercial STR-platform source is registered. This run "
                    "uses ACS1 B25004 seasonal/recreational/occasional-use "
                    "vacancy share as the free vacation-home/STR-adjacent proxy."
                ),
            },
            "remote_work_proxy": {
                "status": "tested_with_acs1_b08301_proxy",
                "registry_tables": ["B08301"],
                "fallback_used_in_current_artifacts": "ACS1 B25068 gross-rent bedroom mix",
            },
        },
    }


def main() -> None:
    NONCOMPOSITIONAL_OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_levels.parquet"
    fd_path = NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_fd.parquet"
    regression_path = (
        NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_fd_regressions.parquet"
    )
    regression_csv_path = (
        NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_fd_regressions.csv"
    )
    summary_path = NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_summary.json"

    levels.to_parquet(levels_path, index=False)
    fd.to_parquet(fd_path, index=False)
    regressions.to_parquet(regression_path, index=False)
    regressions.to_csv(regression_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"levels rows: {len(levels)} -> {levels_path}")
    print(f"fd rows: {len(fd)} -> {fd_path}")
    print(f"regression rows: {len(regressions)} -> {regression_path}")
    print(f"summary -> {summary_path}")
    if not regressions.empty:
        terms = regressions[
            regressions["term"].str.contains("supply_constraint|bedroom_share", regex=True)
        ]
        print(terms)


if __name__ == "__main__":
    main()
