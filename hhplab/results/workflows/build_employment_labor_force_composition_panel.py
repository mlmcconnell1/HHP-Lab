"""Build pooled ACS1 labor-market screens for rent and homelessness changes.

This workflow uses metro-native ACS1 B23025 employment-status measures already
present in the curated ACS1 artifacts. It tests whether labor-market tightness
or employment growth explains rent growth beyond overall population growth,
using the standard ACS1 lag rule: vintage E is aligned to PIT year E + 1.
"""

from __future__ import annotations

import glob
import json
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows.build_household_size_composition_panel import (
    ACS1_METRO_GLOB,
    OUT,
    _as_msa_id,
    _vintage_from_acs1_path,
    load_pooled_base_panel,
)
from hhplab.results.workflows.build_household_size_composition_panel import (
    ROOT as _ROOT,
)

ROOT = _ROOT

EMPLOYMENT_STATUS_COLUMNS = [
    "pop_16_plus",
    "civilian_labor_force",
    "unemployed_count",
    "unemployment_rate_acs1",
]
LABOR_MARKET_GROWTH_COLUMNS = [
    "log_civilian_labor_force_per_panel_person",
    "log_employed_count_per_panel_person",
]
LABOR_MARKET_RATE_COLUMNS = [
    "labor_force_participation_rate",
    "employment_to_population_16_plus",
    "unemployment_rate_acs1",
]
FD_EMPLOYMENT_COLUMNS = [
    *[f"d_{column}" for column in LABOR_MARKET_GROWTH_COLUMNS],
    *[f"d_{column}" for column in LABOR_MARKET_RATE_COLUMNS],
]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce").astype("float64")
    right = pd.to_numeric(denominator, errors="coerce").astype("float64")
    return left / right.where(right > 0)


def _safe_log(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log(numeric.where(numeric > 0))


def load_acs1_employment_panel() -> pd.DataFrame:
    files = sorted(glob.glob(ACS1_METRO_GLOB))
    if not files:
        raise FileNotFoundError(f"No ACS1 metro files matched {ACS1_METRO_GLOB}")

    frames: list[pd.DataFrame] = []
    for file_name in files:
        vintage = _vintage_from_acs1_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=["metro_id", "acs1_vintage", *EMPLOYMENT_STATUS_COLUMNS],
        )
        frame["msa_id"] = _as_msa_id(frame["metro_id"])
        frame["acs1_vintage_used"] = vintage
        frame["year"] = vintage + 1
        frames.append(frame[["msa_id", "year", "acs1_vintage_used", *EMPLOYMENT_STATUS_COLUMNS]])

    return pd.concat(frames, ignore_index=True)


def add_employment_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.copy()
    levels["employed_count"] = pd.to_numeric(
        levels["civilian_labor_force"], errors="coerce"
    ) - pd.to_numeric(levels["unemployed_count"], errors="coerce")
    levels["labor_force_participation_rate"] = _safe_ratio(
        levels["civilian_labor_force"],
        levels["pop_16_plus"],
    )
    levels["employment_to_population_16_plus"] = _safe_ratio(
        levels["employed_count"],
        levels["pop_16_plus"],
    )
    levels["log_pop_16_plus"] = _safe_log(levels["pop_16_plus"])
    levels["log_civilian_labor_force"] = _safe_log(levels["civilian_labor_force"])
    levels["log_employed_count"] = _safe_log(levels["employed_count"])
    levels["log_pop_16_plus_per_panel_person"] = levels["log_pop_16_plus"] - levels["log_pop"]
    levels["log_civilian_labor_force_per_panel_person"] = (
        levels["log_civilian_labor_force"] - levels["log_pop"]
    )
    levels["log_employed_count_per_panel_person"] = levels["log_employed_count"] - levels["log_pop"]
    return levels


def add_first_differences(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_total_rate"] = grouped["log_total_rate"].diff()
    levels["d_log_shelt_rate"] = grouped["log_shelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in [*LABOR_MARKET_GROWTH_COLUMNS, *LABOR_MARKET_RATE_COLUMNS]:
        levels[f"d_{column}"] = grouped[column].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    employment = load_acs1_employment_panel()
    merged = base.merge(employment, on=["msa_id", "year"], how="left")
    return add_first_differences(add_employment_columns(merged))


def _model_specs() -> Iterable[ModelSpec]:
    for column in FD_EMPLOYMENT_COLUMNS:
        yield ModelSpec(
            name=f"rent_fd_{column.removeprefix('d_')}",
            outcome="d_log_zori",
            predictors=("d_log_pop", column),
        )
        yield ModelSpec(
            name=f"unsheltered_fd_{column.removeprefix('d_')}",
            outcome="d_log_unshelt_rate",
            predictors=("d_log_zori", "d_log_pop", column),
        )


def fit_clustered_fd_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [spec.outcome, *spec.predictors, "year", "msa_id"]
        sample = fd.dropna(subset=required).copy()
        if sample.empty:
            continue

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
    complete_fd = fd.dropna(
        subset=[
            "d_log_zori",
            "d_log_unshelt_rate",
            "d_log_pop",
            *FD_EMPLOYMENT_COLUMNS,
        ]
    )
    level_summary = (
        levels[LABOR_MARKET_RATE_COLUMNS + LABOR_MARKET_GROWTH_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        complete_fd[
            [
                "d_log_zori",
                "d_log_unshelt_rate",
                "d_log_pop",
                *FD_EMPLOYMENT_COLUMNS,
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
        "complete_fd_rows_for_labor_market_screens": int(len(complete_fd)),
        "labor_market_level_summary": json.loads(level_summary.to_json()),
        "levels_missing_labor_market_columns": {
            column: int(levels[column].isna().sum())
            for column in [
                *EMPLOYMENT_STATUS_COLUMNS,
                "employed_count",
                *LABOR_MARKET_GROWTH_COLUMNS,
                *LABOR_MARKET_RATE_COLUMNS,
            ]
        },
        "fd_missing_labor_market_diff_columns": {
            column: int(fd[column].isna().sum()) for column in FD_EMPLOYMENT_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "employment_labor_force_composition_levels.parquet"
    fd_path = OUT / "employment_labor_force_composition_fd.parquet"
    regression_path = OUT / "employment_labor_force_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "employment_labor_force_composition_fd_regressions.csv"
    summary_path = OUT / "employment_labor_force_composition_summary.json"

    levels.to_parquet(levels_path, index=False)
    fd.to_parquet(fd_path, index=False)
    regressions.to_parquet(regression_path, index=False)
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
        print(regressions[regressions["term"].isin(FD_EMPLOYMENT_COLUMNS)])


if __name__ == "__main__":
    main()
