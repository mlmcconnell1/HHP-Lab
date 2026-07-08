"""Build pooled top-150 renter-household share panels and FD screens.

ACS5 tract B25003 is already materialized into MSA measures. This script uses
those existing curated outputs and applies the standard ACS lag convention:
ACS5 vintage end year E is used for PIT year E + 1.
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
    OUT,
    ROOT,
    _as_msa_id,
    load_pooled_base_panel,
)

MEASURES_GLOB = str(ROOT / "data" / "curated" / "measures" / "measures__msa__A*.parquet")

ACS5_COUNT_COLUMNS = [
    "acs_total_population",
    "total_households",
    "owner_households",
    "renter_households",
]
COMPOSITION_COLUMNS = [
    "renter_households_per_acs_person",
    "renter_households_per_panel_person",
    "renter_household_share",
]
LEVEL_SUMMARY_COLUMNS = [
    *COMPOSITION_COLUMNS,
    "owner_household_share",
    "renter_to_owner_household_ratio",
]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]


def _acs_end_year_from_measures_path(path: str) -> int:
    return int(path.split("__A", maxsplit=1)[1].split("@", maxsplit=1)[0])


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype("float64") / denominator.astype("float64").where(denominator > 0)


def _safe_log(series: pd.Series) -> pd.Series:
    values = series.astype("float64").where(series > 0)
    return pd.Series(np.log(values), index=series.index)


def load_acs5_renter_household_panel() -> pd.DataFrame:
    files = sorted(glob.glob(MEASURES_GLOB))
    if not files:
        raise FileNotFoundError(f"No MSA ACS5 measure files matched {MEASURES_GLOB}")

    frames: list[pd.DataFrame] = []
    for file_name in files:
        acs_end_year = _acs_end_year_from_measures_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=[
                "msa_id",
                "acs_vintage",
                "total_population",
                "total_households",
                "owner_households",
                "renter_households",
            ],
        )
        frame["msa_id"] = _as_msa_id(frame["msa_id"])
        frame["acs5_vintage_used"] = acs_end_year
        frame["year"] = acs_end_year + 1
        frame = frame.rename(columns={"total_population": "acs_total_population"})
        frames.append(frame[["msa_id", "year", "acs5_vintage_used", *ACS5_COUNT_COLUMNS]])

    return pd.concat(frames, ignore_index=True)


def add_composition_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.copy()
    levels["renter_households_per_acs_person"] = _safe_ratio(
        levels["renter_households"],
        levels["acs_total_population"],
    )
    levels["renter_households_per_panel_person"] = _safe_ratio(
        levels["renter_households"],
        levels["population"],
    )
    levels["renter_household_share"] = _safe_ratio(
        levels["renter_households"],
        levels["total_households"],
    )
    levels["owner_household_share"] = _safe_ratio(
        levels["owner_households"],
        levels["total_households"],
    )
    levels["renter_to_owner_household_ratio"] = _safe_ratio(
        levels["renter_households"],
        levels["owner_households"],
    )
    levels["log_renter_households"] = _safe_log(levels["renter_households"])
    levels["log_total_households"] = _safe_log(levels["total_households"])
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
    for column in [*ACS5_COUNT_COLUMNS, *LEVEL_SUMMARY_COLUMNS]:
        levels[f"d_{column}"] = grouped[column].diff()
    levels["d_log_renter_households"] = grouped["log_renter_households"].diff()
    levels["d_log_total_households"] = grouped["log_total_households"].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    renter_households = load_acs5_renter_household_panel()
    merged = base.merge(renter_households, on=["msa_id", "year"], how="left")
    merged = add_composition_columns(merged)
    return add_first_differences(merged)


def _model_specs(composition_columns: Iterable[str] = COMPOSITION_COLUMNS) -> Iterable[ModelSpec]:
    for column in composition_columns:
        diff_column = f"d_{column}"
        yield ModelSpec(
            name=f"rent_fd_{column}",
            outcome="d_log_zori",
            predictors=("d_log_pop", diff_column),
        )
        yield ModelSpec(
            name=f"unsheltered_fd_{column}",
            outcome="d_log_unshelt_rate",
            predictors=("d_log_zori", "d_log_pop", diff_column),
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
    main = "renter_household_share"
    complete_fd = fd.dropna(
        subset=["d_log_zori", "d_log_unshelt_rate", "d_log_pop", f"d_{main}"]
    )
    level_summary = (
        levels[LEVEL_SUMMARY_COLUMNS]
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
                *[f"d_{column}" for column in COMPOSITION_COLUMNS],
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
        "acs5_vintages_used": [
            int(vintage) for vintage in sorted(levels["acs5_vintage_used"].dropna().unique())
        ],
        "complete_fd_rows_for_renter_household_share": int(len(complete_fd)),
        "level_composition_summary": json.loads(level_summary.to_json()),
        "levels_missing_acs5_counts": {
            column: int(levels[column].isna().sum()) for column in ACS5_COUNT_COLUMNS
        },
        "fd_missing_composition_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum())
            for column in COMPOSITION_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "renter_household_share_composition_levels.parquet"
    fd_path = OUT / "renter_household_share_composition_fd.parquet"
    regression_path = OUT / "renter_household_share_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "renter_household_share_composition_fd_regressions.csv"
    summary_path = OUT / "renter_household_share_composition_summary.json"

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
        print(regressions[regressions["term"].str.contains("renter_household")])


if __name__ == "__main__":
    main()
