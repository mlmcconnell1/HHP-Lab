"""Build pooled top-150 recent-mover income composition panels and FD screens.

ACS1 B07011 reports median income by geographical mobility status, but it does
not publish a single all-movers median. This script therefore keeps the
origin-specific medians and ratios explicit. The primary gentrification-pressure
proxy is the median income of movers from a different state relative to the
overall ACS1 mobility-universe median income.
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

INCOME_COLUMNS = [
    "median_income_mobility_total",
    "median_income_same_house_1_year_ago",
    "median_income_moved_within_county",
    "median_income_moved_diff_county_same_state",
    "median_income_moved_diff_state",
    "median_income_moved_from_abroad",
]
MOVER_INCOME_COLUMNS = [
    column for column in INCOME_COLUMNS if column.startswith("median_income_moved_")
]
MAIN_RATIO_COLUMNS = [
    "moved_within_county_income_ratio_total",
    "moved_diff_county_same_state_income_ratio_total",
    "moved_diff_state_income_ratio_total",
    "moved_from_abroad_income_ratio_total",
]
SAME_HOUSE_RATIO_COLUMNS = [
    "moved_within_county_income_ratio_same_house",
    "moved_diff_county_same_state_income_ratio_same_house",
    "moved_diff_state_income_ratio_same_house",
    "moved_from_abroad_income_ratio_same_house",
]
RATIO_COLUMNS = [*MAIN_RATIO_COLUMNS, *SAME_HOUSE_RATIO_COLUMNS]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]


def _ratio_name(mover_column: str, denominator: str) -> str:
    stem = mover_column.removeprefix("median_income_")
    return f"{stem}_income_ratio_{denominator}"


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype("float64") / denominator.astype("float64").where(denominator > 0)


def load_acs1_recent_mover_income_panel() -> pd.DataFrame:
    files = sorted(glob.glob(ACS1_METRO_GLOB))
    if not files:
        raise FileNotFoundError(f"No ACS1 metro files matched {ACS1_METRO_GLOB}")

    frames: list[pd.DataFrame] = []
    for file_name in files:
        vintage = _vintage_from_acs1_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=["metro_id", "acs1_vintage", *INCOME_COLUMNS],
        )
        frame["msa_id"] = _as_msa_id(frame["metro_id"])
        frame["acs1_vintage_used"] = vintage
        frame["year"] = vintage + 1
        frames.append(frame[["msa_id", "year", "acs1_vintage_used", *INCOME_COLUMNS]])

    income = pd.concat(frames, ignore_index=True)
    for mover_column in MOVER_INCOME_COLUMNS:
        income[_ratio_name(mover_column, "total")] = _safe_ratio(
            income[mover_column],
            income["median_income_mobility_total"],
        )
        income[_ratio_name(mover_column, "same_house")] = _safe_ratio(
            income[mover_column],
            income["median_income_same_house_1_year_ago"],
        )
    return income


def add_first_differences(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_total_rate"] = grouped["log_total_rate"].diff()
    levels["d_log_shelt_rate"] = grouped["log_shelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in [*INCOME_COLUMNS, *RATIO_COLUMNS]:
        levels[f"d_{column}"] = grouped[column].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    income = load_acs1_recent_mover_income_panel()
    merged = base.merge(income, on=["msa_id", "year"], how="left")
    return add_first_differences(merged)


def _model_specs(ratio_columns: Iterable[str] = RATIO_COLUMNS) -> Iterable[ModelSpec]:
    for column in ratio_columns:
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
    main = "moved_diff_state_income_ratio_total"
    complete_fd = fd.dropna(
        subset=["d_log_zori", "d_log_unshelt_rate", "d_log_pop", f"d_{main}"]
    )
    level_ratios = (
        levels[RATIO_COLUMNS]
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
                *[f"d_{column}" for column in MAIN_RATIO_COLUMNS],
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
        "complete_fd_rows_for_diff_state_ratio": int(len(complete_fd)),
        "level_ratio_summary": json.loads(level_ratios.to_json()),
        "levels_missing_income_columns": {
            column: int(levels[column].isna().sum()) for column in INCOME_COLUMNS
        },
        "fd_missing_ratio_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum()) for column in RATIO_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "recent_mover_income_composition_levels.parquet"
    fd_path = OUT / "recent_mover_income_composition_fd.parquet"
    regression_path = OUT / "recent_mover_income_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "recent_mover_income_composition_fd_regressions.csv"
    summary_path = OUT / "recent_mover_income_composition_summary.json"

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
        print(regressions[regressions["term"].str.contains("income_ratio")])


if __name__ == "__main__":
    main()
