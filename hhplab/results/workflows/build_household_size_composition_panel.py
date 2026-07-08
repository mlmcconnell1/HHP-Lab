"""Build pooled top-150 household-size composition panels and FD screens.

This tests whether ACS1 B25010 average household-size shifts add explanatory
power for rent growth or unsheltered-rate growth beyond population changes.
ACS1 vintages are PIT-aligned with the standard lag convention:
ACS1 vintage E is used for PIT year E + 1.
"""

from __future__ import annotations

import glob
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows._paths import REPO_ROOT

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "composition_rent_population"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
ACS1_METRO_GLOB = str(ROOT / "data" / "curated" / "acs" / "acs1_metro__A*@Dcensusmsa2023.parquet")

EXCLUDED_YEARS = {2021}
HOUSEHOLD_SIZE_COLUMNS = [
    "average_household_size_total",
    "average_household_size_owner_occupied",
    "average_household_size_renter_occupied",
]
CORE_COLUMNS = [
    "msa_id",
    "msa_name",
    "year",
    "population",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "unshelt_per_1000",
    "zori",
    "log_zori",
    "log_unshelt_rate",
    "log_total_rate",
    "log_shelt_rate",
    "log_pop",
]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]


def _as_msa_id(series: pd.Series) -> pd.Series:
    return series.astype("string").str.zfill(5)


def _primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def load_pooled_base_panel() -> pd.DataFrame:
    top50 = pd.read_parquet(TOP50_PANEL)
    top50 = top50[[column for column in CORE_COLUMNS if column in top50.columns]].copy()
    top50["cohort"] = "top50"

    rank51_150 = pd.read_parquet(RANK51_150_PANEL)
    rank51_150 = rank51_150[
        [column for column in CORE_COLUMNS if column in rank51_150.columns]
    ].copy()
    rank51_150["cohort"] = "rank51_150"

    overlap = set(_as_msa_id(top50["msa_id"])) & set(_as_msa_id(rank51_150["msa_id"]))
    if overlap:
        raise ValueError(f"Unexpected msa_id overlap between cohorts: {sorted(overlap)[:5]}")

    pooled = pd.concat([top50, rank51_150], ignore_index=True)
    pooled["msa_id"] = _as_msa_id(pooled["msa_id"])
    pooled = pooled[~pooled["year"].isin(EXCLUDED_YEARS)].copy()
    pooled["primary_state"] = pooled["msa_name"].map(_primary_state)
    return pooled.sort_values(["msa_id", "year"]).reset_index(drop=True)


def _vintage_from_acs1_path(path: str | Path) -> int:
    name = Path(path).name
    return int(name.split("__A", maxsplit=1)[1].split("@", maxsplit=1)[0])


def load_acs1_household_size_panel() -> pd.DataFrame:
    files = sorted(glob.glob(ACS1_METRO_GLOB))
    if not files:
        raise FileNotFoundError(f"No ACS1 metro files matched {ACS1_METRO_GLOB}")

    frames: list[pd.DataFrame] = []
    for file_name in files:
        vintage = _vintage_from_acs1_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=[
                "metro_id",
                "acs1_vintage",
                *HOUSEHOLD_SIZE_COLUMNS,
            ],
        )
        frame["msa_id"] = _as_msa_id(frame["metro_id"])
        frame["acs1_vintage_used"] = vintage
        frame["year"] = vintage + 1
        frames.append(frame[["msa_id", "year", "acs1_vintage_used", *HOUSEHOLD_SIZE_COLUMNS]])

    return pd.concat(frames, ignore_index=True)


def add_first_differences(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_total_rate"] = grouped["log_total_rate"].diff()
    levels["d_log_shelt_rate"] = grouped["log_shelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in HOUSEHOLD_SIZE_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    household_size = load_acs1_household_size_panel()
    merged = base.merge(household_size, on=["msa_id", "year"], how="left")
    return add_first_differences(merged)


def _model_specs() -> Iterable[ModelSpec]:
    for column in HOUSEHOLD_SIZE_COLUMNS:
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
    complete_fd = fd.dropna(
        subset=[
            "d_log_zori",
            "d_log_unshelt_rate",
            "d_log_pop",
            "d_average_household_size_renter_occupied",
        ]
    )
    correlations = (
        complete_fd[
            [
                "d_log_zori",
                "d_log_unshelt_rate",
                "d_log_pop",
                *[f"d_{column}" for column in HOUSEHOLD_SIZE_COLUMNS],
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
        "complete_fd_rows_for_renter_size": int(len(complete_fd)),
        "levels_missing_household_size": {
            column: int(levels[column].isna().sum()) for column in HOUSEHOLD_SIZE_COLUMNS
        },
        "fd_missing_household_size_diff": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum())
            for column in HOUSEHOLD_SIZE_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "household_size_composition_levels.parquet"
    fd_path = OUT / "household_size_composition_fd.parquet"
    regression_path = OUT / "household_size_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "household_size_composition_fd_regressions.csv"
    summary_path = OUT / "household_size_composition_summary.json"

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
        print(regressions[regressions["term"].str.startswith("d_average_household_size")])


if __name__ == "__main__":
    main()
