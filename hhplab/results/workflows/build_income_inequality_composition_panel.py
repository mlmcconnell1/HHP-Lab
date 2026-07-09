"""Build pooled top-150 income-inequality panels and rent-growth screens.

This workflow uses the ACS5 tract-derived ``gini_index`` measure already
materialized into curated MSA outputs. It follows the standard ACS lag rule:
ACS5 vintage end year E is aligned to PIT year E + 1.
"""

from __future__ import annotations

import glob
import json
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows.build_household_size_composition_panel import (
    OUT,
    _as_msa_id,
    load_pooled_base_panel,
)
from hhplab.results.workflows.build_household_size_composition_panel import (
    ROOT as _ROOT,
)
from hhplab.results.workflows.build_renter_household_share_composition_panel import (
    MEASURES_GLOB,
    _acs_end_year_from_measures_path,
)

INEQUALITY_COLUMNS = ["gini_index"]
ROOT = _ROOT


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]


def load_acs5_income_inequality_panel() -> pd.DataFrame:
    files = sorted(glob.glob(MEASURES_GLOB))
    if not files:
        raise FileNotFoundError(f"No MSA ACS5 measure files matched {MEASURES_GLOB}")

    output_columns = ["msa_id", "year", "acs5_vintage_used", *INEQUALITY_COLUMNS]
    frames: list[pd.DataFrame] = []
    for file_name in files:
        acs_end_year = _acs_end_year_from_measures_path(file_name)
        frame = pd.read_parquet(
            file_name,
            columns=["msa_id", "acs_vintage", *INEQUALITY_COLUMNS],
        )
        if frame.empty:
            continue
        frame["msa_id"] = _as_msa_id(frame["msa_id"])
        frame["gini_index"] = pd.to_numeric(frame["gini_index"], errors="coerce").astype("float64")
        frame["acs5_vintage_used"] = acs_end_year
        frame["year"] = acs_end_year + 1
        frames.append(frame[output_columns])

    if not frames:
        return pd.DataFrame(columns=output_columns)
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
    for column in INEQUALITY_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    inequality = load_acs5_income_inequality_panel()
    merged = base.merge(inequality, on=["msa_id", "year"], how="left")
    return add_first_differences(merged)


def _model_specs() -> Iterable[ModelSpec]:
    for column in INEQUALITY_COLUMNS:
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
    main_column = "gini_index"
    complete_fd = fd.dropna(
        subset=["d_log_zori", "d_log_unshelt_rate", "d_log_pop", "d_gini_index"]
    )
    level_summary = (
        levels[INEQUALITY_COLUMNS]
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
                f"d_{main_column}",
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
        "complete_fd_rows_for_gini_index": int(len(complete_fd)),
        "level_inequality_summary": json.loads(level_summary.to_json()),
        "levels_missing_inequality_columns": {
            column: int(levels[column].isna().sum()) for column in INEQUALITY_COLUMNS
        },
        "fd_missing_inequality_growth_columns": {
            f"d_{column}": int(fd[f"d_{column}"].isna().sum()) for column in INEQUALITY_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "income_inequality_composition_levels.parquet"
    fd_path = OUT / "income_inequality_composition_fd.parquet"
    regression_path = OUT / "income_inequality_composition_fd_regressions.parquet"
    regression_csv_path = OUT / "income_inequality_composition_fd_regressions.csv"
    summary_path = OUT / "income_inequality_composition_summary.json"

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
        print(regressions[regressions["term"].str.contains("gini_index")])


if __name__ == "__main__":
    main()
