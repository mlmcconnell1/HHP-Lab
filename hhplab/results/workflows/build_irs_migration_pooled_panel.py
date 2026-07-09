"""Build pooled IRS migration panels and direct migrant-income rent screens.

This workflow joins the already-curated IRS SOI MSA migration panel to the
pooled top-150 PIT/ZORI panel using the documented IRS alignment convention:
IRS flow year Y is treated as the preceding exposure for PIT year Y + 1.

The primary screened question for this result workflow is whether direct
migrant-income measures such as inflow AGI per return, outflow AGI per return,
or the inflow-minus-outflow AGI gap predict subsequent rent growth on the
pooled top-150 sample.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.results.workflows._paths import REPO_ROOT

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "irs_migration_pooled"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
IRS_COVARIATE_PANEL = (
    ROOT
    / "data"
    / "curated"
    / "covariates"
    / "covariate_panel__irs_soi_migration__Y2012-2023.parquet"
)

# PIT years usable given rank-51-150 starts 2015 and IRS (shifted +1) tops
# out at PIT year 2024 (IRS year 2023).
PANEL_YEARS = {2015, 2016, 2017, 2018, 2019, 2020, 2022, 2023, 2024}

CORE_COLUMNS = [
    "msa_id",
    "msa_name",
    "year",
    "population",
    "sanctuary",
    "pit_unsheltered",
    "unshelt_per_1000",
    "zori",
    "log_zori",
    "log_unshelt_rate",
    "log_pop",
]
DIRECT_INCOME_COLUMNS = [
    "inflow_agi_per_return_k",
    "outflow_agi_per_return_k",
    "churn_agi_per_return_k",
    "inflow_outflow_agi_gap_k",
]
LEVEL_SUMMARY_COLUMNS = [
    "inflow_rate",
    "outflow_rate",
    "churn_rate",
    "net_rate",
    *DIRECT_INCOME_COLUMNS,
]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]
    family: str
    fixed_effects: tuple[str, ...]


def primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce").astype("float64")
    right = pd.to_numeric(denominator, errors="coerce").astype("float64")
    return left / right.where(right > 0)


def _safe_log_positive(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    result = pd.Series(np.nan, index=numeric.index, dtype="float64")
    mask = numeric > 0
    result.loc[mask] = np.log(numeric.loc[mask])
    return result


def _census_region_or_na(state: object) -> object:
    try:
        return census_region(state)
    except ValueError:
        return pd.NA


def load_pooled_base_panel() -> pd.DataFrame:
    top50 = pd.read_parquet(TOP50_PANEL)[CORE_COLUMNS].copy()
    top50["cohort"] = "top50"
    rank51_150 = pd.read_parquet(RANK51_150_PANEL)[CORE_COLUMNS].copy()
    rank51_150["cohort"] = "rank51_150"
    overlap = set(top50.msa_id) & set(rank51_150.msa_id)
    if overlap:
        raise ValueError(f"Unexpected msa_id overlap between cohorts: {overlap}")
    pooled = pd.concat([top50, rank51_150], ignore_index=True)
    pooled = pooled[pooled.year.isin(PANEL_YEARS)].sort_values(["msa_id", "year"])
    pooled["primary_state"] = pooled["msa_name"].map(primary_state)
    return pooled.reset_index(drop=True)


def merge_irs_migration(base: pd.DataFrame) -> pd.DataFrame:
    irs = pd.read_parquet(IRS_COVARIATE_PANEL)
    irs = irs[irs.geo_type == "msa"][
        [
            "msa_id",
            "year",
            "inflow_returns",
            "inflow_agi_thousands",
            "outflow_returns",
            "outflow_agi_thousands",
            "net_returns",
            "net_agi_thousands",
            "intra_msa_returns",
            "intra_msa_agi_thousands",
            "coverage_ratio",
        ]
    ].rename(columns={"year": "irs_year", "coverage_ratio": "irs_coverage_ratio"})
    # IRS year = PIT year - 1 by project convention.
    irs["year"] = irs["irs_year"] + 1

    merged = base.merge(irs, on=["msa_id", "year"], how="left")
    merged["inflow_rate"] = merged["inflow_returns"] / merged["population"] * 1000
    merged["outflow_rate"] = merged["outflow_returns"] / merged["population"] * 1000
    merged["churn_rate"] = merged["inflow_rate"] + merged["outflow_rate"]
    merged["net_rate"] = merged["inflow_rate"] - merged["outflow_rate"]
    merged["inflow_agi_per_return_k"] = _safe_ratio(
        merged["inflow_agi_thousands"],
        merged["inflow_returns"],
    )
    merged["outflow_agi_per_return_k"] = _safe_ratio(
        merged["outflow_agi_thousands"],
        merged["outflow_returns"],
    )
    merged["churn_agi_per_return_k"] = _safe_ratio(
        merged["inflow_agi_thousands"] + merged["outflow_agi_thousands"],
        merged["inflow_returns"] + merged["outflow_returns"],
    )
    merged["inflow_outflow_agi_gap_k"] = (
        merged["inflow_agi_per_return_k"] - merged["outflow_agi_per_return_k"]
    )
    for column in ("inflow_rate", "outflow_rate"):
        merged[f"log_{column.replace('_rate', '')}_rate"] = _safe_log_positive(merged[column])
    return merged


def add_diffs(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    if "primary_state" not in levels.columns and "msa_name" in levels.columns:
        levels["primary_state"] = levels["msa_name"].map(primary_state)
    levels["region"] = levels["primary_state"].map(_census_region_or_na)
    levels["primary_state_year"] = (
        levels["primary_state"].astype("string") + "_" + levels["year"].astype("Int64").astype("string")
    )
    levels["region_year"] = (
        levels["region"].astype("string") + "_" + levels["year"].astype("Int64").astype("string")
    )

    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["d_net_rate"] = grouped["net_rate"].diff()
    levels["d_log_inflow_rate"] = grouped["log_inflow_rate"].diff()
    levels["d_log_outflow_rate"] = grouped["log_outflow_rate"].diff()
    levels["d_log_zori_x_churn_rate"] = levels["d_log_zori"] * levels["churn_rate"]
    return levels


def build_levels_panel() -> pd.DataFrame:
    pooled = load_pooled_base_panel()
    merged = merge_irs_migration(pooled)
    return add_diffs(merged)


def _model_specs() -> Iterable[ModelSpec]:
    fixed_effect_variants = (
        ("year", "year_fe"),
        ("region_year", "region_year_fe"),
        ("primary_state_year", "state_year_fe"),
    )
    for column in DIRECT_INCOME_COLUMNS:
        for fixed_effect, label in fixed_effect_variants:
            yield ModelSpec(
                name=f"rent_fd_{column}_{label}",
                outcome="d_log_zori",
                predictors=("d_log_pop", column),
                family="direct_income_channel",
                fixed_effects=(fixed_effect,),
            )
    for fixed_effect, label in fixed_effect_variants:
        yield ModelSpec(
            name=f"rent_fd_inflow_outflow_agi_per_return_joint_{label}",
            outcome="d_log_zori",
            predictors=("d_log_pop", "inflow_agi_per_return_k", "outflow_agi_per_return_k"),
            family="direct_income_joint_channel",
            fixed_effects=(fixed_effect,),
        )


def fit_clustered_models(fd: pd.DataFrame) -> pd.DataFrame:
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
        x = pd.concat([sample[list(spec.predictors)].astype("float64"), *fe_parts], axis=1)
        x = sm.add_constant(x, has_constant="add")
        y = sample[spec.outcome].astype("float64")
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
                    "fixed_effects": "+".join(spec.fixed_effects),
                    "term": term,
                    "estimate": float(result.params[term]),
                    "std_error": float(result.bse[term]),
                    "t_stat": float(result.tvalues[term]),
                    "p_value": float(result.pvalues[term]),
                    "nobs": int(result.nobs),
                    "clusters": int(sample["msa_id"].nunique()),
                    "r_squared": float(result.rsquared),
                    "std_error_type": "clustered:msa_id",
                }
            )
    return pd.DataFrame(rows)


def summarize_panel(levels: pd.DataFrame, fd: pd.DataFrame) -> dict[str, object]:
    main_column = "inflow_agi_per_return_k"
    complete_fd = fd.dropna(subset=["d_log_zori", "d_log_pop", main_column])
    level_summary = (
        levels[LEVEL_SUMMARY_COLUMNS]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        complete_fd[["d_log_zori", "d_log_pop", *DIRECT_INCOME_COLUMNS]]
        .corr()
        .round(6)
    )
    sample_sizes = {
        spec.name: int(len(fd.dropna(subset=[spec.outcome, *spec.predictors, *spec.fixed_effects, "msa_id"])))
        for spec in _model_specs()
    }
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "primary_model_family": "direct_income_channel",
        "rent_model_fixed_effects": ["year", "region_year", "primary_state_year"],
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "irs_years_used": [int(year) for year in sorted(levels["irs_year"].dropna().unique())],
        "complete_fd_rows_for_inflow_agi_per_return_k": int(len(complete_fd)),
        "timing_sample_sizes": sample_sizes,
        "irs_migration_level_summary": json.loads(level_summary.to_json()),
        "levels_missing_direct_income_columns": {
            column: int(levels[column].isna().sum()) for column in DIRECT_INCOME_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "irs_migration_pooled_levels.parquet"
    fd_path = OUT / "irs_migration_pooled_fd.parquet"
    regression_path = OUT / "irs_migration_pooled_regressions.parquet"
    regression_csv_path = OUT / "irs_migration_pooled_regressions.csv"
    summary_path = OUT / "irs_migration_pooled_summary.json"

    levels.to_parquet(levels_path, index=False)
    fd.to_parquet(fd_path, index=False)
    regressions.to_parquet(regression_path, index=False)
    regressions.to_csv(regression_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"pooled cohorts: {levels.cohort.value_counts().to_dict()}")
    print(f"levels rows: {len(levels)} -> {levels_path}")
    print(f"fd rows: {len(fd)} -> {fd_path}")
    print(f"regression rows: {len(regressions)} -> {regression_path}")
    print(f"summary -> {summary_path}")
    if not regressions.empty:
        print(regressions[regressions["term"].isin(DIRECT_INCOME_COLUMNS)])


if __name__ == "__main__":
    main()
