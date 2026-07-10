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
from hhplab.results.workflows._paths import DATA_ROOT, OUTPUTS_ROOT, REPO_ROOT, write_result_parquet
from hhplab.results.workflows.build_household_size_composition_panel import (
    load_pooled_base_panel as load_shared_pooled_base_panel,
)

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "irs_migration_pooled"

IRS_COVARIATE_PANEL = (
    DATA_ROOT / "curated" / "covariates" / "covariate_panel__irs_soi_migration__Y2012-2023.parquet"
)
SANCTUARY_PANEL = (
    DATA_ROOT / "curated/sanctuary/sanctuary_msa_panel__D20250805xMcensus_msa_2023.parquet"
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


@dataclass(frozen=True)
class SampleFilter:
    name: str
    description: str


OUTFLOW_ROBUSTNESS_FILTERS = (
    SampleFilter(
        name="full_sample",
        description="Unfiltered state-year FE outflow AGI-per-return sample.",
    ),
    SampleFilter(
        name="drop_negative_outflow_agi",
        description="Drop rows with negative outflow AGI per return.",
    ),
    SampleFilter(
        name="trim_outflow_agi_1_99",
        description=(
            "Keep rows between the sample 1st and 99th percentiles of outflow AGI per return."
        ),
    ),
    SampleFilter(
        name="exclude_2020",
        description="Exclude PIT/ZORI year 2020.",
    ),
    SampleFilter(
        name="exclude_sf_san_jose",
        description="Exclude San Francisco-Oakland and San Jose-Sunnyvale-Santa Clara MSAs.",
    ),
    SampleFilter(
        name="exclude_2020_and_sf_san_jose",
        description="Exclude PIT/ZORI year 2020 and the San Francisco/San Jose MSAs.",
    ),
)


OUTFLOW_ROBUSTNESS_MODEL = ModelSpec(
    name="rent_fd_outflow_agi_per_return_k_state_year_fe",
    outcome="d_log_zori",
    predictors=("d_log_pop", "outflow_agi_per_return_k"),
    family="direct_income_outflow_robustness",
    fixed_effects=("primary_state_year",),
)


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
    pooled = load_shared_pooled_base_panel()
    sanctuary = pd.read_parquet(SANCTUARY_PANEL, columns=["msa_id", "doj_sanctuary_msa"])
    sanctuary["msa_id"] = sanctuary["msa_id"].astype("string").str.zfill(5)
    sanctuary = sanctuary.rename(columns={"doj_sanctuary_msa": "sanctuary"})
    pooled = pooled.merge(sanctuary, on="msa_id", how="left", validate="many_to_one")
    pooled["sanctuary"] = pooled["sanctuary"].fillna(0).astype("int64")
    pooled = pooled[pooled.year.isin(PANEL_YEARS)].sort_values(["msa_id", "year"])
    pooled["primary_state"] = pooled["msa_name"].map(primary_state)
    return pooled[[*CORE_COLUMNS, "cohort", "primary_state"]].reset_index(drop=True)


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
        levels["primary_state"].astype("string")
        + "_"
        + levels["year"].astype("Int64").astype("string")
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


def _fit_model_rows(
    sample: pd.DataFrame,
    *,
    spec: ModelSpec,
    sample_filter: SampleFilter | None = None,
) -> list[dict[str, object]]:
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

    rows: list[dict[str, object]] = []
    for term in spec.predictors:
        row = {
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
        if sample_filter is not None:
            row["sample_filter"] = sample_filter.name
            row["sample_filter_description"] = sample_filter.description
        rows.append(row)
    return rows


def _complete_model_sample(fd: pd.DataFrame, spec: ModelSpec) -> pd.DataFrame:
    required = [spec.outcome, *spec.predictors, *spec.fixed_effects, "msa_id"]
    return fd.dropna(subset=required).copy()


def fit_clustered_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        sample = _complete_model_sample(fd, spec)
        if sample.empty:
            continue
        rows.extend(_fit_model_rows(sample, spec=spec))
    return pd.DataFrame(rows)


def _is_sf_or_san_jose(sample: pd.DataFrame) -> pd.Series:
    msa_name = sample["msa_name"].astype("string").str.lower()
    return msa_name.str.contains("san francisco-oakland", na=False) | msa_name.str.contains(
        "san jose-sunnyvale-santa clara",
        na=False,
    )


def apply_outflow_robustness_filter(
    sample: pd.DataFrame,
    sample_filter: SampleFilter,
) -> pd.DataFrame:
    if sample_filter.name == "full_sample":
        return sample.copy()
    if sample_filter.name == "drop_negative_outflow_agi":
        return sample[sample["outflow_agi_per_return_k"] >= 0].copy()
    if sample_filter.name == "trim_outflow_agi_1_99":
        lower = sample["outflow_agi_per_return_k"].quantile(0.01)
        upper = sample["outflow_agi_per_return_k"].quantile(0.99)
        return sample[sample["outflow_agi_per_return_k"].between(lower, upper)].copy()
    if sample_filter.name == "exclude_2020":
        return sample[sample["year"] != 2020].copy()
    if sample_filter.name == "exclude_sf_san_jose":
        return sample[~_is_sf_or_san_jose(sample)].copy()
    if sample_filter.name == "exclude_2020_and_sf_san_jose":
        return sample[(sample["year"] != 2020) & ~_is_sf_or_san_jose(sample)].copy()
    raise ValueError(f"Unknown IRS outflow robustness sample filter: {sample_filter.name}")


def fit_outflow_robustness_models(fd: pd.DataFrame) -> pd.DataFrame:
    base_sample = _complete_model_sample(fd, OUTFLOW_ROBUSTNESS_MODEL)
    rows: list[dict[str, object]] = []
    for sample_filter in OUTFLOW_ROBUSTNESS_FILTERS:
        sample = apply_outflow_robustness_filter(base_sample, sample_filter)
        if sample.empty:
            continue
        rows.extend(
            _fit_model_rows(
                sample,
                spec=OUTFLOW_ROBUSTNESS_MODEL,
                sample_filter=sample_filter,
            )
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
    correlations = complete_fd[["d_log_zori", "d_log_pop", *DIRECT_INCOME_COLUMNS]].corr().round(6)
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


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_models(fd)
    robustness = fit_outflow_robustness_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "irs_migration_pooled_levels.parquet"
    fd_path = OUT / "irs_migration_pooled_fd.parquet"
    regression_path = OUT / "irs_migration_pooled_regressions.parquet"
    regression_csv_path = OUT / "irs_migration_pooled_regressions.csv"
    robustness_path = OUT / "irs_migration_pooled_outflow_robustness.parquet"
    robustness_csv_path = OUT / "irs_migration_pooled_outflow_robustness.csv"
    summary_path = OUT / "irs_migration_pooled_summary.json"

    write_result_parquet(levels, levels_path, index=False)
    write_result_parquet(fd, fd_path, index=False)
    write_result_parquet(regressions, regression_path, index=False)
    regressions.to_csv(regression_csv_path, index=False)
    write_result_parquet(robustness, robustness_path, index=False)
    robustness.to_csv(robustness_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return {
        "pooled_cohorts": levels.cohort.value_counts().to_dict(),
        "summary": summary,
        "regressions": json.loads(regressions.to_json(orient="records")),
        "outflow_robustness": json.loads(robustness.to_json(orient="records")),
        "outputs": {
            "levels_parquet": str(levels_path),
            "fd_parquet": str(fd_path),
            "regressions_parquet": str(regression_path),
            "regressions_csv": str(regression_csv_path),
            "outflow_robustness_parquet": str(robustness_path),
            "outflow_robustness_csv": str(robustness_csv_path),
            "summary_json": str(summary_path),
        },
    }


def main() -> None:
    result = run()
    summary = result["summary"]
    outputs = result["outputs"]
    regressions = pd.DataFrame(result["regressions"])

    print(f"pooled cohorts: {result['pooled_cohorts']}")
    print(f"levels rows: {summary['levels_rows']} -> {outputs['levels_parquet']}")
    print(f"fd rows: {summary['fd_rows_year_gap_1']} -> {outputs['fd_parquet']}")
    print(f"regression rows: {len(regressions)} -> {outputs['regressions_parquet']}")
    print(
        "outflow robustness rows: "
        f"{len(result['outflow_robustness'])} -> {outputs['outflow_robustness_parquet']}"
    )
    print(f"summary -> {outputs['summary_json']}")
    if not regressions.empty:
        print(regressions[regressions["term"].isin(DIRECT_INCOME_COLUMNS)])


if __name__ == "__main__":
    main()
