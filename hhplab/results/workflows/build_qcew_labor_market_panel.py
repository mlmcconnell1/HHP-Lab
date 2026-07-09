"""Build pooled top-150 QCEW labor-market screens for rent growth.

This workflow uses BLS QCEW county-native annual totals (Total Covered
ownership, all-industries), aggregated to MSA through the registered `qcew`
covariate pipeline. It tests employment growth and real wage growth as
rent-growth channels, alongside their nominal counterparts for comparison,
using this project's standard 3-tier fixed-effect robustness ladder (year,
census-region x year, primary-state x year) rather than a single year-FE
specification.

BLS's public per-area annual industry file (`industry/10.csv`) only serves
2014 onward for this project's ingest path, so the pooled top-150 base panel
(top50 2010-2025, rank51-150 2015-2025) is truncated to 2014-2025 here -- an
availability ceiling, not a build failure.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.covariates.aggregate import aggregate_covariate_source
from hhplab.covariates.ingest import default_covariate_output_path
from hhplab.naming import covariate_panel_filename
from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT, write_result_parquet
from hhplab.results.workflows.build_household_size_composition_panel import (
    _as_msa_id,
    load_pooled_base_panel,
)

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "qcew_labor_market"

QCEW_SOURCE_ID = "qcew"
QCEW_START_YEAR = 2014
QCEW_END_YEAR = 2025
QCEW_PANEL_PATH_ENV = "HHPLAB_QCEW_PANEL_PATH"
QCEW_CURATED_PATH_ENV = "HHPLAB_QCEW_CURATED_PATH"
QCEW_DOWNLOAD_URL_TEMPLATE = "https://data.bls.gov/cew/data/api/{year}/a/industry/10.csv"
QCEW_MIN_COVERAGE_RATIO = 1.0

CPI_U_PATH = REPO_ROOT / "data" / "curated" / "cpi" / "cpi_u__Aall.parquet"

QCEW_LEVEL_COLUMNS = (
    "qcew_annual_avg_emplvl",
    "qcew_total_annual_wages",
    "qcew_annual_avg_weekly_wage",
)
# Nominal counterparts are computed (needed to derive the real columns) but not modeled
# separately: CPI-U has no cross-MSA variation, so under any spec that already includes
# year (or state x year / region x year) fixed effects, subtracting it from the outcome
# is absorbed exactly by the fixed effects and cannot change the wage-growth coefficient.
NOMINAL_GROWTH_COLUMNS = ("d_log_qcew_total_annual_wages", "d_log_qcew_annual_avg_weekly_wage")
REAL_GROWTH_COLUMNS = (
    "d_log_qcew_total_annual_wages_real",
    "d_log_qcew_annual_avg_weekly_wage_real",
)
EMPLOYMENT_GROWTH_COLUMN = "d_log_qcew_annual_avg_emplvl"

PRIMARY_CHANNEL_COLUMNS = (EMPLOYMENT_GROWTH_COLUMN, *REAL_GROWTH_COLUMNS)


@dataclass(frozen=True)
class RegressionSpec:
    family: str
    model: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    focal_term: str


def _safe_log(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log(numeric.where(numeric > 0))


def ensure_qcew_msa_panel(years: list[int]) -> Path:
    panel_override = os.environ.get(QCEW_PANEL_PATH_ENV)
    panel_path = (
        Path(panel_override)
        if panel_override
        else REPO_ROOT
        / "data"
        / "curated"
        / "covariates"
        / covariate_panel_filename(QCEW_SOURCE_ID, min(years), max(years))
    )
    if panel_path.exists():
        return panel_path

    curated_override = os.environ.get(QCEW_CURATED_PATH_ENV)
    curated_path = (
        Path(curated_override)
        if curated_override
        else default_covariate_output_path(QCEW_SOURCE_ID)
    )
    if not curated_path.exists():
        sample_url = QCEW_DOWNLOAD_URL_TEMPLATE.format(year=max(years))
        raise FileNotFoundError(
            "QCEW curated covariate is missing. Download official BLS QCEW county annual "
            f"industry-total files (e.g. {sample_url} for each target year), then run "
            "`uv run hhplab ingest covariate --source qcew --raw-path <dir-of-yearly-files>` "
            "followed by "
            "`uv run hhplab aggregate covariate --source qcew --target-geo msa "
            f"--years {min(years)}-{max(years)}`."
        )

    return aggregate_covariate_source(
        QCEW_SOURCE_ID,
        curated_path=curated_path,
        years=years,
        target_geo="msa",
        output_path=panel_path if panel_override else None,
        force=bool(panel_override),
    )


def load_qcew_msa_panel(*, years: list[int]) -> pd.DataFrame:
    panel_path = ensure_qcew_msa_panel(years)
    frame = pd.read_parquet(panel_path)
    if "geo_type" in frame.columns:
        frame = frame[frame["geo_type"].eq("msa")].copy()
    if "msa_id" not in frame.columns:
        frame["msa_id"] = frame["geo_id"]

    required = {
        "msa_id",
        "year",
        "annual_avg_emplvl",
        "total_annual_wages",
        "annual_avg_weekly_wage",
        "coverage_ratio",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"QCEW MSA panel {panel_path} is missing required columns: {missing}")

    frame["msa_id"] = _as_msa_id(frame["msa_id"])
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    filtered = frame[frame["year"].isin(years)].copy()

    below_min_coverage = int((filtered["coverage_ratio"] < QCEW_MIN_COVERAGE_RATIO).sum())
    complete_coverage = filtered[filtered["coverage_ratio"] >= QCEW_MIN_COVERAGE_RATIO].copy()

    renamed = complete_coverage.rename(
        columns={
            "annual_avg_emplvl": "qcew_annual_avg_emplvl",
            "total_annual_wages": "qcew_total_annual_wages",
            "annual_avg_estabs": "qcew_annual_avg_estabs",
            "annual_avg_weekly_wage": "qcew_annual_avg_weekly_wage",
            "avg_annual_pay": "qcew_avg_annual_pay",
            "coverage_ratio": "qcew_coverage_ratio",
        }
    )
    selected = [
        "msa_id",
        "year",
        "qcew_annual_avg_emplvl",
        "qcew_total_annual_wages",
        "qcew_annual_avg_weekly_wage",
        "qcew_coverage_ratio",
    ]
    renamed.attrs["dropped_partial_coverage_row_count"] = below_min_coverage
    return renamed[selected].sort_values(["msa_id", "year"]).reset_index(drop=True)


def load_cpi_u_index() -> pd.DataFrame:
    if not CPI_U_PATH.exists():
        raise FileNotFoundError(
            f"CPI-U curated index not found at {CPI_U_PATH}. Run "
            "`uv run hhplab ingest cpi-u --start-year <start> --end-year <end>` first."
        )
    cpi = pd.read_parquet(CPI_U_PATH, columns=["year", "cpi_u"])
    cpi["year"] = pd.to_numeric(cpi["year"], errors="coerce").astype("Int64")
    return cpi.drop_duplicates("year").sort_values("year").reset_index(drop=True)


def add_qcew_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    levels["log_qcew_annual_avg_emplvl"] = _safe_log(levels["qcew_annual_avg_emplvl"])
    levels["log_qcew_total_annual_wages"] = _safe_log(levels["qcew_total_annual_wages"])
    levels["log_qcew_annual_avg_weekly_wage"] = _safe_log(levels["qcew_annual_avg_weekly_wage"])
    levels["log_cpi_u"] = _safe_log(levels["cpi_u"])

    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["d_log_cpi_u"] = grouped["log_cpi_u"].diff()
    levels[EMPLOYMENT_GROWTH_COLUMN] = grouped["log_qcew_annual_avg_emplvl"].diff()
    levels["d_log_qcew_total_annual_wages"] = grouped["log_qcew_total_annual_wages"].diff()
    levels["d_log_qcew_annual_avg_weekly_wage"] = grouped["log_qcew_annual_avg_weekly_wage"].diff()
    levels["d_log_qcew_total_annual_wages_real"] = (
        levels["d_log_qcew_total_annual_wages"] - levels["d_log_cpi_u"]
    )
    levels["d_log_qcew_annual_avg_weekly_wage_real"] = (
        levels["d_log_qcew_annual_avg_weekly_wage"] - levels["d_log_cpi_u"]
    )
    return levels


def build_levels_panel() -> tuple[pd.DataFrame, int]:
    base = load_pooled_base_panel()
    base = base[base["year"] >= QCEW_START_YEAR].copy()
    years = [int(year) for year in sorted(base["year"].dropna().unique())]

    qcew = load_qcew_msa_panel(years=years)
    dropped_partial_coverage = qcew.attrs.get("dropped_partial_coverage_row_count", 0)
    cpi = load_cpi_u_index()

    merged = base.merge(qcew, on=["msa_id", "year"], how="left")
    merged = merged.merge(cpi, on="year", how="left")
    return add_qcew_columns(merged), dropped_partial_coverage


def _effect_series(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "primary_state_year":
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if effect == "region_year":
        regions = sample["primary_state"].map(census_region)
        return regions.astype("string") + "_" + sample["year"].astype("string")
    return sample[effect].astype("string")


def _design_matrix(sample: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    parts = [sample[list(spec.predictors)].astype("float64")]
    for effect in spec.fixed_effects:
        dummies = pd.get_dummies(
            _effect_series(sample, effect),
            prefix=effect,
            drop_first=True,
            dtype=float,
        )
        parts.append(dummies)
    return sm.add_constant(pd.concat(parts, axis=1), has_constant="add")


def _model_specs() -> Iterable[RegressionSpec]:
    fe_tiers = (("year",), ("primary_state_year",), ("region_year",))
    fe_suffix = {
        "year": "year_fe",
        "primary_state_year": "state_year_fe",
        "region_year": "region_year_fe",
    }

    for column in PRIMARY_CHANNEL_COLUMNS:
        for fixed_effects in fe_tiers:
            suffix = fe_suffix[fixed_effects[0]]
            yield RegressionSpec(
                family=column,
                model=f"rent_fd_{column.removeprefix('d_log_qcew_')}_{suffix}",
                outcome="d_log_zori",
                predictors=("d_log_pop", column),
                fixed_effects=fixed_effects,
                focal_term=column,
            )

    for column in PRIMARY_CHANNEL_COLUMNS:
        for fixed_effects in fe_tiers:
            suffix = fe_suffix[fixed_effects[0]]
            yield RegressionSpec(
                family=column,
                model=f"unsheltered_fd_{column.removeprefix('d_log_qcew_')}_{suffix}",
                outcome="d_log_unshelt_rate",
                predictors=("d_log_zori", "d_log_pop", column),
                fixed_effects=fixed_effects,
                focal_term=column,
            )


def fit_clustered_fd_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [
            spec.outcome,
            *spec.predictors,
            "msa_id",
            "year",
            *(["primary_state"] if spec.fixed_effects[0] != "year" else []),
        ]
        sample = fd.dropna(subset=required).copy()
        if sample.empty:
            continue

        x = _design_matrix(sample, spec)
        y = sample[spec.outcome].astype("float64")
        result = sm.OLS(y, x).fit(
            cov_type="cluster",
            cov_kwds={"groups": sample["msa_id"].astype(str)},
        )

        rows.append(
            {
                "family": spec.family,
                "model": spec.model,
                "outcome": spec.outcome,
                "term": spec.focal_term,
                "fixed_effects": "+".join(spec.fixed_effects),
                "estimate": float(result.params[spec.focal_term]),
                "std_error": float(result.bse[spec.focal_term]),
                "t_stat": float(result.tvalues[spec.focal_term]),
                "p_value": float(result.pvalues[spec.focal_term]),
                "nobs": int(result.nobs),
                "clusters": int(sample["msa_id"].nunique()),
                "r_squared": float(result.rsquared),
                "std_error_type": "clustered:msa_id",
            }
        )
    return pd.DataFrame(rows)


def summarize_panel(
    levels: pd.DataFrame,
    fd: pd.DataFrame,
    regressions: pd.DataFrame,
    dropped_partial_coverage: int,
) -> dict[str, object]:
    growth_columns = [
        EMPLOYMENT_GROWTH_COLUMN,
        *NOMINAL_GROWTH_COLUMNS,
        *REAL_GROWTH_COLUMNS,
    ]
    complete_fd = fd.dropna(subset=["d_log_zori", "d_log_pop", *growth_columns])
    correlations = (
        complete_fd[["d_log_zori", "d_log_unshelt_rate", "d_log_pop", *growth_columns]]
        .corr()
        .round(6)
    )
    return {
        "qcew_year_range_available": [QCEW_START_YEAR, QCEW_END_YEAR],
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "qcew_msa_year_rows_dropped_for_partial_county_coverage": int(dropped_partial_coverage),
        "min_coverage_ratio_required": QCEW_MIN_COVERAGE_RATIO,
        "complete_fd_rows_for_qcew_screens": int(len(complete_fd)),
        "levels_missing_qcew_columns": {
            column: int(levels[column].isna().sum()) for column in QCEW_LEVEL_COLUMNS
        },
        "fd_missing_qcew_diff_columns": {
            column: int(fd[column].isna().sum()) for column in growth_columns
        },
        "fd_correlations": json.loads(correlations.to_json()),
        "regression_count": int(len(regressions)),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels, dropped_partial_coverage = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd, regressions, dropped_partial_coverage)

    levels_path = OUT / "qcew_labor_market_levels.parquet"
    fd_path = OUT / "qcew_labor_market_fd.parquet"
    regression_path = OUT / "qcew_labor_market_regressions.parquet"
    regression_csv_path = OUT / "qcew_labor_market_regressions.csv"
    summary_path = OUT / "qcew_labor_market_summary.json"

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
        print(regressions[regressions["outcome"] == "d_log_zori"].to_string(index=False))


if __name__ == "__main__":
    main()
