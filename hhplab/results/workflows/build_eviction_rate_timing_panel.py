"""Build pooled top-150 eviction-rate timing panels and rent-growth screens.

This workflow uses the Eviction Lab national county estimates for 2000-2018,
aggregated to MSA through the registered `eviction_lab_national` covariate
pipeline. It tests eviction-rate change as a candidate rent-growth channel only
through timing-aware first-difference screens:

* lagged eviction-rate growth predicting current rent growth
* same-year eviction-rate growth as a descriptive simultaneity screen
* future eviction-rate growth as a placebo for reverse causality
* future eviction-rate growth regressed directly on current rent growth
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

from hhplab.covariates.aggregate import (
    aggregate_covariate_source,
    default_covariate_panel_path,
)
from hhplab.covariates.ingest import default_covariate_output_path
from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT
from hhplab.results.workflows.build_household_size_composition_panel import (
    _as_msa_id,
    load_pooled_base_panel,
)

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "eviction_rate_timing"

EVICTION_SOURCE_ID = "eviction_lab_national"
EVICTION_CURATED_PATH_ENV = "HHPLAB_EVICTION_NATIONAL_CURATED_PATH"
EVICTION_PANEL_PATH_ENV = "HHPLAB_EVICTION_NATIONAL_PANEL_PATH"
EVICTION_DOWNLOAD_URL = (
    "https://eviction-lab-data-downloads.s3.amazonaws.com/"
    "estimating-eviction-prevalance-across-us/county_eviction_estimates_2000_2018.csv"
)
EVICTION_END_YEAR = 2018


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]
    family: str


def _safe_log1p(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log1p(numeric.where(numeric >= 0))


def ensure_eviction_msa_panel(years: list[int]) -> Path:
    panel_override = os.environ.get(EVICTION_PANEL_PATH_ENV)
    panel_path = (
        Path(panel_override) if panel_override else default_covariate_panel_path(EVICTION_SOURCE_ID)
    )
    if panel_path.exists():
        return panel_path

    curated_override = os.environ.get(EVICTION_CURATED_PATH_ENV)
    curated_path = (
        Path(curated_override)
        if curated_override
        else default_covariate_output_path(EVICTION_SOURCE_ID)
    )
    if not curated_path.exists():
        raise FileNotFoundError(
            "Eviction Lab national curated covariate is missing. Download the official county "
            f"estimates CSV from {EVICTION_DOWNLOAD_URL}, then run "
            "`uv run hhplab ingest covariate --source eviction_lab_national --raw-path <file>` "
            "followed by "
            "`uv run hhplab aggregate covariate --source eviction_lab_national --target-geo msa`."
        )

    aggregate_kwargs: dict[str, object] = {
        "curated_path": curated_path,
        "years": years,
        "target_geo": "msa",
    }
    if panel_override:
        aggregate_kwargs["output_path"] = panel_path
        aggregate_kwargs["force"] = True
    return aggregate_covariate_source(EVICTION_SOURCE_ID, **aggregate_kwargs)


def load_eviction_msa_panel(*, years: list[int]) -> pd.DataFrame:
    panel_path = ensure_eviction_msa_panel(years)
    frame = pd.read_parquet(panel_path)
    if "geo_type" in frame.columns:
        frame = frame[frame["geo_type"].eq("msa")].copy()
    if "msa_id" not in frame.columns:
        if "geo_id" not in frame.columns:
            raise ValueError(
                f"Eviction Lab MSA panel {panel_path} is missing both 'msa_id' and 'geo_id'."
            )
        frame["msa_id"] = frame["geo_id"]

    required = {"msa_id", "year", "eviction_filings", "eviction_rate"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            f"Eviction Lab MSA panel {panel_path} is missing required columns: {missing}"
        )

    frame["msa_id"] = _as_msa_id(frame["msa_id"])
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    filtered = frame[frame["year"].isin(years)].copy()
    selected = [
        "msa_id",
        "year",
        "eviction_filings",
        "eviction_rate",
        *[
            column
            for column in ("coverage_ratio", "county_count", "membership_county_count")
            if column in filtered.columns
        ],
    ]
    renamed = filtered[selected].rename(columns={"coverage_ratio": "eviction_coverage_ratio"})
    return renamed.sort_values(["msa_id", "year"]).reset_index(drop=True)


def add_timing_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    levels["log_eviction_rate"] = _safe_log1p(levels["eviction_rate"])
    levels["log_eviction_filings"] = _safe_log1p(levels["eviction_filings"])

    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["d_log_eviction_rate"] = grouped["log_eviction_rate"].diff()
    levels["d_log_eviction_filings"] = grouped["log_eviction_filings"].diff()
    levels["eviction_rate_lag1_gap"] = grouped["year_gap"].shift(1)
    levels["eviction_rate_lead1_gap"] = grouped["year_gap"].shift(-1)
    levels["d_log_eviction_rate_lag1"] = grouped["d_log_eviction_rate"].shift(1)
    levels["d_log_eviction_rate_lead1"] = grouped["d_log_eviction_rate"].shift(-1)
    levels.loc[levels["eviction_rate_lag1_gap"] != 1, "d_log_eviction_rate_lag1"] = pd.NA
    levels.loc[levels["eviction_rate_lead1_gap"] != 1, "d_log_eviction_rate_lead1"] = pd.NA
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    base = base[base["year"] <= EVICTION_END_YEAR].copy()
    years = [int(year) for year in sorted(base["year"].dropna().unique())]
    eviction = load_eviction_msa_panel(years=years)
    merged = base.merge(eviction, on=["msa_id", "year"], how="left")
    return add_timing_columns(merged)


def _model_specs() -> Iterable[ModelSpec]:
    yield ModelSpec(
        name="rent_fd_eviction_rate_same_year",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_eviction_rate"),
        family="same_year_screen",
    )
    yield ModelSpec(
        name="rent_fd_eviction_rate_lag1",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_eviction_rate_lag1"),
        family="lagged_channel",
    )
    yield ModelSpec(
        name="rent_fd_eviction_rate_lead1_placebo",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_log_eviction_rate_lead1"),
        family="lead_placebo",
    )
    yield ModelSpec(
        name="future_eviction_rate_fd_on_rent",
        outcome="d_log_eviction_rate_lead1",
        predictors=("d_log_pop", "d_log_zori"),
        family="reverse_causality",
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
    level_summary = (
        levels[["eviction_filings", "eviction_rate"]]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        fd[
            [
                "d_log_zori",
                "d_log_pop",
                "d_log_eviction_rate",
                "d_log_eviction_rate_lag1",
                "d_log_eviction_rate_lead1",
            ]
        ]
        .corr()
        .round(6)
    )
    coverage = levels.get("eviction_coverage_ratio")
    coverage_summary = (
        {}
        if coverage is None
        else {
            "count": int(coverage.notna().sum()),
            "min": float(coverage.min()),
            "median": float(coverage.median()),
            "mean": float(coverage.mean()),
        }
        if coverage.notna().any()
        else {"count": 0}
    )
    sample_sizes = {
        spec.name: int(len(fd.dropna(subset=[spec.outcome, *spec.predictors, "year", "msa_id"])))
        for spec in _model_specs()
    }
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "eviction_rate_transform": "log1p(percent_rate)",
        "timing_sample_sizes": sample_sizes,
        "eviction_level_summary": json.loads(level_summary.to_json()),
        "eviction_coverage_summary": coverage_summary,
        "levels_missing_eviction_columns": {
            column: int(levels[column].isna().sum())
            for column in ("eviction_filings", "eviction_rate", "log_eviction_rate")
        },
        "fd_missing_eviction_timing_columns": {
            column: int(fd[column].isna().sum())
            for column in (
                "d_log_eviction_rate",
                "d_log_eviction_rate_lag1",
                "d_log_eviction_rate_lead1",
            )
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "eviction_rate_timing_levels.parquet"
    fd_path = OUT / "eviction_rate_timing_fd.parquet"
    regression_path = OUT / "eviction_rate_timing_regressions.parquet"
    regression_csv_path = OUT / "eviction_rate_timing_regressions.csv"
    summary_path = OUT / "eviction_rate_timing_summary.json"

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
        print(regressions[regressions["term"].str.contains("eviction|d_log_zori", regex=True)])


if __name__ == "__main__":
    main()
