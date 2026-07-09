"""Test whether BPS mix-adjusted permit valuation leads MSA rent growth.

This completes the actual channel test that coclab-mzpm7.13 ("Assess
construction cost inflation as a rent-growth channel") was scoped for.
coclab-mzpm7.17 built the covariate and devdocs/bps_valuation_benchmark.md
validated it against BLS PPI and checked distinctness from the existing
permits-scarcity exposure, but neither step regressed the covariate against
rent growth. This workflow does that, using this project's standard pooled
top-150 base panel and 3-tier fixed-effect robustness ladder (year,
primary-state x year, census-region x year) with MSA-clustered SEs.

Two specifications are tested per FE tier:

* contemporaneous: d_log_zori(t) ~ d_log_pop(t) + d_log_bps_valuation(t)
* lead: d_log_zori(t) ~ d_log_pop(t) + d_log_bps_valuation_lag1(t)
  ("does last year's construction-cost growth predict this year's rent
  growth" -- the lead direction implied by a supply-cost channel)

A reverse-direction placebo is also fit (d_log_bps_valuation(t) ~
d_log_pop(t) + d_log_zori_lag1(t)) to check whether any observed lead-lag
association could instead be rent growth pulling forward permit valuations
(e.g. developers pricing permits to a hot market) rather than costs driving
rents.

See devdocs/bps_valuation_benchmark.md for the covariate's known coverage
limitation: bps_mix_adjusted_permit_value_per_unit_thousands is missing
non-randomly (worse for small MSAs and 2008-2012 years), so this screen's
sample is a size-skewed complete-case subset of the full pooled panel, not
the full top-150 sample.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.covariates.census_bps_contract import (
    CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
    CENSUS_BPS_SOURCE_ID,
)
from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT, write_result_parquet
from hhplab.results.workflows.build_household_size_composition_panel import (
    _as_msa_id,
    load_pooled_base_panel,
)

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "bps_valuation_rent_channel"

BPS_MSA_PANEL_PATH = (
    REPO_ROOT / "data/curated/covariates/covariate_panel__census_bps__Y2000-2024.parquet"
)

BPS_VALUATION_COLUMN = CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN
D_LOG_BPS_VALUATION = "d_log_bps_valuation"
D_LOG_BPS_VALUATION_LAG1 = "d_log_bps_valuation_lag1"
D_LOG_ZORI_LAG1 = "d_log_zori_lag1"


@dataclass(frozen=True)
class RegressionSpec:
    model: str
    direction: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: str
    focal_term: str


def _safe_log(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log(numeric.where(numeric > 0))


def load_bps_msa_panel() -> pd.DataFrame:
    if not BPS_MSA_PANEL_PATH.exists():
        raise FileNotFoundError(
            f"Census BPS MSA panel not found: {BPS_MSA_PANEL_PATH}. Run "
            f"`uv run hhplab aggregate covariate --source {CENSUS_BPS_SOURCE_ID} "
            "--target-geo msa --years 2000-2024` first."
        )
    frame = pd.read_parquet(BPS_MSA_PANEL_PATH)
    frame = frame[frame["geo_type"].eq("msa")].copy()
    frame["msa_id"] = _as_msa_id(frame["msa_id"])
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    return frame[["msa_id", "year", BPS_VALUATION_COLUMN]].copy()


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    bps = load_bps_msa_panel()
    merged = base.merge(bps, on=["msa_id", "year"], how="left")
    merged = merged.sort_values(["msa_id", "year"]).copy()
    merged["log_bps_valuation"] = _safe_log(merged[BPS_VALUATION_COLUMN])

    grouped = merged.groupby("msa_id", sort=False)
    merged["year_gap"] = grouped["year"].diff()
    merged["d_log_zori"] = grouped["log_zori"].diff()
    merged["d_log_pop"] = grouped["log_pop"].diff()
    merged[D_LOG_BPS_VALUATION] = grouped["log_bps_valuation"].diff()
    merged[D_LOG_BPS_VALUATION_LAG1] = merged.groupby("msa_id")[D_LOG_BPS_VALUATION].shift(1)
    merged[D_LOG_ZORI_LAG1] = merged.groupby("msa_id")["d_log_zori"].shift(1)
    merged["lag1_year_gap"] = merged.groupby("msa_id")["year_gap"].shift(1)
    return merged


def _effect_series(sample: pd.DataFrame, fixed_effects: str) -> pd.Series:
    if fixed_effects == "primary_state_year":
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if fixed_effects == "region_year":
        regions = sample["primary_state"].map(census_region)
        return regions.astype("string") + "_" + sample["year"].astype("string")
    return sample["year"].astype("string")


def _design_matrix(sample: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    parts = [sample[list(spec.predictors)].astype("float64")]
    dummies = pd.get_dummies(
        _effect_series(sample, spec.fixed_effects),
        prefix=spec.fixed_effects,
        drop_first=True,
        dtype=float,
    )
    parts.append(dummies)
    return sm.add_constant(pd.concat(parts, axis=1), has_constant="add")


FE_TIERS = ("year", "primary_state_year", "region_year")
FE_SUFFIX = {
    "year": "year_fe",
    "primary_state_year": "state_year_fe",
    "region_year": "region_year_fe",
}


def _model_specs() -> Iterable[RegressionSpec]:
    for fixed_effects in FE_TIERS:
        suffix = FE_SUFFIX[fixed_effects]
        yield RegressionSpec(
            model=f"rent_fd_contemporaneous_{suffix}",
            direction="contemporaneous",
            outcome="d_log_zori",
            predictors=("d_log_pop", D_LOG_BPS_VALUATION),
            fixed_effects=fixed_effects,
            focal_term=D_LOG_BPS_VALUATION,
        )
    for fixed_effects in FE_TIERS:
        suffix = FE_SUFFIX[fixed_effects]
        yield RegressionSpec(
            model=f"rent_fd_bps_leads_rent_{suffix}",
            direction="lead",
            outcome="d_log_zori",
            predictors=("d_log_pop", D_LOG_BPS_VALUATION_LAG1),
            fixed_effects=fixed_effects,
            focal_term=D_LOG_BPS_VALUATION_LAG1,
        )
    for fixed_effects in FE_TIERS:
        suffix = FE_SUFFIX[fixed_effects]
        yield RegressionSpec(
            model=f"bps_fd_rent_leads_bps_{suffix}",
            direction="reverse_placebo",
            outcome=D_LOG_BPS_VALUATION,
            predictors=("d_log_pop", D_LOG_ZORI_LAG1),
            fixed_effects=fixed_effects,
            focal_term=D_LOG_ZORI_LAG1,
        )


def fit_clustered_fd_models(levels: pd.DataFrame) -> pd.DataFrame:
    fd = levels[levels["year_gap"] == 1].copy()
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [spec.outcome, *spec.predictors, "msa_id", "year", "primary_state"]
        if spec.direction != "contemporaneous":
            required.append("lag1_year_gap")
        sample = fd.dropna(subset=required).copy()
        if spec.direction != "contemporaneous":
            sample = sample[sample["lag1_year_gap"] == 1].copy()
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
                "model": spec.model,
                "direction": spec.direction,
                "outcome": spec.outcome,
                "term": spec.focal_term,
                "fixed_effects": spec.fixed_effects,
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


def summarize(levels: pd.DataFrame, regressions: pd.DataFrame) -> dict[str, object]:
    fd = levels[levels["year_gap"] == 1].copy()
    contemporaneous_complete = fd.dropna(subset=["d_log_zori", "d_log_pop", D_LOG_BPS_VALUATION])
    lead_complete = fd.dropna(
        subset=["d_log_zori", "d_log_pop", D_LOG_BPS_VALUATION_LAG1, "lag1_year_gap"]
    )
    lead_complete = lead_complete[lead_complete["lag1_year_gap"] == 1]
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count_pooled_base_panel": int(levels["msa_id"].nunique()),
        "contemporaneous_complete_case_rows": int(len(contemporaneous_complete)),
        "contemporaneous_complete_case_msa_count": int(
            contemporaneous_complete["msa_id"].nunique()
        ),
        "lead_complete_case_rows": int(len(lead_complete)),
        "lead_complete_case_msa_count": int(lead_complete["msa_id"].nunique()),
        "raw_correlation_contemporaneous": float(
            contemporaneous_complete["d_log_zori"].corr(
                contemporaneous_complete[D_LOG_BPS_VALUATION]
            )
        )
        if not contemporaneous_complete.empty
        else None,
        "raw_correlation_lead": float(
            lead_complete["d_log_zori"].corr(lead_complete[D_LOG_BPS_VALUATION_LAG1])
        )
        if not lead_complete.empty
        else None,
        "regression_count": int(len(regressions)),
        "regressions": regressions.to_dict(orient="records"),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    regressions = fit_clustered_fd_models(levels)
    summary = summarize(levels, regressions)

    levels_path = OUT / "bps_valuation_rent_channel_levels.parquet"
    regression_path = OUT / "bps_valuation_rent_channel_regressions.parquet"
    regression_csv_path = OUT / "bps_valuation_rent_channel_regressions.csv"
    summary_path = OUT / "bps_valuation_rent_channel_summary.json"

    write_result_parquet(levels, levels_path, index=False)
    write_result_parquet(regressions, regression_path, index=False)
    regressions.to_csv(regression_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return {
        **summary,
        "outputs": {
            "levels_parquet": str(levels_path),
            "regressions_parquet": str(regression_path),
            "regressions_csv": str(regression_csv_path),
            "summary_json": str(summary_path),
        },
    }


def main() -> None:
    result = run()
    outputs = result["outputs"]

    print(f"levels rows: {result['levels_rows']} -> {outputs['levels_parquet']}")
    print(f"regression rows: {result['regression_count']} -> {outputs['regressions_parquet']}")
    print(f"summary -> {outputs['summary_json']}")
    if result["regressions"]:
        print(pd.DataFrame(result["regressions"]).to_string(index=False))


if __name__ == "__main__":
    main()
