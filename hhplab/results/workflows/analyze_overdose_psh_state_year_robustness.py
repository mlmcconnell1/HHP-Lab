"""Track PSH-to-overdose lag checks under year and state-year fixed effects."""

from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import t as student_t

from hhplab.results.workflows._paths import OUTPUTS_ROOT, write_result_parquet

INPUT = OUTPUTS_ROOT / "overdose_lag" / "overdose_lag_levels.parquet"
OUT = OUTPUTS_ROOT / "overdose_psh_state_year_robustness"
RESULTS = OUT / "overdose_psh_state_year_robustness_regressions.parquet"
SUMMARY = OUT / "overdose_psh_state_year_robustness_summary.json"
MIN_COVERAGE = 0.8


@dataclass(frozen=True)
class RegressionSpec:
    model: str
    outcome: str
    predictors: tuple[str, ...]
    focal_term: str
    fixed_effects: tuple[str, ...]
    sample: str


SPECS = (
    RegressionSpec(
        "psh_levels_lag1_year_fe",
        "log_overdose_rate",
        ("log_psh_rate_lag1", "log_zori"),
        "log_psh_rate_lag1",
        ("msa_id", "year"),
        "levels_lag1",
    ),
    RegressionSpec(
        "psh_levels_lag1_state_year_fe",
        "log_overdose_rate",
        ("log_psh_rate_lag1", "log_zori"),
        "log_psh_rate_lag1",
        ("msa_id", "primary_state_year"),
        "levels_lag1",
    ),
    RegressionSpec(
        "psh_fd_lag1_year_fe",
        "d_log_overdose_rate",
        ("d_log_psh_rate_lag1", "d_log_psh_rate", "d_log_zori"),
        "d_log_psh_rate_lag1",
        ("year",),
        "fd_lag1",
    ),
    RegressionSpec(
        "psh_fd_lag1_state_year_fe",
        "d_log_overdose_rate",
        ("d_log_psh_rate_lag1", "d_log_psh_rate", "d_log_zori"),
        "d_log_psh_rate_lag1",
        ("primary_state_year",),
        "fd_lag1",
    ),
)


def primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def prepare_sample(frame: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    sample = frame.copy()
    if "primary_state" not in sample.columns:
        sample["primary_state"] = sample["msa_name"].map(primary_state)
    sample["primary_state_year"] = (
        sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    )
    sample = sample.loc[sample["overdose_coverage_ratio"] >= MIN_COVERAGE]
    if spec.sample == "fd_lag1":
        sample = sample.loc[(sample["year_gap"] == 1) & (sample["lag1_year_gap"] == 1)]
    required = [spec.outcome, *spec.predictors, "msa_id", *spec.fixed_effects]
    return sample.dropna(subset=list(dict.fromkeys(required))).copy()


def fit_spec(frame: pd.DataFrame, spec: RegressionSpec) -> dict[str, object]:
    sample = prepare_sample(frame, spec)
    parts = [sample[list(spec.predictors)].astype("float64")]
    for effect in spec.fixed_effects:
        parts.append(
            pd.get_dummies(
                sample[effect].astype("string"), prefix=effect, drop_first=True, dtype=float
            )
        )
    design = sm.add_constant(pd.concat(parts, axis=1), has_constant="add")
    model = sm.OLS(sample[spec.outcome].astype("float64"), design)
    result = model.fit(
        cov_type="cluster",
        cov_kwds={"groups": sample["msa_id"].astype(str), "use_correction": False},
    )
    rank = int(np.linalg.matrix_rank(design.to_numpy()))
    dof = int(result.nobs) - rank
    t_stat = float(result.params[spec.focal_term] / result.bse[spec.focal_term])
    p_value = float(2 * student_t.sf(abs(t_stat), df=dof))
    return {
        "model": spec.model,
        "outcome": spec.outcome,
        "term": spec.focal_term,
        "estimate": float(result.params[spec.focal_term]),
        "std_error": float(result.bse[spec.focal_term]),
        "t_stat": t_stat,
        "p_value": p_value,
        "nobs": int(result.nobs),
        "clusters": int(sample["msa_id"].nunique()),
        "design_rank": rank,
        "dof": dof,
        "r_squared": float(result.rsquared),
        "fixed_effects": "+".join(spec.fixed_effects),
        "std_error_type": "clustered:msa_id;small_sample_correction=false",
        "sample_filter": spec.sample,
    }


def run() -> dict[str, object]:
    if not INPUT.exists():
        raise FileNotFoundError(
            f"Required overdose lag panel not found: {INPUT}. Run "
            "`uv run hhplab build result overdose-lag --json` first."
        )
    frame = pd.read_parquet(INPUT)
    results = pd.DataFrame([fit_spec(frame, spec) for spec in SPECS])
    OUT.mkdir(parents=True, exist_ok=True)
    write_result_parquet(results, RESULTS, index=False)
    summary = {"models": json.loads(results.to_json(orient="records"))}
    SUMMARY.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "outputs": {"regressions_parquet": str(RESULTS), "summary_json": str(SUMMARY)},
    }


def main() -> None:
    print(json.dumps(run(), indent=2))


if __name__ == "__main__":
    main()
