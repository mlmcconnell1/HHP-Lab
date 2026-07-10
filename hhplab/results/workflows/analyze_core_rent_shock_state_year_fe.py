"""Reproduce the pooled top-150 rent-shock fixed-effects robustness check."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows._paths import OUTPUTS_ROOT, write_result_parquet
from hhplab.results.workflows.build_vera_hic_pit_longitudinal_pooled import primary_state

TOP150_INPUT = (
    OUTPUTS_ROOT
    / "msa_rank51_150_longitudinal_2015_2025_source_top150"
    / "panel__msa__Y2015-2025@Mcensusmsa2023.parquet"
)
OUT = OUTPUTS_ROOT / "core_rent_shock_state_year_fe"
REGRESSIONS_PARQUET = OUT / "core_rent_shock_state_year_fe_regressions.parquet"
REGRESSIONS_CSV = OUT / "core_rent_shock_state_year_fe_regressions.csv"
SUMMARY_JSON = OUT / "core_rent_shock_state_year_fe_summary.json"
ASYMMETRY_PARQUET = OUT / "core_rent_shock_asymmetry_regressions.parquet"

ANALYSIS_START_YEAR = 2016
COHORT_REFERENCE_YEAR = 2020
TOP50_SIZE = 50
REQUIRED_COLUMNS = (
    "msa_id",
    "msa_name",
    "year",
    "year_gap",
    "d_log_zori",
    "population",
)


@dataclass(frozen=True)
class RegressionSpec:
    model: str
    fixed_effect: str


REGRESSION_SPECS = (
    RegressionSpec(model="rent_shock_year_fe", fixed_effect="year"),
    RegressionSpec(model="rent_shock_state_year_fe", fixed_effect="primary_state_year"),
)
ASYMMETRY_COHORTS = ("top50", "rank51_150", "pooled")
ASYMMETRY_COVERAGE_SAMPLES = ("all", "zori_coverage_80")

DOCUMENTED_BENCHMARKS = {
    "rent_shock_year_fe": {"estimate": 1.921, "std_error": 0.425, "p_value": 0.0001},
    "rent_shock_state_year_fe": {
        "estimate": 1.795,
        "std_error": 0.693,
        "p_value": 0.0095,
    },
}


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if "d_log_unsheltered_rate" not in normalized.columns:
        aliases = ("d_log_unshelt_rate", "d_log_unshelt_per_1000")
        source = next((column for column in aliases if column in normalized.columns), None)
        if source is None:
            raise ValueError(
                "Pooled rent-shock input requires d_log_unsheltered_rate or "
                "a supported unsheltered-rate alias. Rebuild the top-150 panel artifacts."
            )
        normalized["d_log_unsheltered_rate"] = normalized[source]
    if "year_gap" not in normalized.columns:
        normalized = normalized.sort_values(["msa_id", "year"])
        normalized["year_gap"] = normalized.groupby("msa_id")["year"].diff()
    missing = sorted(set(REQUIRED_COLUMNS) - set(normalized.columns))
    if missing:
        raise ValueError(
            f"Pooled rent-shock input is missing columns {missing}. "
            "Rebuild the top-150 panel artifacts."
        )
    return normalized


def build_pooled_fd_panel(top150: pd.DataFrame) -> pd.DataFrame:
    top150 = _normalize_columns(top150)
    top150 = top150.sort_values(["msa_id", "year"])
    top150["d_log_population"] = top150.groupby("msa_id")["population"].transform(
        lambda values: np.log(pd.to_numeric(values, errors="coerce")).diff()
    )
    reference = top150.loc[top150["year"] == COHORT_REFERENCE_YEAR].dropna(subset=["population"])
    if reference["msa_id"].nunique() < TOP50_SIZE:
        raise ValueError(
            f"Top-150 source requires at least {TOP50_SIZE} populated MSAs in "
            f"reference year {COHORT_REFERENCE_YEAR}."
        )
    top50_ids = set(reference.nlargest(TOP50_SIZE, "population")["msa_id"])
    pooled = top150.loc[(top150["year"] >= ANALYSIS_START_YEAR) & (top150["year_gap"] == 1)].copy()
    pooled["cohort"] = pooled["msa_id"].map(
        lambda msa_id: "top50" if msa_id in top50_ids else "rank51_150"
    )
    pooled["primary_state"] = pooled["msa_name"].map(primary_state)
    return pooled.sort_values(["msa_id", "year"]).reset_index(drop=True)


def _fixed_effect(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "year":
        return sample["year"].astype("string")
    if effect == "primary_state_year":
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    raise ValueError(f"Unsupported fixed effect: {effect}")


def fit_spec(frame: pd.DataFrame, spec: RegressionSpec) -> dict[str, object]:
    required = [
        "msa_id",
        "year",
        "primary_state",
        "d_log_unsheltered_rate",
        "d_log_zori",
    ]
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Regression spec '{spec.model}' requires columns {missing}.")
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        raise ValueError(f"Regression spec '{spec.model}' has no complete observations.")
    dummies = pd.get_dummies(
        _fixed_effect(sample, spec.fixed_effect),
        prefix=spec.fixed_effect,
        drop_first=True,
        dtype=float,
    )
    design = sm.add_constant(
        pd.concat([sample[["d_log_zori"]].astype("float64"), dummies], axis=1),
        has_constant="add",
    )
    result = sm.OLS(sample["d_log_unsheltered_rate"].astype("float64"), design).fit(
        cov_type="cluster", cov_kwds={"groups": sample["msa_id"].astype(str)}
    )
    benchmark = DOCUMENTED_BENCHMARKS[spec.model]
    estimate = float(result.params["d_log_zori"])
    std_error = float(result.bse["d_log_zori"])
    p_value = float(result.pvalues["d_log_zori"])
    return {
        "model": spec.model,
        "outcome": "d_log_unsheltered_rate",
        "term": "d_log_zori",
        "estimate": estimate,
        "std_error": std_error,
        "t_stat": float(result.tvalues["d_log_zori"]),
        "p_value": p_value,
        "nobs": int(result.nobs),
        "clusters": int(sample["msa_id"].nunique()),
        "states": int(sample["primary_state"].nunique()),
        "r_squared": float(result.rsquared),
        "fixed_effects": spec.fixed_effect,
        "std_error_type": "clustered:msa_id",
        "documented_estimate": benchmark["estimate"],
        "documented_std_error": benchmark["std_error"],
        "documented_p_value": benchmark["p_value"],
        "estimate_difference": estimate - benchmark["estimate"],
        "exact_rounded_reproduction": round(estimate, 3) == benchmark["estimate"],
    }


def fit_asymmetry_spec(
    frame: pd.DataFrame,
    spec: RegressionSpec,
    *,
    cohort: str,
    coverage_sample: str,
) -> dict[str, object]:
    """Estimate separate slopes for positive and negative annual rent changes."""
    sample = frame if cohort == "pooled" else frame.loc[frame["cohort"] == cohort]
    if coverage_sample == "zori_coverage_80":
        sample = sample.loc[sample["zori_coverage_ratio"] >= 0.8]
    elif coverage_sample != "all":
        raise ValueError(f"Unsupported coverage sample: {coverage_sample}")
    required = [
        "msa_id",
        "year",
        "primary_state",
        "d_log_unsheltered_rate",
        "d_log_zori",
        "d_log_population",
    ]
    sample = sample.dropna(subset=required).copy()
    sample["rent_increase"] = sample["d_log_zori"].clip(lower=0)
    sample["rent_decrease"] = sample["d_log_zori"].clip(upper=0)
    dummies = pd.get_dummies(
        _fixed_effect(sample, spec.fixed_effect),
        prefix=spec.fixed_effect,
        drop_first=True,
        dtype=float,
    )
    design = sm.add_constant(
        pd.concat(
            [sample[["rent_increase", "rent_decrease", "d_log_population"]], dummies],
            axis=1,
        ).astype("float64"),
        has_constant="add",
    )
    result = sm.OLS(sample["d_log_unsheltered_rate"].astype("float64"), design).fit(
        cov_type="cluster", cov_kwds={"groups": sample["msa_id"].astype(str)}
    )
    equality_p = float(result.wald_test("rent_increase = rent_decrease", scalar=True).pvalue)
    return {
        "model": f"rent_asymmetry_{cohort}_{coverage_sample}_{spec.fixed_effect}",
        "cohort": cohort,
        "coverage_sample": coverage_sample,
        "fixed_effects": spec.fixed_effect,
        "nobs": int(result.nobs),
        "clusters": int(sample["msa_id"].nunique()),
        "rent_decrease_rows": int(sample["d_log_zori"].lt(0).sum()),
        "rent_decrease_msas": int(sample.loc[sample["d_log_zori"].lt(0), "msa_id"].nunique()),
        "rent_increase_estimate": float(result.params["rent_increase"]),
        "rent_increase_std_error": float(result.bse["rent_increase"]),
        "rent_increase_p_value": float(result.pvalues["rent_increase"]),
        "rent_decrease_estimate": float(result.params["rent_decrease"]),
        "rent_decrease_std_error": float(result.bse["rent_decrease"]),
        "rent_decrease_p_value": float(result.pvalues["rent_decrease"]),
        "slope_equality_p_value": equality_p,
        "std_error_type": "clustered:msa_id",
    }


def load_required_parquet(path: Path, build_command: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required rent-shock panel artifact not found: {path}. Run `{build_command}` first."
        )
    return pd.read_parquet(path)


def run() -> dict[str, object]:
    top150 = load_required_parquet(
        TOP150_INPUT,
        "uv run hhplab build recipe --recipe "
        "recipes/msa-rank51-150-longitudinal-2015-2025.yaml --json",
    )
    pooled = build_pooled_fd_panel(top150)
    regressions = pd.DataFrame([fit_spec(pooled, spec) for spec in REGRESSION_SPECS])
    asymmetry = pd.DataFrame(
        [
            fit_asymmetry_spec(
                pooled,
                spec,
                cohort=cohort,
                coverage_sample=coverage_sample,
            )
            for cohort in ASYMMETRY_COHORTS
            for coverage_sample in ASYMMETRY_COVERAGE_SAMPLES
            for spec in REGRESSION_SPECS
        ]
    )
    complete = pooled.dropna(
        subset=["d_log_unsheltered_rate", "d_log_zori", "msa_id", "year", "primary_state"]
    )
    summary = {
        "sample_rows": int(len(complete)),
        "msa_count": int(complete["msa_id"].nunique()),
        "state_count": int(complete["primary_state"].nunique()),
        "rows_by_cohort": {
            str(key): int(value) for key, value in complete.groupby("cohort").size().items()
        },
        "rows_by_year": {
            str(key): int(value) for key, value in complete.groupby("year").size().items()
        },
        "all_exact_rounded_reproductions": bool(regressions["exact_rounded_reproduction"].all()),
        "models": json.loads(regressions.to_json(orient="records")),
        "rent_change_asymmetry": json.loads(asymmetry.to_json(orient="records")),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    write_result_parquet(regressions, REGRESSIONS_PARQUET, index=False)
    write_result_parquet(asymmetry, ASYMMETRY_PARQUET, index=False)
    regressions.to_csv(REGRESSIONS_CSV, index=False)
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "outputs": {
            "regressions_parquet": str(REGRESSIONS_PARQUET),
            "regressions_csv": str(REGRESSIONS_CSV),
            "summary_json": str(SUMMARY_JSON),
            "asymmetry_parquet": str(ASYMMETRY_PARQUET),
        },
    }


def main() -> None:
    print(json.dumps(run(), indent=2))


if __name__ == "__main__":
    main()
