"""Run tracked robustness checks for composition-rent-population screens.

This script consumes the panel artifacts created by the composition builder
scripts and reproduces the follow-up fixed-effect checks documented in
devdocs/composition_rent_population_findings.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.census_regions import census_region
from hhplab.results.workflows.build_household_size_composition_panel import OUT

FD_INPUTS = {
    "renter_household_share": OUT / "renter_household_share_composition_fd.parquet",
}
LEVEL_INPUTS = {
    "renter_household_share": OUT / "renter_household_share_composition_levels.parquet",
    "household_size": OUT / "household_size_composition_levels.parquet",
    "recent_mover_income": OUT / "recent_mover_income_composition_levels.parquet",
}

ROBUSTNESS_PARQUET = OUT / "composition_rent_population_robustness_regressions.parquet"
ROBUSTNESS_CSV = OUT / "composition_rent_population_robustness_regressions.csv"
ROBUSTNESS_SUMMARY = OUT / "composition_rent_population_robustness_summary.json"


@dataclass(frozen=True)
class RegressionSpec:
    family: str
    model: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    sample_filter: str


FD_RENTER_SHARE_SPECS = (
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("year",),
        sample_filter="fd_year_gap_1",
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_fd_renter_household_share_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_renter_household_share"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
    ),
)

LEVEL_FE_SPECS = (
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_state_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "primary_state_year"),
        sample_filter="levels_complete_case",
    ),
    RegressionSpec(
        family="renter_household_share",
        model="rent_levels_renter_household_share_msa_region_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "renter_household_share"),
        fixed_effects=("msa_id", "region_year"),
        sample_filter="levels_complete_case",
    ),
    RegressionSpec(
        family="household_size",
        model="rent_levels_renter_household_size_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "average_household_size_renter_occupied"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
    ),
    RegressionSpec(
        family="recent_mover_income",
        model="rent_levels_moved_diff_state_income_ratio_msa_year_fe",
        outcome="log_zori",
        predictors=("log_pop", "moved_diff_state_income_ratio_total"),
        fixed_effects=("msa_id", "year"),
        sample_filter="levels_complete_case",
    ),
)


def _effect_series(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "primary_state_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "primary_state_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the composition panels from the base panel."
            )
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if effect == "region_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "region_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the composition panels from the base panel."
            )
        regions = sample["primary_state"].map(census_region)
        return regions.astype("string") + "_" + sample["year"].astype("string")
    if effect not in sample.columns:
        raise ValueError(f"Fixed effect column '{effect}' is missing from the regression sample.")
    return sample[effect].astype("string")


def _design_matrix(sample: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    x_parts = [sample[list(spec.predictors)].astype("float64")]
    for effect in spec.fixed_effects:
        dummies = pd.get_dummies(
            _effect_series(sample, effect),
            prefix=effect,
            drop_first=True,
            dtype=float,
        )
        x_parts.append(dummies)
    return sm.add_constant(pd.concat(x_parts, axis=1), has_constant="add")


def fit_spec(frame: pd.DataFrame, spec: RegressionSpec) -> pd.DataFrame:
    required = [spec.outcome, *spec.predictors, "msa_id", *spec.fixed_effects]
    for derived_effect in ("primary_state_year", "region_year"):
        if derived_effect in required:
            required.remove(derived_effect)
            required.extend(["primary_state", "year"])
    missing_required = sorted(set(required) - set(frame.columns))
    if missing_required:
        if "primary_state" in missing_required and any(
            effect in spec.fixed_effects for effect in ("primary_state_year", "region_year")
        ):
            raise ValueError(
                f"{'+'.join(spec.fixed_effects)} fixed effects require columns "
                f"{missing_required}. Rebuild the composition panels from the base panel."
            )
        raise ValueError(
            f"Regression spec '{spec.model}' requires missing column(s) {missing_required}."
        )
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        return pd.DataFrame()

    x = _design_matrix(sample, spec)
    y = sample[spec.outcome].astype("float64")
    result = sm.OLS(y, x).fit(
        cov_type="cluster",
        cov_kwds={"groups": sample["msa_id"].astype(str)},
    )

    rows: list[dict[str, object]] = []
    for term in spec.predictors:
        rows.append(
            {
                "family": spec.family,
                "model": spec.model,
                "outcome": spec.outcome,
                "term": term,
                "estimate": float(result.params[term]),
                "std_error": float(result.bse[term]),
                "t_stat": float(result.tvalues[term]),
                "p_value": float(result.pvalues[term]),
                "nobs": int(result.nobs),
                "clusters": int(sample["msa_id"].nunique()),
                "r_squared": float(result.rsquared),
                "fixed_effects": "+".join(spec.fixed_effects),
                "sample_filter": spec.sample_filter,
                "std_error_type": "clustered:msa_id",
            }
        )
    return pd.DataFrame(rows)


def load_required_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required composition panel artifact not found: {path}. "
            "Run `uv run hhplab build result composition-rent-population --json` "
            "or the corresponding `python -m hhplab.results.workflows.build_*` "
            "module first."
        )
    return pd.read_parquet(path)


def run_robustness_checks() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    renter_fd = load_required_parquet(FD_INPUTS["renter_household_share"])
    for spec in FD_RENTER_SHARE_SPECS:
        frames.append(fit_spec(renter_fd, spec))

    for spec in LEVEL_FE_SPECS:
        levels = load_required_parquet(LEVEL_INPUTS[spec.family])
        frames.append(fit_spec(levels, spec))

    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def summarize_regressions(regressions: pd.DataFrame) -> dict[str, object]:
    if regressions.empty:
        return {"regression_rows": 0, "models": []}
    focal_terms = {
        "d_renter_household_share",
        "renter_household_share",
        "average_household_size_renter_occupied",
        "moved_diff_state_income_ratio_total",
    }
    focal = regressions[regressions["term"].isin(focal_terms)].copy()
    return {
        "regression_rows": int(len(regressions)),
        "models": sorted(regressions["model"].unique().tolist()),
        "focal_terms": json.loads(focal.to_json(orient="records")),
    }


def write_outputs(regressions: pd.DataFrame) -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    regressions.to_parquet(ROBUSTNESS_PARQUET, index=False)
    regressions.to_csv(ROBUSTNESS_CSV, index=False)
    summary = summarize_regressions(regressions)
    ROBUSTNESS_SUMMARY.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    regressions = run_robustness_checks()
    summary = write_outputs(regressions)
    print(f"regression rows: {len(regressions)} -> {ROBUSTNESS_PARQUET}")
    print(f"csv -> {ROBUSTNESS_CSV}")
    print(f"summary -> {ROBUSTNESS_SUMMARY}")
    if summary["regression_rows"]:
        print(pd.DataFrame(summary["focal_terms"]))


if __name__ == "__main__":
    main()
