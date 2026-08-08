"""Run tracked robustness checks for non-compositional rent-population screens.

This script consumes the panel artifact created by
build_noncompositional_rent_population_panel.py and reproduces the
state x year fixed-effects robustness check documented in
devdocs/noncompositional_rent_population_findings.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.geographies.census.regions import census_region
from hhplab.results.workflows._paths import write_result_parquet
from hhplab.results.workflows.build_noncompositional_rent_population_panel import (
    BPS_SHORT_WINDOW,
    NONCOMPOSITIONAL_OUT,
    SUPPLY_EXPOSURE_END_YEAR,
)

FD_INPUT = NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_fd.parquet"

ROBUSTNESS_PARQUET = (
    NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_robustness_regressions.parquet"
)
ROBUSTNESS_CSV = (
    NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_robustness_regressions.csv"
)
ROBUSTNESS_SUMMARY = (
    NONCOMPOSITIONAL_OUT / "noncompositional_rent_population_robustness_summary.json"
)


@dataclass(frozen=True)
class RegressionSpec:
    family: str
    model: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    sample_filter: str


STATE_YEAR_FE_SPECS = (
    RegressionSpec(
        family="supply_constraint",
        model="rent_fd_population_x_supply_constraint_bps_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "supply_constraint_bps", "d_log_pop_x_supply_constraint_bps"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
    ),
    RegressionSpec(
        family="supply_constraint",
        model="rent_fd_population_x_supply_constraint_bps_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "supply_constraint_bps", "d_log_pop_x_supply_constraint_bps"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
    ),
    RegressionSpec(
        family="short_term_rental_proxy",
        model="rent_fd_seasonal_recreational_vacancy_share_state_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_seasonal_recreational_vacancy_share"),
        fixed_effects=("primary_state_year",),
        sample_filter="fd_year_gap_1",
    ),
    RegressionSpec(
        family="short_term_rental_proxy",
        model="rent_fd_seasonal_recreational_vacancy_share_region_year_fe",
        outcome="d_log_zori",
        predictors=("d_log_pop", "d_seasonal_recreational_vacancy_share"),
        fixed_effects=("region_year",),
        sample_filter="fd_year_gap_1",
    ),
)


def _effect_series(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "primary_state_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "primary_state_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the noncompositional panel from the base panel."
            )
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if effect == "region_year":
        missing = {"primary_state", "year"} - set(sample.columns)
        if missing:
            raise ValueError(
                "region_year fixed effects require columns "
                f"{sorted(missing)}. Rebuild the noncompositional panel from the base panel."
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


def _validate_supply_model_sample(spec: RegressionSpec, sample: pd.DataFrame) -> None:
    if spec.family != "supply_constraint" or sample.empty:
        return
    if "year" not in sample.columns:
        raise ValueError(f"Regression spec '{spec.model}' requires a year column.")

    overlap = sorted(
        int(year)
        for year in sample["year"].dropna().unique()
        if int(year) <= SUPPLY_EXPOSURE_END_YEAR + 1
    )
    if overlap:
        raise ValueError(
            f"{spec.model} supply-constraint sample overlaps the "
            f"{min(BPS_SHORT_WINDOW)}-{SUPPLY_EXPOSURE_END_YEAR} BPS exposure window "
            f"in first-difference analysis year(s) {overlap}. Restrict supply-constraint "
            f"outcomes to years after {SUPPLY_EXPOSURE_END_YEAR + 1} so the exposure "
            "is pre-sample."
        )


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
                f"{missing_required}. Rebuild the noncompositional panel from the base panel."
            )
        raise ValueError(
            f"Regression spec '{spec.model}' requires missing column(s) {missing_required}."
        )
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        return pd.DataFrame()
    _validate_supply_model_sample(spec, sample)

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
            f"Required non-compositional panel artifact not found: {path}. "
            "Run `uv run hhplab build result noncompositional-rent-population --json` "
            "or `python -m hhplab.results.workflows."
            "build_noncompositional_rent_population_panel` first."
        )
    return pd.read_parquet(path)


def run_robustness_checks() -> pd.DataFrame:
    fd = load_required_parquet(FD_INPUT)
    frames = [fit_spec(fd, spec) for spec in STATE_YEAR_FE_SPECS]
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True)


def summarize_regressions(regressions: pd.DataFrame) -> dict[str, object]:
    if regressions.empty:
        return {"regression_rows": 0, "models": []}
    focal_terms = {"supply_constraint_bps", "d_seasonal_recreational_vacancy_share"}
    focal = regressions[regressions["term"].isin(focal_terms)].copy()
    return {
        "regression_rows": int(len(regressions)),
        "models": sorted(regressions["model"].unique().tolist()),
        "focal_terms": json.loads(focal.to_json(orient="records")),
    }


def write_outputs(regressions: pd.DataFrame) -> dict[str, object]:
    NONCOMPOSITIONAL_OUT.mkdir(parents=True, exist_ok=True)
    write_result_parquet(regressions, ROBUSTNESS_PARQUET, index=False)
    regressions.to_csv(ROBUSTNESS_CSV, index=False)
    summary = summarize_regressions(regressions)
    ROBUSTNESS_SUMMARY.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def run() -> dict[str, object]:
    regressions = run_robustness_checks()
    summary = write_outputs(regressions)
    return {
        "summary": summary,
        "regressions": json.loads(regressions.to_json(orient="records")),
        "outputs": {
            "regressions_parquet": str(ROBUSTNESS_PARQUET),
            "regressions_csv": str(ROBUSTNESS_CSV),
            "summary_json": str(ROBUSTNESS_SUMMARY),
        },
    }


def main() -> None:
    result = run()
    summary = result["summary"]
    outputs = result["outputs"]
    print(f"regression rows: {summary['regression_rows']} -> {outputs['regressions_parquet']}")
    print(f"csv -> {outputs['regressions_csv']}")
    print(f"summary -> {outputs['summary_json']}")
    if summary["regression_rows"]:
        print(pd.DataFrame(summary["focal_terms"]))


if __name__ == "__main__":
    main()
