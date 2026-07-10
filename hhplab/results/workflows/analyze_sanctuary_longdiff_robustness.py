"""Track state-clustered and leave-California-out sanctuary long-difference checks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT, write_result_parquet

HOMELESSNESS_INPUT = REPO_ROOT / "outputs" / "tot_longdiff.parquet"
BEDS_INPUT = REPO_ROOT / "outputs" / "top50_msa_beds_longdiff.parquet"
MSA_PANEL_INPUT = (
    OUTPUTS_ROOT
    / "msa_rank51_150_longitudinal_2015_2025_source_top150"
    / "panel__msa__Y2015-2025@Mcensusmsa2023.parquet"
)
OUT = OUTPUTS_ROOT / "sanctuary_longdiff_robustness"
RESULTS_PARQUET = OUT / "sanctuary_longdiff_robustness_regressions.parquet"
SUMMARY_JSON = OUT / "sanctuary_longdiff_robustness_summary.json"


@dataclass(frozen=True)
class OutcomeSpec:
    name: str
    column: str
    source: str


OUTCOMES = (
    OutcomeSpec("unsheltered_growth", "d_log_unshelt_rate_15_25", "homelessness"),
    OutcomeSpec("sheltered_growth", "d_log_shelt_rate_15_25", "homelessness"),
    OutcomeSpec("beds_growth", "d_log_beds_15_25", "beds"),
)
SPECIFICATIONS = ("hc1", "state_clustered", "exclude_california_hc1")


def primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def add_primary_state(frame: pd.DataFrame, msa_panel: pd.DataFrame) -> pd.DataFrame:
    required = {"msa_id", "msa_name"}
    missing = sorted(required - set(msa_panel.columns))
    if missing:
        raise ValueError(f"MSA reference panel is missing columns {missing}.")
    names = msa_panel[["msa_id", "msa_name"]].drop_duplicates()
    if names["msa_id"].duplicated().any():
        raise ValueError("MSA reference panel maps an msa_id to multiple names.")
    names["primary_state"] = names["msa_name"].map(primary_state)
    merged = frame.merge(
        names[["msa_id", "primary_state"]], on="msa_id", how="left", validate="many_to_one"
    )
    if merged["primary_state"].isna().any():
        missing_ids = sorted(merged.loc[merged["primary_state"].isna(), "msa_id"].unique())
        raise ValueError(f"MSA state mapping is missing msa_id values {missing_ids}.")
    return merged


def fit_outcome(frame: pd.DataFrame, outcome: OutcomeSpec) -> pd.DataFrame:
    required = [outcome.column, "sanctuary", "primary_state"]
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Outcome '{outcome.name}' requires columns {missing}.")
    base = frame.dropna(subset=required).copy()
    rows: list[dict[str, object]] = []
    for specification in SPECIFICATIONS:
        sample = (
            base.loc[base["primary_state"] != "CA"].copy()
            if specification == "exclude_california_hc1"
            else base
        )
        design = sm.add_constant(sample[["sanctuary"]].astype("float64"), has_constant="add")
        kwargs = (
            {"cov_type": "cluster", "cov_kwds": {"groups": sample["primary_state"]}}
            if specification == "state_clustered"
            else {"cov_type": "HC1"}
        )
        result = sm.OLS(sample[outcome.column].astype("float64"), design).fit(**kwargs)
        rows.append(
            {
                "outcome": outcome.name,
                "outcome_column": outcome.column,
                "specification": specification,
                "estimate": float(result.params["sanctuary"]),
                "std_error": float(result.bse["sanctuary"]),
                "t_stat": float(result.tvalues["sanctuary"]),
                "p_value": float(result.pvalues["sanctuary"]),
                "nobs": int(result.nobs),
                "state_count": int(sample["primary_state"].nunique()),
                "sanctuary_msa_count": int(sample["sanctuary"].sum()),
                "std_error_type": "clustered:primary_state"
                if specification == "state_clustered"
                else "HC1",
                "california_excluded": specification == "exclude_california_hc1",
            }
        )
    return pd.DataFrame(rows)


def load_required(path: Path, remediation: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required sanctuary robustness input not found: {path}. {remediation}"
        )
    return pd.read_parquet(path)


def run() -> dict[str, object]:
    homelessness = load_required(
        HOMELESSNESS_INPUT, "Restore the tracked report long-difference artifacts."
    )
    beds = load_required(BEDS_INPUT, "Restore the tracked report long-difference artifacts.")
    msa_panel = load_required(
        MSA_PANEL_INPUT,
        "Run `uv run hhplab build recipe --recipe "
        "recipes/msa-rank51-150-longitudinal-2015-2025.yaml --json` first.",
    )
    frames = {
        "homelessness": add_primary_state(homelessness, msa_panel),
        "beds": add_primary_state(beds, msa_panel),
    }
    results = pd.concat(
        [fit_outcome(frames[outcome.source], outcome) for outcome in OUTCOMES],
        ignore_index=True,
    )
    OUT.mkdir(parents=True, exist_ok=True)
    write_result_parquet(
        results,
        RESULTS_PARQUET,
        index=False,
        extra={
            "source_artifacts": [str(HOMELESSNESS_INPUT), str(BEDS_INPUT), str(MSA_PANEL_INPUT)]
        },
    )
    summary = {
        "regression_rows": int(len(results)),
        "results": json.loads(results.to_json(orient="records")),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "outputs": {"regressions_parquet": str(RESULTS_PARQUET), "summary_json": str(SUMMARY_JSON)},
    }


def main() -> None:
    print(json.dumps(run(), indent=2))


if __name__ == "__main__":
    main()
