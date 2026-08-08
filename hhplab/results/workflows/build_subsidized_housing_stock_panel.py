"""Build pooled top-150 subsidized-housing-stock panels and rent-growth screens.

This workflow uses HUD's Picture of Subsidized Households (`hud_psh`) covariate
source. The preferred input is an MSA-ready panel in
`data/curated/covariates/covariate_panel__hud_psh__Y2000-ongoing.parquet`.
If only the county-native curated covariate exists, the workflow aggregates it
to MSA first using the registered covariate rollup.

HUD PSH is calendar-year aligned in this project, so source year Y is joined to
the same PIT/ZORI analysis year Y.
"""

from __future__ import annotations

import json
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
from hhplab.geographies.census.regions import census_region
from hhplab.results.workflows._paths import OUTPUTS_ROOT, write_result_parquet
from hhplab.results.workflows.build_household_size_composition_panel import (
    ROOT as _ROOT,
)
from hhplab.results.workflows.build_household_size_composition_panel import (
    _as_msa_id,
    load_pooled_base_panel,
)

ROOT = _ROOT
OUT = OUTPUTS_ROOT / "subsidized_housing_stock"

HUD_PSH_SOURCE_ID = "hud_psh"
HUD_PSH_COLUMNS = [
    "subsidized_households",
    "housing_choice_vouchers",
]
HUD_PSH_RATE_COLUMNS = [f"{column}_per_1000" for column in HUD_PSH_COLUMNS]
HUD_PSH_LOG_RATE_COLUMNS = [f"log_{column}_per_1000" for column in HUD_PSH_COLUMNS]
HUD_PSH_FD_COLUMNS = [f"d_log_{column}_per_1000" for column in HUD_PSH_COLUMNS]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    outcome: str
    predictors: tuple[str, ...]
    fixed_effects: str


def _safe_log(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log(numeric.where(numeric > 0))


def _safe_per_1000(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce").astype("float64")
    right = pd.to_numeric(denominator, errors="coerce").astype("float64")
    return left / right.where(right > 0) * 1000


def ensure_hud_psh_msa_panel(years: list[int]) -> Path:
    msa_panel_path = default_covariate_panel_path(HUD_PSH_SOURCE_ID)
    if msa_panel_path.exists():
        return msa_panel_path

    county_curated_path = default_covariate_output_path(HUD_PSH_SOURCE_ID)
    if not county_curated_path.exists():
        raise FileNotFoundError(
            "HUD PSH MSA covariate panel is missing and the county-native curated source "
            f"is also absent ({county_curated_path}). Stage the provider workbook and run "
            "`uv run hhplab ingest covariate --source hud_psh --raw-path <file>` followed by "
            "`uv run hhplab aggregate covariate --source hud_psh --target-geo msa`."
        )

    return aggregate_covariate_source(
        HUD_PSH_SOURCE_ID,
        curated_path=county_curated_path,
        years=years,
        target_geo="msa",
    )


def load_hud_psh_msa_panel(*, years: list[int]) -> pd.DataFrame:
    panel_path = ensure_hud_psh_msa_panel(years)
    frame = pd.read_parquet(panel_path)
    if "geo_type" in frame.columns:
        frame = frame[frame["geo_type"].eq("msa")].copy()
    if "msa_id" not in frame.columns:
        if "geo_id" not in frame.columns:
            raise ValueError(
                f"HUD PSH MSA panel {panel_path} is missing both 'msa_id' and 'geo_id'."
            )
        frame["msa_id"] = frame["geo_id"]

    required = {"msa_id", "year", *HUD_PSH_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"HUD PSH MSA panel {panel_path} is missing required columns: {missing}")

    frame["msa_id"] = _as_msa_id(frame["msa_id"])
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    filtered = frame[frame["year"].isin(years)].copy()
    selected = [
        "msa_id",
        "year",
        *HUD_PSH_COLUMNS,
        *[
            column
            for column in ("coverage_ratio", "definition_version")
            if column in filtered.columns
        ],
    ]
    renamed = filtered[selected].rename(columns={"coverage_ratio": "hud_psh_coverage_ratio"})
    return renamed.sort_values(["msa_id", "year"]).reset_index(drop=True)


def add_subsidized_housing_columns(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.copy()
    for column in HUD_PSH_COLUMNS:
        rate_column = f"{column}_per_1000"
        log_rate_column = f"log_{column}_per_1000"
        levels[rate_column] = _safe_per_1000(levels[column], levels["population"])
        levels[log_rate_column] = _safe_log(levels[rate_column])
    return levels


def add_first_differences(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id", sort=False)
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_total_rate"] = grouped["log_total_rate"].diff()
    levels["d_log_shelt_rate"] = grouped["log_shelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    for column in HUD_PSH_LOG_RATE_COLUMNS:
        levels[f"d_{column}"] = grouped[column].diff()
    return levels


def build_levels_panel() -> pd.DataFrame:
    base = load_pooled_base_panel()
    years = [int(year) for year in sorted(base["year"].dropna().unique())]
    hud_psh = load_hud_psh_msa_panel(years=years)
    merged = base.merge(hud_psh, on=["msa_id", "year"], how="left")
    return add_first_differences(add_subsidized_housing_columns(merged))


def _model_specs() -> Iterable[ModelSpec]:
    for column in HUD_PSH_COLUMNS:
        for fixed_effects, suffix in (
            ("year", ""),
            ("region_year", "_region_year_fe"),
            ("primary_state_year", "_state_year_fe"),
        ):
            yield ModelSpec(
                name=f"rent_fd_log_{column}_per_1000{suffix}",
                outcome="d_log_zori",
                predictors=("d_log_pop", f"d_log_{column}_per_1000"),
                fixed_effects=fixed_effects,
            )


def _effect_series(sample: pd.DataFrame, effect: str) -> pd.Series:
    if effect == "year":
        return sample["year"].astype("string")
    if effect == "primary_state_year":
        return sample["primary_state"].astype("string") + "_" + sample["year"].astype("string")
    if effect == "region_year":
        regions = sample["primary_state"].map(census_region)
        return regions.astype("string") + "_" + sample["year"].astype("string")
    raise ValueError(f"Unsupported fixed effect: {effect}")


def fit_clustered_fd_models(fd: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in _model_specs():
        required = [spec.outcome, *spec.predictors, "year", "msa_id", "primary_state"]
        sample = fd.dropna(subset=required).copy()
        if sample.empty:
            continue

        fixed_effect_dummies = pd.get_dummies(
            _effect_series(sample, spec.fixed_effects),
            prefix=spec.fixed_effects,
            drop_first=True,
            dtype=float,
        )
        x = pd.concat([sample[list(spec.predictors)].astype(float), fixed_effect_dummies], axis=1)
        x = sm.add_constant(x, has_constant="add")
        y = sample[spec.outcome].astype(float)
        result = sm.OLS(y, x).fit(
            cov_type="cluster",
            cov_kwds={"groups": sample["msa_id"].astype(str)},
        )

        for term in spec.predictors:
            rows.append(
                {
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
                    "fixed_effects": spec.fixed_effects,
                    "std_error_type": "clustered:msa_id",
                }
            )
    return pd.DataFrame(rows)


def summarize_panel(levels: pd.DataFrame, fd: pd.DataFrame) -> dict[str, object]:
    complete_fd = fd.dropna(subset=["d_log_zori", "d_log_pop", *HUD_PSH_FD_COLUMNS])
    level_summary = (
        levels[[*HUD_PSH_COLUMNS, *HUD_PSH_RATE_COLUMNS]]
        .agg(["mean", "median", "count"])
        .transpose()
        .round({"mean": 6, "median": 6})
    )
    correlations = (
        complete_fd[
            [
                "d_log_zori",
                "d_log_pop",
                *HUD_PSH_FD_COLUMNS,
            ]
        ]
        .corr()
        .round(6)
    )
    coverage = levels.get("hud_psh_coverage_ratio")
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
    return {
        "levels_rows": int(len(levels)),
        "fd_rows_year_gap_1": int(len(fd)),
        "msa_count": int(levels["msa_id"].nunique()),
        "years": [int(year) for year in sorted(levels["year"].dropna().unique())],
        "complete_fd_rows_for_subsidized_housing_screens": int(len(complete_fd)),
        "hud_psh_level_summary": json.loads(level_summary.to_json()),
        "hud_psh_coverage_summary": coverage_summary,
        "levels_missing_hud_psh_columns": {
            column: int(levels[column].isna().sum())
            for column in [*HUD_PSH_COLUMNS, *HUD_PSH_RATE_COLUMNS]
        },
        "fd_missing_hud_psh_diff_columns": {
            column: int(fd[column].isna().sum()) for column in HUD_PSH_FD_COLUMNS
        },
        "fd_correlations": json.loads(correlations.to_json()),
    }


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    levels = build_levels_panel()
    fd = levels[levels["year_gap"] == 1].copy()
    regressions = fit_clustered_fd_models(fd)
    summary = summarize_panel(levels, fd)

    levels_path = OUT / "subsidized_housing_stock_levels.parquet"
    fd_path = OUT / "subsidized_housing_stock_fd.parquet"
    regression_path = OUT / "subsidized_housing_stock_fd_regressions.parquet"
    regression_csv_path = OUT / "subsidized_housing_stock_fd_regressions.csv"
    summary_path = OUT / "subsidized_housing_stock_summary.json"

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
        print(regressions[regressions["term"].str.contains("subsidized|voucher")])


if __name__ == "__main__":
    main()
