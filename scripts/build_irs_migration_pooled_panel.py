"""Pooled top-150 first-difference panel joining IRS SOI county-to-county
migration (already registered as covariate `irs_soi_migration`, already
rolled up to MSA in the curated covariate panel) to PIT unsheltered
homelessness and ZORI rent, replicating and extending the top-50-only
`outputs/top50_msa_migration_fd.parquet` churn x rent-shock interaction to
the pooled top-50 + rank-51-150 cohort.

IRS SOI year alignment: curated rows use the later filing year, and this
project's convention (devdocs/irs_soi_migration_workflow.md) treats an IRS
flow labeled year Y as a preceding exposure for PIT year Y+1. Verified
directly against the existing top-50 file: its "year"==2023 row's
inflow_returns matches the raw IRS panel's year==2022 value exactly, i.e.
irs_year = pit_year - 1. Replicated here the same way.

churn_rate/inflow_rate/outflow_rate/net_rate are returns-per-1000-population
(verified exactly against the existing top-50 file); the interaction term
is the contemporaneous churn *level* times the rent-shock *difference*
(also verified exactly, not a diff-in-diff).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "irs_migration_pooled"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
IRS_COVARIATE_PANEL = (
    ROOT
    / "data"
    / "curated"
    / "covariates"
    / "covariate_panel__irs_soi_migration__Y2012-2023.parquet"
)

EXCLUDED_YEARS = {2021}
# PIT years usable given rank-51-150 starts 2015 and IRS (shifted +1) tops
# out at pit_year=2024 (irs_year=2023).
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


def primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def load_pooled_base_panel() -> pd.DataFrame:
    top50 = pd.read_parquet(TOP50_PANEL)[CORE_COLUMNS].copy()
    top50["cohort"] = "top50"
    rank51_150 = pd.read_parquet(RANK51_150_PANEL)[CORE_COLUMNS].copy()
    rank51_150["cohort"] = "rank51_150"
    overlap = set(top50.msa_id) & set(rank51_150.msa_id)
    if overlap:
        raise ValueError(f"Unexpected msa_id overlap between cohorts: {overlap}")
    pooled = pd.concat([top50, rank51_150], ignore_index=True)
    pooled = pooled[pooled.year.isin(PANEL_YEARS)].sort_values(["msa_id", "year"])
    pooled["primary_state"] = pooled["msa_name"].map(primary_state)
    return pooled.reset_index(drop=True)


def merge_irs_migration(base: pd.DataFrame) -> pd.DataFrame:
    irs = pd.read_parquet(IRS_COVARIATE_PANEL)
    irs = irs[irs.geo_type == "msa"][
        ["msa_id", "year", "inflow_returns", "outflow_returns", "net_returns",
         "intra_msa_returns", "coverage_ratio"]
    ].rename(columns={"year": "irs_year", "coverage_ratio": "irs_coverage_ratio"})
    # irs_year = pit_year - 1 (verified against the existing top-50 panel).
    irs["year"] = irs["irs_year"] + 1

    merged = base.merge(irs, on=["msa_id", "year"], how="left")
    merged["inflow_rate"] = merged["inflow_returns"] / merged["population"] * 1000
    merged["outflow_rate"] = merged["outflow_returns"] / merged["population"] * 1000
    merged["churn_rate"] = merged["inflow_rate"] + merged["outflow_rate"]
    merged["net_rate"] = merged["inflow_rate"] - merged["outflow_rate"]
    for col in ("inflow_rate", "outflow_rate"):
        values = merged[col].astype("float64")
        merged[f"log_{col.replace('_rate', '')}_rate"] = np.where(
            values > 0, np.log(values), np.nan
        )
    return merged


def add_diffs(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id")
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["d_net_rate"] = grouped["net_rate"].diff()
    levels["d_log_inflow_rate"] = grouped["log_inflow_rate"].diff()
    levels["d_log_outflow_rate"] = grouped["log_outflow_rate"].diff()
    levels["d_log_zori_x_churn_rate"] = levels["d_log_zori"] * levels["churn_rate"]
    return levels


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_base_panel()
    merged = merge_irs_migration(pooled)
    merged = add_diffs(merged)

    levels_path = OUT / "irs_migration_pooled_levels.parquet"
    merged.to_parquet(levels_path, index=False)
    fd = merged[merged.year_gap == 1].copy()
    fd_path = OUT / "irs_migration_pooled_fd.parquet"
    fd.to_parquet(fd_path, index=False)

    print(f"pooled cohorts: {merged.cohort.value_counts().to_dict()}")
    print(f"levels rows: {len(merged)} -> {levels_path}")
    print(f"fd (year_gap==1) rows: {len(fd)} -> {fd_path}")
    print("fd rows per year:")
    print(fd.groupby("year").size())
    print(
        "fd rows with non-null churn_rate/d_log_zori_x_churn_rate:",
        fd.dropna(subset=["churn_rate", "d_log_zori_x_churn_rate", "d_log_unshelt_rate"]).shape[0],
    )
    print("distinct primary_state count:", merged.primary_state.nunique())


if __name__ == "__main__":
    main()
