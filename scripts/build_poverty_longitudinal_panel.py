"""Pooled top-150 longitudinal panel adding ACS5 poverty rate to the core
rent-shock -> unsheltered-growth design, to check whether poverty is an
independent predictor and/or whether it explains away the rent elasticity.

Poverty (`msa_poverty_rate`, population-weighted ACS5 tract rollup via
`hhplab aggregate acs --target-geo msa`, table C17002) is merged using this
project's standard ACS lag rule: ACS vintage ending year E is the covariate
for PIT year E+1. Built via `data/curated/measures/measures__msa__A*.parquet`
(2009-2024 vintages, already materialized).
"""

from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "poverty_longitudinal"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
MEASURES_GLOB = str(ROOT / "data" / "curated" / "measures" / "measures__msa__A*.parquet")

EXCLUDED_YEARS = {2021}

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
    pooled = pooled[~pooled.year.isin(EXCLUDED_YEARS)].sort_values(["msa_id", "year"])
    pooled["primary_state"] = pooled["msa_name"].map(primary_state)
    return pooled.reset_index(drop=True)


def load_poverty_panel() -> pd.DataFrame:
    files = sorted(glob.glob(MEASURES_GLOB))
    frames = [
        pd.read_parquet(f, columns=["msa_id", "acs_vintage", "msa_poverty_rate"])
        for f in files
    ]
    poverty = pd.concat(frames, ignore_index=True)
    poverty["acs_end_year"] = poverty["acs_vintage"].str.split("-").str[-1].astype(int)
    # ACS lag rule: vintage ending year E is the covariate for PIT year E+1.
    poverty["year"] = poverty["acs_end_year"] + 1
    return poverty[["msa_id", "year", "msa_poverty_rate"]].rename(
        columns={"msa_poverty_rate": "poverty_rate"}
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_base_panel()
    poverty = load_poverty_panel()
    merged = pooled.merge(poverty, on=["msa_id", "year"], how="left")

    merged = merged.sort_values(["msa_id", "year"]).copy()
    grouped = merged.groupby("msa_id")
    merged["year_gap"] = grouped["year"].diff()
    merged["d_log_zori"] = grouped["log_zori"].diff()
    merged["d_log_unshelt_rate"] = grouped["log_unshelt_rate"].diff()
    merged["d_log_pop"] = grouped["log_pop"].diff()
    merged["d_poverty_rate"] = grouped["poverty_rate"].diff()

    out_path = OUT / "poverty_longitudinal_levels.parquet"
    merged.to_parquet(out_path, index=False)
    fd = merged[merged.year_gap == 1].copy()
    fd_path = OUT / "poverty_longitudinal_fd.parquet"
    fd.to_parquet(fd_path, index=False)

    print(f"pooled cohorts: {merged.cohort.value_counts().to_dict()}")
    print(f"levels rows: {len(merged)} -> {out_path}")
    print(f"fd (year_gap==1) rows: {len(fd)} -> {fd_path}")
    print("non-null poverty_rate rows per year (levels):")
    print(merged.dropna(subset=["poverty_rate"]).groupby("year").size())
    print(
        "fd rows with complete case (unshelt, zori, poverty):",
        fd.dropna(subset=["d_log_unshelt_rate", "d_log_zori", "d_poverty_rate"]).shape[0],
    )


if __name__ == "__main__":
    main()
