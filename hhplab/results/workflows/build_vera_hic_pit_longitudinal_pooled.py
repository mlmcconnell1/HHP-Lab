"""Pooled top-150 longitudinal panel (2015-2020+2022-2023) joining Vera
county jail population to HIC beds (by category) and PIT homelessness
counts, for an entity+year-FE lag design.

Same design as build_vera_hic_pit_longitudinal.py (top-50-only, 2010-2023)
but pools top-50 + rank-51-150 (150 MSAs, no overlap) for cross-sectional
power, at the cost of the shorter window rank-51-150's PIT base panel
allows (2015-2020+2022-2023 -- 2021 excluded: COVID PIT disruption). Vera's
reliable jail-coverage window is 1999-2023 and ZORI itself only starts
2015, so this window is not actually narrower than what the top-50-only
design's rent-controlled specs used in practice (see
devdocs/vera_jail_hic_pit_longitudinal.md's design correction) -- this
version simply adds the 100 rank-51-150 MSAs across the same years.

Connecticut is absent entirely from Vera's county file (state-run jails,
not a project bug) -- the 4 CT MSAs in the rank-51-150 cohort
(Bridgeport-Stamford-Danbury, Hartford, New Haven, Waterbury-Shelton) drop
out of every jail-predictor spec via complete-case filtering.
"""

from __future__ import annotations

import glob

import numpy as np
import pandas as pd

from hhplab.msa import read_msa_county_membership
from hhplab.results.workflows._paths import REPO_ROOT

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "vera_hic_pit_longitudinal_pooled"
HIC_BY_CATEGORY_DIR = ROOT / "outputs" / "overdose_lag" / "hic_by_category"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
VERA_COUNTY = (
    ROOT
    / "data"
    / "curated"
    / "vera"
    / "vera_incarceration_county__Y1970-2026@C2020.parquet"
)

EXCLUDED_YEARS = {2021}
PANEL_YEARS = {2015, 2016, 2017, 2018, 2019, 2020, 2022, 2023}
COVID_JAIL_YEAR = 2020

HIC_BED_COLUMNS = {
    "es": "hic_es_year_round_beds",
    "psh": "hic_psh_year_round_beds",
    "total_beds": "hic_total_beds",
}

CORE_COLUMNS = [
    "msa_id",
    "msa_name",
    "year",
    "population",
    "sanctuary",
    "pit_unsheltered",
    "pit_sheltered",
    "pit_total",
    "unshelt_per_1000",
    "zori",
    "log_zori",
    "log_unshelt_rate",
    "log_total_rate",
    "log_shelt_rate",
    "log_pop",
]

PIT_LOG_COLUMN = {
    "unshelt": "log_unshelt_rate",
    "total": "log_total_rate",
    "shelt": "log_shelt_rate",
}


def primary_state(msa_name: str) -> str:
    return msa_name.rsplit(",", 1)[-1].strip().split("-")[0]


def _safe_log(series: pd.Series) -> pd.Series:
    values = series.astype("float64").where(series > 0)
    return pd.Series(np.log(values), index=series.index)


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


def merge_hic_categories(base: pd.DataFrame) -> pd.DataFrame:
    files = sorted(glob.glob(str(HIC_BY_CATEGORY_DIR / "panel__msa-rollup-hic__*.parquet")))
    hic = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    hic = hic[hic.year.isin(PANEL_YEARS)]
    cols = list(HIC_BED_COLUMNS.values())
    merged = base.merge(
        hic[["msa_id", "year", "coc_population_coverage_ratio"] + cols],
        on=["msa_id", "year"],
        how="left",
    )
    for key, col in HIC_BED_COLUMNS.items():
        merged[f"{key}_per_1000"] = merged[col] / merged["population"] * 1000
    return merged


def aggregate_vera_to_msa(definition_version: str = "census_msa_2023") -> pd.DataFrame:
    membership = read_msa_county_membership(definition_version)[
        ["msa_id", "county_fips"]
    ].drop_duplicates()
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)

    vera = pd.read_parquet(
        VERA_COUNTY, columns=["county_fips", "year", "total_jail_pop"]
    )
    vera["county_fips"] = vera["county_fips"].astype(str).str.zfill(5)
    vera = vera[vera.year.isin(PANEL_YEARS)]

    joined = membership.merge(vera, on="county_fips", how="left")
    rows = []
    for (msa_id, year), group in joined.groupby(["msa_id", "year"]):
        available = group.loc[group["total_jail_pop"].notna()]
        rows.append(
            {
                "msa_id": msa_id,
                "year": year,
                "total_jail_pop": group["total_jail_pop"].sum(min_count=1),
                "vera_county_count": len(available),
                "vera_county_expected": group["county_fips"].nunique(),
            }
        )
    return pd.DataFrame(rows)


def add_diffs(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id")
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["lag1_year_gap"] = grouped["year_gap"].shift(1)

    log_cols = dict(PIT_LOG_COLUMN)
    log_cols["jail"] = "log_jail_rate"
    for key in HIC_BED_COLUMNS:
        log_cols[key] = f"log_{key}_rate"

    for key, log_col in log_cols.items():
        d_col = "d_log_jail_rate" if key == "jail" else f"d_log_{key}_rate"
        if log_col not in levels.columns:
            continue
        levels[d_col] = grouped[log_col].diff()
        levels[f"{log_col}_lag1"] = grouped[log_col].shift(1)
        levels[f"{d_col}_lag1"] = levels.groupby("msa_id")[d_col].shift(1)
    return levels


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_base_panel()
    pooled = merge_hic_categories(pooled)

    vera_msa = aggregate_vera_to_msa()
    merged = pooled.merge(vera_msa, on=["msa_id", "year"], how="left")
    merged["jail_per_1000"] = merged["total_jail_pop"] / merged["population"] * 1000
    merged["log_jail_rate"] = _safe_log(merged["jail_per_1000"])
    for key in HIC_BED_COLUMNS:
        col = f"{key}_per_1000"
        merged[f"log_{key}_rate"] = _safe_log(merged[col])

    merged = add_diffs(merged)

    out_path = OUT / "vera_hic_pit_longitudinal_pooled_levels.parquet"
    merged.to_parquet(out_path, index=False)
    fd = merged[merged.year_gap == 1].copy()
    fd_path = OUT / "vera_hic_pit_longitudinal_pooled_fd.parquet"
    fd.to_parquet(fd_path, index=False)

    print(f"pooled cohorts: {merged.cohort.value_counts().to_dict()}")
    print(f"levels rows: {len(merged)} -> {out_path}")
    print(f"fd (year_gap==1) rows: {len(fd)} -> {fd_path}")
    print("rows per year:")
    print(merged.groupby("year").size())
    print("non-null jail_per_1000 rows per year:")
    print(merged.dropna(subset=["jail_per_1000"]).groupby("year").size())
    print("distinct primary_state count:", merged.primary_state.nunique())


if __name__ == "__main__":
    main()
