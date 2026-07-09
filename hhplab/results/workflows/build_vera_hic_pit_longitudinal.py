"""Top-50-only longitudinal panel (2010-2023) joining Vera county jail
population to HIC beds (by category) and PIT homelessness counts, for an
entity+year-FE lag design.

Unlike scripts/build_vera_hic_pit_panel.py (pooled top-150, 2015-2020+2022-23,
contemporaneous correlations only), this trades cross-sectional breadth for
time depth: top-50 alone reaches back to 2010 (rank-51-150's PIT base panel
starts 2015 and isn't extended here), giving ~11 usable annual transitions
per MSA instead of ~6-7, which is what a lag/timing design actually needs.

Window: 2010-2020 + 2022-2023 (2021 excluded: COVID PIT disruption). Vera's
reliable jail-coverage window is 1999-2023; 2024-2025 excluded because Vera's
own coverage tapers sharply there (see
coclab-vera-incarceration-catalog-registration-tqwe8).

Adds a coarse `primary_state` (first state code in the MSA name, e.g. "TX"
from "Austin-Round Rock-San Marcos, TX") for a state x year FE robustness
check -- jail population is driven substantially by state-level sentencing/
release policy in a way MSA entity+year FE alone won't absorb, especially
for multi-state MSAs sharing a state-level policy shock.
"""

from __future__ import annotations

import glob

import numpy as np
import pandas as pd

from hhplab.msa import read_msa_county_membership
from hhplab.results.workflows._paths import REPO_ROOT, write_result_parquet

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "vera_hic_pit_longitudinal"
HIC_BY_CATEGORY_DIR = ROOT / "outputs" / "overdose_lag" / "hic_by_category"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
VERA_COUNTY = (
    ROOT / "data" / "curated" / "vera" / "vera_incarceration_county__Y1970-2026@C2020.parquet"
)

EXCLUDED_YEARS = {2021}
PANEL_YEARS = {2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2022, 2023}
# 2020 flagged separately: most US jails saw large, policy-driven (COVID
# decarceration) population drops in 2020 unrelated to local homelessness
# dynamics -- not excluded by default, but every headline spec is also run
# excluding it as a sensitivity check.
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


def load_base_panel() -> pd.DataFrame:
    df = pd.read_parquet(TOP50_PANEL)[CORE_COLUMNS].copy()
    df = df[df.year.isin(PANEL_YEARS)].sort_values(["msa_id", "year"])
    df["primary_state"] = df["msa_name"].map(primary_state)
    return df.reset_index(drop=True)


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

    vera = pd.read_parquet(VERA_COUNTY, columns=["county_fips", "year", "total_jail_pop"])
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
        d_col = f"d_log_{key}_rate" if key != "jail" else "d_log_jail_rate"
        if log_col not in levels.columns:
            continue
        levels[d_col] = grouped[log_col].diff()
        levels[f"{log_col}_lag1"] = grouped[log_col].shift(1)
        levels[f"{d_col}_lag1"] = levels.groupby("msa_id")[d_col].shift(1)
    return levels


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    base = load_base_panel()
    base = merge_hic_categories(base)

    vera_msa = aggregate_vera_to_msa()
    merged = base.merge(vera_msa, on=["msa_id", "year"], how="left")
    merged["jail_per_1000"] = merged["total_jail_pop"] / merged["population"] * 1000
    merged["log_jail_rate"] = _safe_log(merged["jail_per_1000"])
    for key in HIC_BED_COLUMNS:
        col = f"{key}_per_1000"
        merged[f"log_{key}_rate"] = _safe_log(merged[col])

    merged = add_diffs(merged)

    out_path = OUT / "vera_hic_pit_longitudinal_levels.parquet"
    write_result_parquet(merged, out_path, index=False)
    fd = merged[merged.year_gap == 1].copy()
    fd_path = OUT / "vera_hic_pit_longitudinal_fd.parquet"
    write_result_parquet(fd, fd_path, index=False)

    return {
        "levels_rows": int(len(merged)),
        "fd_rows_year_gap_1": int(len(fd)),
        "rows_by_year": {
            int(year): int(count) for year, count in merged.groupby("year").size().items()
        },
        "non_null_jail_per_1000_rows_by_year": {
            int(year): int(count)
            for year, count in merged.dropna(subset=["jail_per_1000"])
            .groupby("year")
            .size()
            .items()
        },
        "distinct_primary_state_count": int(merged.primary_state.nunique()),
        "outputs": {
            "levels_parquet": str(out_path),
            "fd_parquet": str(fd_path),
        },
    }


def main() -> None:
    result = run()
    outputs = result["outputs"]

    print(f"levels rows: {result['levels_rows']} -> {outputs['levels_parquet']}")
    print(f"fd (year_gap==1) rows: {result['fd_rows_year_gap_1']} -> {outputs['fd_parquet']}")
    print("rows per year:")
    print(result["rows_by_year"])
    print("non-null jail_per_1000 rows per year:")
    print(result["non_null_jail_per_1000_rows_by_year"])
    print("distinct primary_state count:", result["distinct_primary_state_count"])


if __name__ == "__main__":
    main()
