"""Build a pooled top-150 MSA panel joining unsheltered homelessness to CDC
provisional overdose deaths, for screening a homelessness -> overdose lag
relationship.

Pools the top-50 longitudinal panel with the rank-51-150 replication panel
(150 distinct MSAs, no overlap), merges CDC's January-aligned MSA overdose
rollup, and emits both a levels panel and an annual first-difference panel
restricted to CDC's actual coverage window (2020-2025, excluding 2021 on both
sides: PIT 2021 is COVID-disrupted and CDC 2021 is a national fentanyl-driven
outlier year).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from hhplab.results.workflows._paths import REPO_ROOT

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "overdose_lag"

TOP50_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    ROOT
    / "outputs"
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
CDC_MSA = (
    ROOT
    / "data"
    / "curated"
    / "cdc"
    / "cdc_overdose__msa__Y2020-2025@Mcensusmsa2023xC2023.parquet"
)
# Built by the `hhplab aggregate coc-measure` loop documented in
# devdocs/overdose_homelessness_lag_screen.md (era-matched boundaries:
# B2020 for 2020, B2024 for 2022-2025; county vintage 2023 for CT planning
# region coverage), then concatenated across years.
HIC_BY_CATEGORY = OUT / "hic_by_category_pooled.parquet"

HIC_BED_COLUMNS = [
    "hic_es_year_round_beds",
    "hic_th_year_round_beds",
    "hic_sh_year_round_beds",
    "hic_rrh_year_round_beds",
    "hic_psh_year_round_beds",
    "hic_oph_year_round_beds",
    "hic_total_beds",
]
HIC_CATEGORY_LABEL = {
    "es": "Emergency Shelter",
    "th": "Transitional Housing",
    "sh": "Safe Haven",
    "rrh": "Rapid Re-Housing",
    "psh": "Permanent Supportive Housing",
    "oph": "Other Permanent Housing",
    "total_beds": "All HIC beds",
}

# Both PIT 2021 (COVID enumeration disruption) and CDC 2021 (national
# fentanyl-driven overdose spike, not a local-homelessness-driven signal)
# are excluded from this analysis.
EXCLUDED_YEARS = {2021}
CDC_YEARS = {2020, 2022, 2023, 2024, 2025}
MIN_OVERDOSE_COVERAGE = 0.8

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
    return pooled.reset_index(drop=True)


def merge_overdose(pooled: pd.DataFrame) -> pd.DataFrame:
    cdc = pd.read_parquet(CDC_MSA)
    cdc = cdc[cdc.year.isin(CDC_YEARS)][
        ["msa_id", "year", "overdose_deaths_12mo", "coverage_ratio"]
    ].rename(columns={"coverage_ratio": "overdose_coverage_ratio"})
    merged = pooled.merge(cdc, on=["msa_id", "year"], how="inner")
    merged["overdose_per_1000"] = (
        merged["overdose_deaths_12mo"] / merged["population"] * 1000
    )
    merged["log_overdose_rate"] = np.where(
        merged["overdose_per_1000"] > 0, np.log(merged["overdose_per_1000"]), np.nan
    )
    return merged


HOMELESS_MARGINS = ("unshelt", "total", "shelt")
MARGIN_LOG_COLUMN = {
    "unshelt": "log_unshelt_rate",
    "total": "log_total_rate",
    "shelt": "log_shelt_rate",
}


def add_diffs(levels: pd.DataFrame) -> pd.DataFrame:
    levels = levels.sort_values(["msa_id", "year"]).copy()
    grouped = levels.groupby("msa_id")
    levels["year_gap"] = grouped["year"].diff()
    levels["d_log_overdose_rate"] = grouped["log_overdose_rate"].diff()
    levels["d_log_zori"] = grouped["log_zori"].diff()
    levels["d_log_pop"] = grouped["log_pop"].diff()
    levels["overdose_coverage_ratio_lag1"] = grouped["overdose_coverage_ratio"].shift(1)
    levels["lag1_year_gap"] = grouped["year_gap"].shift(1)
    for margin, log_col in MARGIN_LOG_COLUMN.items():
        d_col = f"d_log_{margin}_rate"
        levels[d_col] = grouped[log_col].diff()
        levels[f"{log_col}_lag1"] = grouped[log_col].shift(1)
        levels[f"{d_col}_lag1"] = levels.groupby("msa_id")[d_col].shift(1)
    return levels


def merge_hic_categories(levels: pd.DataFrame) -> pd.DataFrame:
    """Attach per-1000 HIC bed-category rates. Requires HIC_BY_CATEGORY to
    already exist (see the `hhplab aggregate coc-measure` loop documented in
    devdocs/overdose_homelessness_lag_screen.md)."""
    if not HIC_BY_CATEGORY.exists():
        return levels
    hic = pd.read_parquet(HIC_BY_CATEGORY)
    merged = levels.merge(
        hic[["msa_id", "year", "coc_population_coverage_ratio"] + HIC_BED_COLUMNS],
        on=["msa_id", "year"],
        how="left",
    )
    for col in HIC_BED_COLUMNS:
        rate_col = col.replace("hic_", "").replace("_year_round_beds", "") + "_per_1000"
        merged[rate_col] = merged[col] / merged["population"] * 1000
    return merged


def add_psh_diffs(levels: pd.DataFrame) -> pd.DataFrame:
    """PSH log-rate, first difference, and lag terms, mirroring the
    unshelt/total/shelt treatment in add_diffs. Requires merge_hic_categories
    to have already run (needs psh_per_1000) and add_diffs to have already
    run (needs year_gap)."""
    if "psh_per_1000" not in levels.columns:
        return levels
    levels = levels.sort_values(["msa_id", "year"]).copy()
    levels["log_psh_rate"] = np.where(
        levels["psh_per_1000"] > 0, np.log(levels["psh_per_1000"]), np.nan
    )
    grouped = levels.groupby("msa_id")
    levels["d_log_psh_rate"] = grouped["log_psh_rate"].diff()
    levels["log_psh_rate_lag1"] = grouped["log_psh_rate"].shift(1)
    levels["d_log_psh_rate_lag1"] = levels.groupby("msa_id")["d_log_psh_rate"].shift(1)
    return levels


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_base_panel()
    merged = merge_overdose(pooled)
    merged = add_diffs(merged)
    merged = merge_hic_categories(merged)
    merged = add_psh_diffs(merged)

    levels_path = OUT / "overdose_lag_levels.parquet"
    merged.to_parquet(levels_path, index=False)

    fd = merged[merged.year_gap == 1].copy()
    fd_path = OUT / "overdose_lag_fd.parquet"
    fd.to_parquet(fd_path, index=False)

    covered = merged[merged.overdose_coverage_ratio >= MIN_OVERDOSE_COVERAGE]
    return {
        "pooled_cohorts": merged.cohort.value_counts().to_dict(),
        "levels_rows": int(len(merged)),
        "fd_rows_year_gap_1": int(len(fd)),
        "rows_by_year": {int(year): int(count) for year, count in merged.groupby("year").size().items()},
        "rows_by_year_meeting_overdose_coverage": {
            int(year): int(count) for year, count in covered.groupby("year").size().items()
        },
        "minimum_overdose_coverage": MIN_OVERDOSE_COVERAGE,
        "outputs": {
            "levels_parquet": str(levels_path),
            "fd_parquet": str(fd_path),
        },
    }


def main() -> None:
    result = run()
    outputs = result["outputs"]

    print(f"pooled cohorts: {result['pooled_cohorts']}")
    print(f"levels rows: {result['levels_rows']} -> {outputs['levels_parquet']}")
    print(f"fd (year_gap==1) rows: {result['fd_rows_year_gap_1']} -> {outputs['fd_parquet']}")
    print("rows per year (levels):")
    print(result["rows_by_year"])
    print("rows per year meeting overdose_coverage_ratio>=0.8 (levels):")
    print(result["rows_by_year_meeting_overdose_coverage"])


if __name__ == "__main__":
    main()
