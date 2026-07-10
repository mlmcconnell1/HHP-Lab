"""Build a pooled top-150 MSA panel joining Vera county jail population to
HIC beds (by category) and PIT homelessness counts, for contemporaneous
correlation screening.

Vera's incarceration-trends data is county-level and not yet a registered
covariate source (bead coclab-vera-incarceration-catalog-registration-tqwe8),
so this rolls it up to MSA by hand using the same msa_county_membership
crosswalk the rest of the project uses, with a population-weighted coverage
ratio (unlike CDC overdose's county-count-weighted ratio -- see bead
coclab-cdc-overdose-coverage-population-weight-wrwfx -- this one is built
correctly from the start).

Window: 2015-2020 + 2022-2023 (2021 excluded: COVID PIT disruption). Vera's
reliable jail-coverage window is 1999-2023 (verified 2026-07-07); this
script does not reach further back only because the pooled top50+rank51-150
PIT base panels don't either (rank51-150 starts at 2015).

Connecticut is absent entirely from Vera's county file (CT abolished county
government and runs jails at the state level, not a project bug) -- Hartford,
Bridgeport-Stamford-Norwalk, and New Haven MSAs will have zero Vera coverage.
"""

from __future__ import annotations

import glob

import numpy as np
import pandas as pd

from hhplab.msa import read_msa_county_membership
from hhplab.results.workflows._paths import DATA_ROOT, OUTPUTS_ROOT, REPO_ROOT, write_result_parquet

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "vera_hic_pit"
HIC_BY_CATEGORY_DIR = OUTPUTS_ROOT / "overdose_lag" / "hic_by_category"

TOP50_PANEL = OUTPUTS_ROOT / "top50_msa_longitudinal_2010_2025.parquet"
RANK51_150_PANEL = (
    OUTPUTS_ROOT
    / "msa_rank51_150_replication"
    / "panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet"
)
VERA_COUNTY = (
    DATA_ROOT
    / "curated"
    / "vera"
    / "vera_incarceration_county__Y1970-2026@C2020.parquet"
)

EXCLUDED_YEARS = {2021}
# Vera's reliable jail-coverage window is 1999-2023 (verified 2026-07-07,
# see coclab-vera-incarceration-catalog-registration-tqwe8); 2024-2025 taper
# off sharply and are excluded here.
PANEL_YEARS = {2015, 2016, 2017, 2018, 2019, 2020, 2022, 2023}

HIC_BED_COLUMNS = [
    "hic_es_year_round_beds",
    "hic_th_year_round_beds",
    "hic_sh_year_round_beds",
    "hic_rrh_year_round_beds",
    "hic_psh_year_round_beds",
    "hic_oph_year_round_beds",
    "hic_total_beds",
]

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
    pooled = pooled[pooled.year.isin(PANEL_YEARS)].sort_values(["msa_id", "year"])
    return pooled.reset_index(drop=True)


def aggregate_vera_to_msa(definition_version: str = "census_msa_2023") -> pd.DataFrame:
    membership = read_msa_county_membership(definition_version)[
        ["msa_id", "county_fips"]
    ].drop_duplicates()
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)

    vera = pd.read_parquet(VERA_COUNTY, columns=["county_fips", "year", "total_jail_pop"])
    vera["county_fips"] = vera["county_fips"].astype(str).str.zfill(5)
    vera = vera[vera.year.isin(PANEL_YEARS)]

    joined = membership.merge(vera, on="county_fips", how="left")

    # Population-weighted coverage ratio (available-county population share
    # of expected MSA population), unlike CDC's county-count-weighted ratio.
    pep = pd.read_parquet(TOP50_PANEL, columns=["msa_id", "year", "population"])
    pep_r150 = pd.read_parquet(RANK51_150_PANEL, columns=["msa_id", "year", "population"])
    msa_pop = pd.concat([pep, pep_r150], ignore_index=True).drop_duplicates(["msa_id", "year"])

    rows = []
    for (msa_id, year), group in joined.groupby(["msa_id", "year"]):
        expected_counties = group["county_fips"].nunique()
        available = group.loc[group["total_jail_pop"].notna()]
        total_jail_pop = group["total_jail_pop"].sum(min_count=1)
        rows.append(
            {
                "msa_id": msa_id,
                "year": year,
                "total_jail_pop": total_jail_pop,
                "vera_county_count": len(available),
                "vera_county_expected": expected_counties,
            }
        )
    vera_msa = pd.DataFrame(rows)
    vera_msa = vera_msa.merge(msa_pop, on=["msa_id", "year"], how="left")
    return vera_msa


def merge_hic_categories(base: pd.DataFrame) -> pd.DataFrame:
    files = sorted(glob.glob(str(HIC_BY_CATEGORY_DIR / "panel__msa-rollup-hic__*.parquet")))
    hic = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    hic = hic[hic.year.isin(PANEL_YEARS)]
    merged = base.merge(
        hic[["msa_id", "year", "coc_population_coverage_ratio"] + HIC_BED_COLUMNS],
        on=["msa_id", "year"],
        how="left",
    )
    for col in HIC_BED_COLUMNS:
        rate_col = col.replace("hic_", "").replace("_year_round_beds", "") + "_per_1000"
        merged[rate_col] = merged[col] / merged["population"] * 1000
    return merged


def run() -> dict[str, object]:
    OUT.mkdir(parents=True, exist_ok=True)
    pooled = load_pooled_base_panel()
    pooled = merge_hic_categories(pooled)

    vera_msa = aggregate_vera_to_msa()
    merged = pooled.merge(vera_msa, on=["msa_id", "year"], how="left", suffixes=("", "_vera"))
    merged["jail_per_1000"] = merged["total_jail_pop"] / merged["population"] * 1000
    merged["log_jail_rate"] = np.where(
        merged["jail_per_1000"] > 0, np.log(merged["jail_per_1000"]), np.nan
    )

    out_path = OUT / "vera_hic_pit_levels.parquet"
    write_result_parquet(merged, out_path, index=False)

    ct_msas = merged.loc[merged.msa_name.str.contains("CT", na=False), "msa_name"].unique()
    return {
        "pooled_cohorts": merged.cohort.value_counts().to_dict(),
        "rows": int(len(merged)),
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
        "ct_msas_expected_all_null_jail_coverage": [str(msa) for msa in ct_msas],
        "outputs": {"levels_parquet": str(out_path)},
    }


def main() -> None:
    result = run()

    print(f"pooled cohorts: {result['pooled_cohorts']}")
    print(f"rows: {result['rows']} -> {result['outputs']['levels_parquet']}")
    print("rows per year:")
    print(result["rows_by_year"])
    print("non-null jail_per_1000 rows per year:")
    print(result["non_null_jail_per_1000_rows_by_year"])
    print(
        "CT MSAs in cohort (expect all-null jail coverage): "
        f"{result['ct_msas_expected_all_null_jail_coverage']}"
    )


if __name__ == "__main__":
    main()
