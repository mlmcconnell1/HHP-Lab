"""Build the MSA supply-instrument panels for bead coclab-znltg.

Inputs
------
- outputs/top50_msa_longitudinal_2015_2025.parquet  (base levels panel, 50 MSAs)
- data/curated/covariates/covariate_panel__census_bps__Y1980-ongoing.parquet
  (MSA-year permitted units, 2010-2024, census_msa_2023)
- data/curated/covariates/covariate__census_bps__Y2000-2024.parquet
  (county-year permits for the 2000-2009 pre-PEP window)
- data/curated/msa/msa_county_membership__census_msa_2023.parquet
- data/curated/zori/zori__msa__Y2015-2025@... (all-MSA rent index for the
  leave-one-out national shift)
- data/raw/saiz_elasticity/saiz2010_supply_elasticity.dta (Saiz 2010 QJE)

Outputs under ``outputs/supply_iv/``:

- ``top50_msa_supply_iv_fd.parquet``: adjacent-year first differences with
  exposure x shift instruments (n=400)
- ``top50_msa_supply_iv_longdiff.parquet``: 2015->2025 long differences with
  static supply-constraint instruments (n=50)
- ``saiz_match_audit.csv``: name-match audit for the Saiz join
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "supply_iv"

BASE_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2015_2025.parquet"
BPS_MSA = ROOT / "data/curated/covariates/covariate_panel__census_bps__Y1980-ongoing.parquet"
BPS_COUNTY = ROOT / "data/curated/covariates/covariate__census_bps__Y2000-2024.parquet"
MEMBERSHIP = ROOT / "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
ZORI_MSA = (
    ROOT
    / "data/curated/zori/zori__msa__Y2015-2025@Mcensusmsa2023xC2023__wpopulation__mpit_january__balanced.parquet"
)
SAIZ = ROOT / "data/raw/saiz_elasticity/saiz2010_supply_elasticity.dta"

# Pre-sample exposure windows (the FD estimation sample starts with the
# 2015->2016 transition).
BPS_SHORT_WINDOW = range(2010, 2015)
BPS_LONG_WINDOW = range(2000, 2015)

# Saiz (2010) metros are 1999 MSA/NECMA definitions; map top-50 CBSA titles
# whose principal city does not literally appear in the Saiz name.
SAIZ_NAME_OVERRIDES: dict[str, str] = {
    "Virginia Beach-Chesapeake-Norfolk, VA-NC": "Norfolk-Virginia Beach-Newport News, VA-NC",
    "Urban Honolulu, HI": "Honolulu, HI",
    "Louisville/Jefferson County, KY-IN": "Louisville, KY-IN",
}

# Sacramento-Roseville-Folsom, CA has no Saiz (2010) metro: the source file's
# 269 metros do not include Sacramento. Saiz-instrumented specs run on 49 MSAs.


def zscore(values: pd.Series) -> pd.Series:
    return (values - values.mean()) / values.std(ddof=0)


def build_bps_exposures(top50: pd.DataFrame) -> pd.DataFrame:
    """Pre-sample permits intensity per MSA, short (2010-14) and long (2000-14)."""
    bps = pd.read_parquet(BPS_MSA)
    bps = bps[bps["msa_id"].isin(top50["msa_id"])].copy()
    short = bps[bps["year"].isin(BPS_SHORT_WINDOW)].copy()
    short["permits_per_1000"] = (
        short["permitted_units"] / short["population_weight_denominator"] * 1000
    )
    exposure = (
        short.groupby("msa_id")["permits_per_1000"].mean().rename("bps_permits_per_1000_1014")
    )

    county = pd.read_parquet(BPS_COUNTY)
    county = county[county["year"].isin(BPS_LONG_WINDOW)]
    members = pd.read_parquet(MEMBERSHIP)
    county_to_msa = members[["county_fips", "msa_id"]].drop_duplicates()
    long_msa = (
        county.merge(county_to_msa, on="county_fips", how="inner")
        .groupby(["msa_id", "year"], as_index=False)["permitted_units"]
        .sum()
    )
    pop_2010 = (
        bps[bps["year"] == 2010]
        .set_index("msa_id")["population_weight_denominator"]
        .rename("pop_2010")
    )
    long_mean = (
        long_msa[long_msa["msa_id"].isin(top50["msa_id"])]
        .groupby("msa_id")["permitted_units"]
        .mean()
        .rename("mean_units_0014")
    )
    frame = pd.concat([exposure, long_mean, pop_2010], axis=1)
    frame["bps_permits_per_1000_0014"] = frame["mean_units_0014"] / frame["pop_2010"] * 1000
    frame["supply_constraint_bps"] = zscore(-np.log(frame["bps_permits_per_1000_1014"]))
    frame["supply_constraint_bps_long"] = zscore(-np.log(frame["bps_permits_per_1000_0014"]))
    return frame[
        [
            "bps_permits_per_1000_1014",
            "bps_permits_per_1000_0014",
            "supply_constraint_bps",
            "supply_constraint_bps_long",
        ]
    ].reset_index()


def match_saiz(top50: pd.DataFrame) -> pd.DataFrame:
    """Join Saiz (2010) supply measures onto top-50 CBSAs by principal city."""
    saiz = pd.read_stata(SAIZ)
    saiz["saiz_name"] = saiz["msaname"].astype(str).str.strip()
    rows = []
    for _, row in top50.drop_duplicates("msa_id").iterrows():
        title = SAIZ_NAME_OVERRIDES.get(row["msa_name"], row["msa_name"])
        city_part, _, state_part = title.partition(",")
        primary_city = city_part.split("-")[0].strip()
        primary_state = state_part.strip().split("-")[0][:2]
        candidates = saiz[
            saiz["saiz_name"].str.startswith(primary_city)
            & saiz["saiz_name"].str.contains(primary_state)
        ]
        if len(candidates) == 0:
            candidates = saiz[saiz["saiz_name"].str.startswith(primary_city)]
        best = (
            candidates.sort_values("population", ascending=False).iloc[0]
            if len(candidates)
            else None
        )
        rows.append(
            {
                "msa_id": row["msa_id"],
                "msa_name": row["msa_name"],
                "saiz_name": best["saiz_name"] if best is not None else None,
                "saiz_elasticity": float(best["elasticity"]) if best is not None else np.nan,
                "saiz_unaval": float(best["unaval"]) if best is not None else np.nan,
                "saiz_wrluri": float(best["WRLURI"]) if best is not None else np.nan,
                "match_rule": (
                    "override"
                    if row["msa_name"] in SAIZ_NAME_OVERRIDES
                    else "city_state"
                    if best is not None
                    else "unmatched"
                ),
            }
        )
    matched = pd.DataFrame(rows)
    matched["saiz_inv_elasticity_z"] = zscore(1.0 / matched["saiz_elasticity"])
    matched["saiz_unaval_z"] = zscore(matched["saiz_unaval"])
    matched.to_csv(OUT / "saiz_match_audit.csv", index=False)
    return matched


def leave_one_out_shift(top50_ids: pd.Series) -> pd.DataFrame:
    """Population-weighted leave-one-out national ZORI growth per MSA-year."""
    zori = pd.read_parquet(ZORI_MSA)
    zori = zori.sort_values(["msa_id", "year"])
    zori["log_zori"] = np.log(zori["zori"])
    zori["d_log_zori"] = zori.groupby("msa_id")["log_zori"].diff()
    zori["year_gap"] = zori.groupby("msa_id")["year"].diff()
    changes = zori[(zori["year_gap"] == 1) & zori["d_log_zori"].notna()].copy()
    changes["weight"] = changes["total_population"]
    totals = changes.groupby("year").apply(
        lambda g: pd.Series(
            {
                "sum_wd": float((g["weight"] * g["d_log_zori"]).sum()),
                "sum_w": float(g["weight"].sum()),
            }
        ),
        include_groups=False,
    )
    own = changes[changes["msa_id"].isin(top50_ids)][
        ["msa_id", "year", "d_log_zori", "weight"]
    ].merge(totals, on="year")
    own["national_d_log_zori_loo"] = (own["sum_wd"] - own["weight"] * own["d_log_zori"]) / (
        own["sum_w"] - own["weight"]
    )
    return own[["msa_id", "year", "national_d_log_zori_loo"]]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    base = pd.read_parquet(BASE_PANEL).sort_values(["msa_id", "year"])
    top50 = base[["msa_id", "msa_name", "sanctuary"]].drop_duplicates("msa_id")

    exposures = build_bps_exposures(top50)
    saiz = match_saiz(base[["msa_id", "msa_name"]].drop_duplicates("msa_id"))
    static = exposures.merge(
        saiz[
            [
                "msa_id",
                "saiz_elasticity",
                "saiz_inv_elasticity_z",
                "saiz_unaval_z",
                "saiz_wrluri",
                "match_rule",
            ]
        ],
        on="msa_id",
        how="left",
    )

    # Adjacent-year first differences (2021 PIT gap drops the 2021 and 2022 rows).
    fd = base.copy()
    for column in ["log_unshelt_rate", "log_zori", "log_pop"]:
        fd[f"d_{column}"] = fd.groupby("msa_id")[column].diff()
    fd["year_gap"] = fd.groupby("msa_id")["year"].diff()
    fd = fd[fd["year_gap"] == 1].copy()

    shift = leave_one_out_shift(top50["msa_id"])
    fd = fd.merge(shift, on=["msa_id", "year"], how="left").merge(static, on="msa_id", how="left")
    fd["bartik_bps"] = fd["supply_constraint_bps"] * fd["national_d_log_zori_loo"]
    fd["bartik_bps_long"] = fd["supply_constraint_bps_long"] * fd["national_d_log_zori_loo"]
    fd["bartik_saiz"] = fd["saiz_inv_elasticity_z"] * fd["national_d_log_zori_loo"]
    fd["bartik_unaval"] = fd["saiz_unaval_z"] * fd["national_d_log_zori_loo"]
    fd.to_parquet(OUT / "top50_msa_supply_iv_fd.parquet", index=False)

    # 2015 -> 2025 long differences with static instruments (n=50).
    wide = base.pivot_table(
        index="msa_id", columns="year", values=["log_unshelt_rate", "log_zori", "log_pop"]
    )
    longdiff = pd.DataFrame(
        {
            "d_log_unshelt_rate_15_25": wide[("log_unshelt_rate", 2025)]
            - wide[("log_unshelt_rate", 2015)],
            "d_log_zori_15_25": wide[("log_zori", 2025)] - wide[("log_zori", 2015)],
            "d_log_pop_15_25": wide[("log_pop", 2025)] - wide[("log_pop", 2015)],
        }
    ).reset_index()
    longdiff = longdiff.merge(static, on="msa_id", how="left").merge(
        top50[["msa_id", "msa_name", "sanctuary"]], on="msa_id", how="left"
    )
    longdiff.to_parquet(OUT / "top50_msa_supply_iv_longdiff.parquet", index=False)

    print("fd rows:", len(fd), "| longdiff rows:", len(longdiff))
    print("saiz matches:", saiz["match_rule"].value_counts().to_dict())
    print(
        "exposure spread (permits/1000, 2010-14):",
        static["bps_permits_per_1000_1014"].describe()[["min", "50%", "max"]].round(2).to_dict(),
    )


if __name__ == "__main__":
    main()
