"""Build MSA supply-instrument panels.

The original analysis used the top-50 MSA panel. Bead coclab-bof1y later
extended the permits-constraint long-difference design to the available top-150
cohort; this script keeps both constructions tracked and reproducible.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from hhplab.results.workflows._paths import REPO_ROOT, write_result_parquet

ROOT = REPO_ROOT
OUT = ROOT / "outputs" / "supply_iv"

TOP50_BASE_PANEL = ROOT / "outputs" / "top50_msa_longitudinal_2015_2025.parquet"
TOP150_BASE_PANEL = (
    ROOT.parent
    / "HHP-Data"
    / "msa_rank51_150_longitudinal_2015_2025_source_top150"
    / "panel__msa__Y2015-2025@Mcensusmsa2023.parquet"
)
BPS_MSA = ROOT / "data/curated/covariates/covariate_panel__census_bps__Y1980-ongoing.parquet"
BPS_COUNTY = ROOT / "data/curated/covariates/covariate__census_bps__Y2000-2024.parquet"
MEMBERSHIP = ROOT / "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
ZORI_MSA = (
    ROOT / "data/curated/zori/"
    "zori__msa__Y2015-2025@Mcensusmsa2023xC2023__wpopulation__mpit_january__balanced.parquet"
)
SAIZ = ROOT / "data/raw/saiz_elasticity/saiz2010_supply_elasticity.dta"
SANCTUARY_PANEL = (
    ROOT / "data/curated/sanctuary/sanctuary_msa_panel__D20250805xMcensus_msa_2023.parquet"
)

# Pre-sample exposure windows. The FD estimation sample starts with the
# 2015->2016 transition.
BPS_SHORT_WINDOW = range(2010, 2015)
BPS_LONG_WINDOW = range(2000, 2015)

# Saiz (2010) metros are 1999 MSA/NECMA definitions; map CBSA titles whose
# principal city does not literally appear in the Saiz name.
SAIZ_NAME_OVERRIDES: dict[str, str] = {
    "Virginia Beach-Chesapeake-Norfolk, VA-NC": "Norfolk-Virginia Beach-Newport News, VA-NC",
    "Urban Honolulu, HI": "Honolulu, HI",
    "Louisville/Jefferson County, KY-IN": "Louisville, KY-IN",
}

# The historical complete-case top-150 artifact predates the CT BPS backfill and
# excluded these four metros. Keep that build reproducible alongside the fixed
# full top-150 panel, whose manifest includes all 150 MSAs.
TOP150_LEGACY_COMPLETE_CASE_EXCLUSIONS: tuple[str, ...] = ("14860", "25540", "35300", "47930")


@dataclass(frozen=True)
class InputPaths:
    bps_msa: Path = BPS_MSA
    bps_county: Path = BPS_COUNTY
    membership: Path = MEMBERSHIP
    zori_msa: Path = ZORI_MSA
    saiz: Path = SAIZ
    sanctuary_panel: Path = SANCTUARY_PANEL


DEFAULT_INPUT_PATHS = InputPaths()


@dataclass(frozen=True)
class CohortSpec:
    name: str
    requested_msa_count: int
    base_panel: Path
    output_prefix: str
    include_saiz: bool = False
    include_long_bps: bool = False
    complete_case_exclusions: tuple[str, ...] = ()


TOP50_SPEC = CohortSpec(
    name="top50",
    requested_msa_count=50,
    base_panel=TOP50_BASE_PANEL,
    output_prefix="top50_msa_supply_iv",
    include_saiz=True,
    include_long_bps=True,
)
TOP150_SPEC = CohortSpec(
    name="top150",
    requested_msa_count=150,
    base_panel=TOP150_BASE_PANEL,
    output_prefix="top150_msa_supply_iv",
    complete_case_exclusions=TOP150_LEGACY_COMPLETE_CASE_EXCLUSIONS,
)

BASE_OUTPUT_COLUMNS = [
    "msa_id",
    "msa_name",
    "year",
    "pit_unsheltered",
    "pit_sheltered",
    "pit_total",
    "zori",
    "coverage_ratio",
    "population",
    "sanctuary",
    "unshelt_per_1000",
    "log_unshelt_rate",
    "log_zori",
    "log_pop",
]


def zscore(values: pd.Series) -> pd.Series:
    return (values - values.mean()) / values.std(ddof=0)


def _require_files(paths: Iterable[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Supply-IV input file(s) are missing: "
            f"{missing}. Rebuild the corresponding curated/source panels first."
        )


def _normalize_base_panel(base: pd.DataFrame, *, paths: InputPaths) -> pd.DataFrame:
    frame = base.copy()
    frame["msa_id"] = frame["msa_id"].astype(str)
    rename = {
        "zori_coverage_ratio": "coverage_ratio",
        "log_unshelt_per_1000": "log_unshelt_rate",
    }
    frame = frame.rename(columns={old: new for old, new in rename.items() if old in frame.columns})
    if "log_pop" not in frame.columns:
        frame["log_pop"] = np.log(frame["population"])
    if "sanctuary" not in frame.columns:
        sanctuary = pd.read_parquet(paths.sanctuary_panel)
        sanctuary["msa_id"] = sanctuary["msa_id"].astype(str)
        sanctuary = sanctuary[["msa_id", "doj_sanctuary_msa"]].rename(
            columns={"doj_sanctuary_msa": "sanctuary"}
        )
        frame = frame.merge(sanctuary, on="msa_id", how="left")
        frame["sanctuary"] = frame["sanctuary"].fillna(0).astype(int)
    missing = sorted(set(BASE_OUTPUT_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"Base panel is missing required supply-IV columns: {missing}")
    return frame.loc[:, BASE_OUTPUT_COLUMNS].sort_values(["msa_id", "year"]).reset_index(drop=True)


def build_bps_exposures(
    cohort: pd.DataFrame,
    *,
    paths: InputPaths | None = None,
    include_long_bps: bool = False,
) -> pd.DataFrame:
    """Pre-sample permits intensity per MSA."""
    paths = DEFAULT_INPUT_PATHS if paths is None else paths
    bps = pd.read_parquet(paths.bps_msa)
    bps["msa_id"] = bps["msa_id"].astype(str)
    cohort_ids = set(cohort["msa_id"])
    bps = bps[bps["msa_id"].isin(cohort_ids)].copy()
    short = bps[bps["year"].isin(BPS_SHORT_WINDOW)].copy()
    short["permits_per_1000"] = (
        short["permitted_units"] / short["population_weight_denominator"] * 1000
    )
    frame = (
        short.groupby("msa_id")["permits_per_1000"]
        .mean()
        .rename("bps_permits_per_1000_1014")
        .to_frame()
    )

    if include_long_bps:
        county = pd.read_parquet(paths.bps_county)
        county = county[county["year"].isin(BPS_LONG_WINDOW)]
        members = pd.read_parquet(paths.membership)
        members["msa_id"] = members["msa_id"].astype(str)
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
            long_msa[long_msa["msa_id"].isin(cohort_ids)]
            .groupby("msa_id")["permitted_units"]
            .mean()
            .rename("mean_units_0014")
        )
        frame = pd.concat([frame, long_mean, pop_2010], axis=1)
        frame["bps_permits_per_1000_0014"] = frame["mean_units_0014"] / frame["pop_2010"] * 1000

    frame["supply_constraint_bps"] = zscore(-np.log(frame["bps_permits_per_1000_1014"]))
    if include_long_bps:
        frame["supply_constraint_bps_long"] = zscore(-np.log(frame["bps_permits_per_1000_0014"]))
    columns = ["bps_permits_per_1000_1014", "supply_constraint_bps"]
    if include_long_bps:
        columns.insert(1, "bps_permits_per_1000_0014")
        columns.append("supply_constraint_bps_long")
    return frame[columns].reset_index()


def match_saiz(
    cohort: pd.DataFrame,
    *,
    paths: InputPaths | None = None,
    audit_path: Path,
) -> pd.DataFrame:
    """Join Saiz (2010) supply measures onto CBSAs by principal city."""
    paths = DEFAULT_INPUT_PATHS if paths is None else paths
    saiz = pd.read_stata(paths.saiz)
    saiz["saiz_name"] = saiz["msaname"].astype(str).str.strip()
    rows = []
    for _, row in cohort.drop_duplicates("msa_id").iterrows():
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
    matched.to_csv(audit_path, index=False)
    return matched


def leave_one_out_shift(
    cohort_ids: pd.Series,
    *,
    paths: InputPaths | None = None,
) -> pd.DataFrame:
    """Population-weighted leave-one-out national ZORI growth per MSA-year."""
    paths = DEFAULT_INPUT_PATHS if paths is None else paths
    zori = pd.read_parquet(paths.zori_msa)
    zori["msa_id"] = zori["msa_id"].astype(str)
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
    own = changes[changes["msa_id"].isin(cohort_ids)][
        ["msa_id", "year", "d_log_zori", "weight"]
    ].merge(totals, on="year")
    own["national_d_log_zori_loo"] = (own["sum_wd"] - own["weight"] * own["d_log_zori"]) / (
        own["sum_w"] - own["weight"]
    )
    return own[["msa_id", "year", "national_d_log_zori_loo"]]


def build_supply_iv_panel(
    spec: CohortSpec,
    *,
    paths: InputPaths | None = None,
    out_dir: Path = OUT,
    suffix: str = "",
    exclude_msa_ids: Iterable[str] = (),
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    paths = DEFAULT_INPUT_PATHS if paths is None else paths
    _require_files([spec.base_panel, paths.bps_msa, paths.zori_msa, paths.sanctuary_panel])
    if spec.include_long_bps:
        _require_files([paths.bps_county, paths.membership])
    if spec.include_saiz:
        _require_files([paths.saiz])

    out_dir.mkdir(parents=True, exist_ok=True)
    base = _normalize_base_panel(pd.read_parquet(spec.base_panel), paths=paths)
    excluded = set(str(msa_id) for msa_id in exclude_msa_ids)
    excluded_rows = (
        base.loc[base["msa_id"].isin(excluded), ["msa_id", "msa_name"]]
        .drop_duplicates()
        .sort_values("msa_id")
        .to_dict(orient="records")
    )
    if excluded:
        base = base[~base["msa_id"].isin(excluded)].copy()
    cohort = base[["msa_id", "msa_name", "sanctuary"]].drop_duplicates("msa_id")

    static = build_bps_exposures(cohort, paths=paths, include_long_bps=spec.include_long_bps)
    if spec.include_saiz:
        saiz = match_saiz(
            cohort[["msa_id", "msa_name"]],
            paths=paths,
            audit_path=out_dir / "saiz_match_audit.csv",
        )
        static = static.merge(
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

    fd = base.copy()
    for column in ["log_unshelt_rate", "log_zori", "log_pop"]:
        fd[f"d_{column}"] = fd.groupby("msa_id")[column].diff()
    fd["year_gap"] = fd.groupby("msa_id")["year"].diff()
    fd = fd[fd["year_gap"] == 1].copy()

    shift = leave_one_out_shift(cohort["msa_id"], paths=paths)
    fd = fd.merge(shift, on=["msa_id", "year"], how="left").merge(static, on="msa_id", how="left")
    fd["bartik_bps"] = fd["supply_constraint_bps"] * fd["national_d_log_zori_loo"]
    if spec.include_long_bps:
        fd["bartik_bps_long"] = fd["supply_constraint_bps_long"] * fd["national_d_log_zori_loo"]
    if spec.include_saiz:
        fd["bartik_saiz"] = fd["saiz_inv_elasticity_z"] * fd["national_d_log_zori_loo"]
        fd["bartik_unaval"] = fd["saiz_unaval_z"] * fd["national_d_log_zori_loo"]

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
        cohort[["msa_id", "msa_name", "sanctuary"]], on="msa_id", how="left"
    )

    fd_path = out_dir / f"{spec.output_prefix}_fd{suffix}.parquet"
    longdiff_path = out_dir / f"{spec.output_prefix}_longdiff{suffix}.parquet"
    manifest_path = out_dir / f"{spec.output_prefix}{suffix}_manifest.json"
    write_result_parquet(fd, fd_path, index=False)
    write_result_parquet(longdiff, longdiff_path, index=False)
    manifest: dict[str, object] = {
        "requested_msa_count": spec.requested_msa_count,
        "included_msa_count": int(cohort["msa_id"].nunique()),
    }
    if excluded:
        manifest["excluded_missing_bps_2010_2014"] = excluded_rows
    manifest.update(
        {
            "fd_rows": int(len(fd)),
            "longdiff_rows": int(len(longdiff)),
            "fd_path": str(fd_path),
            "longdiff_path": str(longdiff_path),
        }
    )
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return fd, longdiff, manifest


def build_top150_outputs(
    *,
    paths: InputPaths | None = None,
    out_dir: Path = OUT,
) -> list[dict[str, object]]:
    paths = DEFAULT_INPUT_PATHS if paths is None else paths
    _, _, standard_manifest = build_supply_iv_panel(TOP150_SPEC, paths=paths, out_dir=out_dir)
    _, _, complete_case_manifest = build_supply_iv_panel(
        TOP150_SPEC,
        paths=paths,
        out_dir=out_dir,
        suffix="_completecase",
        exclude_msa_ids=TOP150_SPEC.complete_case_exclusions,
    )
    return [standard_manifest, complete_case_manifest]


def run(argv: list[str] | None = None) -> dict[str, object]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--msa-count",
        choices=["50", "150", "all"],
        default="all",
        help="MSA cohort to build. Default builds both tracked supply-IV cohorts.",
    )
    args = parser.parse_args([] if argv is None else argv)

    manifests: list[dict[str, object]] = []
    if args.msa_count in {"50", "all"}:
        _fd, _longdiff, manifest = build_supply_iv_panel(TOP50_SPEC)
        manifests.append(manifest)
    if args.msa_count in {"150", "all"}:
        manifests.extend(build_top150_outputs())
    return {
        "msa_count": args.msa_count,
        "cohort_count": len(manifests),
        "manifests": manifests,
    }


def main(argv: list[str] | None = None) -> None:
    result = run(argv)
    for manifest in result["manifests"]:
        print(
            f"msa_count={manifest['requested_msa_count']} fd rows: "
            f"{manifest['fd_rows']} | longdiff rows: {manifest['longdiff_rows']}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
