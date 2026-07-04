"""Generate top-50 non-PR Census MSA CoC PIT plus ACS5 contract-rent panel."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "outputs" / "top50_msa_nonpr_coc_pit_contract_rent_2010_2020"

YEARS = (2010, 2020)
PIT_VINTAGE = 2020
COC_BOUNDARY_VINTAGE = 2020
COUNTY_VINTAGE = 2023
MSA_DEFINITION_VERSION = "census_msa_2023"

PIT_PATH = ROOT / "data/curated/pit/pit_vintage__P2020.parquet"
MSA_COC_XWALK_PATH = (
    ROOT / "data/curated/xwalks/msa_coc_xwalk__B2020xMcensus_msa_2023xC2023.parquet"
)
MSA_DEFINITIONS_PATH = ROOT / "data/curated/msa/msa_definitions__census_msa_2023.parquet"
MSA_MEMBERSHIP_PATH = ROOT / "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
PL_BLOCKS_PATH = ROOT / "data/curated/census/pl_blocks__N2020xK2020.parquet"
ACS_CONTRACT_RENT_TEMPLATE = (
    ROOT / "data/curated/acs_contract_rent_cache/acs5_contract_rent_tracts__A{year}.parquet"
)

PIT_MEASURES = ("pit_total", "pit_sheltered", "pit_unsheltered")

CONTRACT_RENT_BIN_COLUMNS = (
    "contract_rent_distribution_total",
    "contract_rent_distribution_with_cash_rent",
    "contract_rent_distribution_cash_rent_lt_100",
    "contract_rent_distribution_cash_rent_100_to_149",
    "contract_rent_distribution_cash_rent_150_to_199",
    "contract_rent_distribution_cash_rent_200_to_249",
    "contract_rent_distribution_cash_rent_250_to_299",
    "contract_rent_distribution_cash_rent_300_to_349",
    "contract_rent_distribution_cash_rent_350_to_399",
    "contract_rent_distribution_cash_rent_400_to_449",
    "contract_rent_distribution_cash_rent_450_to_499",
    "contract_rent_distribution_cash_rent_500_to_549",
    "contract_rent_distribution_cash_rent_550_to_599",
    "contract_rent_distribution_cash_rent_600_to_649",
    "contract_rent_distribution_cash_rent_650_to_699",
    "contract_rent_distribution_cash_rent_700_to_749",
    "contract_rent_distribution_cash_rent_750_to_799",
    "contract_rent_distribution_cash_rent_800_to_899",
    "contract_rent_distribution_cash_rent_900_to_999",
    "contract_rent_distribution_cash_rent_1000_to_1249",
    "contract_rent_distribution_cash_rent_1250_to_1499",
    "contract_rent_distribution_cash_rent_1500_to_1999",
    "contract_rent_distribution_cash_rent_2000_plus",
    "contract_rent_distribution_cash_rent_2000_to_2499",
    "contract_rent_distribution_cash_rent_2500_to_2999",
    "contract_rent_distribution_cash_rent_3000_to_3499",
    "contract_rent_distribution_cash_rent_3500_plus",
    "contract_rent_distribution_no_cash_rent",
)

OUTPUT_COLUMNS = (
    "geo_type",
    "geo_id",
    "msa_id",
    "cbsa_code",
    "msa_name",
    "year",
    "msa_population_rank_2020_census_nonpr",
    "msa_2020_census_population",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "median_contract_rent_weighted",
    "contract_rent_cash_renter_households",
    *CONTRACT_RENT_BIN_COLUMNS,
    "acs5_vintage",
    "tract_vintage",
    "rent_source",
    "pit_vintage",
    "pit_allocation_basis",
    "coc_boundary_vintage",
    "county_vintage",
    "msa_definition_version",
)


def _read_parquet(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    return pq.read_table(path, columns=columns).to_pandas()


def _load_top50_nonpr_msa() -> pd.DataFrame:
    membership = pd.read_parquet(MSA_MEMBERSHIP_PATH)
    definitions = pd.read_parquet(MSA_DEFINITIONS_PATH)
    blocks = _read_parquet(PL_BLOCKS_PATH, columns=["county_fips", "total_population"])

    county_population = (
        blocks.assign(
            county_fips=lambda df: df["county_fips"].astype(str).str.zfill(5),
            total_population=lambda df: pd.to_numeric(df["total_population"], errors="coerce"),
        )
        .groupby("county_fips", as_index=False)["total_population"]
        .sum(min_count=1)
    )
    msa_population = (
        membership.loc[membership["state_name"].ne("Puerto Rico")]
        .assign(
            msa_id=lambda df: df["msa_id"].astype(str).str.zfill(5),
            county_fips=lambda df: df["county_fips"].astype(str).str.zfill(5),
        )
        .merge(county_population, on="county_fips", how="left")
        .groupby("msa_id", as_index=False)["total_population"]
        .sum(min_count=1)
        .rename(columns={"total_population": "msa_2020_census_population"})
    )
    ranked = (
        msa_population.merge(
            definitions[["msa_id", "cbsa_code", "msa_name", "area_type"]].assign(
                msa_id=lambda df: df["msa_id"].astype(str).str.zfill(5),
            ),
            on="msa_id",
            how="left",
        )
        .loc[lambda df: df["area_type"].eq("Metropolitan Statistical Area")]
        .loc[lambda df: ~df["msa_name"].str.endswith(", PR", na=False)]
        .sort_values(["msa_2020_census_population", "msa_id"], ascending=[False, True])
        .head(50)
        .reset_index(drop=True)
    )
    ranked["msa_population_rank_2020_census_nonpr"] = ranked.index + 1
    ranked["geo_type"] = "msa"
    ranked["geo_id"] = ranked["msa_id"]
    return ranked[
        [
            "geo_type",
            "geo_id",
            "msa_id",
            "cbsa_code",
            "msa_name",
            "msa_population_rank_2020_census_nonpr",
            "msa_2020_census_population",
        ]
    ]


def _rollup_pit(selected: pd.DataFrame) -> pd.DataFrame:
    pit = pd.read_parquet(PIT_PATH)
    xwalk = pd.read_parquet(MSA_COC_XWALK_PATH)
    selected_ids = set(selected["msa_id"].astype(str))
    source = pit.loc[pit["pit_year"].isin(YEARS), ["pit_year", "coc_id", *PIT_MEASURES]].copy()
    source = source.rename(columns={"pit_year": "year"})
    merged = source.merge(
        xwalk.loc[
            xwalk["msa_id"].astype(str).isin(selected_ids),
            ["coc_id", "msa_id", "allocation_share"],
        ].assign(msa_id=lambda df: df["msa_id"].astype(str).str.zfill(5)),
        on="coc_id",
        how="inner",
    )
    for measure in PIT_MEASURES:
        merged[measure] = pd.to_numeric(merged[measure], errors="coerce") * pd.to_numeric(
            merged["allocation_share"],
            errors="coerce",
        )
    return merged.groupby(["msa_id", "year"], as_index=False)[list(PIT_MEASURES)].sum(min_count=1)


def _aggregate_contract_rent(year: int, selected: pd.DataFrame) -> pd.DataFrame:
    path = ACS_CONTRACT_RENT_TEMPLATE.with_name(
        ACS_CONTRACT_RENT_TEMPLATE.name.format(year=year),
    )
    acs = pd.read_parquet(path)
    membership = pd.read_parquet(MSA_MEMBERSHIP_PATH)
    selected_ids = set(selected["msa_id"].astype(str))
    msa_counties = membership.loc[
        membership["msa_id"].astype(str).isin(selected_ids),
        ["msa_id", "county_fips"],
    ].assign(
        msa_id=lambda df: df["msa_id"].astype(str).str.zfill(5),
        county_fips=lambda df: df["county_fips"].astype(str).str.zfill(5),
    )

    df = acs.copy()
    df["county_fips"] = df["tract_geoid"].astype(str).str[:5].str.zfill(5)
    df = df.merge(msa_counties, on="county_fips", how="inner")
    available_bins = [column for column in CONTRACT_RENT_BIN_COLUMNS if column in df.columns]
    for column in [*available_bins, "median_contract_rent"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    grouped_bins = df.groupby("msa_id", as_index=False)[available_bins].sum(min_count=1)
    if "contract_rent_distribution_with_cash_rent" not in grouped_bins:
        raise ValueError(f"{path} is missing contract_rent_distribution_with_cash_rent")

    rows: list[dict[str, object]] = []
    for msa_id, group in df.groupby("msa_id", sort=True):
        weights = pd.to_numeric(
            group["contract_rent_distribution_with_cash_rent"],
            errors="coerce",
        )
        medians = pd.to_numeric(group["median_contract_rent"], errors="coerce")
        valid = weights.gt(0) & medians.notna()
        weighted_median = (
            float((medians.loc[valid] * weights.loc[valid]).sum() / weights.loc[valid].sum())
            if valid.any()
            else None
        )
        rows.append(
            {
                "msa_id": msa_id,
                "median_contract_rent_weighted": weighted_median,
                "contract_rent_cash_renter_households": float(weights.sum()),
                "acs5_vintage": year,
                "tract_vintage": 2010 if year == 2010 else 2020,
                "rent_source": str(path.relative_to(ROOT)),
            }
        )
    medians_df = pd.DataFrame(rows)
    result = grouped_bins.merge(medians_df, on="msa_id", how="outer")
    for column in CONTRACT_RENT_BIN_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    result["year"] = year
    return result


def _build_panel() -> pd.DataFrame:
    selected = _load_top50_nonpr_msa()
    pit = _rollup_pit(selected)
    rent_columns = [
        "msa_id",
        *CONTRACT_RENT_BIN_COLUMNS,
        "median_contract_rent_weighted",
        "contract_rent_cash_renter_households",
        "acs5_vintage",
        "tract_vintage",
        "rent_source",
        "year",
    ]
    rent_records: list[dict[str, object]] = []
    for year in YEARS:
        rent_records.extend(
            _aggregate_contract_rent(year, selected)
            .reindex(columns=rent_columns)
            .to_dict(orient="records"),
        )
    rent = pd.DataFrame.from_records(rent_records, columns=rent_columns)
    panel = (
        selected.merge(pd.DataFrame({"year": YEARS}), how="cross")
        .merge(pit, on=["msa_id", "year"], how="left")
        .merge(rent, on=["msa_id", "year"], how="left")
        .sort_values(["msa_population_rank_2020_census_nonpr", "year"])
        .reset_index(drop=True)
    )
    panel["pit_vintage"] = PIT_VINTAGE
    panel["pit_allocation_basis"] = "coc_area"
    panel["coc_boundary_vintage"] = COC_BOUNDARY_VINTAGE
    panel["county_vintage"] = COUNTY_VINTAGE
    panel["msa_definition_version"] = MSA_DEFINITION_VERSION
    return panel.loc[:, OUTPUT_COLUMNS]


def _write_outputs(panel: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = (
        "panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__"
        "Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020"
    )
    parquet_path = OUTPUT_DIR / f"{stem}.parquet"
    csv_path = OUTPUT_DIR / f"{stem}.csv"
    preview_path = OUTPUT_DIR / f"{stem}.preview.csv"
    summary_path = OUTPUT_DIR / f"{stem}.summary.json"

    provenance = ProvenanceBlock(
        boundary_vintage=str(COC_BOUNDARY_VINTAGE),
        county_vintage=str(COUNTY_VINTAGE),
        acs_vintage="2010,2020",
        weighting="coc_area_for_pit; county_membership_for_acs",
        geo_type="msa",
        definition_version=MSA_DEFINITION_VERSION,
        extra={
            "dataset_type": "requested_top50_nonpr_msa_coc_pit_contract_rent_panel",
            "row_grain": "msa_id x year",
            "years": list(YEARS),
            "ranking": "top 50 Census MSAs by 2020 PL block population, excluding Puerto Rico MSAs",
            "input_artifacts": {
                "pit": str(PIT_PATH.relative_to(ROOT)),
                "msa_coc_xwalk": str(MSA_COC_XWALK_PATH.relative_to(ROOT)),
                "msa_definitions": str(MSA_DEFINITIONS_PATH.relative_to(ROOT)),
                "msa_membership": str(MSA_MEMBERSHIP_PATH.relative_to(ROOT)),
                "pl_blocks": str(PL_BLOCKS_PATH.relative_to(ROOT)),
                "acs_contract_rent_2010": str(
                    ACS_CONTRACT_RENT_TEMPLATE.with_name(
                        ACS_CONTRACT_RENT_TEMPLATE.name.format(year=2010),
                    ).relative_to(ROOT),
                ),
                "acs_contract_rent_2020": str(
                    ACS_CONTRACT_RENT_TEMPLATE.with_name(
                        ACS_CONTRACT_RENT_TEMPLATE.name.format(year=2020),
                    ).relative_to(ROOT),
                ),
            },
        },
    )
    write_parquet_with_provenance(panel, parquet_path, provenance)
    panel.to_csv(csv_path, index=False)
    panel.head(20).to_csv(preview_path, index=False)

    summary = {
        "row_count": int(len(panel)),
        "msa_count": int(panel["msa_id"].nunique()),
        "years": list(YEARS),
        "contains_pr_msa": bool(panel["msa_name"].str.endswith(", PR", na=False).any()),
        "outputs": {
            "parquet": str(parquet_path.relative_to(ROOT)),
            "csv": str(csv_path.relative_to(ROOT)),
            "preview_csv": str(preview_path.relative_to(ROOT)),
            "summary_json": str(summary_path.relative_to(ROOT)),
        },
        "top_10": panel.loc[
            panel["year"].eq(2020),
            [
                "msa_population_rank_2020_census_nonpr",
                "msa_id",
                "msa_name",
                "msa_2020_census_population",
                "pit_total",
                "median_contract_rent_weighted",
            ],
        ]
        .head(10)
        .to_dict(orient="records"),
        "columns": list(panel.columns),
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    readme = f"""# Top 50 Non-PR Census MSAs: CoC PIT and ACS5 Contract Rent, 2010/2020

This requested output rolls HUD CoC PIT counts to the top 50 non-Puerto Rico
Census MSAs by 2020 PL 94-171 block population, using Census MSA 2023 county
membership for the population ranking.

Files:

- `{parquet_path.relative_to(ROOT)}`
- `{csv_path.relative_to(ROOT)}`
- `{preview_path.relative_to(ROOT)}`
- `{summary_path.relative_to(ROOT)}`

Panel grain: one row per `msa_id` and `year` for 2010 and 2020.

Included measures:

- CoC PIT rollup: `pit_total`, `pit_sheltered`, `pit_unsheltered`
- ACS5 contract-rent bins from terminal years 2010 and 2020
- `median_contract_rent_weighted`, a cash-renter-household weighted mean of tract
  `median_contract_rent`

Allocation notes:

- PIT uses the existing 2020 CoC boundary to Census MSA 2023 area allocation:
  `{MSA_COC_XWALK_PATH.relative_to(ROOT)}`.
- ACS uses Census MSA 2023 county membership; tracts are assigned to MSAs by
  their county FIPS because Census MSAs are county-defined.
- Puerto Rico MSAs are excluded before selecting the top 50.
"""
    (OUTPUT_DIR / "README.md").write_text(readme, encoding="utf-8")


def main() -> None:
    panel = _build_panel()
    _write_outputs(panel)
    print(f"Wrote {len(panel)} rows to {OUTPUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
