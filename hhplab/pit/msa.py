"""MSA-level PIT aggregation from CoC-native PIT counts plus CoC->MSA crosswalks."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.analysis_geo import MSA_ID_COL
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

MSA_PIT_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "cbsa_code",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "covered_coc_count",
    "expected_coc_count",
    "allocation_share_sum",
    "expected_allocation_share_sum",
    "allocation_coverage_ratio",
    "missing_cocs",
    "boundary_vintage",
    "county_vintage",
    "definition_version",
    "allocation_method",
    "share_column",
)

REQUIRED_CROSSWALK_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "msa_id",
    "cbsa_code",
    "boundary_vintage",
    "county_vintage",
    "definition_version",
    "allocation_method",
    "share_column",
    "allocation_share",
)


def _normalize_pit_year_column(pit_df: pd.DataFrame) -> pd.DataFrame:
    df = pit_df.copy()
    if "pit_year" in df.columns and "year" not in df.columns:
        df = df.rename(columns={"pit_year": "year"})
    if "year" not in df.columns:
        raise ValueError(
            "PIT data must have 'year' or 'pit_year' column. "
            f"Available: {list(df.columns)}"
        )
    return df


def _validate_inputs(pit_df: pd.DataFrame, crosswalk_df: pd.DataFrame) -> None:
    if "coc_id" not in pit_df.columns:
        raise ValueError(
            "PIT data must have 'coc_id' column. "
            f"Available: {list(pit_df.columns)}"
        )
    if "pit_total" not in pit_df.columns:
        raise ValueError(
            "PIT data must have 'pit_total' column. "
            f"Available: {list(pit_df.columns)}"
        )

    missing = [col for col in REQUIRED_CROSSWALK_COLUMNS if col not in crosswalk_df.columns]
    if missing:
        raise ValueError(
            "CoC-to-MSA crosswalk must have columns "
            f"{missing}. Available: {list(crosswalk_df.columns)}"
        )


def _resolve_single_value(series: pd.Series, field_name: str) -> str:
    values = sorted({str(value) for value in series.dropna().unique()})
    if len(values) != 1:
        raise ValueError(
            f"Crosswalk must have exactly one {field_name} value, found {values}"
        )
    return values[0]


def _is_missing_scalar(value: object) -> bool:
    """Return True for scalar missing values without inspecting tuple/list contents."""
    return bool(pd.api.types.is_scalar(value) and pd.isna(value))


def aggregate_pit_to_msa(
    pit_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    *,
    definition_version: str | None = None,
    boundary_vintage: str | None = None,
    county_vintage: str | None = None,
) -> pd.DataFrame:
    """Aggregate CoC-level PIT counts to MSA outputs using crosswalk allocation shares."""
    df = _normalize_pit_year_column(pit_df)
    crosswalk = crosswalk_df.copy()
    _validate_inputs(df, crosswalk)

    resolved_definition = definition_version or _resolve_single_value(
        crosswalk["definition_version"], "definition_version"
    )
    resolved_boundary = boundary_vintage or _resolve_single_value(
        crosswalk["boundary_vintage"], "boundary_vintage"
    )
    resolved_county = county_vintage or _resolve_single_value(
        crosswalk["county_vintage"], "county_vintage"
    )
    allocation_method = _resolve_single_value(
        crosswalk["allocation_method"], "allocation_method"
    )
    share_column = _resolve_single_value(crosswalk["share_column"], "share_column")

    if share_column != "allocation_share":
        raise ValueError(
            f"Unsupported share_column '{share_column}'. Expected 'allocation_share'."
        )

    expected_meta = (
        crosswalk.groupby([MSA_ID_COL, "cbsa_code"], as_index=False)
        .agg(
            expected_coc_count=("coc_id", "nunique"),
            expected_allocation_share_sum=("allocation_share", "sum"),
            expected_cocs=("coc_id", lambda s: tuple(sorted({str(value) for value in s}))),
        )
        .sort_values([MSA_ID_COL, "cbsa_code"])
        .reset_index(drop=True)
    )

    merged = df.merge(
        crosswalk[
            [
                "coc_id",
                MSA_ID_COL,
                "cbsa_code",
                "allocation_share",
            ]
        ],
        on="coc_id",
        how="inner",
    )

    results: list[dict[str, object]] = []
    years = sorted(pd.to_numeric(df["year"], errors="raise").astype(int).unique().tolist())

    for year in years:
        year_alloc = merged[merged["year"] == year].copy()
        if not year_alloc.empty:
            year_alloc["pit_total_weighted"] = (
                pd.to_numeric(year_alloc["pit_total"], errors="raise")
                * year_alloc["allocation_share"]
            )
        year_summary = (
            year_alloc.groupby([MSA_ID_COL, "cbsa_code"], as_index=False)
            .agg(
                pit_total=("pit_total_weighted", "sum"),
                covered_coc_count=("coc_id", "nunique"),
                allocation_share_sum=("allocation_share", "sum"),
                found_cocs=("coc_id", lambda s: tuple(sorted({str(value) for value in s}))),
            )
            if not year_alloc.empty
            else pd.DataFrame(
                columns=[
                    MSA_ID_COL,
                    "cbsa_code",
                    "pit_total",
                    "covered_coc_count",
                    "allocation_share_sum",
                    "found_cocs",
                ]
            )
        )

        sheltered_summary = pd.DataFrame(columns=[MSA_ID_COL, "cbsa_code", "pit_sheltered"])
        if "pit_sheltered" in year_alloc.columns:
            sheltered_rows = year_alloc.dropna(subset=["pit_sheltered"]).copy()
            if not sheltered_rows.empty:
                sheltered_rows["pit_sheltered_weighted"] = (
                    sheltered_rows["pit_sheltered"] * sheltered_rows["allocation_share"]
                )
                sheltered_summary = sheltered_rows.groupby(
                    [MSA_ID_COL, "cbsa_code"], as_index=False
                ).agg(pit_sheltered=("pit_sheltered_weighted", "sum"))

        unsheltered_summary = pd.DataFrame(columns=[MSA_ID_COL, "cbsa_code", "pit_unsheltered"])
        if "pit_unsheltered" in year_alloc.columns:
            unsheltered_rows = year_alloc.dropna(subset=["pit_unsheltered"]).copy()
            if not unsheltered_rows.empty:
                unsheltered_rows["pit_unsheltered_weighted"] = (
                    unsheltered_rows["pit_unsheltered"] * unsheltered_rows["allocation_share"]
                )
                unsheltered_summary = unsheltered_rows.groupby(
                    [MSA_ID_COL, "cbsa_code"], as_index=False
                ).agg(pit_unsheltered=("pit_unsheltered_weighted", "sum"))

        year_result = expected_meta.merge(year_summary, on=[MSA_ID_COL, "cbsa_code"], how="left")
        year_result = year_result.merge(
            sheltered_summary, on=[MSA_ID_COL, "cbsa_code"], how="left"
        )
        year_result = year_result.merge(
            unsheltered_summary, on=[MSA_ID_COL, "cbsa_code"], how="left"
        )
        year_result["year"] = year

        for row in year_result.itertuples(index=False):
            found_cocs_raw = getattr(row, "found_cocs", ())
            if _is_missing_scalar(found_cocs_raw):
                found_cocs = set()
            else:
                found_cocs = set(found_cocs_raw or ())
            expected_cocs = set(row.expected_cocs)
            missing_cocs = ",".join(sorted(expected_cocs - found_cocs))
            covered_coc_count = int(row.covered_coc_count) if pd.notna(row.covered_coc_count) else 0
            allocation_share_sum = (
                float(row.allocation_share_sum) if pd.notna(row.allocation_share_sum) else 0.0
            )
            expected_share = float(row.expected_allocation_share_sum)
            coverage_ratio = (
                allocation_share_sum / expected_share if expected_share > 0 else 0.0
            )

            results.append(
                {
                    "msa_id": row.msa_id,
                    "cbsa_code": row.cbsa_code,
                    "year": year,
                    "pit_total": float(row.pit_total) if pd.notna(row.pit_total) else pd.NA,
                    "pit_sheltered": (
                        float(row.pit_sheltered) if pd.notna(row.pit_sheltered) else pd.NA
                    ),
                    "pit_unsheltered": (
                        float(row.pit_unsheltered)
                        if pd.notna(row.pit_unsheltered)
                        else pd.NA
                    ),
                    "covered_coc_count": covered_coc_count,
                    "expected_coc_count": int(row.expected_coc_count),
                    "allocation_share_sum": allocation_share_sum,
                    "expected_allocation_share_sum": expected_share,
                    "allocation_coverage_ratio": coverage_ratio,
                    "missing_cocs": missing_cocs,
                    "boundary_vintage": resolved_boundary,
                    "county_vintage": resolved_county,
                    "definition_version": resolved_definition,
                    "allocation_method": allocation_method,
                    "share_column": share_column,
                }
            )

    result_df = pd.DataFrame(results, columns=list(MSA_PIT_COLUMNS))
    if result_df.empty:
        return result_df

    for col in ("pit_total", "pit_sheltered", "pit_unsheltered"):
        result_df[col] = result_df[col].astype("Float64")
    result_df["covered_coc_count"] = result_df["covered_coc_count"].astype(int)
    result_df["expected_coc_count"] = result_df["expected_coc_count"].astype(int)
    result_df["allocation_share_sum"] = result_df["allocation_share_sum"].astype(float)
    result_df["expected_allocation_share_sum"] = result_df["expected_allocation_share_sum"].astype(
        float
    )
    result_df["allocation_coverage_ratio"] = result_df["allocation_coverage_ratio"].astype(float)
    return result_df.sort_values([MSA_ID_COL, "year"]).reset_index(drop=True)


def save_msa_pit(
    pit_df: pd.DataFrame,
    *,
    pit_year: int,
    definition_version: str,
    boundary_vintage: str,
    county_vintage: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Persist MSA-level PIT output with provenance."""
    from hhplab.naming import msa_pit_filename

    if output_dir is None:
        output_dir = curated_dir("pit")
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / msa_pit_filename(
        pit_year,
        definition_version,
        boundary_vintage,
        county_vintage,
    )
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        geo_type="msa",
        definition_version=definition_version,
        weighting="area",
        extra={
            "dataset_type": "msa_pit",
            "pit_year": pit_year,
            "source_geometry": "coc",
            "share_column": "allocation_share",
            "allocation_method": "area",
        },
    )
    write_parquet_with_provenance(pit_df, output_path, provenance)
    return output_path
