"""MSA-level PIT aggregation from CoC-native PIT counts plus CoC->MSA crosswalks."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.analysis_geo import MSA_ID_COL
from hhplab.msa import aggregate_coc_to_msa_fractional_rollup
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
    additive_columns = [
        column
        for column in ("pit_total", "pit_sheltered", "pit_unsheltered")
        if column in df.columns
    ]
    rollup = aggregate_coc_to_msa_fractional_rollup(
        df,
        crosswalk,
        additive_measure_columns=additive_columns,
        source_dataset_id="pit_coc",
        year_column="year",
    )
    cbsa_lookup = crosswalk[[MSA_ID_COL, "cbsa_code"]].drop_duplicates(MSA_ID_COL)
    rollup = rollup.merge(cbsa_lookup, on=MSA_ID_COL, how="left")
    for optional_column in ("pit_sheltered", "pit_unsheltered"):
        if optional_column not in rollup.columns:
            rollup[optional_column] = pd.NA
    result_df = pd.DataFrame(
        {
            "msa_id": rollup["msa_id"],
            "cbsa_code": rollup["cbsa_code"],
            "year": rollup["year"],
            "pit_total": rollup["pit_total"],
            "pit_sheltered": rollup["pit_sheltered"],
            "pit_unsheltered": rollup["pit_unsheltered"],
            "covered_coc_count": rollup["covered_coc_count"],
            "expected_coc_count": rollup["coc_count"],
            "allocation_share_sum": rollup["allocation_share_sum"],
            "expected_allocation_share_sum": rollup["expected_allocation_share_sum"],
            "allocation_coverage_ratio": rollup["coc_population_coverage_ratio"],
            "missing_cocs": rollup["missing_cocs"],
            "boundary_vintage": resolved_boundary,
            "county_vintage": resolved_county,
            "definition_version": resolved_definition,
            "allocation_method": allocation_method,
            "share_column": share_column,
        },
        columns=list(MSA_PIT_COLUMNS),
    )
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
