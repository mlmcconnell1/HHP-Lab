"""Small-area estimation allocation helpers for ACS components."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.acs.variables import ACS5_SAE_SUPPORT_COLUMNS
from hhplab.acs.variables_acs1 import ACS1_SAE_SOURCE_COLUMNS
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.measures import (
    ACS1_IMPUTATION_MEASURE_SPECS,
    ACS1ImputationMeasureSpec,
)

SAE_ALLOCATION_METHOD = "tract_share_within_county"
ACS1_IMPUTATION_METHOD = "acs1_controlled_acs5_tract_share"

RENTER_BURDEN_30_PLUS_COLUMNS: tuple[str, ...] = (
    "sae_gross_rent_pct_income_30_to_34_9",
    "sae_gross_rent_pct_income_35_to_39_9",
    "sae_gross_rent_pct_income_40_to_49_9",
    "sae_gross_rent_pct_income_50_plus",
)

OWNER_WITH_MORTGAGE_BURDEN_30_PLUS_COLUMNS: tuple[str, ...] = (
    "sae_owner_costs_pct_income_with_mortgage_30_to_34_9",
    "sae_owner_costs_pct_income_with_mortgage_35_to_39_9",
    "sae_owner_costs_pct_income_with_mortgage_40_to_49_9",
    "sae_owner_costs_pct_income_with_mortgage_50_plus",
)

OWNER_WITHOUT_MORTGAGE_BURDEN_30_PLUS_COLUMNS: tuple[str, ...] = (
    "sae_owner_costs_pct_income_without_mortgage_30_to_34_9",
    "sae_owner_costs_pct_income_without_mortgage_35_to_39_9",
    "sae_owner_costs_pct_income_without_mortgage_40_to_49_9",
    "sae_owner_costs_pct_income_without_mortgage_50_plus",
)

HOUSEHOLD_INCOME_BINS: tuple[tuple[str, float, float | None], ...] = (
    ("sae_household_income_lt_10000", 0.0, 10000.0),
    ("sae_household_income_10000_to_14999", 10000.0, 15000.0),
    ("sae_household_income_15000_to_19999", 15000.0, 20000.0),
    ("sae_household_income_20000_to_24999", 20000.0, 25000.0),
    ("sae_household_income_25000_to_29999", 25000.0, 30000.0),
    ("sae_household_income_30000_to_34999", 30000.0, 35000.0),
    ("sae_household_income_35000_to_39999", 35000.0, 40000.0),
    ("sae_household_income_40000_to_44999", 40000.0, 45000.0),
    ("sae_household_income_45000_to_49999", 45000.0, 50000.0),
    ("sae_household_income_50000_to_59999", 50000.0, 60000.0),
    ("sae_household_income_60000_to_74999", 60000.0, 75000.0),
    ("sae_household_income_75000_to_99999", 75000.0, 100000.0),
    ("sae_household_income_100000_to_124999", 100000.0, 125000.0),
    ("sae_household_income_125000_to_149999", 125000.0, 150000.0),
    ("sae_household_income_150000_to_199999", 150000.0, 200000.0),
    ("sae_household_income_200000_plus", 200000.0, None),
)

GROSS_RENT_BINS: tuple[tuple[str, float, float | None], ...] = (
    ("sae_gross_rent_distribution_cash_rent_lt_100", 0.0, 100.0),
    ("sae_gross_rent_distribution_cash_rent_100_to_149", 100.0, 150.0),
    ("sae_gross_rent_distribution_cash_rent_150_to_199", 150.0, 200.0),
    ("sae_gross_rent_distribution_cash_rent_200_to_249", 200.0, 250.0),
    ("sae_gross_rent_distribution_cash_rent_250_to_299", 250.0, 300.0),
    ("sae_gross_rent_distribution_cash_rent_300_to_349", 300.0, 350.0),
    ("sae_gross_rent_distribution_cash_rent_350_to_399", 350.0, 400.0),
    ("sae_gross_rent_distribution_cash_rent_400_to_449", 400.0, 450.0),
    ("sae_gross_rent_distribution_cash_rent_450_to_499", 450.0, 500.0),
    ("sae_gross_rent_distribution_cash_rent_500_to_549", 500.0, 550.0),
    ("sae_gross_rent_distribution_cash_rent_550_to_599", 550.0, 600.0),
    ("sae_gross_rent_distribution_cash_rent_600_to_649", 600.0, 650.0),
    ("sae_gross_rent_distribution_cash_rent_650_to_699", 650.0, 700.0),
    ("sae_gross_rent_distribution_cash_rent_700_to_749", 700.0, 750.0),
    ("sae_gross_rent_distribution_cash_rent_750_to_799", 750.0, 800.0),
    ("sae_gross_rent_distribution_cash_rent_800_to_899", 800.0, 900.0),
    ("sae_gross_rent_distribution_cash_rent_900_to_999", 900.0, 1000.0),
    ("sae_gross_rent_distribution_cash_rent_1000_to_1249", 1000.0, 1250.0),
    ("sae_gross_rent_distribution_cash_rent_1250_to_1499", 1250.0, 1500.0),
    ("sae_gross_rent_distribution_cash_rent_1500_to_1999", 1500.0, 2000.0),
    ("sae_gross_rent_distribution_cash_rent_2000_to_2499", 2000.0, 2500.0),
    ("sae_gross_rent_distribution_cash_rent_2500_to_2999", 2500.0, 3000.0),
    ("sae_gross_rent_distribution_cash_rent_3000_to_3499", 3000.0, 3500.0),
    ("sae_gross_rent_distribution_cash_rent_3500_plus", 3500.0, None),
)


def _json_list(values: Iterable[str]) -> str:
    return json.dumps(sorted(set(values)))


def _json_dict(values: dict[str, float | None]) -> str:
    return json.dumps(values, sort_keys=True)


def _single_or_json(values: pd.Series) -> str | None:
    unique_values = sorted(values.dropna().astype(str).unique())
    if not unique_values:
        return None
    if len(unique_values) == 1:
        return unique_values[0]
    return json.dumps(unique_values)


def _component_columns(component_columns: Iterable[str] | None) -> list[str]:
    if component_columns is not None:
        return list(component_columns)
    support_columns = set(ACS5_SAE_SUPPORT_COLUMNS)
    return [column for column in ACS1_SAE_SOURCE_COLUMNS if column in support_columns]


def _require_columns(df: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}.")


def _sum_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    numeric = [pd.to_numeric(df[column], errors="coerce") for column in columns]
    return pd.concat(numeric, axis=1).sum(axis=1, min_count=1).astype("Float64")


def _safe_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    rate = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    valid = numerator.notna() & denominator.notna() & (denominator > 0)
    rate.loc[valid] = numerator.loc[valid] / denominator.loc[valid]
    return rate


def _validation_diff(
    allocated_total: float | None,
    source_total: float | None,
) -> tuple[float | None, float | None]:
    if allocated_total is None or source_total is None:
        return None, None
    abs_diff = float(allocated_total - source_total)
    if source_total == 0:
        rel_diff = 0.0 if abs_diff == 0 else float("inf")
    else:
        rel_diff = abs_diff / abs(float(source_total))
    return abs_diff, rel_diff


def _assert_within_tolerance(
    *,
    spec: ACS1ImputationMeasureSpec,
    target_id: str,
    column: str,
    abs_diff: float | None,
    rel_diff: float | None,
) -> None:
    if abs_diff is None or rel_diff is None:
        return
    if (
        abs(abs_diff) > spec.validation_abs_tolerance
        and abs(rel_diff) > spec.validation_rel_tolerance
    ):
        raise ValueError(
            "ACS1 imputation failed conservation validation for "
            f"{spec.name} target {target_id} column {column}: "
            f"abs_diff={abs_diff}, rel_diff={rel_diff}. "
            "Check ACS1 target totals, ACS5 tract support, and crosswalk weights."
        )


def acs1_imputation_source_columns(
    specs: Iterable[ACS1ImputationMeasureSpec] = ACS1_IMPUTATION_MEASURE_SPECS,
) -> list[str]:
    """Return ACS1 target columns required by the requested imputation specs."""
    return list(dict.fromkeys(column for spec in specs for column in spec.acs1_source_columns))


def acs5_imputation_support_columns(
    specs: Iterable[ACS1ImputationMeasureSpec] = ACS1_IMPUTATION_MEASURE_SPECS,
) -> list[str]:
    """Return ACS5 tract support columns required by requested imputation specs."""
    return list(dict.fromkeys(column for spec in specs for column in spec.acs5_support_columns))


def prepare_acs1_imputation_targets(
    df: pd.DataFrame,
    *,
    specs: Iterable[ACS1ImputationMeasureSpec] = ACS1_IMPUTATION_MEASURE_SPECS,
    target_id_col: str = "county_fips",
    acs1_vintage: int | str | None = None,
    target_geo_type: str = "county",
) -> pd.DataFrame:
    """Select and validate ACS1 target counts for generic imputation.

    The adapter exposes count columns only. Rate measures declare numerator
    and denominator count columns through their measure specs; percentages are
    intentionally not accepted as substitutes.
    """
    spec_list = tuple(specs)
    for spec in spec_list:
        spec.validate()
    if target_geo_type == "tract":
        raise ValueError(
            "ACS1 tract target data is not available from Census. Use a supported "
            "ACS1 target geography such as county, place, or metro, or provide a "
            "pre-materialized modeled tract artifact."
        )

    columns = acs1_imputation_source_columns(spec_list)
    required = [target_id_col, *columns]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(
            "ACS1 imputation target adapter is missing required count columns "
            f"{missing}. For rate measures, provide numerator and denominator "
            "counts separately; do not provide percentages only."
        )

    if acs1_vintage is None:
        if "acs1_vintage" not in df.columns:
            raise ValueError(
                "ACS1 imputation target adapter requires acs1_vintage either as "
                "an argument or as an input column."
            )
        vintages = sorted(df["acs1_vintage"].dropna().astype(str).unique())
        if len(vintages) != 1:
            raise ValueError(
                "ACS1 imputation target adapter requires one acs1_vintage. "
                f"Found vintages: {vintages}."
            )
        resolved_vintage = vintages[0]
    else:
        resolved_vintage = str(acs1_vintage)

    result = pd.DataFrame(
        {
            target_id_col: df[target_id_col].astype("string"),
            "target_geo_type": target_geo_type,
            "target_geo_id": df[target_id_col].astype("string"),
            "acs1_vintage": resolved_vintage,
        }
    )
    for column in columns:
        result[column] = pd.to_numeric(df[column], errors="coerce")
        result.loc[result[column] < 0, column] = pd.NA
        result[column] = result[column].astype("Float64")

    if result[target_id_col].duplicated(keep=False).any():
        duplicated = sorted(
            result.loc[result[target_id_col].duplicated(keep=False), target_id_col]
            .dropna()
            .astype(str)
            .unique()
        )
        raise ValueError(
            f"ACS1 imputation target adapter requires one row per {target_id_col}. "
            f"Duplicates: {duplicated}."
        )

    result = result.sort_values(target_id_col).reset_index(drop=True)
    result.attrs["target_id_col"] = target_id_col
    result.attrs["target_geo_type"] = target_geo_type
    result.attrs["acs1_vintage"] = resolved_vintage
    result.attrs["source_columns"] = columns
    return result


def prepare_acs5_imputation_support(
    df: pd.DataFrame,
    *,
    specs: Iterable[ACS1ImputationMeasureSpec] = ACS1_IMPUTATION_MEASURE_SPECS,
    target_id_col: str | None = "county_fips",
    acs_vintage: int | str | None = None,
    tract_vintage: int | str | None = None,
) -> pd.DataFrame:
    """Select and validate ACS5 tract support counts for generic imputation."""
    spec_list = tuple(specs)
    for spec in spec_list:
        spec.validate()
    columns = acs5_imputation_support_columns(spec_list)
    required = ["tract_geoid", *columns]
    if target_id_col is not None:
        required.append(target_id_col)
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(
            "ACS5 imputation support adapter is missing required tract count "
            f"columns {missing}. Build ACS5 tract support first, for example "
            "`hhplab ingest acs-population --year YEAR`, then normalize SAE support."
        )

    support = pd.DataFrame({"tract_geoid": df["tract_geoid"].astype("string").str.zfill(11)})
    if target_id_col is not None:
        support[target_id_col] = df[target_id_col].astype("string")
    support["county_fips"] = (
        df["county_fips"].astype("string").str.zfill(5)
        if "county_fips" in df.columns
        else support["tract_geoid"].str.slice(0, 5)
    )

    if acs_vintage is None:
        if "acs_vintage" not in df.columns:
            raise ValueError(
                "ACS5 imputation support adapter requires acs_vintage either as "
                "an argument or as an input column."
            )
        resolved_acs_vintage = df["acs_vintage"].dropna().astype(str).unique()
        if len(resolved_acs_vintage) != 1:
            raise ValueError(
                "ACS5 imputation support adapter requires one acs_vintage. "
                f"Found vintages: {sorted(resolved_acs_vintage)}."
            )
        acs_vintage_value = resolved_acs_vintage[0]
    else:
        acs_vintage_value = str(acs_vintage)

    if tract_vintage is None:
        if "tract_vintage" not in df.columns:
            raise ValueError(
                "ACS5 imputation support adapter requires tract_vintage either as "
                "an argument or as an input column."
            )
        resolved_tract_vintage = df["tract_vintage"].dropna().astype(str).unique()
        if len(resolved_tract_vintage) != 1:
            raise ValueError(
                "ACS5 imputation support adapter requires one tract_vintage. "
                f"Found vintages: {sorted(resolved_tract_vintage)}."
            )
        tract_vintage_value = resolved_tract_vintage[0]
    else:
        tract_vintage_value = str(tract_vintage)

    support["acs_vintage"] = acs_vintage_value
    support["tract_vintage"] = tract_vintage_value
    for column in columns:
        support[column] = pd.to_numeric(df[column], errors="coerce")
        support.loc[support[column] < 0, column] = pd.NA
        support[column] = support[column].astype("Float64")

    support = support.sort_values("tract_geoid").reset_index(drop=True)
    support.attrs["target_id_col"] = target_id_col
    support.attrs["acs_vintage"] = acs_vintage_value
    support.attrs["tract_vintage"] = tract_vintage_value
    support.attrs["support_columns"] = columns
    return support


def build_sae_provenance(
    *,
    acs1_vintage: int | str,
    acs5_vintage: int | str,
    tract_vintage: int | str,
    target_geo_type: str,
    target_vintage: int | str | None = None,
    allocation_method: str = SAE_ALLOCATION_METHOD,
    denominator_source: str | None = None,
    crosswalk_id: str | None = None,
    source_dataset_path: str | Path | None = None,
    support_dataset_path: str | Path | None = None,
    crosswalk_path: str | Path | None = None,
    requested_measures: Iterable[str] | None = None,
    derived_output_columns: Iterable[str] | None = None,
    diagnostics_summary: dict[str, Any] | None = None,
    source_tables: Iterable[str] | None = None,
    support_tables: Iterable[str] | None = None,
    source_row_count: int | None = None,
    support_row_count: int | None = None,
    output_row_count: int | None = None,
) -> ProvenanceBlock:
    """Build embedded provenance for an SAE artifact."""
    extra: dict[str, Any] = {
        "dataset": "acs_sae",
        "acs1_vintage": str(acs1_vintage),
        "acs5_terminal_vintage": str(acs5_vintage),
        "allocation_method": allocation_method,
    }
    optional_extra = {
        "denominator_source": denominator_source,
        "crosswalk_id": crosswalk_id,
        "source_dataset_path": None if source_dataset_path is None else str(source_dataset_path),
        "support_dataset_path": None if support_dataset_path is None else str(support_dataset_path),
        "crosswalk_path": None if crosswalk_path is None else str(crosswalk_path),
        "source_row_count": source_row_count,
        "support_row_count": support_row_count,
        "output_row_count": output_row_count,
    }
    extra.update({key: value for key, value in optional_extra.items() if value is not None})
    if requested_measures is not None:
        extra["requested_measures"] = list(requested_measures)
    if derived_output_columns is not None:
        extra["derived_output_columns"] = list(derived_output_columns)
    if diagnostics_summary is not None:
        extra["diagnostics_summary"] = diagnostics_summary
    if source_tables is not None:
        extra["source_tables"] = list(source_tables)
    if support_tables is not None:
        extra["support_tables"] = list(support_tables)

    provenance = ProvenanceBlock(
        boundary_vintage=None if target_vintage is None else str(target_vintage),
        tract_vintage=str(tract_vintage),
        acs_vintage=str(acs5_vintage),
        weighting=allocation_method,
        geo_type=target_geo_type,
        extra=extra,
    )
    provenance.notation = provenance.generate_notation()
    return provenance


def write_sae_parquet_with_provenance(
    df: pd.DataFrame,
    path: str | Path,
    *,
    acs1_vintage: int | str,
    acs5_vintage: int | str,
    tract_vintage: int | str,
    target_geo_type: str,
    target_vintage: int | str | None = None,
    allocation_method: str = SAE_ALLOCATION_METHOD,
    denominator_source: str | None = None,
    crosswalk_id: str | None = None,
    source_dataset_path: str | Path | None = None,
    support_dataset_path: str | Path | None = None,
    crosswalk_path: str | Path | None = None,
    requested_measures: Iterable[str] | None = None,
    derived_output_columns: Iterable[str] | None = None,
    diagnostics_summary: dict[str, Any] | None = None,
    source_tables: Iterable[str] | None = None,
    support_tables: Iterable[str] | None = None,
    source_row_count: int | None = None,
    support_row_count: int | None = None,
) -> Path:
    """Write an SAE artifact with embedded lineage metadata."""
    provenance = build_sae_provenance(
        acs1_vintage=acs1_vintage,
        acs5_vintage=acs5_vintage,
        tract_vintage=tract_vintage,
        target_geo_type=target_geo_type,
        target_vintage=target_vintage,
        allocation_method=allocation_method,
        denominator_source=denominator_source,
        crosswalk_id=crosswalk_id,
        source_dataset_path=source_dataset_path,
        support_dataset_path=support_dataset_path,
        crosswalk_path=crosswalk_path,
        requested_measures=requested_measures,
        derived_output_columns=derived_output_columns,
        diagnostics_summary=diagnostics_summary,
        source_tables=source_tables,
        support_tables=support_tables,
        source_row_count=source_row_count,
        support_row_count=support_row_count,
        output_row_count=len(df),
    )
    return write_parquet_with_provenance(df, path, provenance)


def impute_acs1_targets_to_tracts(
    acs1_targets: pd.DataFrame,
    acs5_tract_support: pd.DataFrame,
    *,
    specs: Iterable[ACS1ImputationMeasureSpec] = ACS1_IMPUTATION_MEASURE_SPECS,
    target_id_col: str = "county_fips",
    target_geo_type: str = "county",
    year: int | str | None = None,
    tract_crosswalk: pd.DataFrame | None = None,
    share_column: str = "area_share",
    crosswalk_id: str | None = None,
) -> pd.DataFrame:
    """Impute ACS1 target totals to tracts using ACS5 tract support shares.

    Rate specs allocate numerator and denominator counts independently, then
    recompute the rate. If ``tract_crosswalk`` is omitted, ``acs5_tract_support``
    must already carry ``target_id_col``.
    """
    spec_list = tuple(specs)
    if not spec_list:
        raise ValueError("At least one ACS1 imputation measure spec is required.")
    for spec in spec_list:
        spec.validate()

    source_columns = list(
        dict.fromkeys(column for spec in spec_list for column in spec.acs1_source_columns)
    )
    support_columns = list(
        dict.fromkeys(column for spec in spec_list for column in spec.acs5_support_columns)
    )
    _require_columns(
        acs1_targets,
        [target_id_col, "acs1_vintage", *source_columns],
        "acs1_targets",
    )
    _require_columns(
        acs5_tract_support,
        ["tract_geoid", "acs_vintage", "tract_vintage", *support_columns],
        "acs5_tract_support",
    )

    targets = acs1_targets.copy()
    support = acs5_tract_support.copy()
    targets[target_id_col] = targets[target_id_col].astype("string")
    support["tract_geoid"] = support["tract_geoid"].astype("string").str.zfill(11)

    if targets[target_id_col].duplicated(keep=False).any():
        duplicated = sorted(
            targets.loc[targets[target_id_col].duplicated(keep=False), target_id_col]
            .dropna()
            .astype(str)
            .unique()
        )
        raise ValueError(
            f"acs1_targets must have one row per {target_id_col}. Duplicates: {duplicated}."
        )

    if tract_crosswalk is None:
        _require_columns(support, [target_id_col], "acs5_tract_support")
        support[target_id_col] = support[target_id_col].astype("string")
        support["_imputation_share"] = 1.0
        merged = support
    else:
        _require_columns(
            tract_crosswalk,
            [target_id_col, "tract_geoid", share_column],
            "tract_crosswalk",
        )
        xwalk = tract_crosswalk.copy()
        xwalk[target_id_col] = xwalk[target_id_col].astype("string")
        xwalk["tract_geoid"] = xwalk["tract_geoid"].astype("string").str.zfill(11)
        xwalk["_imputation_share"] = pd.to_numeric(xwalk[share_column], errors="coerce")
        merged = xwalk[[target_id_col, "tract_geoid", "_imputation_share"]].merge(
            support,
            on="tract_geoid",
            how="left",
        )

    for column in [*source_columns, *support_columns]:
        if column in targets.columns:
            targets[column] = pd.to_numeric(targets[column], errors="coerce")
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")
    merged["_imputation_share"] = pd.to_numeric(
        merged["_imputation_share"],
        errors="coerce",
    )

    target_index = targets.set_index(target_id_col, drop=True)
    result = merged[["tract_geoid", target_id_col, "acs_vintage", "tract_vintage"]].copy()
    result["geo_type"] = "tract"
    result["geo_id"] = result["tract_geoid"]
    result["year"] = year if year is not None else result["acs_vintage"]
    result["target_geo_type"] = target_geo_type
    result["target_geo_id"] = result[target_id_col]
    result["acs1_vintage_used"] = result[target_id_col].map(target_index["acs1_vintage"])
    result["acs5_vintage_used"] = result["acs_vintage"].astype("string")
    result["tract_vintage_used"] = result["tract_vintage"].astype("string")
    result["acs1_imputation_method"] = ACS1_IMPUTATION_METHOD
    result["acs1_imputation_denominator_source"] = "acs5_tract_support"
    result["acs1_imputation_crosswalk_id"] = crosswalk_id
    result["is_modeled"] = True
    result["is_synthetic"] = True

    zero_by_index: dict[int, set[str]] = {int(index): set() for index in merged.index}
    missing_by_index: dict[int, set[str]] = {int(index): set() for index in merged.index}
    validation_abs_diffs: dict[int, list[float]] = {int(index): [] for index in merged.index}
    validation_rel_diffs: dict[int, list[float]] = {int(index): [] for index in merged.index}

    for spec in spec_list:
        component_pairs: list[tuple[str, str]] = []
        if spec.value_kind == "rate":
            assert spec.numerator_output_column is not None
            assert spec.denominator_source_column is not None
            assert spec.denominator_output_column is not None
            numerator_source = spec.numerator_source_columns[0]
            component_pairs = [
                (numerator_source, spec.numerator_output_column),
                (spec.denominator_source_column, spec.denominator_output_column),
            ]
        else:
            component_pairs = [(spec.acs1_source_columns[0], spec.output_column)]

        for source_column, output_column in component_pairs:
            support_total = (
                merged.assign(_weighted_support=merged[source_column] * merged["_imputation_share"])
                .groupby(target_id_col, dropna=False)["_weighted_support"]
                .sum(min_count=1)
            )
            source_total = target_index[source_column]
            row_support_total = merged[target_id_col].map(support_total)
            row_source_total = merged[target_id_col].map(source_total)
            weighted_support = merged[source_column] * merged["_imputation_share"]
            allocated = pd.Series(pd.NA, index=merged.index, dtype="Float64")
            valid = (
                row_source_total.notna()
                & weighted_support.notna()
                & row_support_total.notna()
                & (row_support_total > 0)
            )
            allocated.loc[valid] = (
                row_source_total.loc[valid]
                * weighted_support.loc[valid]
                / row_support_total.loc[valid]
            )

            zero_mask = row_support_total.notna() & (row_support_total == 0)
            if spec.zero_denominator_policy == "zero_count":
                zero_source = zero_mask & row_source_total.fillna(0).eq(0)
                allocated.loc[zero_source] = 0.0
            for index in merged.index[zero_mask]:
                zero_by_index[int(index)].add(source_column)

            missing_mask = weighted_support.isna() | row_source_total.isna()
            for index in merged.index[missing_mask]:
                missing_by_index[int(index)].add(source_column)

            result[output_column] = allocated.astype("Float64")

            allocated_by_target = result.groupby(target_id_col, dropna=False)[output_column].sum(
                min_count=1
            )
            for target_id in sorted(merged[target_id_col].dropna().astype(str).unique()):
                allocated_value = allocated_by_target.get(target_id, pd.NA)
                source_value = source_total.get(target_id, pd.NA)
                allocated_float = None if pd.isna(allocated_value) else float(allocated_value)
                source_float = None if pd.isna(source_value) else float(source_value)
                abs_diff, rel_diff = _validation_diff(allocated_float, source_float)
                _assert_within_tolerance(
                    spec=spec,
                    target_id=target_id,
                    column=source_column,
                    abs_diff=abs_diff,
                    rel_diff=rel_diff,
                )
                target_mask = result[target_id_col].astype(str).eq(target_id)
                for index in result.index[target_mask]:
                    if abs_diff is not None:
                        validation_abs_diffs[int(index)].append(abs(abs_diff))
                    if rel_diff is not None:
                        validation_rel_diffs[int(index)].append(abs(rel_diff))

        if spec.value_kind == "rate":
            assert spec.numerator_output_column is not None
            assert spec.denominator_output_column is not None
            result[spec.output_column] = _safe_rate(
                result[spec.numerator_output_column],
                result[spec.denominator_output_column],
            )

    result["acs1_imputation_source_county_count"] = (
        result[target_id_col].map(targets.groupby(target_id_col).size()).fillna(0).astype("Int64")
    )
    result["acs1_imputation_tract_count"] = (
        result[target_id_col]
        .map(result.groupby(target_id_col)["tract_geoid"].nunique())
        .fillna(0)
        .astype("Int64")
    )
    result["acs1_imputation_zero_denominator_count"] = [
        len(zero_by_index[int(index)]) for index in merged.index
    ]
    result["acs1_imputation_missing_support_count"] = [
        len(missing_by_index[int(index)]) for index in merged.index
    ]
    result["acs1_imputation_validation_abs_diff"] = [
        max(validation_abs_diffs[int(index)], default=0.0) for index in merged.index
    ]
    result["acs1_imputation_validation_rel_diff"] = [
        max(validation_rel_diffs[int(index)], default=0.0) for index in merged.index
    ]

    output_columns = [
        "geo_type",
        "geo_id",
        "year",
        "target_geo_type",
        "target_geo_id",
        target_id_col,
        "tract_geoid",
        "acs1_vintage_used",
        "acs5_vintage_used",
        "tract_vintage_used",
        "acs1_imputation_method",
        "acs1_imputation_denominator_source",
        "acs1_imputation_crosswalk_id",
        "is_modeled",
        "is_synthetic",
        *dict.fromkeys(column for spec in spec_list for column in spec.output_columns),
        "acs1_imputation_source_county_count",
        "acs1_imputation_tract_count",
        "acs1_imputation_zero_denominator_count",
        "acs1_imputation_missing_support_count",
        "acs1_imputation_validation_abs_diff",
        "acs1_imputation_validation_rel_diff",
    ]
    result = (
        result[output_columns].sort_values([target_id_col, "tract_geoid"]).reset_index(drop=True)
    )
    result.attrs["target_id_col"] = target_id_col
    result.attrs["target_geo_type"] = target_geo_type
    result.attrs["imputation_method"] = ACS1_IMPUTATION_METHOD
    result.attrs["measure_specs"] = [spec.name for spec in spec_list]
    return result


def _quantile_from_bins(
    row: pd.Series,
    *,
    total_column: str,
    bins: tuple[tuple[str, float, float | None], ...],
    quantile: float,
) -> tuple[float | None, dict[str, object]]:
    total = pd.to_numeric(pd.Series([row.get(total_column)]), errors="coerce").iloc[0]
    if pd.isna(total) or total <= 0:
        return None, {
            "status": "unsupported",
            "reason": "zero_or_missing_denominator",
            "quantile": quantile,
        }

    target = float(total) * quantile
    cumulative = 0.0
    skipped_null_bin = False
    for column, lower, upper in bins:
        count = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
        if pd.isna(count):
            skipped_null_bin = True
            continue
        if count <= 0:
            continue
        next_cumulative = cumulative + float(count)
        if target <= next_cumulative:
            if upper is None:
                return None, {
                    "status": "unsupported",
                    "reason": "quantile_in_open_ended_bin",
                    "bin": column,
                    "lower_bound": lower,
                    "quantile": quantile,
                }
            fraction = (target - cumulative) / float(count)
            return lower + fraction * (upper - lower), {
                "status": "ok",
                "bin": column,
                "interpolation": "linear_within_bin",
                "quantile": quantile,
            }
        cumulative = next_cumulative

    return None, {
        "status": "unsupported",
        "reason": (
            "null_bins_prevent_reaching_quantile"
            if skipped_null_bin
            else "distribution_total_exceeds_bin_sum"
        ),
        "quantile": quantile,
    }


def _normalize_county_key(series: pd.Series) -> pd.Series:
    return series.astype("string").str.zfill(5)


def _diagnostic_lists(
    support: pd.DataFrame,
    component_columns: list[str],
) -> tuple[dict[int, list[str]], dict[int, list[str]]]:
    missing_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}
    partial_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}

    for column in component_columns:
        missing_mask = support[column].isna()
        for index in support.index[missing_mask]:
            missing_by_index[int(index)].append(column)

        coverage = support.groupby("county_fips", dropna=False)[column].agg(
            missing_count=lambda s: int(s.isna().sum()),
            nonmissing_count=lambda s: int(s.notna().sum()),
        )
        partial_counties = set(
            coverage[(coverage["missing_count"] > 0) & (coverage["nonmissing_count"] > 0)].index
        )
        partial_mask = support["county_fips"].isin(partial_counties)
        for index in support.index[partial_mask]:
            partial_by_index[int(index)].append(column)

    return missing_by_index, partial_by_index


def allocate_acs1_county_to_tracts(
    county_source: pd.DataFrame,
    tract_support: pd.DataFrame,
    *,
    component_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Allocate ACS1 county components to ACS5 tracts using within-county shares.

    If a support column has partial county coverage, non-null tracts receive the
    full county source total for that component. This conserves ACS1 county
    totals under the assumption that the missing tracts follow the observed
    tract distribution.
    """
    components = _component_columns(component_columns)
    if not components:
        raise ValueError("No SAE component columns were requested for allocation.")

    _require_columns(county_source, ["county_fips", "acs1_vintage", *components], "county_source")
    _require_columns(
        tract_support,
        ["tract_geoid", "county_fips", "acs_vintage", "tract_vintage", *components],
        "tract_support",
    )

    source = county_source.copy()
    support = tract_support.copy()
    source["county_fips"] = _normalize_county_key(source["county_fips"])
    support["county_fips"] = _normalize_county_key(support["county_fips"])
    support["tract_geoid"] = support["tract_geoid"].astype("string").str.zfill(11)

    duplicated_source = source["county_fips"].duplicated(keep=False)
    if duplicated_source.any():
        counties = sorted(source.loc[duplicated_source, "county_fips"].astype(str).unique())
        raise ValueError(
            f"county_source must have one row per county_fips. Duplicates: {counties}."
        )

    source_counties = set(source["county_fips"].dropna().astype(str))
    support_counties = set(support["county_fips"].dropna().astype(str))
    missing_support_counties = sorted(source_counties - support_counties)
    missing_source_counties = sorted(support_counties - source_counties)

    source = source.set_index("county_fips", drop=True)
    for column in components:
        source[column] = pd.to_numeric(source[column], errors="coerce")
        support[column] = pd.to_numeric(support[column], errors="coerce")

    result = support[["tract_geoid", "county_fips", "acs_vintage", "tract_vintage"]].copy()
    result["source_county_fips"] = result["county_fips"]
    result["acs1_vintage"] = result["county_fips"].map(source["acs1_vintage"])
    result["allocation_method"] = SAE_ALLOCATION_METHOD

    missing_by_index, partial_by_index = _diagnostic_lists(support, components)
    zero_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}
    residuals_by_county: dict[str, dict[str, float | None]] = {
        county: {} for county in sorted(support_counties)
    }

    for column in components:
        county_support_total = support.groupby("county_fips", dropna=False)[column].sum(min_count=1)
        source_total = source[column]
        support_total = support["county_fips"].map(county_support_total)
        source_value = support["county_fips"].map(source_total)

        valid = (
            source_value.notna()
            & support[column].notna()
            & support_total.notna()
            & (support_total > 0)
        )
        allocated = pd.Series(pd.NA, index=support.index, dtype="Float64")
        allocated.loc[valid] = (
            source_value.loc[valid] * support.loc[valid, column] / support_total.loc[valid]
        )
        result[f"sae_{column}"] = allocated.astype("Float64")

        zero_mask = support_total.notna() & (support_total == 0)
        for index in support.index[zero_mask]:
            zero_by_index[int(index)].append(column)

        allocated_by_county = result.groupby("county_fips", dropna=False)[f"sae_{column}"].sum(
            min_count=1,
        )
        for county in sorted(support_counties):
            allocated_total = allocated_by_county.get(county, pd.NA)
            county_source_total = source_total.get(county, pd.NA)
            if pd.isna(allocated_total) or pd.isna(county_source_total):
                residuals_by_county[county][column] = None
            else:
                residuals_by_county[county][column] = float(allocated_total - county_source_total)

    result["sae_missing_support_columns"] = [
        _json_list(missing_by_index[int(index)]) for index in support.index
    ]
    result["sae_zero_denominator_columns"] = [
        _json_list(zero_by_index[int(index)]) for index in support.index
    ]
    result["sae_partial_coverage_columns"] = [
        _json_list(partial_by_index[int(index)]) for index in support.index
    ]
    result["sae_missing_support_count"] = [
        len(missing_by_index[int(index)]) for index in support.index
    ]
    result["sae_zero_denominator_count"] = [
        len(zero_by_index[int(index)]) for index in support.index
    ]
    result["sae_source_county_count"] = (
        result["county_fips"]
        .map(
            pd.Series(1, index=source.index),
        )
        .fillna(0)
        .astype("Int64")
    )
    result["sae_support_tract_count"] = (
        result["county_fips"]
        .map(
            support.groupby("county_fips").size(),
        )
        .astype("Int64")
    )
    result["sae_allocation_residuals"] = result["county_fips"].map(
        {county: _json_dict(residuals) for county, residuals in residuals_by_county.items()}
    )

    result = result.sort_values(["county_fips", "tract_geoid"]).reset_index(drop=True)
    result.attrs["allocation_method"] = SAE_ALLOCATION_METHOD
    result.attrs["component_columns"] = components
    result.attrs["missing_support_counties"] = missing_support_counties
    result.attrs["missing_source_counties"] = missing_source_counties
    return result


def _allocated_component_columns(
    tract_allocations: pd.DataFrame,
    component_columns: Iterable[str] | None,
) -> list[str]:
    if component_columns is None:
        return [
            column
            for column in tract_allocations.columns
            if column.startswith("sae_")
            and column
            not in {
                "sae_missing_support_columns",
                "sae_zero_denominator_columns",
                "sae_partial_coverage_columns",
                "sae_missing_support_count",
                "sae_zero_denominator_count",
                "sae_source_county_count",
                "sae_support_tract_count",
                "sae_allocation_residuals",
            }
        ]
    return [
        column if column.startswith("sae_") else f"sae_{column}" for column in component_columns
    ]


def rollup_sae_tracts_to_geos(
    tract_allocations: pd.DataFrame,
    tract_crosswalk: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
    share_column: str = "area_share",
    component_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Roll allocated SAE tract components to analysis geographies."""
    components = _allocated_component_columns(tract_allocations, component_columns)
    if not components:
        raise ValueError("No allocated SAE component columns were available to roll up.")

    _require_columns(
        tract_allocations,
        [
            "tract_geoid",
            "source_county_fips",
            "acs1_vintage",
            "acs_vintage",
            "tract_vintage",
            *components,
        ],
        "tract_allocations",
    )
    _require_columns(tract_crosswalk, [geo_id_col, "tract_geoid", share_column], "tract_crosswalk")

    allocations = tract_allocations.copy()
    xwalk = tract_crosswalk.copy()
    allocations["tract_geoid"] = allocations["tract_geoid"].astype("string").str.zfill(11)
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype("string").str.zfill(11)
    xwalk[share_column] = pd.to_numeric(xwalk[share_column], errors="coerce")

    merged = xwalk.merge(allocations, on="tract_geoid", how="left")
    for column in components:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
        merged[f"weighted_{column}"] = merged[column] * merged[share_column]

    grouped = merged.groupby(geo_id_col, dropna=False)
    result = pd.DataFrame({geo_id_col: sorted(merged[geo_id_col].dropna().unique())})
    result["target_geo_type"] = geo_id_col.removesuffix("_id")
    result["target_geo_id"] = result[geo_id_col]

    for column in components:
        totals = grouped[f"weighted_{column}"].sum(min_count=1)
        result[column] = result[geo_id_col].map(totals).astype("Float64")

    diagnostics = grouped.agg(
        acs1_vintage=("acs1_vintage", _single_or_json),
        acs5_vintage=("acs_vintage", _single_or_json),
        tract_vintage=("tract_vintage", _single_or_json),
        sae_source_county_count=("source_county_fips", lambda s: int(s.dropna().nunique())),
        sae_crosswalk_tract_count=("tract_geoid", lambda s: int(s.dropna().nunique())),
        sae_allocated_tract_count=("allocation_method", lambda s: int(s.notna().sum())),
    ).reset_index()
    diagnostics["sae_missing_allocation_tract_count"] = (
        diagnostics["sae_crosswalk_tract_count"] - diagnostics["sae_allocated_tract_count"]
    )
    diagnostics["sae_crosswalk_coverage_ratio"] = diagnostics[
        "sae_allocated_tract_count"
    ] / diagnostics["sae_crosswalk_tract_count"].replace(0, pd.NA)
    share_sum = grouped[share_column].sum(min_count=1)
    diagnostics["sae_crosswalk_share_sum"] = (
        diagnostics[geo_id_col].map(share_sum).astype("Float64")
    )
    nan_share_tracts = (
        merged.loc[merged[share_column].isna()].groupby(geo_id_col)["tract_geoid"].nunique()
    )
    diagnostics["sae_nan_share_tract_count"] = (
        diagnostics[geo_id_col].map(nan_share_tracts).fillna(0).astype("Int64")
    )

    if "sae_missing_support_count" in merged.columns:
        missing_support = grouped["sae_missing_support_count"].sum(min_count=1)
    else:
        missing_support = pd.Series(dtype="float64")
    if "sae_zero_denominator_count" in merged.columns:
        zero_denominator = grouped["sae_zero_denominator_count"].sum(min_count=1)
    else:
        zero_denominator = pd.Series(dtype="float64")
    if "sae_partial_coverage_columns" in merged.columns:
        partial_coverage = grouped["sae_partial_coverage_columns"].apply(
            lambda s: int(sum(bool(json.loads(value)) for value in s.dropna()))
        )
    else:
        partial_coverage = pd.Series(dtype="float64")

    result = result.merge(diagnostics, on=geo_id_col, how="left")
    result["sae_missing_support_count"] = (
        result[geo_id_col].map(missing_support).fillna(0).astype("Int64")
    )
    result["sae_zero_denominator_count"] = (
        result[geo_id_col].map(zero_denominator).fillna(0).astype("Int64")
    )
    result["sae_partial_coverage_count"] = (
        result[geo_id_col].map(partial_coverage).fillna(0).astype("Int64")
    )
    source_counties = grouped["source_county_fips"].apply(
        lambda s: _json_list(s.dropna().astype(str))
    )
    result["sae_source_counties"] = result[geo_id_col].map(source_counties)

    ordered_columns = [
        geo_id_col,
        "target_geo_type",
        "target_geo_id",
        "acs1_vintage",
        "acs5_vintage",
        "tract_vintage",
        *components,
        "sae_source_county_count",
        "sae_source_counties",
        "sae_crosswalk_tract_count",
        "sae_allocated_tract_count",
        "sae_missing_allocation_tract_count",
        "sae_crosswalk_coverage_ratio",
        "sae_crosswalk_share_sum",
        "sae_nan_share_tract_count",
        "sae_missing_support_count",
        "sae_zero_denominator_count",
        "sae_partial_coverage_count",
    ]
    result = result[ordered_columns].sort_values(geo_id_col).reset_index(drop=True)
    result.attrs["geo_id_col"] = geo_id_col
    result.attrs["share_column"] = share_column
    result.attrs["component_columns"] = components
    return result


def derive_sae_burden_measures(df: pd.DataFrame) -> pd.DataFrame:
    """Derive renter and owner burden rates from allocated SAE burden bins."""
    renter_required = [
        "sae_gross_rent_pct_income_total",
        "sae_gross_rent_pct_income_not_computed",
        *RENTER_BURDEN_30_PLUS_COLUMNS,
    ]
    owner_required = [
        "sae_owner_costs_pct_income_with_mortgage_total",
        "sae_owner_costs_pct_income_with_mortgage_not_computed",
        "sae_owner_costs_pct_income_without_mortgage_total",
        "sae_owner_costs_pct_income_without_mortgage_not_computed",
        *OWNER_WITH_MORTGAGE_BURDEN_30_PLUS_COLUMNS,
        *OWNER_WITHOUT_MORTGAGE_BURDEN_30_PLUS_COLUMNS,
    ]
    _require_columns(df, [*renter_required, *owner_required], "SAE burden input")

    result = df.copy()
    result["sae_rent_burden_not_computed_count"] = pd.to_numeric(
        result["sae_gross_rent_pct_income_not_computed"],
        errors="coerce",
    ).astype("Float64")
    rent_total = pd.to_numeric(result["sae_gross_rent_pct_income_total"], errors="coerce")
    result["sae_rent_burden_denominator"] = (
        rent_total - result["sae_rent_burden_not_computed_count"].fillna(0)
    ).astype("Float64")
    result["sae_rent_burden_30_plus_count"] = _sum_columns(
        result,
        RENTER_BURDEN_30_PLUS_COLUMNS,
    )
    result["sae_rent_burden_50_plus_count"] = pd.to_numeric(
        result["sae_gross_rent_pct_income_50_plus"],
        errors="coerce",
    ).astype("Float64")
    result["sae_rent_burden_30_plus"] = _safe_rate(
        result["sae_rent_burden_30_plus_count"],
        result["sae_rent_burden_denominator"],
    )
    result["sae_rent_burden_50_plus"] = _safe_rate(
        result["sae_rent_burden_50_plus_count"],
        result["sae_rent_burden_denominator"],
    )

    owner_not_computed = _sum_columns(
        result,
        [
            "sae_owner_costs_pct_income_with_mortgage_not_computed",
            "sae_owner_costs_pct_income_without_mortgage_not_computed",
        ],
    )
    owner_total = _sum_columns(
        result,
        [
            "sae_owner_costs_pct_income_with_mortgage_total",
            "sae_owner_costs_pct_income_without_mortgage_total",
        ],
    )
    result["sae_owner_cost_burden_not_computed_count"] = owner_not_computed
    result["sae_owner_cost_burden_denominator"] = (
        owner_total - owner_not_computed.fillna(0)
    ).astype("Float64")
    result["sae_owner_cost_burden_30_plus_count"] = _sum_columns(
        result,
        [
            *OWNER_WITH_MORTGAGE_BURDEN_30_PLUS_COLUMNS,
            *OWNER_WITHOUT_MORTGAGE_BURDEN_30_PLUS_COLUMNS,
        ],
    )
    result["sae_owner_cost_burden_50_plus_count"] = _sum_columns(
        result,
        [
            "sae_owner_costs_pct_income_with_mortgage_50_plus",
            "sae_owner_costs_pct_income_without_mortgage_50_plus",
        ],
    )
    result["sae_owner_cost_burden_30_plus"] = _safe_rate(
        result["sae_owner_cost_burden_30_plus_count"],
        result["sae_owner_cost_burden_denominator"],
    )
    result["sae_owner_cost_burden_50_plus"] = _safe_rate(
        result["sae_owner_cost_burden_50_plus_count"],
        result["sae_owner_cost_burden_denominator"],
    )

    result["sae_burden_rate_diagnostics"] = [
        json.dumps(
            {
                "rent_denominator_zero": bool(pd.notna(rent_denominator) and rent_denominator == 0),
                "owner_denominator_zero": bool(
                    pd.notna(owner_denominator) and owner_denominator == 0
                ),
                "not_computed_excluded": True,
            },
            sort_keys=True,
        )
        for rent_denominator, owner_denominator in zip(
            result["sae_rent_burden_denominator"],
            result["sae_owner_cost_burden_denominator"],
            strict=True,
        )
    ]
    return result


def derive_sae_distribution_measures(
    df: pd.DataFrame,
    *,
    families: Iterable[str] = ("household_income", "gross_rent"),
) -> pd.DataFrame:
    """Derive median and quantile measures from allocated SAE distributions."""
    family_list = list(families)
    unsupported = sorted(set(family_list) - {"household_income", "gross_rent"})
    if unsupported:
        raise ValueError(f"Unsupported SAE distribution measure families: {unsupported}.")

    result = df.copy()
    if "household_income" in family_list:
        _require_columns(
            result,
            ["sae_household_income_total", *[column for column, _, _ in HOUSEHOLD_INCOME_BINS]],
            "SAE household income distribution input",
        )
        row_diagnostics: list[dict[str, object]] = [{} for _ in range(len(result))]
        quantiles = {
            "sae_household_income_quintile_cutoff_20": 0.20,
            "sae_household_income_quintile_cutoff_40": 0.40,
            "sae_household_income_median": 0.50,
            "sae_household_income_quintile_cutoff_60": 0.60,
            "sae_household_income_quintile_cutoff_80": 0.80,
        }
        for output_column, quantile in quantiles.items():
            values: list[float | None] = []
            for row_index, row in enumerate(result.itertuples(index=False)):
                row_series = pd.Series(row._asdict())
                value, diagnostic = _quantile_from_bins(
                    row_series,
                    total_column="sae_household_income_total",
                    bins=HOUSEHOLD_INCOME_BINS,
                    quantile=quantile,
                )
                values.append(value)
                row_diagnostics[row_index][output_column] = diagnostic
            result[output_column] = pd.Series(values, dtype="Float64")
        result["sae_household_income_distribution_diagnostics"] = [
            json.dumps(diagnostic, sort_keys=True) for diagnostic in row_diagnostics
        ]

    if "gross_rent" in family_list:
        _require_columns(
            result,
            [
                "sae_gross_rent_distribution_with_cash_rent",
                *[column for column, _, _ in GROSS_RENT_BINS],
            ],
            "SAE gross rent distribution input",
        )
        values = []
        diagnostics = []
        for _, row in result.iterrows():
            value, diagnostic = _quantile_from_bins(
                row,
                total_column="sae_gross_rent_distribution_with_cash_rent",
                bins=GROSS_RENT_BINS,
                quantile=0.50,
            )
            values.append(value)
            diagnostics.append(diagnostic)
        result["sae_gross_rent_median"] = pd.Series(values, dtype="Float64")
        result["sae_gross_rent_distribution_diagnostics"] = [
            json.dumps(diagnostic, sort_keys=True) for diagnostic in diagnostics
        ]

    return result


def compare_sae_to_direct_counties(
    sae_geo: pd.DataFrame,
    county_source: pd.DataFrame,
    geo_county_coverage: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
    measure_columns: Iterable[str],
    coverage_column: str = "county_area_coverage_ratio",
    full_coverage_tolerance: float = 1e-9,
) -> pd.DataFrame:
    """Compare SAE geography estimates with direct whole-county source sums."""
    measures = list(measure_columns)
    if not measures:
        raise ValueError("measure_columns must contain at least one measure.")

    _require_columns(sae_geo, [geo_id_col, *measures], "sae_geo")
    _require_columns(county_source, ["county_fips"], "county_source")
    _require_columns(
        geo_county_coverage,
        [geo_id_col, "county_fips", coverage_column],
        "geo_county_coverage",
    )

    source = county_source.copy()
    source["county_fips"] = _normalize_county_key(source["county_fips"])
    coverage = geo_county_coverage.copy()
    coverage["county_fips"] = _normalize_county_key(coverage["county_fips"])
    coverage[coverage_column] = pd.to_numeric(coverage[coverage_column], errors="coerce")

    rows: list[dict[str, object]] = []
    coverage_geo_ids = set(coverage[geo_id_col].dropna().astype(str))
    sae_geo_ids = set(sae_geo[geo_id_col].dropna().astype(str))
    grouped_coverage = coverage.groupby(geo_id_col, dropna=False)

    for geo_id in sorted(coverage_geo_ids | sae_geo_ids):
        if geo_id not in coverage_geo_ids:
            members = coverage.iloc[0:0]
            member_counties: list[str] = []
            comparable = False
            reason = "not_in_coverage"
        else:
            members = grouped_coverage.get_group(geo_id)
            member_counties = sorted(members["county_fips"].dropna().astype(str).unique())
            coverages = members[coverage_column]
            has_partial = bool((coverages < 1.0 - full_coverage_tolerance).any())
            has_missing_coverage = bool(coverages.isna().any())
            if has_missing_coverage:
                comparable = False
                reason = "missing_containment"
            elif has_partial and len(member_counties) > 1:
                comparable = False
                reason = "mixed_containment"
            elif has_partial:
                comparable = False
                reason = "partial_county"
            else:
                comparable = True
                reason = "whole_county"

        sae_rows = sae_geo[sae_geo[geo_id_col] == geo_id]
        source_rows = source[source["county_fips"].isin(member_counties)]
        for measure in measures:
            direct_value = pd.NA
            absolute_difference = pd.NA
            relative_difference = pd.NA
            if comparable:
                _require_columns(source, [measure], "county_source")
                direct_value = pd.to_numeric(source_rows[measure], errors="coerce").sum(
                    min_count=1,
                )
            sae_value = pd.NA
            if not sae_rows.empty:
                sae_value = pd.to_numeric(sae_rows[measure], errors="coerce").sum(
                    min_count=1,
                )
            if comparable and pd.notna(direct_value) and pd.notna(sae_value):
                absolute_difference = float(sae_value - direct_value)
                if direct_value != 0:
                    relative_difference = float(absolute_difference / direct_value)

            rows.append(
                {
                    geo_id_col: geo_id,
                    "measure": measure,
                    "direct_county_value": direct_value,
                    "sae_value": sae_value,
                    "absolute_difference": absolute_difference,
                    "relative_difference": relative_difference,
                    "comparable": comparable,
                    "comparability_reason": reason,
                    "source_county_count": len(member_counties),
                    "source_counties": _json_list(member_counties),
                }
            )

    result = pd.DataFrame(rows)
    numeric_columns = [
        "direct_county_value",
        "sae_value",
        "absolute_difference",
        "relative_difference",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").astype("Float64")
    return result.sort_values([geo_id_col, "measure"]).reset_index(drop=True)
