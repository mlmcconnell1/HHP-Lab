"""Resample algorithms: identity, aggregate, allocate, and pop_share enrichment.

Owns the pure transformation logic for moving a dataset from its native
geometry onto a recipe target geometry, plus ``_attach_dynamic_pop_share``
which derives population weights from a transform's declared support
dataset.  ``_execute_resample`` in ``hhplab.recipe.executor`` remains
the orchestration glue that picks the right helper.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from hhplab.geo.ct_planning_regions import CT_LEGACY_COUNTY_VINTAGE
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    _get_transform,
    _record_step_note,
)
from hhplab.recipe.executor_ct_alignment import (
    _load_ct_county_alignment_crosswalk,
    _needs_ct_planning_to_legacy_alignment,
    _translate_ct_planning_values_to_legacy,
)
from hhplab.recipe.executor_inputs import (
    _load_support_dataset_for_year,
    _resolve_geo_column,
    _validate_columns,
)
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.probes import get_weighted_transform_requirements

# Maps geometry types to the column name used as join key in crosswalks.
_XWALK_JOIN_KEYS: dict[str, str] = {
    "tract": "tract_geoid",
    "county": "county_fips",
    "coc": "coc_id",
    "metro": "metro_id",
    "msa": "msa_id",
}

# Columns in crosswalks that are NOT the target geography identifier.
_XWALK_NON_GEO_COLS: set[str] = {
    "tract_geoid", "county_fips", "area_share", "pop_share",
    "intersection_area", "tract_area", "county_area", "coc_area", "geo_area",
    "boundary_vintage", "tract_vintage", "definition_version",
    "cbsa_code", "county_vintage", "allocation_method", "share_column",
    "share_denominator", "allocation_share",
    "intersecting_county_count", "intersecting_county_fips",
    "acs_vintage", "weighting_method", "area_weight", "population_weight",
    "household_weight", "renter_household_weight",
    "area_denominator", "population_denominator", "household_denominator",
    "renter_household_denominator", "county_area_total",
    "county_population_total", "county_household_total",
    "county_renter_household_total", "geo_area_total", "geo_population_total",
    "geo_household_total", "geo_renter_household_total",
    "county_area_coverage_ratio", "county_population_coverage_ratio",
    "county_household_coverage_ratio", "county_renter_household_coverage_ratio",
    "tract_count", "missing_population_tract_count",
    "missing_household_tract_count", "missing_renter_household_tract_count",
}


def _detect_xwalk_target_col(
    xwalk: pd.DataFrame,
    source_key: str,
) -> str:
    """Detect the target geography column in a crosswalk.

    Finds the geo-ID column by looking for known candidates that aren't
    the source join key.  Raises ``ExecutorError`` when no candidate is
    found, rather than silently returning a default that may not exist.
    """
    candidates = [
        c for c in xwalk.columns
        if c not in _XWALK_NON_GEO_COLS and c != source_key
        and c in {"coc_id", "metro_id", "msa_id", "geo_id"}
    ]
    if len(candidates) == 1:
        return candidates[0]
    # Prefer more specific names
    for c in ("coc_id", "metro_id", "msa_id", "geo_id"):
        if c in candidates:
            return c
    raise ExecutorError(
        f"Cannot detect target geography column in crosswalk. "
        f"Columns: {list(xwalk.columns)}, source_key: {source_key!r}. "
        f"Expected one of: coc_id, metro_id, msa_id, geo_id."
    )


def _attach_dynamic_pop_share(
    *,
    xwalk: pd.DataFrame,
    task: ResampleTask,
    ctx: ExecutionContext,
    step_notes: list[str] | None = None,
) -> pd.DataFrame:
    """Populate pop_share from a transform's declared population source."""
    if task.transform_id is None:
        return xwalk
    if "pop_share" in xwalk.columns and xwalk["pop_share"].notna().any():
        return xwalk

    transform = _get_transform(ctx.recipe, task.transform_id)
    reqs = get_weighted_transform_requirements(transform)
    if reqs is None:
        return xwalk
    population_source, population_field = reqs

    source_key = _XWALK_JOIN_KEYS.get(task.effective_geometry.type)
    if source_key is None or source_key not in xwalk.columns:
        raise ExecutorError(
            f"Transform '{task.transform_id}' cannot derive pop_share for "
            f"geometry type '{task.effective_geometry.type}'."
        )

    weights_df = _load_support_dataset_for_year(
        ctx=ctx,
        dataset_id=population_source,
        year=task.year,
    )
    weights_geo_col = _resolve_geo_column(
        weights_df,
        ctx.recipe.datasets[population_source].geo_column,
    )
    if population_field not in weights_df.columns:
        raise ExecutorError(
            f"Dataset '{population_source}' year {task.year}: missing "
            f"population field '{population_field}'. "
            f"Available: {sorted(weights_df.columns)}"
        )

    target_col = _detect_xwalk_target_col(xwalk, source_key)
    weights = weights_df[[weights_geo_col, population_field]].copy()
    if _needs_ct_planning_to_legacy_alignment(
        xwalk=xwalk,
        source_values=weights[weights_geo_col],
        source_key=source_key,
    ):
        legacy_vintage = (
            int(task.effective_geometry.vintage)
            if task.effective_geometry.vintage is not None
            else CT_LEGACY_COUNTY_VINTAGE
        )
        ct_bridge = _load_ct_county_alignment_crosswalk(
            ctx=ctx,
            legacy_vintage=legacy_vintage,
        )
        _record_step_note(
            ctx,
            step_notes,
            "Connecticut special-case alignment applied: translated "
            f"planning-region population_source '{population_source}' to "
            "legacy counties before deriving pop_share.",
        )
        weights = _translate_ct_planning_values_to_legacy(
            df=weights,
            geo_col=weights_geo_col,
            value_columns=[population_field],
            crosswalk=ct_bridge,
            year_value=task.year if "year" in weights_df.columns else None,
        )
    weights = weights.rename(columns={weights_geo_col: source_key})
    weights[population_field] = pd.to_numeric(
        weights[population_field],
        errors="coerce",
    )

    enriched = xwalk.merge(weights, on=source_key, how="left")
    group_sum = enriched.groupby(target_col)[population_field].transform("sum")
    enriched["pop_share"] = np.where(
        group_sum > 0,
        enriched[population_field] / group_sum,
        np.nan,
    )
    return enriched.drop(columns=[population_field])


def _resample_identity(
    df: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Identity resample: passthrough with column standardisation."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    cols = [geo_col] + task.measures
    if (
        task.year_column == "acs1_vintage"
        and task.year_column in df.columns
        and task.year_column not in cols
    ):
        cols.insert(1, task.year_column)
    if "year" in df.columns and "year" not in cols:
        insert_at = 2 if len(cols) > 1 and cols[1] == task.year_column else 1
        cols.insert(insert_at, "year")

    result = df[cols].copy()
    if geo_col != "geo_id":
        result = result.rename(columns={geo_col: "geo_id"})
    return result


def _resample_aggregate(
    df: pd.DataFrame,
    xwalk: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Aggregate resample: many-to-few via crosswalk weights."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    # Determine join key in crosswalk based on effective geometry type
    geo_type = task.effective_geometry.type
    xwalk_key = _XWALK_JOIN_KEYS.get(geo_type)
    if xwalk_key is None or xwalk_key not in xwalk.columns:
        raise ExecutorError(
            f"Resample aggregate for '{task.dataset_id}' year {task.year}: "
            f"crosswalk does not have expected join key '{xwalk_key}' "
            f"for geometry type '{geo_type}'. "
            f"Crosswalk columns: {sorted(xwalk.columns)}"
        )

    base_weight_col = task.weight_column or "area_share"
    if base_weight_col not in xwalk.columns:
        declared_weight_col = (
            str(xwalk["share_column"].dropna().iloc[0])
            if "share_column" in xwalk.columns and xwalk["share_column"].notna().any()
            else None
        )
        if declared_weight_col is not None and declared_weight_col in xwalk.columns:
            base_weight_col = declared_weight_col
    if base_weight_col not in xwalk.columns:
        raise ExecutorError(
            "Crosswalk missing weight column for aggregate resampling. "
            f"Available: {sorted(xwalk.columns)}"
        )
    has_pop_share = "pop_share" in xwalk.columns and xwalk["pop_share"].notna().any()

    # Detect the target geography column in the crosswalk
    target_col = _detect_xwalk_target_col(xwalk, xwalk_key)

    # Merge dataset with crosswalk
    xwalk_cols = [target_col, xwalk_key, base_weight_col]
    if "pop_share" in xwalk.columns:
        xwalk_cols.append("pop_share")
    merged = df.merge(
        xwalk[xwalk_cols],
        left_on=geo_col,
        right_on=xwalk_key,
        how="inner",
    )

    if merged.empty:
        raise ExecutorError(
            f"Resample aggregate for '{task.dataset_id}' year {task.year}: "
            f"zero rows after joining dataset ({geo_col}) with crosswalk "
            f"({xwalk_key}). Check that geo-ID formats match."
        )

    # Resolve per-measure aggregation methods
    measure_aggs: dict[str, str] = task.measure_aggregations or {
        m: "sum" for m in task.measures
    }

    # Convert measures to float64 to handle nullable Pandas types (Int64, Float64)
    for m in task.measures:
        before_notna = merged[m].notna().sum()
        merged[m] = pd.to_numeric(merged[m], errors="coerce").astype("float64")
        lost = before_notna - merged[m].notna().sum()
        if lost > 0:
            warnings.warn(
                f"Dataset '{task.dataset_id}' year {task.year}: "
                f"{lost} non-numeric value(s) in measure '{m}' coerced to NaN.",
                stacklevel=2,
            )
    merged[base_weight_col] = merged[base_weight_col].astype("float64")
    if "pop_share" in merged.columns:
        merged["pop_share"] = pd.to_numeric(merged["pop_share"], errors="coerce").astype("float64")

    # Group measures by aggregation method for efficient dispatch
    agg_groups: dict[str, list[str]] = {}
    for m, agg in measure_aggs.items():
        agg_groups.setdefault(agg, []).append(m)

    # Aggregate per geography unit, per method
    result_parts: list[pd.DataFrame] = []
    for agg, measures in agg_groups.items():
        if agg == "sum":
            part = merged.copy()
            for m in measures:
                part[m] = part[m] * part[base_weight_col]
            result_parts.append(part.groupby(target_col)[measures].sum().reset_index())
        elif agg == "mean":
            result_parts.append(merged.groupby(target_col)[measures].mean().reset_index())
        elif agg == "weighted_mean":
            weight_col = "pop_share" if has_pop_share else base_weight_col
            rows = []
            for geo_id, group in merged.groupby(target_col):
                w = group[weight_col].values
                w_sum = w.sum()
                row: dict[str, object] = {target_col: geo_id}
                for m in measures:
                    vals = group[m].values
                    mask = ~np.isnan(vals)
                    if w_sum > 0 and mask.any():
                        row[m] = (vals[mask] * w[mask]).sum() / w[mask].sum()
                    else:
                        row[m] = None
                rows.append(row)
            result_parts.append(pd.DataFrame(rows))
        else:
            raise ExecutorError(
                f"Unsupported aggregation method '{agg}' for "
                f"dataset '{task.dataset_id}'."
            )

    # Merge all result parts on target column
    result = result_parts[0]
    for part in result_parts[1:]:
        result = result.merge(part, on=target_col, how="outer")

    if target_col != "geo_id":
        result = result.rename(columns={target_col: "geo_id"})
    result["year"] = task.year
    return result


def _resample_allocate(
    df: pd.DataFrame,
    xwalk: pd.DataFrame,
    task: ResampleTask,
) -> pd.DataFrame:
    """Allocate resample: few-to-many via crosswalk weights."""
    geo_col = _resolve_geo_column(df, task.geo_column)
    _validate_columns(df, task.measures, task.dataset_id, task.year)

    # For allocate, the target geometry is finer-grained.
    # The crosswalk connects target (fine) <-> source (coarse).
    target_type = task.to_geometry.type
    target_key = _XWALK_JOIN_KEYS.get(target_type)
    if target_key is None or target_key not in xwalk.columns:
        raise ExecutorError(
            f"Resample allocate for '{task.dataset_id}' year {task.year}: "
            f"crosswalk does not have target key '{target_key}' "
            f"for target geometry '{target_type}'. "
            f"Crosswalk columns: {sorted(xwalk.columns)}"
        )

    weight_col = "area_share"
    if weight_col not in xwalk.columns:
        raise ExecutorError(
            f"Crosswalk missing weight column '{weight_col}'. "
            f"Available: {sorted(xwalk.columns)}"
        )

    # Detect the source geography column in the crosswalk
    source_col = _detect_xwalk_target_col(xwalk, target_key)

    # Merge dataset (coarse) with crosswalk
    merged = df.merge(
        xwalk[[source_col, target_key, weight_col]],
        left_on=geo_col,
        right_on=source_col,
        how="inner",
    )

    if merged.empty:
        raise ExecutorError(
            f"Resample allocate for '{task.dataset_id}' year {task.year}: "
            f"zero rows after join."
        )

    # Allocate: distribute source value by weight
    for m in task.measures:
        merged[m] = merged[m] * merged[weight_col]

    result = merged[[target_key] + task.measures].copy()
    result = result.rename(columns={target_key: "geo_id"})
    result["year"] = task.year
    return result
