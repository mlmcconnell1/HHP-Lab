"""ACS measure builder for CoC-level statistics.

Builds CoC-level demographic measures from ACS 5-year estimates by:
1. Fetching tract-level ACS data from Census API
2. Joining with tract-to-CoC crosswalks
3. Aggregating using area or population weighting

Aggregation Algorithm
---------------------

This module uses **weighted tract-level aggregation** to produce CoC-level
estimates. The algorithm differs by measure type:

**Count variables** (population, poverty counts):
    CoC_estimate = Σ(tract_value × weight)

    where weight is either:
    - area_share: fraction of tract area falling within the CoC
    - pop_share: population-proportional weight (tract_pop × area_share / total)

**Median variables** (income, rent):
    CoC_estimate = Σ(tract_median × pop_weight) / Σ(pop_weight)

    These are population-weighted averages of tract medians, NOT true medians
    computed from underlying household distributions.

Why This Approach Is Acceptable
-------------------------------

1. **Standard practice in policy research**: This method aligns with HUD's own
   CoC-level reporting and academic research (e.g., Byrne et al., 2012). The
   Census Bureau does not publish CoC-level tabulations, making tract-based
   aggregation the established approach.

2. **ACS design constraints**: ACS 5-year estimates are published at tract
   level. Public Use Microdata Samples (PUMS) use larger PUMAs (~100k people)
   that do not nest within CoC boundaries, making true microdata pooling
   infeasible for most CoCs.

3. **Reasonable approximation for large aggregates**: CoCs typically span
   dozens to hundreds of tracts. At this scale, weighted tract aggregation
   converges toward true population values. The Central Limit Theorem applies:
   random tract-level deviations tend to cancel when aggregated.

4. **Explicit diagnostics**: The `coverage_ratio` field quantifies what
   fraction of the CoC is captured by the crosswalk, allowing users to
   identify problematic estimates.

Known Limitations vs True Pooled Microdata
------------------------------------------

1. **Median estimates are approximate**: Averaging tract medians ≠ true
   population median. If income distributions vary significantly across tracts,
   the weighted average can over- or under-estimate the true CoC median.
   Example: A CoC with one wealthy tract (median $100k) and one poor tract
   (median $30k), equally weighted, yields $65k—which may not represent the
   true median if tract populations differ substantially.

2. **MOE propagation not implemented**: ACS estimates include margins of error
   (MOE). Proper error propagation for aggregated estimates requires variance
   formulas that account for covariance structure. This module does not yet
   compute aggregated MOEs. Users should treat CoC estimates as point estimates
   only.

3. **Ecological inference risk**: Tract-level rates (e.g., poverty rate) may
   not reflect within-CoC variation. Using aggregated rates for individual-level
   inference is subject to ecological fallacy.

4. **Boundary mismatch artifacts**: When CoC boundaries cut through tracts,
   area weighting assumes population is uniformly distributed—which is false
   for tracts containing both urban and rural areas. Population weighting
   mitigates this but does not eliminate it.

5. **Temporal mismatch**: ACS 5-year estimates pool data across 5 years (e.g.,
   2018-2022 for the 2022 vintage). CoC boundaries may change during that
   period. This module assumes boundaries are static for the aggregation.

6. **Small-CoC instability**: For CoCs with few tracts or low populations,
   estimates are more sensitive to individual tract values and crosswalk
   precision.

7. **Housing-market representativeness**: Population-weighted tract coverage
   does not guarantee housing-market representativeness. High-density tracts
   may have systematically different rental markets, vacancy rates, or housing
   stock than lower-density tracts. This will be addressed in Phase 3
   sensitivity analyses.

References
----------
- Byrne, T., et al. (2012). "Predicting Homelessness Using ACS Data."
- HUD Exchange CoC Analysis Tools methodology documentation.
- Census Bureau ACS Handbook, Chapter 12: "Working with ACS Data."
"""

from typing import Literal

import pandas as pd

from coclab.acs.variables import COUNT_COLUMNS, MEDIAN_COLUMNS


def _maybe_remap_ct_planning_regions(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    acs_vintage: str,
) -> pd.DataFrame:
    """Attempt to remap CT planning-region GEOIDs to legacy county GEOIDs."""
    import warnings

    if "GEOID" not in acs_data.columns:
        return acs_data

    ct_in_acs = acs_data["GEOID"].astype(str).str.startswith("09").any()
    ct_in_xwalk = (
        crosswalk["tract_geoid"].astype(str).str.startswith("09").any()
        if "tract_geoid" in crosswalk.columns
        else crosswalk["GEOID"].astype(str).str.startswith("09").any()
    )
    if not ct_in_acs or not ct_in_xwalk:
        return acs_data

    # Only attempt remap for ACS vintages that use planning regions (2022+).
    acs_end_year = int(acs_vintage.split("-")[1] if "-" in acs_vintage else acs_vintage)
    if acs_end_year < 2022:
        return acs_data

    try:
        from coclab.geo.ct_planning_regions import (
            build_ct_tract_planning_region_map,
            remap_ct_planning_region_geoids,
        )
    except Exception as exc:  # pragma: no cover - import errors should surface in runtime
        warnings.warn(
            f"Unable to load CT planning-region helpers ({exc}); skipping CT remap.",
            UserWarning,
            stacklevel=2,
        )
        return acs_data

    tract_vintage = None
    if "tract_vintage" in crosswalk.columns:
        tract_vintage = str(crosswalk["tract_vintage"].iloc[0])

    if tract_vintage is None:
        import warnings

        warnings.warn(
            "Crosswalk has no tract_vintage column; skipping CT planning region remap. "
            "Connecticut GEOIDs may not match between ACS and crosswalk.",
            UserWarning,
            stacklevel=2,
        )
        return acs_data

    try:
        mapping = build_ct_tract_planning_region_map(tract_vintage)
    except (FileNotFoundError, ValueError) as exc:
        warnings.warn(
            "CT planning-region GEOID remap skipped. "
            f"{exc}",
            UserWarning,
            stacklevel=2,
        )
        return acs_data

    remapped = remap_ct_planning_region_geoids(acs_data, mapping)
    if not remapped.equals(acs_data):
        warnings.warn(
            "Applied CT planning-region GEOID remap to align ACS tracts with legacy "
            "county-coded crosswalks.",
            UserWarning,
            stacklevel=2,
        )
    return remapped

    # ACS variable mappings and adult-population derivation are defined
    # canonically in coclab.acs.variables — see that module for Census
    # variable codes (B01003, B19013, B25064, C17002, B01001).


def _validate_geoid_overlap(
    crosswalk: pd.DataFrame,
    acs_data: pd.DataFrame,
    min_overlap_threshold: float = 0.5,
) -> None:
    """Validate GEOID overlap between crosswalk and ACS data by state.

    Detects tract vintage mismatches where crosswalk uses different tract
    definitions than the ACS data (e.g., 2010 vs 2020 census tract GEOIDs).

    This is a known issue for Connecticut, which changed from county-based
    tract GEOIDs (09001xxxxx) to planning region-based GEOIDs (0911xxxxxx)
    in the 2020 Census. Other states may have similar issues with tract
    boundary changes between censuses.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Crosswalk with GEOID column.
    acs_data : pd.DataFrame
        ACS data with GEOID column.
    min_overlap_threshold : float
        Minimum fraction of crosswalk tracts that must match ACS data
        before a warning is issued. Default is 0.5 (50%).

    Warns
    -----
    UserWarning
        If any state has less than min_overlap_threshold overlap between
        crosswalk and ACS GEOIDs.
    """
    import logging
    import warnings

    logger = logging.getLogger(__name__)

    if "GEOID" not in crosswalk.columns or "GEOID" not in acs_data.columns:
        return  # Can't validate without GEOID columns

    xwalk_geoids = set(crosswalk["GEOID"].dropna().unique())
    acs_geoids = set(acs_data["GEOID"].dropna().unique())

    if not xwalk_geoids or not acs_geoids:
        return

    # Extract state FIPS (first 2 characters) and check overlap by state
    xwalk_by_state: dict[str, set[str]] = {}
    for geoid in xwalk_geoids:
        state = str(geoid)[:2]
        if state not in xwalk_by_state:
            xwalk_by_state[state] = set()
        xwalk_by_state[state].add(geoid)

    acs_by_state: dict[str, set[str]] = {}
    for geoid in acs_geoids:
        state = str(geoid)[:2]
        if state not in acs_by_state:
            acs_by_state[state] = set()
        acs_by_state[state].add(geoid)

    # Check overlap for each state present in crosswalk
    low_overlap_states = []
    for state, xwalk_tracts in xwalk_by_state.items():
        acs_tracts = acs_by_state.get(state, set())
        if not acs_tracts:
            # No ACS data for this state at all
            low_overlap_states.append((state, 0, len(xwalk_tracts), 0))
            continue

        overlap = xwalk_tracts.intersection(acs_tracts)
        overlap_ratio = len(overlap) / len(xwalk_tracts) if xwalk_tracts else 0

        if overlap_ratio < min_overlap_threshold:
            low_overlap_states.append(
                (state, overlap_ratio, len(xwalk_tracts), len(overlap))
            )

    if low_overlap_states:
        # Format warning message
        state_details = []
        for state, ratio, total, matched in low_overlap_states:
            state_details.append(
                f"  State {state}: {matched}/{total} tracts matched ({ratio:.1%})"
            )

        warning_msg = (
            "Low GEOID overlap detected between crosswalk and ACS data. "
            "This typically indicates a tract vintage mismatch (e.g., crosswalk uses "
            "2020 census tract definitions but ACS data uses 2010 definitions).\n"
            "Affected states:\n" + "\n".join(state_details) + "\n"
            "CoCs in these states will have low coverage_ratio and potentially "
            "missing or underestimated population values."
        )

        logger.warning(warning_msg)
        warnings.warn(warning_msg, UserWarning, stacklevel=3)


def aggregate_to_geo(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    weighting: Literal["area", "population"] = "area",
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Aggregate tract-level ACS data to analysis geography using crosswalk.

    Parameters
    ----------
    acs_data : pd.DataFrame
        Tract-level ACS data with GEOID column.
    crosswalk : pd.DataFrame
        Tract-to-geo crosswalk with tract_geoid, ``geo_id_col``, area_share,
        and optionally pop_share.
    weighting : {"area", "population"}
        Weighting method for median value aggregation. For count variables
        (population, poverty counts), area_share is always used to compute
        actual totals. For median variables (income, rent), this parameter
        controls whether medians are weighted by area overlap alone ("area")
        or by population in overlapping areas ("population").
    geo_id_col : str
        Name of the geography identifier column in the crosswalk.
        Defaults to ``"coc_id"`` for backward compatibility.

    Returns
    -------
    pd.DataFrame
        Geography-level aggregated measures with ``geo_id_col`` as identifier.

    Notes
    -----
    Count variables (total_population, adult_population, etc.) always use
    area_share weighting to produce actual population totals. Using pop_share
    for counts would produce weighted averages instead of sums, since pop_share
    is normalized to sum to 1.0 per geography unit.

    The weighting parameter only affects median variables, controlling whether
    tract medians are weighted by geographic overlap or by population.
    """
    # For count variables, always use area_share to get actual totals
    # For median variables, use the specified weighting method
    median_weight_col = "area_share" if weighting == "area" else "pop_share"

    if "area_share" not in crosswalk.columns:
        raise ValueError("Crosswalk missing required column: area_share")
    if weighting == "population" and "pop_share" not in crosswalk.columns:
        raise ValueError("Crosswalk missing required column: pop_share")

    # Standardize GEOID column names
    xwalk = crosswalk.copy()
    if "tract_geoid" in xwalk.columns:
        xwalk = xwalk.rename(columns={"tract_geoid": "GEOID"})

    # Validate GEOID overlap between crosswalk and ACS data
    # This detects tract vintage mismatches (e.g., 2010 vs 2020 census tract definitions)
    _validate_geoid_overlap(xwalk, acs_data)

    # Join ACS data with crosswalk
    merged = xwalk.merge(acs_data, on="GEOID", how="left")

    # Columns to aggregate — derived from canonical definitions in variables.py
    sum_cols = [c for c in COUNT_COLUMNS if c in acs_data.columns]
    avg_cols = [c for c in MEDIAN_COLUMNS if c in acs_data.columns]

    # Apply weights and aggregate
    results = []
    for geo_id, group in merged.groupby(geo_id_col):
        row = {geo_id_col: geo_id}

        # Weighted sums for population counts - ALWAYS use area_share
        # This computes actual population totals (sum of tract_pop * area_share)
        # Using pop_share here would give weighted averages, not totals
        area_share = pd.to_numeric(group["area_share"], errors="coerce").fillna(0)
        for col in sum_cols:
            if col in group.columns:
                weighted = pd.to_numeric(group[col], errors="coerce").fillna(0) * area_share
                row[col] = weighted.sum()

        # Weighted averages for median values
        # Use the specified weighting method (area or population)
        pop_weights = pd.to_numeric(group["total_population"], errors="coerce").fillna(0) * pd.to_numeric(group[median_weight_col], errors="coerce").fillna(0)

        for col in avg_cols:
            if col in group.columns:
                valid_mask = group[col].notna() & (pop_weights > 0)
                if valid_mask.any():
                    weighted_sum = (group.loc[valid_mask, col] * pop_weights[valid_mask]).sum()
                    row[col] = weighted_sum / pop_weights[valid_mask].sum()
                else:
                    row[col] = pd.NA

        # Coverage ratio: fraction of geo area covered by tracts with ACS data
        # Primary ratio uses total_population availability
        if "intersection_area" in group.columns:
            total_area = group["intersection_area"].sum()
            has_data = group["total_population"].notna()
            covered_area = group.loc[has_data, "intersection_area"].sum()
            row["coverage_ratio"] = covered_area / total_area if total_area > 0 else 0.0
        else:
            # Fallback: fraction of tracts with data (less accurate)
            has_data = group["total_population"].notna()
            row["coverage_ratio"] = has_data.mean()

        # Per-measure coverage ratios for median columns
        for col in avg_cols:
            if col in group.columns:
                col_has_data = group[col].notna()
                if "intersection_area" in group.columns and total_area > 0:
                    col_covered = group.loc[col_has_data, "intersection_area"].sum()
                    row[f"coverage_{col}"] = col_covered / total_area
                else:
                    row[f"coverage_{col}"] = col_has_data.mean()

        results.append(row)

    result_df = pd.DataFrame(results)

    # Derive unemployment_rate from aggregated numerator/denominator
    if "civilian_labor_force" in result_df.columns and "unemployed_count" in result_df.columns:
        clf = result_df["civilian_labor_force"]
        result_df["unemployment_rate"] = result_df["unemployed_count"] / clf.where(clf > 0)

    # Add metadata columns
    result_df["weighting_method"] = weighting
    result_df["source"] = "acs_5yr"

    return result_df


def aggregate_to_coc(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    weighting: Literal["area", "population"] = "area",
) -> pd.DataFrame:
    """Aggregate tract-level ACS data to CoC level using crosswalk.

    Convenience wrapper around :func:`aggregate_to_geo` with
    ``geo_id_col="coc_id"``.  See that function for full documentation.
    """
    return aggregate_to_geo(acs_data, crosswalk, weighting, geo_id_col="coc_id")
