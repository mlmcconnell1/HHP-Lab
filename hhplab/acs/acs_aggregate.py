"""ACS measure builders for CoC-level and metro-level statistics.

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
    CoC_estimate = Σ(tract_median × denominator_weight) / Σ(denominator_weight)

    Income medians use population weights. Gross rent medians use renter
    household weights. Contract rent medians use B25056 with-cash-rent
    household weights when available, with renter households as fallback.
    These are weighted averages of tract medians, NOT true medians computed
    from underlying household distributions.

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

2. **MOE propagation partially implemented**: ACS estimates include margins of
   error (MOE). For count variables (e.g., total_population), MOEs are
   propagated to the CoC level using ``sqrt(sum(area_share² × moe²))``.
   Median variable MOEs are not yet propagated. Proper error propagation for
   medians requires variance formulas that account for covariance structure.

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

from hhplab.acs.variables import COUNT_COLUMNS, MEDIAN_COLUMNS, MOE_COLUMNS
from hhplab.geo.ct_planning_regions import CT_STATE_FIPS
from hhplab.xwalks.apply import apply_crosswalk

RENT_BURDEN_30_PLUS_COLUMNS: tuple[str, ...] = (
    "gross_rent_pct_income_30_to_34_9",
    "gross_rent_pct_income_35_to_39_9",
    "gross_rent_pct_income_40_to_49_9",
    "gross_rent_pct_income_50_plus",
)

RENT_BURDEN_40_PLUS_COLUMNS: tuple[str, ...] = (
    "gross_rent_pct_income_40_to_49_9",
    "gross_rent_pct_income_50_plus",
)

RENT_BURDEN_50_PLUS_COLUMNS: tuple[str, ...] = ("gross_rent_pct_income_50_plus",)

MSA_ACS5_COVARIATE_ALIASES: dict[str, str] = {
    "median_gross_rent": "msa_median_rent",
    "contract_rent_p10": "msa_contract_rent_p10",
    "contract_rent_p25": "msa_contract_rent_p25",
    "contract_rent_p50": "msa_contract_rent_p50",
    "vacancy_rate": "msa_vacancy_rate",
    "poverty_rate": "msa_poverty_rate",
    "median_household_income": "msa_income",
    "rent_burden_30_plus": "msa_rent_burden",
    "rent_burden_40_plus": "msa_rent_burden_40_plus",
    "rent_burden_50_plus": "msa_rent_burden_50_plus",
}

AVERAGE_WEIGHT_DENOMINATORS: dict[str, tuple[str, ...]] = {
    "median_household_income": ("total_population",),
    "per_capita_income": ("total_population",),
    "median_gross_rent": ("renter_households", "total_population"),
    "median_contract_rent": (
        "contract_rent_distribution_with_cash_rent",
        "renter_households",
    ),
    "median_owner_occupied_home_value": ("owner_households", "total_population"),
    "gini_index": ("total_population",),
}

CONTRACT_RENT_BINS: tuple[tuple[str, float, float | None], ...] = (
    ("contract_rent_distribution_cash_rent_lt_100", 0.0, 100.0),
    ("contract_rent_distribution_cash_rent_100_to_149", 100.0, 150.0),
    ("contract_rent_distribution_cash_rent_150_to_199", 150.0, 200.0),
    ("contract_rent_distribution_cash_rent_200_to_249", 200.0, 250.0),
    ("contract_rent_distribution_cash_rent_250_to_299", 250.0, 300.0),
    ("contract_rent_distribution_cash_rent_300_to_349", 300.0, 350.0),
    ("contract_rent_distribution_cash_rent_350_to_399", 350.0, 400.0),
    ("contract_rent_distribution_cash_rent_400_to_449", 400.0, 450.0),
    ("contract_rent_distribution_cash_rent_450_to_499", 450.0, 500.0),
    ("contract_rent_distribution_cash_rent_500_to_549", 500.0, 550.0),
    ("contract_rent_distribution_cash_rent_550_to_599", 550.0, 600.0),
    ("contract_rent_distribution_cash_rent_600_to_649", 600.0, 650.0),
    ("contract_rent_distribution_cash_rent_650_to_699", 650.0, 700.0),
    ("contract_rent_distribution_cash_rent_700_to_749", 700.0, 750.0),
    ("contract_rent_distribution_cash_rent_750_to_799", 750.0, 800.0),
    ("contract_rent_distribution_cash_rent_800_to_899", 800.0, 900.0),
    ("contract_rent_distribution_cash_rent_900_to_999", 900.0, 1000.0),
    ("contract_rent_distribution_cash_rent_1000_to_1249", 1000.0, 1250.0),
    ("contract_rent_distribution_cash_rent_1250_to_1499", 1250.0, 1500.0),
    ("contract_rent_distribution_cash_rent_1500_to_1999", 1500.0, 2000.0),
    ("contract_rent_distribution_cash_rent_2000_to_2499", 2000.0, 2500.0),
    ("contract_rent_distribution_cash_rent_2500_to_2999", 2500.0, 3000.0),
    ("contract_rent_distribution_cash_rent_3000_to_3499", 3000.0, 3500.0),
    ("contract_rent_distribution_cash_rent_3500_plus", 3500.0, None),
)

CONTRACT_RENT_BINS_EARLY: tuple[tuple[str, float, float | None], ...] = (
    *CONTRACT_RENT_BINS[:20],
    ("contract_rent_distribution_cash_rent_2000_plus", 2000.0, None),
)


def _maybe_remap_ct_planning_regions(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    acs_vintage: str,
) -> pd.DataFrame:
    """Attempt to remap CT planning-region GEOIDs to legacy county GEOIDs."""
    import warnings

    if "GEOID" not in acs_data.columns:
        return acs_data

    ct_in_acs = acs_data["GEOID"].astype(str).str.startswith(CT_STATE_FIPS).any()
    ct_in_xwalk = (
        crosswalk["tract_geoid"].astype(str).str.startswith(CT_STATE_FIPS).any()
        if "tract_geoid" in crosswalk.columns
        else crosswalk["GEOID"].astype(str).str.startswith(CT_STATE_FIPS).any()
    )
    if not ct_in_acs or not ct_in_xwalk:
        return acs_data

    # Only attempt remap for ACS vintages that use planning regions (2022+).
    acs_end_year = int(acs_vintage.split("-")[1] if "-" in acs_vintage else acs_vintage)
    if acs_end_year < 2022:
        return acs_data

    try:
        from hhplab.geo.ct_planning_regions import (
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
            f"CT planning-region GEOID remap skipped. {exc}",
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
            low_overlap_states.append((state, overlap_ratio, len(xwalk_tracts), len(overlap)))

    if low_overlap_states:
        # Format warning message
        state_details = []
        for state, ratio, total, matched in low_overlap_states:
            state_details.append(f"  State {state}: {matched}/{total} tracts matched ({ratio:.1%})")

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


def _weighted_mean_denominator(
    group: pd.DataFrame,
    column: str,
    overlap_weight: pd.Series,
) -> pd.Series:
    """Return the best available denominator weights for an averaged ACS estimate."""
    resolved_weights = pd.Series(0.0, index=group.index)
    for denominator_column in AVERAGE_WEIGHT_DENOMINATORS.get(column, ("total_population",)):
        if denominator_column not in group.columns:
            continue
        denominator = pd.to_numeric(group[denominator_column], errors="coerce")
        weights = denominator.fillna(0) * overlap_weight
        fill_mask = (resolved_weights <= 0) & (weights > 0)
        resolved_weights.loc[fill_mask] = weights.loc[fill_mask]
    return resolved_weights


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
    moe_cols = [c for c in MOE_COLUMNS if c in acs_data.columns]
    count_sums_by_geo: dict[object, dict[str, object]] = {}
    if sum_cols:
        count_sums = apply_crosswalk(
            acs_data,
            xwalk,
            value_cols=sum_cols,
            weight_col="area_share",
            geo_id_col=geo_id_col,
            source_id_col="GEOID",
        )
        count_sums_by_geo = count_sums.set_index(geo_id_col)[sum_cols].to_dict("index")

    # Apply weights and aggregate
    results = []
    for geo_id, group in merged.groupby(geo_id_col):
        row = {geo_id_col: geo_id}

        # Weighted sums for population counts - ALWAYS use area_share
        # This computes actual population totals (sum of tract_pop * area_share)
        # Using pop_share here would give weighted averages, not totals
        area_share = pd.to_numeric(group["area_share"], errors="coerce").fillna(0)
        row.update({col: pd.NA for col in sum_cols})
        row.update(count_sums_by_geo.get(geo_id, {}))

        # Weighted averages for scalar estimates. Each column chooses the most
        # defensible denominator available, then applies the requested overlap basis.
        average_overlap_weight = pd.to_numeric(
            group[median_weight_col],
            errors="coerce",
        ).fillna(0)

        for col in avg_cols:
            if col in group.columns:
                average_weights = _weighted_mean_denominator(
                    group,
                    col,
                    average_overlap_weight,
                )
                valid_mask = group[col].notna() & (average_weights > 0)
                if valid_mask.any():
                    weighted_sum = (
                        pd.to_numeric(group.loc[valid_mask, col], errors="coerce")
                        * average_weights[valid_mask]
                    ).sum()
                    row[col] = weighted_sum / average_weights[valid_mask].sum()
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

        # MOE propagation for count-type margins of error
        # Formula: moe_coc = sqrt(sum(area_share^2 * moe_tract^2))
        for col in moe_cols:
            if col in group.columns:
                moe_vals = pd.to_numeric(group[col], errors="coerce").fillna(0)
                row[col] = (area_share**2 * moe_vals**2).sum() ** 0.5

        results.append(row)

    result_df = pd.DataFrame(results)

    # Derive unemployment_rate from aggregated numerator/denominator
    if "civilian_labor_force" in result_df.columns and "unemployed_count" in result_df.columns:
        clf = result_df["civilian_labor_force"]
        result_df["unemployment_rate"] = result_df["unemployed_count"] / clf.where(clf > 0)

    _derive_acs5_covariates(result_df)
    if geo_id_col == "msa_id":
        _apply_msa_acs5_covariate_aliases(result_df)

    # Add metadata columns
    result_df["weighting_method"] = weighting
    result_df["source"] = "acs_5yr"

    return result_df


def _derive_acs5_covariates(result_df: pd.DataFrame) -> None:
    """Derive canonical ACS5 rates from aggregated numerator/denominator columns."""
    if "citizenship_total" in result_df.columns and {
        "naturalized_citizen",
        "not_us_citizen",
    } <= set(result_df.columns):
        denominator = pd.to_numeric(result_df["citizenship_total"], errors="coerce")
        numerator = pd.to_numeric(
            result_df["naturalized_citizen"], errors="coerce"
        ) + pd.to_numeric(result_df["not_us_citizen"], errors="coerce")
        result_df["non_native_share"] = numerator / denominator.where(denominator > 0)

    if "population_below_poverty" in result_df.columns and "poverty_universe" in result_df.columns:
        denominator = pd.to_numeric(result_df["poverty_universe"], errors="coerce")
        numerator = pd.to_numeric(result_df["population_below_poverty"], errors="coerce")
        result_df["poverty_rate"] = numerator / denominator.where(denominator > 0)

    if "vacant_housing_units" in result_df.columns and "total_housing_units" in result_df.columns:
        denominator = pd.to_numeric(result_df["total_housing_units"], errors="coerce")
        numerator = pd.to_numeric(result_df["vacant_housing_units"], errors="coerce")
        result_df["vacancy_rate"] = numerator / denominator.where(denominator > 0)

    if {
        "vacancy_status_total",
        "vacant_seasonal_recreational_occasional",
    } <= set(result_df.columns):
        denominator = pd.to_numeric(result_df["vacancy_status_total"], errors="coerce")
        numerator = pd.to_numeric(
            result_df["vacant_seasonal_recreational_occasional"],
            errors="coerce",
        )
        result_df["seasonal_recreational_vacancy_share"] = numerator / denominator.where(
            denominator > 0
        )

    if "gross_rent_pct_income_total" in result_df.columns and all(
        column in result_df.columns for column in RENT_BURDEN_30_PLUS_COLUMNS
    ):
        denominator = pd.to_numeric(result_df["gross_rent_pct_income_total"], errors="coerce")
        numerator_components = pd.concat(
            [
                pd.to_numeric(result_df[column], errors="coerce")
                for column in RENT_BURDEN_30_PLUS_COLUMNS
            ],
            axis=1,
        )
        numerator = numerator_components.sum(
            axis=1,
            min_count=len(RENT_BURDEN_30_PLUS_COLUMNS),
        )
        numerator = numerator.where(denominator.notna())
        result_df["rent_burden_30_plus"] = numerator / denominator.where(denominator > 0)

    if {
        "gross_rent_pct_income_total",
        "gross_rent_pct_income_not_computed",
        *RENT_BURDEN_40_PLUS_COLUMNS,
    } <= set(result_df.columns):
        denominator = pd.to_numeric(result_df["gross_rent_pct_income_total"], errors="coerce")
        not_computed = pd.to_numeric(
            result_df["gross_rent_pct_income_not_computed"],
            errors="coerce",
        )
        computed_denominator = denominator - not_computed
        result_df["rent_burden_40_plus"] = _rent_burden_rate(
            result_df,
            numerator_columns=RENT_BURDEN_40_PLUS_COLUMNS,
            denominator=computed_denominator,
            total=denominator,
        )
        result_df["rent_burden_50_plus"] = _rent_burden_rate(
            result_df,
            numerator_columns=RENT_BURDEN_50_PLUS_COLUMNS,
            denominator=computed_denominator,
            total=denominator,
        )

    _derive_contract_rent_quantiles(result_df)


def _rent_burden_rate(
    result_df: pd.DataFrame,
    *,
    numerator_columns: tuple[str, ...],
    denominator: pd.Series,
    total: pd.Series,
) -> pd.Series:
    numerator_components = pd.concat(
        [pd.to_numeric(result_df[column], errors="coerce") for column in numerator_columns],
        axis=1,
    )
    numerator = numerator_components.sum(axis=1, min_count=len(numerator_columns))
    numerator = numerator.where(total.notna())
    return numerator / denominator.where(denominator > 0)


CONTRACT_RENT_QUANTILES: tuple[tuple[str, float], ...] = (
    ("contract_rent_p10", 0.10),
    ("contract_rent_p25", 0.25),
    ("contract_rent_p50", 0.50),
)


def _derive_contract_rent_quantiles(result_df: pd.DataFrame) -> None:
    """Derive contract-rent quantiles from aggregated B25056 bin counts."""
    if "contract_rent_distribution_with_cash_rent" not in result_df.columns:
        return

    bins = _available_contract_rent_bins(result_df)
    if not bins:
        return

    for output_column, quantile in CONTRACT_RENT_QUANTILES:
        values: list[object] = []
        for _, row in result_df.iterrows():
            values.append(
                _quantile_from_distribution_row(
                    row,
                    total_column="contract_rent_distribution_with_cash_rent",
                    bins=bins,
                    quantile=quantile,
                )
            )
        result_df[output_column] = values


def _available_contract_rent_bins(
    result_df: pd.DataFrame,
) -> tuple[tuple[str, float, float | None], ...]:
    if "contract_rent_distribution_cash_rent_2000_to_2499" in result_df.columns:
        return tuple(column for column in CONTRACT_RENT_BINS if column[0] in result_df.columns)
    return tuple(column for column in CONTRACT_RENT_BINS_EARLY if column[0] in result_df.columns)


def _quantile_from_distribution_row(
    row: pd.Series,
    *,
    total_column: str,
    bins: tuple[tuple[str, float, float | None], ...],
    quantile: float,
) -> object:
    total = pd.to_numeric(pd.Series([row.get(total_column)]), errors="coerce").iloc[0]
    if pd.isna(total) or total <= 0:
        return pd.NA

    target = float(total) * quantile
    cumulative = 0.0
    for column, lower, upper in bins:
        count = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
        if pd.isna(count) or count <= 0:
            continue
        next_cumulative = cumulative + float(count)
        if target <= next_cumulative:
            if upper is None:
                return pd.NA
            fraction = (target - cumulative) / float(count)
            return lower + fraction * (upper - lower)
        cumulative = next_cumulative

    return pd.NA


def _apply_msa_acs5_covariate_aliases(result_df: pd.DataFrame) -> None:
    """Add MSA-CoC panel-facing aliases for ACS5 MSA covariates."""
    for source_column, alias_column in MSA_ACS5_COVARIATE_ALIASES.items():
        if source_column in result_df.columns:
            result_df[alias_column] = result_df[source_column]


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
