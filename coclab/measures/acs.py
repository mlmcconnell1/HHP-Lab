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

from contextlib import nullcontext
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.sources import CENSUS_API_ACS5


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

CENSUS_API = CENSUS_API_ACS5

# ACS variable mappings
# B01003_001E: Total population
# B19013_001E: Median household income
# B25064_001E: Median gross rent
# C17002_001E: Poverty universe (for whom poverty determined)
# C17002_002E: Below 50% poverty
# C17002_003E: 50-99% poverty
# B01001_001E: Total population by age (sex by age table)
# B01001_003E through B01001_025E: Male age groups
# B01001_027E through B01001_049E: Female age groups
# Adults (18+) are calculated by summing groups for ages 18+
ACS_VARS = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B25064_001E": "median_gross_rent",
    "C17002_001E": "poverty_universe",
    "C17002_002E": "below_50pct_poverty",
    "C17002_003E": "50_to_99pct_poverty",
}

# Variables for deriving adult population (18+) from B01001 (Sex by Age)
# Male 18+: B01001_007E through B01001_025E
# Female 18+: B01001_031E through B01001_049E
ADULT_MALE_VARS = [f"B01001_{i:03d}E" for i in range(7, 26)]  # 007 through 025
ADULT_FEMALE_VARS = [f"B01001_{i:03d}E" for i in range(31, 50)]  # 031 through 049
ADULT_VARS = ADULT_MALE_VARS + ADULT_FEMALE_VARS


def fetch_acs_tract_data(year: int, state_fips: str) -> pd.DataFrame:
    """Fetch ACS 5-year estimates for all tracts in a state.

    Parameters
    ----------
    year : int
        The ACS 5-year estimate end year (e.g., 2022 for 2018-2022 estimates).
    state_fips : str
        Two-digit state FIPS code (e.g., "06" for California).

    Returns
    -------
    pd.DataFrame
        DataFrame with tract GEOID and measure columns.
    """
    url = CENSUS_API.format(year=year)
    # Include base variables plus all adult age variables
    all_vars = list(ACS_VARS.keys()) + ADULT_VARS
    variables = ",".join(all_vars)

    params = {
        "get": f"NAME,{variables}",
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    # First row is headers
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    # Build GEOID from state, county, tract
    df["GEOID"] = df["state"] + df["county"] + df["tract"]

    # Convert all numeric columns
    numeric_cols = list(ACS_VARS.keys()) + ADULT_VARS
    for col in numeric_cols:
        if col in df.columns:
            # Handle negative values (Census uses -666666666 for missing)
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < 0, col] = pd.NA

    # Rename base ACS variables to friendly names
    df = df.rename(columns=ACS_VARS)

    # Calculate adult population (18+) by summing adult age groups
    adult_cols_in_df = [c for c in ADULT_VARS if c in df.columns]
    if adult_cols_in_df:
        df["adult_population"] = df[adult_cols_in_df].fillna(0).sum(axis=1)
        # Set to NA if all components were NA
        all_na = df[adult_cols_in_df].isna().all(axis=1)
        df.loc[all_na, "adult_population"] = pd.NA
    else:
        df["adult_population"] = pd.NA

    # Calculate derived measures for poverty
    if "below_50pct_poverty" in df.columns and "50_to_99pct_poverty" in df.columns:
        df["population_below_poverty"] = df["below_50pct_poverty"].fillna(0) + df[
            "50_to_99pct_poverty"
        ].fillna(0)

    # Select final columns
    keep_cols = [
        "GEOID",
        "NAME",
        "total_population",
        "adult_population",
        "median_household_income",
        "median_gross_rent",
        "poverty_universe",
        "population_below_poverty",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]

    return df[keep_cols].copy()


def fetch_all_states_tract_data(year: int, show_progress: bool = False) -> pd.DataFrame:
    """Fetch ACS tract data for all US states and territories.

    Parameters
    ----------
    year : int
        The ACS 5-year estimate end year.
    show_progress : bool
        If True, display a progress bar. Default is False.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with tract data for all states.
    """
    import click

    # State FIPS codes (50 states + DC + territories)
    state_fips_codes = [
        "01",
        "02",
        "04",
        "05",
        "06",
        "08",
        "09",
        "10",
        "11",
        "12",
        "13",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
        "25",
        "26",
        "27",
        "28",
        "29",
        "30",
        "31",
        "32",
        "33",
        "34",
        "35",
        "36",
        "37",
        "38",
        "39",
        "40",
        "41",
        "42",
        "44",
        "45",
        "46",
        "47",
        "48",
        "49",
        "50",
        "51",
        "53",
        "54",
        "55",
        "56",
        "72",  # Puerto Rico
    ]

    dfs = []
    states_iter = state_fips_codes
    if show_progress:
        states_iter = click.progressbar(
            state_fips_codes,
            label="Fetching ACS tract data",
            show_pos=True,
        )

    with states_iter if show_progress else nullcontext(states_iter) as fips_codes:
        for fips in fips_codes:
            try:
                df = fetch_acs_tract_data(year, fips)
                dfs.append(df)
            except httpx.HTTPStatusError as e:
                print(f"Warning: Failed to fetch data for state {fips}: {e}")
                continue

    if not dfs:
        raise ValueError("No tract data could be fetched")

    return pd.concat(dfs, ignore_index=True)


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


def aggregate_to_coc(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    weighting: Literal["area", "population"] = "area",
) -> pd.DataFrame:
    """Aggregate tract-level ACS data to CoC level using crosswalk.

    Parameters
    ----------
    acs_data : pd.DataFrame
        Tract-level ACS data with GEOID column.
    crosswalk : pd.DataFrame
        Tract-to-CoC crosswalk with tract_geoid, coc_id, area_share, pop_share.
    weighting : {"area", "population"}
        Weighting method for median value aggregation. For count variables
        (population, poverty counts), area_share is always used to compute
        actual totals. For median variables (income, rent), this parameter
        controls whether medians are weighted by area overlap alone ("area")
        or by population in overlapping areas ("population").

    Returns
    -------
    pd.DataFrame
        CoC-level aggregated measures.

    Notes
    -----
    Count variables (total_population, adult_population, etc.) always use
    area_share weighting to produce actual population totals. Using pop_share
    for counts would produce weighted averages instead of sums, since pop_share
    is normalized to sum to 1.0 per CoC.

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

    # Columns to aggregate (sum with weighting)
    sum_cols = [
        "total_population",
        "adult_population",
        "population_below_poverty",
        "poverty_universe",
    ]

    # Columns to aggregate as weighted average
    avg_cols = [
        "median_household_income",
        "median_gross_rent",
    ]

    # Apply weights and aggregate
    results = []
    for coc_id, group in merged.groupby("coc_id"):
        row = {"coc_id": coc_id}

        # Weighted sums for population counts - ALWAYS use area_share
        # This computes actual population totals (sum of tract_pop * area_share)
        # Using pop_share here would give weighted averages, not totals
        for col in sum_cols:
            if col in group.columns:
                weighted = group[col].fillna(0) * group["area_share"].fillna(0)
                row[col] = weighted.sum()

        # Weighted averages for median values
        # Use the specified weighting method (area or population)
        pop_weights = group["total_population"].fillna(0) * group[median_weight_col].fillna(0)

        for col in avg_cols:
            if col in group.columns:
                valid_mask = group[col].notna() & (pop_weights > 0)
                if valid_mask.any():
                    weighted_sum = (group.loc[valid_mask, col] * pop_weights[valid_mask]).sum()
                    row[col] = weighted_sum / pop_weights[valid_mask].sum()
                else:
                    row[col] = pd.NA

        # Coverage ratio: fraction of CoC area covered by tracts with ACS data
        # Computed as sum(intersection_area with data) / sum(intersection_area total)
        if "intersection_area" in group.columns:
            total_area = group["intersection_area"].sum()
            has_data = group["total_population"].notna()
            covered_area = group.loc[has_data, "intersection_area"].sum()
            row["coverage_ratio"] = covered_area / total_area if total_area > 0 else 0.0
        else:
            # Fallback: fraction of tracts with data (less accurate)
            has_data = group["total_population"].notna()
            row["coverage_ratio"] = has_data.mean()

        results.append(row)

    result_df = pd.DataFrame(results)

    # Add metadata columns
    result_df["weighting_method"] = weighting
    result_df["source"] = "acs_5yr"

    return result_df


def build_coc_measures(
    boundary_vintage: str,
    acs_vintage: str | int,
    crosswalk_path: Path,
    weighting: Literal["area", "population"] = "area",
    output_dir: Path | None = None,
    show_progress: bool = False,
) -> pd.DataFrame:
    """Build CoC-level measures from ACS data.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2024").
    acs_vintage : str or int
        ACS 5-year estimate vintage. Can be a range string (e.g., "2019-2023")
        or end year integer (e.g., 2023).
    crosswalk_path : Path
        Path to tract-CoC crosswalk parquet file.
    weighting : {"area", "population"}
        Weighting method for aggregation.
    output_dir : Path, optional
        Output directory for parquet file.
    show_progress : bool
        If True, display a progress bar during ACS fetching. Default is False.

    Returns
    -------
    pd.DataFrame
        CoC-level measures.
    """
    # Normalize acs_vintage to string and extract API year
    acs_vintage_str = str(acs_vintage)
    if "-" in acs_vintage_str:
        # Extract end year from range like "2019-2023"
        api_year = int(acs_vintage_str.split("-")[1])
    else:
        api_year = int(acs_vintage_str)

    # Load crosswalk
    crosswalk = pd.read_parquet(crosswalk_path)

    # Fetch ACS data for all states
    if not show_progress:
        print(f"Fetching ACS {acs_vintage_str} 5-year estimates...")
    acs_data = fetch_all_states_tract_data(api_year, show_progress=show_progress)
    acs_data = _maybe_remap_ct_planning_regions(acs_data, crosswalk, acs_vintage_str)

    # Aggregate to CoC level
    print(f"Aggregating to CoC level using {weighting} weighting...")
    coc_measures = aggregate_to_coc(acs_data, crosswalk, weighting=weighting)

    # Add vintage columns
    coc_measures["boundary_vintage"] = boundary_vintage
    coc_measures["acs_vintage"] = acs_vintage_str

    # Reorder columns
    col_order = [
        "coc_id",
        "boundary_vintage",
        "acs_vintage",
        "weighting_method",
        "total_population",
        "adult_population",
        "population_below_poverty",
        "median_household_income",
        "median_gross_rent",
        "coverage_ratio",
        "source",
    ]
    col_order = [c for c in col_order if c in coc_measures.columns]
    coc_measures = coc_measures[col_order]

    # Save to parquet if output_dir specified
    if output_dir is not None:
        from coclab.naming import measures_filename

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Extract tract_vintage from crosswalk if available
        tract_vintage = None
        if "tract_vintage" in crosswalk.columns:
            tract_vintage = str(crosswalk["tract_vintage"].iloc[0])

        filename = measures_filename(acs_vintage_str, boundary_vintage, tract_vintage)
        output_path = output_dir / filename

        # Build provenance block
        provenance = ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            tract_vintage=tract_vintage,
            acs_vintage=acs_vintage_str,
            weighting=weighting,
            extra={
                "dataset_type": "coc_measures",
                "crosswalk_path": str(crosswalk_path),
            },
        )

        write_parquet_with_provenance(coc_measures, output_path, provenance)
        print(f"Saved to {output_path}")

    return coc_measures
