"""ACS measure builder for CoC-level statistics."""

from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

CENSUS_API = "https://api.census.gov/data/{year}/acs/acs5"

# ACS variable mappings
ACS_VARS = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B25064_001E": "median_gross_rent",
    "C17002_001E": "poverty_universe",
    "C17002_002E": "below_50pct_poverty",
    "C17002_003E": "50_to_99pct_poverty",
    "B01001_001E": "total_pop_by_age",
    "B09021_001E": "adult_population",
}


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
    variables = ",".join(ACS_VARS.keys())

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

    # Rename ACS variables to friendly names
    df = df.rename(columns=ACS_VARS)

    # Convert numeric columns
    numeric_cols = list(ACS_VARS.values())
    for col in numeric_cols:
        if col in df.columns:
            # Handle negative values (Census uses -666666666 for missing)
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < 0, col] = pd.NA

    # Calculate derived measures
    if "below_50pct_poverty" in df.columns and "50_to_99pct_poverty" in df.columns:
        df["population_below_poverty"] = (
            df["below_50pct_poverty"].fillna(0) + df["50_to_99pct_poverty"].fillna(0)
        )

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


def fetch_all_states_tract_data(year: int) -> pd.DataFrame:
    """Fetch ACS tract data for all US states and territories.

    Parameters
    ----------
    year : int
        The ACS 5-year estimate end year.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with tract data for all states.
    """
    # State FIPS codes (50 states + DC + territories)
    state_fips_codes = [
        "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
        "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
        "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
        "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56", "72",  # Puerto Rico
    ]

    dfs = []
    for fips in state_fips_codes:
        try:
            df = fetch_acs_tract_data(year, fips)
            dfs.append(df)
        except httpx.HTTPStatusError as e:
            print(f"Warning: Failed to fetch data for state {fips}: {e}")
            continue

    if not dfs:
        raise ValueError("No tract data could be fetched")

    return pd.concat(dfs, ignore_index=True)


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
        Weighting method for aggregation.

    Returns
    -------
    pd.DataFrame
        CoC-level aggregated measures.
    """
    # Determine weight column
    weight_col = "area_share" if weighting == "area" else "pop_share"

    if weight_col not in crosswalk.columns:
        raise ValueError(f"Crosswalk missing required column: {weight_col}")

    # Standardize GEOID column names
    xwalk = crosswalk.copy()
    if "tract_geoid" in xwalk.columns:
        xwalk = xwalk.rename(columns={"tract_geoid": "GEOID"})

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

        # Weighted sums for population counts
        for col in sum_cols:
            if col in group.columns:
                weighted = group[col].fillna(0) * group[weight_col].fillna(0)
                row[col] = weighted.sum()

        # Weighted averages for median values (weight by population)
        pop_weights = group["total_population"].fillna(0) * group[weight_col].fillna(0)
        total_weight = pop_weights.sum()

        for col in avg_cols:
            if col in group.columns:
                valid_mask = group[col].notna() & (pop_weights > 0)
                if valid_mask.any():
                    weighted_sum = (group.loc[valid_mask, col] * pop_weights[valid_mask]).sum()
                    row[col] = weighted_sum / pop_weights[valid_mask].sum()
                else:
                    row[col] = pd.NA

        # Coverage ratio: sum of weights for tracts with data
        has_data = group["total_population"].notna()
        row["coverage_ratio"] = group.loc[has_data, weight_col].sum()

        results.append(row)

    result_df = pd.DataFrame(results)

    # Add metadata columns
    result_df["weighting_method"] = weighting
    result_df["source"] = "acs_5yr"

    return result_df


def build_coc_measures(
    boundary_vintage: str,
    acs_vintage: int,
    crosswalk_path: Path,
    weighting: Literal["area", "population"] = "area",
    output_dir: Path | None = None,
) -> pd.DataFrame:
    """Build CoC-level measures from ACS data.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2024").
    acs_vintage : int
        ACS 5-year estimate end year.
    crosswalk_path : Path
        Path to tract-CoC crosswalk parquet file.
    weighting : {"area", "population"}
        Weighting method for aggregation.
    output_dir : Path, optional
        Output directory for parquet file.

    Returns
    -------
    pd.DataFrame
        CoC-level measures.
    """
    # Load crosswalk
    crosswalk = pd.read_parquet(crosswalk_path)

    # Fetch ACS data for all states
    print(f"Fetching ACS {acs_vintage} 5-year estimates...")
    acs_data = fetch_all_states_tract_data(acs_vintage)

    # Aggregate to CoC level
    print(f"Aggregating to CoC level using {weighting} weighting...")
    coc_measures = aggregate_to_coc(acs_data, crosswalk, weighting=weighting)

    # Add vintage columns
    coc_measures["boundary_vintage"] = boundary_vintage
    coc_measures["acs_vintage"] = acs_vintage

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
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"coc_measures__{boundary_vintage}__{acs_vintage}.parquet"
        output_path = output_dir / filename
        coc_measures.to_parquet(output_path, index=False)
        print(f"Saved to {output_path}")

    return coc_measures
