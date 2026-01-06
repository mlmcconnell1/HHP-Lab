"""Population rollup engine: aggregate tract population to CoC level.

This module aggregates tract-level population data to CoC boundaries using
existing crosswalks. The rollup uses area-weighted aggregation to estimate
CoC populations from tract data.

Aggregation Algorithm
---------------------

The core aggregation formula is:

    coc_pop = sum(area_share_i * tract_pop_i)

where:
- area_share_i: fraction of tract i that falls within the CoC
- tract_pop_i: total population of tract i

This approach assumes population is uniformly distributed within each tract.
For tracts that partially overlap a CoC boundary, we attribute a proportional
share of the tract's population to the CoC based on area overlap.

Weighting Methods
-----------------

- **area**: Uses area_share weights directly. This is the default and assumes
  uniform population distribution within tracts.

- **population_mass**: Uses the same area_share formula but records the weighting
  method as "population_mass" to indicate the analyst's interpretation that
  population is concentrated (vs uniformly distributed). The computation is
  identical; only the metadata label differs.

Quality Metrics
---------------

- **coverage_ratio**: Fraction of CoC area covered by tracts with population data.
  Should be close to 1.0 if all tracts have ACS data. Values significantly below 1.0
  indicate missing tract population data.

- **max_tract_contribution**: Maximum (area_share * tract_pop) from any single
  tract. High values indicate sensitivity to individual tract estimates.

- **tract_count**: Number of tracts contributing to the CoC estimate.

Usage
-----
    from coclab.acs import build_coc_population_rollup

    path = build_coc_population_rollup(
        boundary_vintage="2025",
        acs_vintage="2019-2023",
        tract_vintage="2023",
        weighting="area"
    )

Output Schema
-------------
- coc_id (str): CoC identifier
- boundary_vintage (str): CoC boundary version
- acs_vintage (str): ACS 5-year estimate period
- tract_vintage (str): Census tract geography version
- weighting_method (str): "area" or "population_mass"
- coc_population (float): aggregated population estimate
- coverage_ratio (float): fraction of CoC area with population data (~1.0)
- max_tract_contribution (float): maximum single tract contribution
- tract_count (int): number of contributing tracts
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

logger = logging.getLogger(__name__)

# Default data directories
DEFAULT_ACS_DIR = Path("data/curated/acs")
DEFAULT_XWALK_DIR = Path("data/curated/xwalks")


def rollup_tract_population(
    tract_pop_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    weighting: Literal["area", "population_mass"] = "area",
) -> pd.DataFrame:
    """Aggregate tract-level population to CoC level using crosswalk.

    Parameters
    ----------
    tract_pop_df : pd.DataFrame
        Tract-level population data with columns:
        - tract_geoid (str): 11-character Census tract GEOID
        - total_population (int/float): population count
    crosswalk_df : pd.DataFrame
        Tract-to-CoC crosswalk with columns:
        - coc_id (str): CoC identifier
        - tract_geoid (str): Census tract GEOID
        - area_share (float): fraction of tract within CoC (0-1)
        - intersection_area (float): area of tract-CoC intersection
    weighting : {"area", "population_mass"}
        Weighting method label. Both use the same area_share formula;
        the label indicates the analyst's interpretation of how population
        is distributed within tracts. Default is "area".

    Returns
    -------
    pd.DataFrame
        CoC-level population estimates with columns:
        - coc_id (str)
        - weighting_method (str)
        - coc_population (float)
        - coverage_ratio (float)
        - max_tract_contribution (float)
        - tract_count (int)

    Raises
    ------
    ValueError
        If required columns are missing from input DataFrames.

    Examples
    --------
    >>> tract_pop = pd.DataFrame({
    ...     "tract_geoid": ["08031001000", "08031001100"],
    ...     "total_population": [5000, 3000]
    ... })
    >>> xwalk = pd.DataFrame({
    ...     "coc_id": ["CO-500", "CO-500"],
    ...     "tract_geoid": ["08031001000", "08031001100"],
    ...     "area_share": [1.0, 0.5]
    ... })
    >>> result = rollup_tract_population(tract_pop, xwalk)
    >>> result["coc_population"].iloc[0]
    6500.0
    """
    # Validate weighting parameter
    if weighting not in ("area", "population_mass"):
        raise ValueError(
            f"weighting must be 'area' or 'population_mass', got {weighting!r}"
        )

    # Validate required columns in tract population data
    tract_required = {"tract_geoid", "total_population"}
    tract_missing = tract_required - set(tract_pop_df.columns)
    if tract_missing:
        raise ValueError(
            f"tract_pop_df missing required columns: {tract_missing}"
        )

    # Validate required columns in crosswalk
    xwalk_required = {"coc_id", "tract_geoid", "area_share", "intersection_area"}
    xwalk_missing = xwalk_required - set(crosswalk_df.columns)
    if xwalk_missing:
        raise ValueError(
            f"crosswalk_df missing required columns: {xwalk_missing}"
        )

    # Compute coc_share: fraction of CoC's total area from each tract
    # This is used for coverage_ratio (should sum to ~1.0 per CoC)
    crosswalk_df = crosswalk_df.copy()
    coc_total_area = crosswalk_df.groupby("coc_id")["intersection_area"].transform("sum")
    crosswalk_df["coc_share"] = crosswalk_df["intersection_area"] / coc_total_area

    # Merge crosswalk with tract population
    merged = crosswalk_df.merge(
        tract_pop_df[["tract_geoid", "total_population"]],
        on="tract_geoid",
        how="left",
    )

    # Compute weighted population for each tract-CoC pair
    # weighted_pop = area_share * tract_population
    # Explicitly convert to float to avoid FutureWarning with nullable Int64
    area_share_values = pd.to_numeric(merged["area_share"], errors="coerce").fillna(0.0)
    total_pop_values = pd.to_numeric(merged["total_population"], errors="coerce").fillna(0.0)
    merged["weighted_pop"] = area_share_values * total_pop_values

    # Aggregate by CoC
    results = []
    for coc_id, group in merged.groupby("coc_id"):
        # Filter to tracts with valid population data
        has_pop = group["total_population"].notna()

        # coc_population = sum(area_share * tract_pop)
        coc_population = group["weighted_pop"].sum()

        # coverage_ratio = sum(coc_share) for tracts with population data
        # coc_share is fraction of CoC area from each tract, so sum should be ~1.0
        coverage_ratio = group.loc[has_pop, "coc_share"].sum()

        # max_tract_contribution = max(area_share * tract_pop)
        max_contribution = group["weighted_pop"].max()

        # tract_count = number of tracts with non-zero contribution
        tract_count = (group["weighted_pop"] > 0).sum()

        results.append({
            "coc_id": coc_id,
            "weighting_method": weighting,
            "coc_population": coc_population,
            "coverage_ratio": coverage_ratio,
            "max_tract_contribution": max_contribution,
            "tract_count": int(tract_count),
        })

    result_df = pd.DataFrame(results)

    # Ensure proper column types
    if len(result_df) > 0:
        result_df["coc_id"] = result_df["coc_id"].astype(str)
        result_df["weighting_method"] = result_df["weighting_method"].astype(str)
        result_df["coc_population"] = result_df["coc_population"].astype(float)
        result_df["coverage_ratio"] = result_df["coverage_ratio"].astype(float)
        result_df["max_tract_contribution"] = result_df["max_tract_contribution"].astype(float)
        result_df["tract_count"] = result_df["tract_count"].astype(int)

    return result_df


def get_tract_population_path(
    acs_vintage: str,
    tract_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the path to cached tract population data.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    base_dir : Path or str, optional
        Base directory for data. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Path to tract population parquet file.
    """
    if base_dir is None:
        base_dir = DEFAULT_ACS_DIR
    else:
        base_dir = Path(base_dir)
    return base_dir / f"tract_population__{acs_vintage}__{tract_vintage}.parquet"


def get_crosswalk_path(
    boundary_vintage: str,
    tract_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the path to tract-CoC crosswalk.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    base_dir : Path or str, optional
        Base directory for crosswalks. Defaults to 'data/curated/xwalks'.

    Returns
    -------
    Path
        Path to crosswalk parquet file.
    """
    if base_dir is None:
        base_dir = DEFAULT_XWALK_DIR
    else:
        base_dir = Path(base_dir)
    return base_dir / f"coc_tract_xwalk__{boundary_vintage}__{tract_vintage}.parquet"


def get_output_path(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the canonical output path for CoC population rollup.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    weighting : str
        Weighting method ("area" or "population_mass").
    base_dir : Path or str, optional
        Base directory for output. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Output path like
        'data/curated/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet'.
    """
    if base_dir is None:
        base_dir = DEFAULT_ACS_DIR
    else:
        base_dir = Path(base_dir)
    filename = (
        f"coc_population_rollup__{boundary_vintage}__{acs_vintage}"
        f"__{tract_vintage}__{weighting}.parquet"
    )
    return base_dir / filename


def build_coc_population_rollup(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: Literal["area", "population_mass"] = "area",
    force: bool = False,
    acs_dir: Path | str | None = None,
    xwalk_dir: Path | str | None = None,
    output_dir: Path | str | None = None,
) -> Path:
    """Build CoC population rollup from tract population and crosswalk.

    Reads tract population data and crosswalk files, aggregates population
    to CoC level using area weighting, and writes output to parquet with
    provenance metadata.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    weighting : {"area", "population_mass"}
        Weighting method. Default is "area".
    force : bool
        If True, rebuild even if output file exists. Default is False.
    acs_dir : Path or str, optional
        Directory containing tract population data. Defaults to 'data/curated/acs'.
    xwalk_dir : Path or str, optional
        Directory containing crosswalk data. Defaults to 'data/curated/xwalks'.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Path to output parquet file.

    Raises
    ------
    FileNotFoundError
        If tract population or crosswalk file is not found.
    ValueError
        If input data is missing required columns.

    Examples
    --------
    >>> path = build_coc_population_rollup(
    ...     boundary_vintage="2025",
    ...     acs_vintage="2019-2023",
    ...     tract_vintage="2023",
    ...     weighting="area"
    ... )
    >>> # Output: data/curated/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet
    """
    # Resolve directories
    acs_dir = Path(acs_dir) if acs_dir else DEFAULT_ACS_DIR
    xwalk_dir = Path(xwalk_dir) if xwalk_dir else DEFAULT_XWALK_DIR
    output_dir = Path(output_dir) if output_dir else DEFAULT_ACS_DIR

    # Get output path
    output_path = get_output_path(
        boundary_vintage, acs_vintage, tract_vintage, weighting, output_dir
    )

    # Check for cached output
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Get input paths
    tract_pop_path = get_tract_population_path(acs_vintage, tract_vintage, acs_dir)
    xwalk_path = get_crosswalk_path(boundary_vintage, tract_vintage, xwalk_dir)

    # Validate inputs exist
    if not tract_pop_path.exists():
        raise FileNotFoundError(
            f"Tract population file not found: {tract_pop_path}. "
            f"Run ingest_tract_population('{acs_vintage}', '{tract_vintage}') first."
        )
    if not xwalk_path.exists():
        raise FileNotFoundError(
            f"Crosswalk file not found: {xwalk_path}. "
            f"Build crosswalk for boundary_vintage={boundary_vintage}, "
            f"tract_vintage={tract_vintage} first."
        )

    logger.info(f"Loading tract population from {tract_pop_path}")
    tract_pop_df = pd.read_parquet(tract_pop_path)

    logger.info(f"Loading crosswalk from {xwalk_path}")
    crosswalk_df = pd.read_parquet(xwalk_path)

    # Perform rollup
    logger.info(f"Aggregating tract population to CoC using {weighting} weighting")
    result_df = rollup_tract_population(tract_pop_df, crosswalk_df, weighting)

    # Add vintage columns
    result_df["boundary_vintage"] = boundary_vintage
    result_df["acs_vintage"] = acs_vintage
    result_df["tract_vintage"] = tract_vintage

    # Reorder columns to match schema
    col_order = [
        "coc_id",
        "boundary_vintage",
        "acs_vintage",
        "tract_vintage",
        "weighting_method",
        "coc_population",
        "coverage_ratio",
        "max_tract_contribution",
        "tract_count",
    ]
    result_df = result_df[col_order]

    # Build provenance metadata
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        tract_vintage=tract_vintage,
        acs_vintage=acs_vintage,
        weighting=weighting,
        extra={
            "dataset": "coc_population_rollup",
            "source_tract_pop": str(tract_pop_path),
            "source_crosswalk": str(xwalk_path),
            "coc_count": len(result_df),
            "total_population": float(result_df["coc_population"].sum()),
        },
    )

    # Write output
    write_parquet_with_provenance(result_df, output_path, provenance)
    logger.info(f"Wrote CoC population rollup to {output_path}")

    return output_path
