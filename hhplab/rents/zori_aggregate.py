"""ZORI aggregation from county to CoC geography.

This module implements Agent C from the ZORI spec: Aggregation Engine (county->CoC v1).

It aggregates county-level ZORI (Zillow Observed Rent Index) data to CoC
(Continuum of Care) geography using area-weighted crosswalks and ACS-based
demographic weights.

Aggregation Formula (per spec section 5.2):
-------------------------------------------
For each CoC i and month t:
- A_it = set of counties with ZORI available at month t
- w_ij = weight of county j in CoC i (crosswalk area_share * ACS weight)
- coverage_ratio_it = sum_{j in A_it} w_ij
- zori_coc_it = sum_{j in A_it} (w_ij / coverage_ratio_it) * zori_jt

If coverage_ratio < min_threshold (default 0.90), set zori_coc = null.

Output Schema (per spec section 4.2):
------------------------------------
- coc_id: CoC identifier
- date: month start date
- zori_coc: aggregated ZORI value (null if coverage < threshold)
- base_geo_type: "county"
- boundary_vintage: CoC boundary vintage
- base_geo_vintage: county vintage year
- acs_vintage: ACS 5-year estimate vintage
- weighting_method: weight method used (renter_households, housing_units, etc.)
- coverage_ratio: share of CoC weight mass with available ZORI
- max_geo_contribution: dominance of largest contributor
- geo_count: number of counties contributing
- provenance: JSON string with full lineage

Usage
-----
    from hhplab.rents.zori_aggregate import aggregate_zori_to_coc

    # Aggregate ZORI to CoC
    output_path = aggregate_zori_to_coc(
        boundary="2025",
        counties="2023",
        acs_vintage="2019-2023",
        weighting="renter_households",
    )

    # Or use the lower-level function
    from hhplab.rents.zori_aggregate import aggregate_monthly
    coc_zori_df = aggregate_monthly(zori_df, xwalk_df, weights_df, min_coverage=0.90)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Literal

import pandas as pd

from hhplab.geo.ct_planning_regions import (
    CT_LEGACY_COUNTY_VINTAGE,
    CT_PLANNING_REGION_VINTAGE,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
    translate_weights_planning_to_legacy,
    translate_zori_legacy_to_planning,
)
from hhplab.naming import county_xwalk_path, discover_zori_ingest
from hhplab.paths import curated_dir
from hhplab.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)
from hhplab.rents.weights import (
    WeightingMethod,
    build_county_weights,
    get_county_weights_path,
)
from hhplab.rents.zori_ingest import ZILLOW_ATTRIBUTION

logger = logging.getLogger(__name__)


# Default minimum coverage threshold (per spec section 5.2).
# ZORI uses a lower threshold than PEP (0.90 vs 0.95) because Zillow
# county-level rent data has more missing coverage -- many rural counties
# lack sufficient rental listings for a ZORI estimate.
DEFAULT_MIN_COVERAGE = 0.90

# Yearly collapse methods
YearlyMethod = Literal["pit_january", "calendar_mean", "calendar_median"]


def _align_ct_geographies(
    zori_df: pd.DataFrame,
    xwalk_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    county_vintage: str | int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Align CT planning-region vs legacy county codes for ZORI aggregation."""
    ct_xwalk_legacy = xwalk_df["county_fips"].apply(is_ct_legacy_county_fips).any()
    ct_xwalk_planning = xwalk_df["county_fips"].apply(is_ct_planning_region_fips).any()
    ct_weights_planning = weights_df["county_fips"].apply(is_ct_planning_region_fips).any()
    ct_zori_legacy = zori_df["geo_id"].apply(is_ct_legacy_county_fips).any()

    if ct_xwalk_legacy and ct_weights_planning:
        try:
            crosswalk = build_ct_county_planning_region_crosswalk(
                legacy_county_vintage=county_vintage,
                planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
            )
            weights_df = translate_weights_planning_to_legacy(weights_df, crosswalk)
            logger.info("Translated CT planning-region ACS weights to legacy counties")
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                "CT county-weight translation skipped (planning->legacy). %s",
                exc,
            )

    if ct_xwalk_planning and ct_zori_legacy:
        try:
            crosswalk = build_ct_county_planning_region_crosswalk(
                legacy_county_vintage=CT_LEGACY_COUNTY_VINTAGE,
                planning_region_vintage=county_vintage,
            )
            zori_df = translate_zori_legacy_to_planning(zori_df, crosswalk)
            logger.info("Translated CT legacy ZORI counties to planning regions")
        except (FileNotFoundError, ValueError) as exc:
            logger.warning(
                "CT ZORI translation skipped (legacy->planning). %s",
                exc,
            )

    return zori_df, weights_df


# =============================================================================
# Load Functions
# =============================================================================


def load_zori(
    geography: str = "county",
    zori_path: Path | str | None = None,
    output_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Load normalized ZORI data from parquet file.

    Parameters
    ----------
    geography : str
        Geography level ("county" or "zip").
    zori_path : Path or str, optional
        Explicit path to ZORI parquet file. If None, uses default path.
    output_dir : Path or str, optional
        Base directory for ZORI data. Checks this directory first, then
        falls back to global 'data/curated/zori' if not found.

    Returns
    -------
    pd.DataFrame
        ZORI DataFrame with columns: geo_type, geo_id, date, zori, ...

    Raises
    ------
    FileNotFoundError
        If ZORI file does not exist in either location.
    """
    if zori_path is not None:
        path = Path(zori_path)
    else:
        path = discover_zori_ingest(geography, output_dir)
        # Fall back to global curated directory if build-local path doesn't exist
        if path is None and output_dir is not None:
            path = discover_zori_ingest(geography, curated_dir("zori"))
            if path is not None:
                logger.info(f"ZORI not found in {output_dir}, using global {path}")

    if path is None or not path.exists():
        raise FileNotFoundError(
            f"ZORI data file not found. Run 'hhplab ingest zori --geography {geography}' first."
        )

    logger.info(f"Loading ZORI data from {path}")
    df = pd.read_parquet(path)
    logger.info(f"Loaded {len(df)} ZORI records for {df['geo_id'].nunique()} geographies")
    return df


def get_xwalk_path(
    boundary: str,
    counties: str,
    xwalk_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for CoC-county crosswalk.

    Parameters
    ----------
    boundary : str
        CoC boundary vintage (e.g., "2025").
    counties : str
        County vintage year (e.g., "2023").
    xwalk_dir : Path or str, optional
        Base directory for crosswalks. Defaults to 'data/curated/xwalks'.

    Returns
    -------
    Path
        Path like 'data/curated/xwalks/xwalk__B2025xC2023.parquet'.
    """
    # Use the canonical naming function from hhplab.naming
    # county_xwalk_path expects base_dir to be the data root (e.g., "data")
    # so we need to extract it from xwalk_dir if provided
    if xwalk_dir is None:
        return county_xwalk_path(boundary, counties)
    else:
        # xwalk_dir is like "data/curated/xwalks", we need "data"
        xwalk_dir = Path(xwalk_dir)
        # Go up two levels: xwalks -> curated -> data
        base_dir = xwalk_dir.parent.parent
        return county_xwalk_path(boundary, counties, base_dir)


def load_crosswalk(
    boundary: str,
    counties: str,
    xwalk_path: Path | str | None = None,
    xwalk_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Load CoC-county crosswalk from parquet file.

    Parameters
    ----------
    boundary : str
        CoC boundary vintage (e.g., "2025").
    counties : str
        County vintage year (e.g., "2023").
    xwalk_path : Path or str, optional
        Explicit path to crosswalk parquet file. If None, uses default path.
    xwalk_dir : Path or str, optional
        Base directory for crosswalks. Defaults to 'data/curated/xwalks'.

    Returns
    -------
    pd.DataFrame
        Crosswalk DataFrame with columns: coc_id, boundary_vintage, county_fips, area_share

    Raises
    ------
    FileNotFoundError
        If crosswalk file does not exist.
    """
    if xwalk_path is not None:
        path = Path(xwalk_path)
    else:
        path = get_xwalk_path(boundary, counties, xwalk_dir)

    if not path.exists():
        raise FileNotFoundError(
            f"CoC-county crosswalk not found: {path}. "
            f"Run 'hhplab generate xwalks --boundary {boundary} --counties {counties}' first."
        )

    logger.info(f"Loading crosswalk from {path}")
    df = pd.read_parquet(path)
    coc_count = df["coc_id"].nunique()
    logger.info(f"Loaded crosswalk with {len(df)} CoC-county pairs for {coc_count} CoCs")
    return df


def load_weights(
    acs_vintage: str,
    weighting: WeightingMethod,
    weights_dir: Path | str | None = None,
    force_build: bool = False,
) -> pd.DataFrame:
    """Load or build county weights from ACS data.

    Parameters
    ----------
    acs_vintage : str
        ACS 5-year vintage (e.g., "2019-2023").
    weighting : str
        Weighting method: "renter_households", "housing_units", or "population".
    weights_dir : Path or str, optional
        Base directory for weights data. Defaults to 'data/curated/acs'.
    force_build : bool
        If True, rebuild weights even if cached.

    Returns
    -------
    pd.DataFrame
        Weights DataFrame with columns: county_fips, weight_value, ...
    """
    if weights_dir is None:
        weights_dir = curated_dir("acs")
    else:
        weights_dir = Path(weights_dir)

    weights_path = get_county_weights_path(acs_vintage, weighting, weights_dir)

    if weights_path.exists() and not force_build:
        logger.info(f"Loading cached weights from {weights_path}")
        return pd.read_parquet(weights_path)

    logger.info(f"Building county weights for ACS {acs_vintage} using {weighting}")
    return build_county_weights(acs_vintage, weighting, force=force_build, output_dir=weights_dir)


# =============================================================================
# Weight Computation
# =============================================================================


def compute_geo_county_weights(
    xwalk_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compute geography-county weights combining crosswalk area shares with ACS weights.

    The combined weight w_ij for county j in geography unit i is:
        w_ij = area_share_ij * weight_value_j / sum_k(area_share_ik * weight_value_k)

    This produces weights that sum to 1 per geography unit.

    Parameters
    ----------
    xwalk_df : pd.DataFrame
        Crosswalk with columns: ``geo_id_col``, county_fips, area_share
    weights_df : pd.DataFrame
        County weights with columns: county_fips, weight_value
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Combined weights with columns: ``geo_id_col``, county_fips, weight
        where weights sum to 1 per geography unit.
    """
    # Merge crosswalk with ACS weights
    merged = xwalk_df.merge(
        weights_df[["county_fips", "weight_value"]],
        on="county_fips",
        how="left",
    )

    # Handle missing weights (counties not in ACS data)
    missing_weights = merged["weight_value"].isna()
    if missing_weights.any():
        missing_count = missing_weights.sum()
        logger.warning(
            f"{missing_count} crosswalk entries have no ACS weight data; "
            f"these counties will be excluded from aggregation"
        )
        merged = merged[~missing_weights].copy()

    # Compute raw weighted contribution: area_share * weight_value
    merged["raw_weight"] = merged["area_share"] * merged["weight_value"]

    # Normalize to sum to 1 per geography unit
    geo_totals = merged.groupby(geo_id_col)["raw_weight"].sum().reset_index()
    geo_totals.columns = [geo_id_col, "geo_total_weight"]

    merged = merged.merge(geo_totals, on=geo_id_col)
    merged["weight"] = merged["raw_weight"] / merged["geo_total_weight"]

    # Select final columns
    result = merged[[geo_id_col, "county_fips", "weight", "area_share"]].copy()

    logger.info(
        f"Computed weights for {result[geo_id_col].nunique()} geography units "
        f"covering {result['county_fips'].nunique()} counties"
    )

    return result


def compute_coc_county_weights(
    xwalk_df: pd.DataFrame,
    weights_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute CoC-county weights.

    Convenience wrapper around :func:`compute_geo_county_weights` with
    ``geo_id_col="coc_id"``.  See that function for full documentation.
    """
    return compute_geo_county_weights(xwalk_df, weights_df, geo_id_col="coc_id")


# =============================================================================
# Monthly Aggregation
# =============================================================================


def aggregate_monthly(
    zori_df: pd.DataFrame,
    xwalk_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Aggregate county ZORI to geography level for each month.

    Implements the aggregation formula:
    - coverage_ratio_it = sum_{j in A_it} w_ij
    - zori_geo_it = sum_{j in A_it} (w_ij / coverage_ratio_it) * zori_jt

    Where A_it is the set of counties with ZORI available for geo unit i
    at month t.

    Parameters
    ----------
    zori_df : pd.DataFrame
        ZORI data with columns: geo_id (county FIPS), date, zori
    xwalk_df : pd.DataFrame
        County crosswalk with columns: ``geo_id_col``, county_fips, area_share
    weights_df : pd.DataFrame
        County weights with columns: county_fips, weight_value
    min_coverage : float
        Minimum coverage ratio threshold. Geo-months below this threshold
        will have zori_coc set to null. Default 0.90.
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Aggregated ZORI with columns:
        - ``geo_id_col``, date, zori_coc
        - coverage_ratio, max_geo_contribution, geo_count
    """
    # Compute combined geo-county weights
    geo_weights = compute_geo_county_weights(xwalk_df, weights_df, geo_id_col=geo_id_col)

    # Detect orphan ZORI counties absent from crosswalk
    zori_counties = set(zori_df["geo_id"].unique())
    xwalk_counties = set(xwalk_df["county_fips"].unique())
    orphan_counties = zori_counties - xwalk_counties
    if orphan_counties:
        logger.warning(
            f"{len(orphan_counties)} ZORI counties absent from crosswalk "
            f"(these will not contribute to any geography): "
            f"{sorted(orphan_counties)[:10]}"
            f"{'...' if len(orphan_counties) > 10 else ''}"
        )

    # Rename zori columns for merge
    zori = zori_df[["geo_id", "date", "zori"]].copy()
    zori = zori.rename(columns={"geo_id": "county_fips"})

    # Build geo set from the full crosswalk, not just geos with surviving weights.
    # Geos whose counties all lack ACS weights should appear with coverage=0.
    all_geos_from_xwalk = xwalk_df[geo_id_col].unique()
    all_geos_from_weights = geo_weights[geo_id_col].unique()
    all_geos = pd.unique(
        pd.concat([pd.Series(all_geos_from_xwalk), pd.Series(all_geos_from_weights)]).values
    )
    all_dates = zori["date"].unique()

    logger.info(f"Aggregating {len(all_dates)} months for {len(all_geos)} geography units")

    # Create full geo x date grid
    geo_date_grid = pd.DataFrame(
        {
            geo_id_col: list(all_geos) * len(all_dates),
            "date": [d for d in all_dates for _ in range(len(all_geos))],
        }
    )
    geo_date_grid = geo_date_grid.sort_values([geo_id_col, "date"]).reset_index(drop=True)

    # Merge weights with ZORI data to get available county-months
    merged = geo_weights.merge(zori, on="county_fips", how="inner")

    # Group by geo unit and date to compute aggregations
    results = []

    for (geo_id, date_val), group in merged.groupby([geo_id_col, "date"]):
        available_weights = group["weight"].sum()
        coverage_ratio = available_weights

        if coverage_ratio > 0:
            normalized_weights = group["weight"] / coverage_ratio
            zori_coc = (normalized_weights * group["zori"]).sum()
            max_contribution = normalized_weights.max()
            geo_count = len(group)
        else:
            zori_coc = None
            max_contribution = None
            geo_count = 0

        if coverage_ratio < min_coverage:
            zori_coc = None

        results.append(
            {
                geo_id_col: geo_id,
                "date": date_val,
                "zori_coc": zori_coc,
                "coverage_ratio": coverage_ratio,
                "max_geo_contribution": max_contribution,
                "geo_count": geo_count,
            }
        )

    result_df = pd.DataFrame(results)

    # Merge with full grid to include geo-months with zero coverage
    full_result = geo_date_grid.merge(
        result_df,
        on=[geo_id_col, "date"],
        how="left",
    )

    full_result["coverage_ratio"] = full_result["coverage_ratio"].fillna(0.0)
    full_result["geo_count"] = full_result["geo_count"].fillna(0).astype(int)

    full_result = full_result.sort_values([geo_id_col, "date"]).reset_index(drop=True)

    valid_count = full_result["zori_coc"].notna().sum()
    total_count = len(full_result)
    logger.info(
        f"Aggregation complete: {valid_count}/{total_count} geo-months "
        f"({100 * valid_count / total_count:.1f}%) have valid ZORI "
        f"(coverage >= {min_coverage})"
    )

    return full_result


# =============================================================================
# Yearly Collapse
# =============================================================================


def collapse_to_yearly(
    monthly_df: pd.DataFrame,
    method: YearlyMethod = "pit_january",
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Collapse monthly ZORI to yearly values.

    Parameters
    ----------
    monthly_df : pd.DataFrame
        Monthly ZORI with columns: ``geo_id_col``, date, zori_coc,
        coverage_ratio, ...
    method : str
        Yearly collapse method:
        - "pit_january": Select January value (aligns with PIT count timing)
        - "calendar_mean": Mean of all months in year
        - "calendar_median": Median of all months in year
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Yearly ZORI with columns:
        - ``geo_id_col``, year, zori_coc, coverage_ratio, method, ...
    """
    df = monthly_df.copy()
    df["year"] = df["date"].dt.year

    if method == "pit_january":
        january = df[df["date"].dt.month == 1].copy()
        result = january.drop(columns=["date"])

    elif method == "calendar_mean":
        agg_funcs = {
            "zori_coc": "mean",
            "coverage_ratio": "mean",
            "max_geo_contribution": "mean",
            "geo_count": "mean",
        }
        result = df.groupby([geo_id_col, "year"]).agg(agg_funcs).reset_index()
        result["geo_count"] = result["geo_count"].round().astype(int)

    elif method == "calendar_median":
        agg_funcs = {
            "zori_coc": "median",
            "coverage_ratio": "median",
            "max_geo_contribution": "median",
            "geo_count": "median",
        }
        result = df.groupby([geo_id_col, "year"]).agg(agg_funcs).reset_index()
        result["geo_count"] = result["geo_count"].round().astype(int)

    else:
        raise ValueError(
            f"Unknown yearly method: {method}. "
            f"Use 'pit_january', 'calendar_mean', or 'calendar_median'"
        )

    result["method"] = method
    result = result.sort_values([geo_id_col, "year"]).reset_index(drop=True)

    logger.info(f"Collapsed to yearly using '{method}': {len(result)} geo-year records")
    return result


# =============================================================================
# Output Path Generation
# =============================================================================


def get_coc_zori_path(
    geography: str,
    boundary: str,
    counties: str,
    acs_vintage: str,
    weighting: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Get canonical output path for CoC-level ZORI data.

    Parameters
    ----------
    geography : str
        Base geography type (e.g., "county").
    boundary : str
        CoC boundary vintage (e.g., "2025").
    counties : str
        County vintage year (e.g., "2023").
    acs_vintage : str
        ACS 5-year vintage (e.g., "2019-2023").
    weighting : str
        Weighting method (e.g., "renter_households").
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/zori'.

    Returns
    -------
    Path
        Output path like 'data/curated/zori/coc_zori__county__b2025__c2023__
        acs2019-2023__wrenter_households.parquet'

    Note:
        New format uses temporal shorthand: zori__A2023@B2025xC2023__wrenter.parquet
    """
    from hhplab.naming import zori_filename as _zori_filename

    if output_dir is None:
        output_dir = curated_dir("zori")
    else:
        output_dir = Path(output_dir)

    filename = _zori_filename(acs_vintage, boundary, counties, weighting)
    return output_dir / filename


def get_coc_zori_yearly_path(
    geography: str,
    boundary: str,
    counties: str,
    acs_vintage: str,
    weighting: str,
    yearly_method: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Get canonical output path for yearly CoC-level ZORI data.

    Parameters
    ----------
    geography : str
        Base geography type (e.g., "county").
    boundary : str
        CoC boundary vintage (e.g., "2025").
    counties : str
        County vintage year (e.g., "2023").
    acs_vintage : str
        ACS 5-year vintage (e.g., "2019-2023").
    weighting : str
        Weighting method (e.g., "renter_households").
    yearly_method : str
        Yearly collapse method (e.g., "pit_january").
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/zori'.

    Returns
    -------
    Path
        Output path like 'data/curated/zori/coc_zori_yearly__county__b2025__
        c2023__acs2019-2023__wrenter_households__mpit_january.parquet'

    Note:
        New format uses temporal shorthand:
        zori_yearly__A2023@B2025xC2023__wrenter__mpit_january.parquet
    """
    from hhplab.naming import zori_yearly_filename as _zori_yearly_filename

    if output_dir is None:
        output_dir = curated_dir("zori")
    else:
        output_dir = Path(output_dir)

    filename = _zori_yearly_filename(acs_vintage, boundary, counties, weighting, yearly_method)
    return output_dir / filename


# =============================================================================
# Main Aggregation Function
# =============================================================================


def aggregate_zori_to_coc(
    boundary: str,
    counties: str,
    acs_vintage: str,
    weighting: WeightingMethod = "renter_households",
    geography: str = "county",
    zori_path: Path | str | None = None,
    xwalk_path: Path | str | None = None,
    output_dir: Path | str | None = None,
    xwalk_dir: Path | str | None = None,
    weights_dir: Path | str | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    to_yearly: bool = False,
    yearly_method: YearlyMethod = "pit_january",
    force: bool = False,
) -> Path:
    """Aggregate ZORI data from county to CoC geography.

    This is the main orchestration function that:
    1. Loads ZORI data, crosswalk, and weights
    2. Computes monthly CoC-level ZORI with coverage metrics
    3. Optionally collapses to yearly values
    4. Writes output parquet with embedded provenance

    Parameters
    ----------
    boundary : str
        CoC boundary vintage (e.g., "2025").
    counties : str
        County vintage year used by the crosswalk (e.g., "2023").
    acs_vintage : str
        ACS 5-year vintage for weights (e.g., "2019-2023").
    weighting : str
        Weighting method: "renter_households", "housing_units", or "population".
        Default is "renter_households".
    geography : str
        Base geography type. Currently only "county" is supported.
    zori_path : Path or str, optional
        Explicit path to ZORI parquet file.
    xwalk_path : Path or str, optional
        Explicit path to crosswalk parquet file.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/zori'.
    xwalk_dir : Path or str, optional
        Directory for crosswalks. Defaults to 'data/curated/xwalks'.
    weights_dir : Path or str, optional
        Directory for ACS weights. Defaults to 'data/curated/acs'.
    min_coverage : float
        Minimum coverage ratio threshold. Default 0.90.
    to_yearly : bool
        If True, also produce yearly collapsed output.
    yearly_method : str
        Yearly collapse method: "pit_january", "calendar_mean", "calendar_median".
    force : bool
        If True, recompute even if output exists.

    Returns
    -------
    Path
        Path to output monthly parquet file.

    Raises
    ------
    FileNotFoundError
        If required input files (ZORI, crosswalk) do not exist.
    ValueError
        If weighting method is invalid or if weights cannot be computed.
    """
    # Validate geography
    if geography != "county":
        raise ValueError(
            f"Unsupported geography: {geography}. Only 'county' is currently supported."
        )

    # Determine output paths
    output_path = get_coc_zori_path(
        geography, boundary, counties, acs_vintage, weighting, output_dir
    )

    # Check if output already exists
    if output_path.exists() and not force:
        logger.info(f"Output already exists: {output_path}. Use --force to recompute.")
        return output_path

    logger.info(
        f"Aggregating ZORI to CoC: boundary={boundary}, counties={counties}, "
        f"acs={acs_vintage}, weighting={weighting}"
    )

    # Resolve actual ZORI path with fallback to global curated directory
    if zori_path is not None:
        zori_source_path = Path(zori_path)
    else:
        zori_source_path = discover_zori_ingest(geography, output_dir)
        if zori_source_path is None and output_dir is not None:
            zori_source_path = discover_zori_ingest(geography, curated_dir("zori"))
            if zori_source_path is not None:
                logger.info(f"ZORI not found in {output_dir}, using global {zori_source_path}")
        if zori_source_path is None:
            raise FileNotFoundError(
                f"ZORI data not found. Run 'hhplab ingest zori --geography {geography}' first."
            )

    # Load input data using resolved path
    zori_df = load_zori(geography, zori_path=zori_source_path)
    xwalk_df = load_crosswalk(boundary, counties, xwalk_path, xwalk_dir)
    weights_df = load_weights(acs_vintage, weighting, weights_dir)

    zori_df, weights_df = _align_ct_geographies(zori_df, xwalk_df, weights_df, counties)

    # Get provenance from source files for lineage tracking
    zori_provenance = read_provenance(zori_source_path)

    # Perform aggregation
    coc_zori_df = aggregate_monthly(zori_df, xwalk_df, weights_df, min_coverage)

    # Add metadata columns per spec section 4.2
    coc_zori_df["base_geo_type"] = geography
    coc_zori_df["boundary_vintage"] = boundary
    coc_zori_df["base_geo_vintage"] = counties
    coc_zori_df["acs_vintage"] = acs_vintage
    coc_zori_df["weighting_method"] = weighting

    # Build provenance JSON for each row
    base_provenance = {
        "metric": "ZORI",
        "source": "Zillow Economic Research",
        "attribution": ZILLOW_ATTRIBUTION,
        "boundary_vintage": boundary,
        "base_geo_type": geography,
        "base_geo_vintage": counties,
        "acs_vintage": acs_vintage,
        "weighting_method": weighting,
        "min_coverage_threshold": min_coverage,
    }

    # Add source file info if available
    if zori_provenance and zori_provenance.extra:
        base_provenance["zori_download_url"] = zori_provenance.extra.get("download_url")
        base_provenance["zori_raw_sha256"] = zori_provenance.extra.get("raw_sha256")

    coc_zori_df["provenance"] = json.dumps(base_provenance)

    # Reorder columns to match spec schema
    col_order = [
        "coc_id",
        "date",
        "zori_coc",
        "base_geo_type",
        "boundary_vintage",
        "base_geo_vintage",
        "acs_vintage",
        "weighting_method",
        "coverage_ratio",
        "max_geo_contribution",
        "geo_count",
        "provenance",
    ]
    coc_zori_df = coc_zori_df[col_order]

    # Build file-level provenance
    file_provenance = ProvenanceBlock(
        boundary_vintage=boundary,
        acs_vintage=acs_vintage,
        weighting=weighting,
        extra={
            "dataset": "coc_zori",
            "geography": geography,
            "base_geo_vintage": counties,
            "metric": "ZORI",
            "source": "Zillow Economic Research",
            "attribution": ZILLOW_ATTRIBUTION,
            "aggregation_method": "weighted_mean",
            "min_coverage_threshold": min_coverage,
            "coc_count": coc_zori_df["coc_id"].nunique(),
            "date_count": coc_zori_df["date"].nunique(),
            "valid_coc_month_count": int(coc_zori_df["zori_coc"].notna().sum()),
            "date_range": [
                coc_zori_df["date"].min().isoformat(),
                coc_zori_df["date"].max().isoformat(),
            ],
            "coverage_ratio_mean": float(coc_zori_df["coverage_ratio"].mean()),
            "coverage_ratio_min": float(coc_zori_df["coverage_ratio"].min()),
            "coverage_ratio_max": float(coc_zori_df["coverage_ratio"].max()),
        },
    )

    # Write output
    write_parquet_with_provenance(coc_zori_df, output_path, file_provenance)
    logger.info(f"Wrote CoC ZORI data to {output_path}")

    # Produce yearly output if requested
    if to_yearly:
        yearly_df = collapse_to_yearly(coc_zori_df, yearly_method)

        # Add metadata columns
        yearly_df["base_geo_type"] = geography
        yearly_df["boundary_vintage"] = boundary
        yearly_df["base_geo_vintage"] = counties
        yearly_df["acs_vintage"] = acs_vintage
        yearly_df["weighting_method"] = weighting
        yearly_df["provenance"] = json.dumps(base_provenance)

        # Reorder columns
        yearly_col_order = [
            "coc_id",
            "year",
            "zori_coc",
            "base_geo_type",
            "boundary_vintage",
            "base_geo_vintage",
            "acs_vintage",
            "weighting_method",
            "coverage_ratio",
            "max_geo_contribution",
            "geo_count",
            "method",
            "provenance",
        ]
        yearly_df = yearly_df[yearly_col_order]

        yearly_path = get_coc_zori_yearly_path(
            geography, boundary, counties, acs_vintage, weighting, yearly_method, output_dir
        )

        yearly_provenance = ProvenanceBlock(
            boundary_vintage=boundary,
            acs_vintage=acs_vintage,
            weighting=weighting,
            extra={
                "dataset": "coc_zori_yearly",
                "geography": geography,
                "base_geo_vintage": counties,
                "metric": "ZORI",
                "source": "Zillow Economic Research",
                "yearly_method": yearly_method,
                "coc_count": yearly_df["coc_id"].nunique(),
                "year_count": yearly_df["year"].nunique(),
                "year_range": [int(yearly_df["year"].min()), int(yearly_df["year"].max())],
            },
        )

        write_parquet_with_provenance(yearly_df, yearly_path, yearly_provenance)
        logger.info(f"Wrote yearly CoC ZORI data to {yearly_path}")

    return output_path
