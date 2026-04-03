"""Aggregate PEP county population estimates to CoC boundaries.

Uses existing CoC-county crosswalks to aggregate county-level
Population Estimates Program data to Continuum of Care geography.

Weighting strategy
------------------
PEP aggregation uses raw ``area_share`` from the crosswalk (uniform
density assumption).  This differs from ZORI aggregation, which
combines ``area_share`` with ACS demographic weights.  The rationale:
PEP is already a population count, so area-based apportionment of
county totals is the natural choice.  Adding ACS weights would
circularly weight population by population.  For large counties with
uneven population distributions this is less precise, but avoids
introducing ACS vintage coupling into what is otherwise a simple
population pipeline.

Usage
-----
    from coclab.pep.aggregate import aggregate_pep_to_coc

    path = aggregate_pep_to_coc(
        boundary_vintage="2024",
        county_vintage="2024",
        weighting="area_share",
    )
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from coclab.naming import coc_pep_filename, county_xwalk_path
from coclab.paths import curated_dir
from coclab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

logger = logging.getLogger(__name__)


# PEP uses a higher coverage threshold than ZORI (0.95 vs 0.90) because
# county-level population estimates have near-complete coverage -- almost
# every county has a PEP estimate, so missing data is unusual and likely
# indicates a real problem rather than expected sparsity.
DEFAULT_MIN_COVERAGE = 0.95


def load_crosswalk(
    boundary_vintage: str,
    county_vintage: str,
    xwalk_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Load CoC-county crosswalk.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage year (e.g., "2024").
    county_vintage : str
        TIGER county vintage year (e.g., "2024").
    xwalk_dir : Path or str, optional
        Directory containing crosswalks. Defaults to 'data/curated/xwalks'.

    Returns
    -------
    pd.DataFrame
        Crosswalk with columns: coc_id, county_fips, area_share

    Raises
    ------
    FileNotFoundError
        If crosswalk file does not exist.
    """
    if xwalk_dir is None:
        xwalk_path = county_xwalk_path(boundary_vintage, county_vintage)
    else:
        # xwalk_dir is like "data/curated/xwalks"; county_xwalk_path expects
        # the data root, so go up two levels (xwalks -> curated -> data).
        base_dir = Path(xwalk_dir).parent.parent
        xwalk_path = county_xwalk_path(boundary_vintage, county_vintage, base_dir)

    if not xwalk_path.exists():
        raise FileNotFoundError(
            f"Crosswalk not found: {xwalk_path}\n"
            f"Run: coclab generate xwalks --boundary {boundary_vintage} --counties {county_vintage}"
        )

    df = pd.read_parquet(xwalk_path)
    logger.info(f"Loaded crosswalk from {xwalk_path}: {len(df)} rows")

    return df


def load_pep_county(
    pep_path: Path | str | None = None,
    pep_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Load PEP county population data.

    When no explicit path is given, discovers all vintage files
    (``pep_county__v*.parquet``) and concatenates them.  For overlapping
    years the latest vintage takes precedence.

    Parameters
    ----------
    pep_path : Path or str, optional
        Explicit path to PEP parquet. If None, discovers vintage files.
    pep_dir : Path or str, optional
        Directory containing PEP data. Defaults to 'data/curated/pep'.

    Returns
    -------
    pd.DataFrame
        County population data with columns: county_fips, year, population

    Raises
    ------
    FileNotFoundError
        If no PEP data files exist.
    """
    if pep_path is not None:
        pep_path = Path(pep_path)
        df = pd.read_parquet(pep_path)
        logger.info(f"Loaded PEP data from {pep_path}: {len(df)} rows")
        return df

    if pep_dir is None:
        pep_dir = curated_dir("pep")
    else:
        pep_dir = Path(pep_dir)

    # Discover vintage files, sorted descending so latest vintage wins on overlap
    vintage_files = sorted(
        pep_dir.glob("pep_county__v[0-9][0-9][0-9][0-9].parquet"),
        reverse=True,
    )

    if not vintage_files:
        raise FileNotFoundError(
            f"PEP county data not found in {pep_dir}\n"
            f"Run: coclab ingest pep"
        )

    frames = [pd.read_parquet(f) for f in vintage_files]
    logger.info(
        "Discovered PEP vintage files: %s",
        ", ".join(f.name for f in vintage_files),
    )

    if len(frames) == 1:
        return frames[0]

    # Concatenate and deduplicate; latest vintage takes precedence
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["county_fips", "year"], keep="first")
    logger.info(f"Combined PEP data: {len(df)} rows")

    return df


def get_output_path(
    boundary_vintage: str,
    county_vintage: str,
    weighting: str,
    start_year: int,
    end_year: int,
    output_dir: Path | str | None = None,
) -> Path:
    """Get canonical output path for CoC-level PEP data.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage.
    county_vintage : str
        TIGER county vintage.
    weighting : str
        Weighting method.
    start_year : int
        First year in output.
    end_year : int
        Last year in output.
    output_dir : Path or str, optional
        Output directory.

    Returns
    -------
    Path
        Output path.
    """
    if output_dir is None:
        output_dir = curated_dir("pep")
    else:
        output_dir = Path(output_dir)

    filename = coc_pep_filename(
        boundary_vintage, county_vintage, weighting, start_year, end_year,
    )
    return output_dir / filename


def aggregate_pep_counties(
    pep_df: pd.DataFrame,
    xwalk_df: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
    weighting: str = "area_share",
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    boundary_vintage: str | None = None,
    county_vintage: str | None = None,
) -> pd.DataFrame:
    """Aggregate PEP county estimates to any analysis geography.

    Pure computation function (no file I/O).  Takes a PEP county DataFrame
    and a county crosswalk, and produces geography-level population
    estimates for all years present in the PEP data.

    Parameters
    ----------
    pep_df : pd.DataFrame
        County-level PEP data with columns: county_fips, year, population.
    xwalk_df : pd.DataFrame
        County crosswalk with ``geo_id_col``, county_fips, and a weight
        column (area_share or as specified by *weighting*).
    geo_id_col : str
        Name of the geography identifier column in the crosswalk.
        Defaults to ``"coc_id"``.
    weighting : str
        Weighting method: ``"area_share"`` (default) or ``"equal"``.
    min_coverage : float
        Minimum coverage ratio threshold.  Geo-years below this have
        population set to null.  Default 0.95.
    boundary_vintage : str, optional
        Boundary vintage to include in metadata columns.
    county_vintage : str, optional
        County vintage to include in metadata columns.

    Returns
    -------
    pd.DataFrame
        Geography-level population estimates with ``geo_id_col``, year,
        reference_date, population, coverage_ratio, county_count,
        max_county_contribution, and metadata columns.
    """
    xwalk_df = xwalk_df.copy()

    # Normalize weighting column name
    if weighting == "area_share" and "area_share" in xwalk_df.columns:
        weight_col = "area_share"
    elif weighting == "equal":
        xwalk_df["equal_weight"] = 1.0
        xwalk_df["equal_weight"] = xwalk_df.groupby(geo_id_col)["equal_weight"].transform(
            lambda x: x / x.sum()
        )
        weight_col = "equal_weight"
    else:
        if weighting in xwalk_df.columns:
            weight_col = weighting
        else:
            raise ValueError(
                f"Weighting '{weighting}' not found in crosswalk. "
                f"Available: {list(xwalk_df.columns)}"
            )

    # Log scope
    years = sorted(pep_df["year"].unique())
    logger.info(f"Aggregating {len(years)} years of PEP data")

    # Pre-compute total weight per geography (constant across years)
    total_weight_per_geo = xwalk_df.groupby(geo_id_col)[weight_col].sum()
    logger.info(f"Crosswalk contains {len(total_weight_per_geo)} geography units")

    # Check for missing counties in PEP data
    xwalk_counties = set(xwalk_df["county_fips"].unique())
    pep_counties = set(pep_df["county_fips"].unique())
    missing_counties = xwalk_counties - pep_counties

    if missing_counties:
        logger.warning(
            f"{len(missing_counties)} counties in crosswalk not found in PEP data: "
            f"{list(missing_counties)[:5]}..."
        )

    # Check for orphan PEP counties absent from crosswalk
    orphan_counties = pep_counties - xwalk_counties
    if orphan_counties:
        logger.warning(
            f"{len(orphan_counties)} PEP counties absent from crosswalk "
            f"(these will not contribute to any geography): "
            f"{sorted(orphan_counties)[:10]}"
            f"{'...' if len(orphan_counties) > 10 else ''}"
        )

    # Single merge: crosswalk x PEP (one row per county-year-geo combo).
    # Inner join drops crosswalk rows whose county has no PEP data for a
    # given year, which is equivalent to the old "filter to notna" step.
    merged = xwalk_df.merge(
        pep_df[["county_fips", "year", "population"]],
        on="county_fips",
        how="inner",
    )

    # Weighted population per row
    merged["weighted_pop"] = merged[weight_col] * merged["population"]

    # Vectorised groupby aggregation (replaces the O(G*Y) Python loop)
    grouped = merged.groupby([geo_id_col, "year"])
    agg_df = grouped.agg(
        population=("weighted_pop", "sum"),
        covered_weight=(weight_col, "sum"),
        county_count=("county_fips", "size"),
        max_weighted_pop=("weighted_pop", "max"),
    ).reset_index()

    # Build scaffold of all geo-year combinations so zero-coverage rows
    # are preserved (the inner join drops geos with no matching counties).
    all_geos = xwalk_df[geo_id_col].unique()
    scaffold = pd.DataFrame(
        [(g, y) for g in all_geos for y in years],
        columns=[geo_id_col, "year"],
    )
    agg_df = scaffold.merge(agg_df, on=[geo_id_col, "year"], how="left")
    agg_df["population"] = agg_df["population"].fillna(0.0)
    agg_df["covered_weight"] = agg_df["covered_weight"].fillna(0.0)
    agg_df["county_count"] = agg_df["county_count"].fillna(0).astype(int)
    agg_df["max_weighted_pop"] = agg_df["max_weighted_pop"].fillna(0.0)

    # Coverage ratio: covered weight / total weight for the geography
    agg_df["total_weight"] = agg_df[geo_id_col].map(total_weight_per_geo)
    agg_df["coverage_ratio"] = agg_df["covered_weight"] / agg_df["total_weight"]
    agg_df.loc[agg_df["total_weight"] == 0, "coverage_ratio"] = 0.0

    # Max county contribution: largest weighted_pop / total population
    agg_df["max_county_contribution"] = (
        agg_df["max_weighted_pop"] / agg_df["population"]
    ).fillna(0.0)
    agg_df.loc[agg_df["population"] == 0, "max_county_contribution"] = 0.0

    # Null population when no covered weight (matches old behaviour)
    agg_df.loc[agg_df["covered_weight"] == 0, "population"] = None

    # Drop helper columns
    result_df = agg_df.drop(
        columns=["covered_weight", "total_weight", "max_weighted_pop"],
    )

    # Add reference date
    result_df["reference_date"] = pd.to_datetime(
        result_df["year"].astype(str) + "-07-01"
    )

    # Add metadata columns
    if boundary_vintage is not None:
        result_df["boundary_vintage"] = boundary_vintage
    if county_vintage is not None:
        result_df["county_vintage"] = county_vintage
    result_df["weighting_method"] = weighting

    # Apply minimum coverage threshold
    low_coverage = result_df["coverage_ratio"] < min_coverage
    low_coverage_count = low_coverage.sum()
    if low_coverage_count > 0:
        logger.warning(
            f"{low_coverage_count} geo-years have coverage < {min_coverage:.0%} "
            f"(population set to null)"
        )
        result_df.loc[low_coverage, "population"] = None

    # Sort
    result_df = result_df.sort_values([geo_id_col, "year"]).reset_index(drop=True)

    return result_df


def aggregate_pep_to_coc(
    boundary_vintage: str,
    county_vintage: str,
    weighting: str = "area_share",
    pep_path: Path | str | None = None,
    xwalk_path: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    output_dir: Path | str | None = None,
    force: bool = False,
) -> Path:
    """Aggregate PEP county estimates to CoC geography.

    Orchestration wrapper that handles I/O, caching, and provenance
    around :func:`aggregate_pep_counties`.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage year (e.g., "2024").
    county_vintage : str
        TIGER county vintage year for crosswalk (e.g., "2024").
    weighting : str
        Weighting method: "area_share" (default) or "equal".
    pep_path : Path or str, optional
        Explicit path to PEP county parquet. If None, auto-detects.
    xwalk_path : Path or str, optional
        Explicit path to crosswalk. If None, auto-detects.
    start_year : int, optional
        First year to include. Defaults to earliest in data.
    end_year : int, optional
        Last year to include. Defaults to latest in data.
    min_coverage : float
        Minimum coverage ratio to include a CoC-year. Default 0.95.
    output_dir : Path or str, optional
        Output directory.
    force : bool
        Recompute even if output exists.

    Returns
    -------
    Path
        Path to output Parquet file.
    """
    # Load PEP data
    pep_df = load_pep_county(pep_path)

    # Apply year filters
    if start_year is not None:
        pep_df = pep_df[pep_df["year"] >= start_year]
    if end_year is not None:
        pep_df = pep_df[pep_df["year"] <= end_year]

    actual_start = int(pep_df["year"].min())
    actual_end = int(pep_df["year"].max())

    # Check output path
    output_path = get_output_path(
        boundary_vintage, county_vintage, weighting,
        actual_start, actual_end, output_dir
    )

    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Load crosswalk
    if xwalk_path is not None:
        xwalk_df = pd.read_parquet(xwalk_path)
    else:
        xwalk_df = load_crosswalk(boundary_vintage, county_vintage)

    # Delegate to core aggregation function
    result_df = aggregate_pep_counties(
        pep_df,
        xwalk_df,
        geo_id_col="coc_id",
        weighting=weighting,
        min_coverage=min_coverage,
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
    )

    # Reorder columns for CoC output
    col_order = [
        "coc_id",
        "year",
        "reference_date",
        "population",
        "coverage_ratio",
        "county_count",
        "max_county_contribution",
        "boundary_vintage",
        "county_vintage",
        "weighting_method",
    ]
    result_df = result_df[col_order]

    # Build provenance
    pep_provenance = read_provenance(pep_path) if pep_path else None
    xwalk_provenance_path = (
        xwalk_path if xwalk_path
        else county_xwalk_path(boundary_vintage, county_vintage)
    )
    xwalk_provenance = (
        read_provenance(xwalk_provenance_path)
        if Path(xwalk_provenance_path).exists()
        else None
    )

    low_coverage_count = (result_df["coverage_ratio"] < min_coverage).sum()

    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        weighting=weighting,
        extra={
            "dataset": "coc_pep_population",
            "source": "Derived from Census PEP county estimates",
            "pep_source": pep_provenance.to_dict() if pep_provenance else None,
            "xwalk_source": xwalk_provenance.to_dict() if xwalk_provenance else None,
            "aggregation_method": "weighted_sum",
            "weighting_method": weighting,
            "min_coverage_threshold": min_coverage,
            "year_range": [actual_start, actual_end],
            "coc_count": result_df["coc_id"].nunique(),
            "row_count": len(result_df),
            "low_coverage_nulled": int(low_coverage_count),
        },
    )

    # Write output
    write_parquet_with_provenance(result_df, output_path, provenance)
    logger.info(f"Wrote CoC-level PEP data to {output_path}")

    # Print summary
    coc_count = result_df["coc_id"].nunique()
    years = sorted(pep_df["year"].unique())
    valid_rows = result_df["population"].notna().sum()
    logger.info(
        f"Summary: {coc_count} CoCs, {len(years)} years, "
        f"{valid_rows}/{len(result_df)} CoC-years with valid population"
    )

    return output_path
