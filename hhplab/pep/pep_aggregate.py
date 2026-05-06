"""Aggregate PEP county population estimates to CoC boundaries.

Uses existing CoC-county crosswalks to aggregate county-level
Population Estimates Program data to Continuum of Care geography.

Weighting strategy
------------------
PEP aggregation supports explicit weight columns from tract-mediated county
crosswalks, such as ``population_weight``, ``household_weight``, or
``renter_household_weight``.  Those columns allocate county population
totals to CoCs with denominator-specific tract evidence while preserving
coverage and contribution diagnostics.

The older raw ``area_share`` path from the direct county crosswalk assumes
uniform population density inside each county.  It remains available for
diagnostics, back-compatibility, and emergency fallback use, but outputs are
flagged as deprecated and callers receive a warning.
When ``population_weight`` comes from fixed decennial tract denominators,
the decennial counts define only the within-county spatial distribution.
Annual scaling uses PEP July 1 county estimates relative to the PEP
estimate for the matching decennial baseline year, not the April 1
decennial count.

Usage
-----
    from hhplab.pep.pep_aggregate import aggregate_pep_to_coc

    path = aggregate_pep_to_coc(
        boundary_vintage="2024",
        county_vintage="2024",
        weighting="area_share",
    )
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import pandas as pd

from hhplab.naming import coc_pep_filename, county_xwalk_path
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

logger = logging.getLogger(__name__)


# PEP uses a higher coverage threshold than ZORI (0.95 vs 0.90) because
# county-level population estimates have near-complete coverage -- almost
# every county has a PEP estimate, so missing data is unusual and likely
# indicates a real problem rather than expected sparsity.
DEFAULT_MIN_COVERAGE = 0.95

DECENNIAL_PEP_BASELINE_SCALING = "decennial_pep_baseline_ratio"
DIRECT_PEP_WEIGHTED_SUM = "direct_pep_weighted_sum"
DIRECT_COUNTY_AREA_ALLOCATION = "direct_county_area_overlap"
TRACT_MEDIATED_COUNTY_ALLOCATION = "tract_mediated_county_weights"
DEPRECATED_DIRECT_COUNTY_AREA_STATUS = "deprecated_direct_county_area"
CURRENT_TRACT_MEDIATED_STATUS = "current_tract_mediated"
DIRECT_COUNTY_AREA_DEPRECATION_NOTICE = (
    "PEP CoC population allocation with weighting='area_share' uses the deprecated "
    "direct county/CoC area-overlap method. It assumes uniform population density "
    "within counties and is retained only for diagnostics, back-compatibility, and "
    "fallback use. Prefer a tract-mediated county crosswalk weight such as "
    "'population_weight' for analytical population panels."
)


def is_deprecated_direct_county_area_weighting(weighting: str) -> bool:
    """Return True when *weighting* selects the deprecated county-area path."""
    return weighting == "area_share"


def warn_deprecated_direct_county_area_weighting(weighting: str) -> None:
    """Emit a user-visible warning for deprecated direct county-area allocation."""
    if not is_deprecated_direct_county_area_weighting(weighting):
        return
    warnings.warn(
        DIRECT_COUNTY_AREA_DEPRECATION_NOTICE,
        FutureWarning,
        stacklevel=2,
    )
    logger.warning(DIRECT_COUNTY_AREA_DEPRECATION_NOTICE)


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
            f"Run: hhplab generate xwalks --boundary {boundary_vintage} --counties {county_vintage}"
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
        raise FileNotFoundError(f"PEP county data not found in {pep_dir}\nRun: hhplab ingest pep")

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
        boundary_vintage,
        county_vintage,
        weighting,
        start_year,
        end_year,
    )
    return output_dir / filename


def _decennial_population_baseline_year(
    xwalk_df: pd.DataFrame,
    *,
    weighting: str,
) -> int | None:
    if weighting != "population_weight" or "denominator_source" not in xwalk_df.columns:
        return None

    sources = set(xwalk_df["denominator_source"].dropna().astype(str))
    if sources != {"decennial"}:
        return None

    if "denominator_vintage" not in xwalk_df.columns:
        raise ValueError(
            "Decennial tract-mediated population_weight requires "
            "denominator_vintage in the crosswalk. Regenerate the "
            "tract-mediated county crosswalk."
        )

    vintages = set(xwalk_df["denominator_vintage"].dropna().astype(str))
    if len(vintages) != 1:
        raise ValueError(
            "Decennial tract-mediated population_weight requires exactly one "
            f"denominator_vintage; found {sorted(vintages)}."
        )
    baseline_year = int(next(iter(vintages)))
    if baseline_year not in {2010, 2020}:
        raise ValueError(
            "Decennial tract-mediated population_weight supports PEP baseline "
            f"years 2010 or 2020; found {baseline_year}."
        )

    required = {"county_population_total", "population_weight"}
    missing = sorted(required - set(xwalk_df.columns))
    if missing:
        raise ValueError(
            "Decennial tract-mediated population_weight requires "
            f"{', '.join(missing)} in the crosswalk. Regenerate the "
            "tract-mediated county crosswalk with decennial denominators."
        )
    return baseline_year


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
        Weighting method. ``"area_share"`` uses the direct county overlay,
        ``"equal"`` gives each county in a geography equal weight, and any
        other value is interpreted as an explicit crosswalk weight column
        such as ``"population_weight"`` from a tract-mediated artifact.
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

    warn_deprecated_direct_county_area_weighting(weighting)

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
    decennial_baseline_year = _decennial_population_baseline_year(
        xwalk_df,
        weighting=weighting,
    )
    population_scaling_method = DIRECT_PEP_WEIGHTED_SUM

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
    if decennial_baseline_year is None:
        merged["weighted_pop"] = merged[weight_col] * merged["population"]
    else:
        baseline = pep_df.loc[
            pep_df["year"] == decennial_baseline_year,
            ["county_fips", "population"],
        ].rename(columns={"population": "baseline_pep_population"})
        if baseline.empty:
            raise ValueError(
                "Decennial tract-mediated population_weight requires PEP "
                f"county estimates for baseline year {decennial_baseline_year}. "
                "Load a PEP vintage that includes the baseline year."
            )
        merged = merged.merge(baseline, on="county_fips", how="left")
        if merged["baseline_pep_population"].isna().any():
            missing = sorted(
                merged.loc[merged["baseline_pep_population"].isna(), "county_fips"].unique()
            )
            raise ValueError(
                "Decennial tract-mediated population_weight is missing PEP "
                f"baseline year {decennial_baseline_year} estimates for "
                f"county_fips values {missing[:10]}"
                f"{'...' if len(missing) > 10 else ''}."
            )
        if (merged["baseline_pep_population"] <= 0).any():
            bad = sorted(
                merged.loc[merged["baseline_pep_population"] <= 0, "county_fips"].unique()
            )
            raise ValueError(
                "Decennial tract-mediated population_weight requires positive "
                f"PEP baseline year {decennial_baseline_year} estimates; "
                f"non-positive county_fips values: {bad[:10]}"
                f"{'...' if len(bad) > 10 else ''}."
            )
        merged["weighted_pop"] = (
            merged[weight_col]
            * merged["county_population_total"]
            * (merged["population"] / merged["baseline_pep_population"])
        )
        population_scaling_method = DECENNIAL_PEP_BASELINE_SCALING

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
    agg_df["max_county_contribution"] = (agg_df["max_weighted_pop"] / agg_df["population"]).fillna(
        0.0
    )
    agg_df.loc[agg_df["population"] == 0, "max_county_contribution"] = 0.0

    # Null population when no covered weight (matches old behaviour)
    agg_df.loc[agg_df["covered_weight"] == 0, "population"] = None

    # Drop helper columns
    result_df = agg_df.drop(
        columns=["covered_weight", "total_weight", "max_weighted_pop"],
    )

    # Add reference date
    result_df["reference_date"] = pd.to_datetime(result_df["year"].astype(str) + "-07-01")

    # Add metadata columns
    if boundary_vintage is not None:
        result_df["boundary_vintage"] = boundary_vintage
    if county_vintage is not None:
        result_df["county_vintage"] = county_vintage
    result_df["weighting_method"] = weighting
    result_df["population_scaling_method"] = population_scaling_method
    if is_deprecated_direct_county_area_weighting(weighting):
        result_df["allocation_method"] = DIRECT_COUNTY_AREA_ALLOCATION
        result_df["allocation_method_status"] = DEPRECATED_DIRECT_COUNTY_AREA_STATUS
    else:
        result_df["allocation_method"] = TRACT_MEDIATED_COUNTY_ALLOCATION
        result_df["allocation_method_status"] = CURRENT_TRACT_MEDIATED_STATUS
    if decennial_baseline_year is not None:
        result_df["population_scaling_baseline_year"] = decennial_baseline_year
    else:
        result_df["population_scaling_baseline_year"] = pd.NA

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


def aggregate_pep_to_coc_many(
    boundary_vintage: str,
    county_vintage: str,
    weightings: list[str] | tuple[str, ...],
    pep_path: Path | str | None = None,
    xwalk_path: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    output_dir: Path | str | None = None,
    force: bool = False,
) -> dict[str, Path]:
    """Aggregate PEP to CoC once per requested weighting column.

    This is a small orchestration convenience for sensitivity workflows.
    A shared PEP source and crosswalk artifact are reused, and each
    requested weighting writes its own deterministic output file.
    """
    if not weightings:
        raise ValueError("At least one weighting must be requested.")

    outputs: dict[str, Path] = {}
    for weighting in weightings:
        outputs[weighting] = aggregate_pep_to_coc(
            boundary_vintage=boundary_vintage,
            county_vintage=county_vintage,
            weighting=weighting,
            pep_path=pep_path,
            xwalk_path=xwalk_path,
            start_year=start_year,
            end_year=end_year,
            min_coverage=min_coverage,
            output_dir=output_dir,
            force=force,
        )
    return outputs


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
        Weighting method: "area_share" (default), "equal", or an explicit
        crosswalk weight column such as "population_weight".
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
    available_start = int(pep_df["year"].min())
    available_end = int(pep_df["year"].max())

    # Apply year filters
    if start_year is not None:
        pep_df = pep_df[pep_df["year"] >= start_year]
    if end_year is not None:
        pep_df = pep_df[pep_df["year"] <= end_year]

    if pep_df.empty:
        requested_range = (
            f"{start_year}-{end_year}"
            if start_year is not None and end_year is not None
            else f"{start_year or available_start}-{end_year or available_end}"
        )
        raise ValueError(
            "No PEP data remains after applying the requested year filter "
            f"({requested_range}). Available years in the loaded data: "
            f"{available_start}-{available_end}. "
            "Ingest an older PEP vintage or adjust --start/--end."
        )

    actual_start = int(pep_df["year"].min())
    actual_end = int(pep_df["year"].max())

    # Check output path
    output_path = get_output_path(
        boundary_vintage, county_vintage, weighting, actual_start, actual_end, output_dir
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
        "allocation_method",
        "allocation_method_status",
        "population_scaling_method",
        "population_scaling_baseline_year",
    ]
    result_df = result_df[col_order]

    # Build provenance
    pep_provenance = read_provenance(pep_path) if pep_path else None
    xwalk_provenance_path = (
        xwalk_path if xwalk_path else county_xwalk_path(boundary_vintage, county_vintage)
    )
    xwalk_provenance = (
        read_provenance(xwalk_provenance_path) if Path(xwalk_provenance_path).exists() else None
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
            "allocation_method": (
                DIRECT_COUNTY_AREA_ALLOCATION
                if is_deprecated_direct_county_area_weighting(weighting)
                else TRACT_MEDIATED_COUNTY_ALLOCATION
            ),
            "allocation_method_status": (
                DEPRECATED_DIRECT_COUNTY_AREA_STATUS
                if is_deprecated_direct_county_area_weighting(weighting)
                else CURRENT_TRACT_MEDIATED_STATUS
            ),
            "deprecated": is_deprecated_direct_county_area_weighting(weighting),
            "deprecation_notice": (
                DIRECT_COUNTY_AREA_DEPRECATION_NOTICE
                if is_deprecated_direct_county_area_weighting(weighting)
                else None
            ),
            "population_scaling_method": (
                result_df["population_scaling_method"].dropna().iloc[0]
                if not result_df.empty
                else DIRECT_PEP_WEIGHTED_SUM
            ),
            "population_scaling_baseline_year": (
                int(result_df["population_scaling_baseline_year"].dropna().iloc[0])
                if result_df["population_scaling_baseline_year"].notna().any()
                else None
            ),
            "population_scaling_rationale": (
                "Decennial tract counts are April 1 counts; PEP county estimates "
                "are July 1 annual estimates. Decennial tract-mediated population "
                "weights use decennial counts for within-county distribution and "
                "PEP_Y / PEP_baseline_year for annual county scaling."
            ),
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
