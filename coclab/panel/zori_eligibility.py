"""ZORI eligibility rules and provenance for panel integration.

This module implements Agent C from the rent_to_income agent instructions:
defining and enforcing the "ZORI-eligible analysis universe" with provenance.

Eligibility Rules
-----------------
A CoC-year is ZORI-eligible if:
- coverage_ratio >= zori_min_coverage (default 0.90)

Notes:
- High dominance is NOT a hard exclusion; it generates warnings only
- CoCs with zero coverage must NOT be imputed

Output Columns
--------------
When ZORI is integrated into the panel:
- zori_coc: CoC-level ZORI (yearly), null if ineligible
- zori_coverage_ratio: Coverage of base geography weights
- zori_is_eligible: Boolean eligibility flag
- rent_to_income: ZORI / (median_household_income / 12), null if ineligible
- zori_excluded_reason: Reason for exclusion (null if eligible)

Provenance Fields
-----------------
- rent_metric: "ZORI"
- rent_alignment: Yearly collapse method (e.g., "pit_january")
- zori_min_coverage: Coverage threshold used

Usage
-----
    from coclab.panel.zori_eligibility import (
        apply_zori_eligibility,
        compute_rent_to_income,
        ZoriProvenance,
    )

    # Apply eligibility rules
    panel_df = apply_zori_eligibility(
        panel_df,
        zori_coverage_col="zori_coverage_ratio",
        min_coverage=0.90,
    )

    # Compute rent_to_income ratio
    panel_df = compute_rent_to_income(panel_df)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Default thresholds
DEFAULT_ZORI_MIN_COVERAGE = 0.90
DEFAULT_DOMINANCE_THRESHOLD = 0.80

# Excluded reason codes
EXCLUDED_MISSING = "missing"
EXCLUDED_LOW_COVERAGE = "low_coverage"
EXCLUDED_ZERO_COVERAGE = "zero_coverage"


@dataclass
class ZoriProvenance:
    """Provenance metadata for ZORI-integrated panel data.

    This class captures the key metadata needed to understand how ZORI
    data was integrated into the analysis panel, including alignment
    method, coverage thresholds, and source attribution.

    Attributes
    ----------
    rent_metric : str
        The rent metric used (always "ZORI" for this module).
    rent_alignment : str
        Method used to align ZORI to panel years (e.g., "pit_january").
    zori_min_coverage : float
        Minimum coverage ratio threshold for eligibility.
    zori_source : str
        Data source attribution.
    boundary_vintage : str, optional
        CoC boundary vintage used for ZORI aggregation.
    acs_vintage : str, optional
        ACS vintage used for ZORI weighting.
    weighting_method : str, optional
        Weighting method used for ZORI aggregation.
    dominance_threshold : float, optional
        Threshold for high dominance warnings.
    extra : dict
        Additional extensible metadata.
    """

    rent_metric: str = "ZORI"
    rent_alignment: str = "pit_january"
    zori_min_coverage: float = DEFAULT_ZORI_MIN_COVERAGE
    zori_source: str = "Zillow Economic Research"
    boundary_vintage: str | None = None
    acs_vintage: str | None = None
    weighting_method: str | None = None
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns
        -------
        dict
            Dictionary representation suitable for provenance embedding.
        """
        d = {
            "rent_metric": self.rent_metric,
            "rent_alignment": self.rent_alignment,
            "zori_min_coverage": self.zori_min_coverage,
            "zori_source": self.zori_source,
        }

        if self.boundary_vintage:
            d["zori_boundary_vintage"] = self.boundary_vintage
        if self.acs_vintage:
            d["zori_acs_vintage"] = self.acs_vintage
        if self.weighting_method:
            d["zori_weighting_method"] = self.weighting_method
        if self.dominance_threshold != DEFAULT_DOMINANCE_THRESHOLD:
            d["zori_dominance_threshold"] = self.dominance_threshold
        if self.extra:
            d.update(self.extra)

        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ZoriProvenance:
        """Create from dictionary.

        Parameters
        ----------
        data : dict
            Dictionary containing provenance fields.

        Returns
        -------
        ZoriProvenance
            Reconstructed provenance object.
        """
        known_fields = {
            "rent_metric",
            "rent_alignment",
            "zori_min_coverage",
            "zori_source",
            "boundary_vintage",
            "acs_vintage",
            "weighting_method",
            "dominance_threshold",
            "extra",
        }

        # Handle field name variations
        boundary = data.get("boundary_vintage") or data.get("zori_boundary_vintage")
        acs = data.get("acs_vintage") or data.get("zori_acs_vintage")
        weighting = data.get("weighting_method") or data.get("zori_weighting_method")
        dominance = data.get("dominance_threshold") or data.get(
            "zori_dominance_threshold", DEFAULT_DOMINANCE_THRESHOLD
        )

        kwargs = {
            "rent_metric": data.get("rent_metric", "ZORI"),
            "rent_alignment": data.get("rent_alignment", "pit_january"),
            "zori_min_coverage": data.get("zori_min_coverage", DEFAULT_ZORI_MIN_COVERAGE),
            "zori_source": data.get("zori_source", "Zillow Economic Research"),
            "boundary_vintage": boundary,
            "acs_vintage": acs,
            "weighting_method": weighting,
            "dominance_threshold": dominance,
        }

        # Collect unknown fields into extra
        all_known = known_fields | {
            "zori_boundary_vintage",
            "zori_acs_vintage",
            "zori_weighting_method",
            "zori_dominance_threshold",
        }
        extra = {k: v for k, v in data.items() if k not in all_known}
        if extra:
            kwargs["extra"] = extra

        return cls(**kwargs)


def determine_exclusion_reason(
    coverage_ratio: float | None,
    zori_value: float | None,
    min_coverage: float = DEFAULT_ZORI_MIN_COVERAGE,
) -> str | None:
    """Determine the reason for ZORI exclusion, if any.

    Parameters
    ----------
    coverage_ratio : float or None
        The coverage ratio for this CoC-year.
    zori_value : float or None
        The ZORI value for this CoC-year.
    min_coverage : float
        Minimum coverage ratio threshold for eligibility.

    Returns
    -------
    str or None
        Exclusion reason code, or None if eligible.
        Possible values: "missing", "zero_coverage", "low_coverage"
    """
    # Check for missing ZORI value
    if zori_value is None or pd.isna(zori_value):
        return EXCLUDED_MISSING

    # Check for missing coverage ratio
    if coverage_ratio is None or pd.isna(coverage_ratio):
        return EXCLUDED_MISSING

    # Check for zero coverage (must not be imputed)
    if coverage_ratio == 0:
        return EXCLUDED_ZERO_COVERAGE

    # Check for low coverage
    if coverage_ratio < min_coverage:
        return EXCLUDED_LOW_COVERAGE

    return None


def apply_zori_eligibility(
    df: pd.DataFrame,
    zori_col: str = "zori_coc",
    coverage_col: str = "zori_coverage_ratio",
    min_coverage: float = DEFAULT_ZORI_MIN_COVERAGE,
    dominance_col: str | None = "zori_max_geo_contribution",
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
) -> pd.DataFrame:
    """Apply ZORI eligibility rules to a panel DataFrame.

    This function:
    1. Adds a boolean `zori_is_eligible` column
    2. Sets `zori_coc` to null for ineligible rows
    3. Adds `zori_excluded_reason` for ineligible rows
    4. Generates warnings for high-dominance CoCs (not a hard exclusion)

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame with ZORI columns.
    zori_col : str
        Name of the ZORI value column.
    coverage_col : str
        Name of the coverage ratio column.
    min_coverage : float
        Minimum coverage ratio threshold for eligibility. Default 0.90.
    dominance_col : str or None
        Name of the max geo contribution column for dominance warnings.
        If None, dominance warnings are skipped.
    dominance_threshold : float
        Threshold above which to warn about high dominance. Default 0.80.

    Returns
    -------
    pd.DataFrame
        DataFrame with eligibility columns added:
        - zori_is_eligible: Boolean flag
        - zori_excluded_reason: Reason code if ineligible, else null

    Notes
    -----
    - High dominance is NOT a hard exclusion; it only generates warnings.
    - CoCs with zero coverage are explicitly excluded (not imputed).
    """
    result = df.copy()

    # Check if ZORI columns exist
    if zori_col not in result.columns:
        logger.warning(f"ZORI column '{zori_col}' not found in DataFrame")
        result["zori_is_eligible"] = False
        result["zori_excluded_reason"] = EXCLUDED_MISSING
        return result

    if coverage_col not in result.columns:
        logger.warning(f"Coverage column '{coverage_col}' not found in DataFrame")
        result["zori_is_eligible"] = False
        result["zori_excluded_reason"] = EXCLUDED_MISSING
        return result

    # Compute exclusion reason for each row
    def get_reason(row):
        return determine_exclusion_reason(
            coverage_ratio=row[coverage_col],
            zori_value=row[zori_col],
            min_coverage=min_coverage,
        )

    result["zori_excluded_reason"] = result.apply(get_reason, axis=1)

    # Set eligibility flag (eligible if no exclusion reason)
    result["zori_is_eligible"] = result["zori_excluded_reason"].isna()

    # Null out ZORI for ineligible rows
    ineligible_mask = ~result["zori_is_eligible"]
    if ineligible_mask.any():
        result.loc[ineligible_mask, zori_col] = None
        logger.info(
            f"Marked {ineligible_mask.sum()} rows as ZORI-ineligible "
            f"(threshold: {min_coverage:.0%})"
        )

    # Check for high dominance and warn (not a hard exclusion)
    if dominance_col and dominance_col in result.columns:
        high_dominance_mask = (
            result[dominance_col].notna()
            & (result[dominance_col] > dominance_threshold)
            & result["zori_is_eligible"]
        )
        if high_dominance_mask.any():
            high_dom_count = high_dominance_mask.sum()
            affected_cocs = result.loc[high_dominance_mask, "coc_id"].unique()
            logger.warning(
                f"WARNING: {high_dom_count} ZORI-eligible observations have high "
                f"dominance (> {dominance_threshold:.0%}). "
                f"Affected CoCs: {len(affected_cocs)}. "
                f"This is a warning only; these rows remain eligible."
            )

    # Log summary statistics
    eligible_count = result["zori_is_eligible"].sum()
    total_count = len(result)
    logger.info(
        f"ZORI eligibility: {eligible_count}/{total_count} "
        f"({100 * eligible_count / total_count:.1f}%) eligible"
    )

    # Log breakdown by exclusion reason
    reason_counts = result[~result["zori_is_eligible"]]["zori_excluded_reason"].value_counts()
    if len(reason_counts) > 0:
        for reason, count in reason_counts.items():
            logger.info(f"  Excluded ({reason}): {count}")

    return result


def compute_rent_to_income(
    df: pd.DataFrame,
    zori_col: str = "zori_coc",
    income_col: str = "median_household_income",
    eligibility_col: str = "zori_is_eligible",
) -> pd.DataFrame:
    """Compute rent_to_income ratio for eligible rows.

    The formula is:
        rent_to_income = zori_coc / (median_household_income / 12.0)

    This represents monthly rent as a fraction of monthly income.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame with ZORI and income columns.
    zori_col : str
        Name of the ZORI value column.
    income_col : str
        Name of the median household income column.
    eligibility_col : str
        Name of the eligibility flag column.

    Returns
    -------
    pd.DataFrame
        DataFrame with `rent_to_income` column added.

    Notes
    -----
    - Returns null if zori_coc is null (including ineligible rows)
    - Returns null if income is null or zero
    - Ineligible rows always get null rent_to_income
    """
    result = df.copy()

    # Check required columns
    if zori_col not in result.columns:
        logger.warning(f"ZORI column '{zori_col}' not found; rent_to_income will be null")
        result["rent_to_income"] = None
        return result

    if income_col not in result.columns:
        logger.warning(f"Income column '{income_col}' not found; rent_to_income will be null")
        result["rent_to_income"] = None
        return result

    # Compute rent_to_income
    # Start with null values
    result["rent_to_income"] = None

    # Only compute for eligible rows with valid data
    eligible_mask = result.get(eligibility_col, True) if eligibility_col in result.columns else True
    if isinstance(eligible_mask, bool):
        eligible_mask = pd.Series([eligible_mask] * len(result))

    zori_valid = result[zori_col].notna()
    income_valid = result[income_col].notna() & (result[income_col] > 0)

    compute_mask = eligible_mask & zori_valid & income_valid

    if compute_mask.any():
        result.loc[compute_mask, "rent_to_income"] = result.loc[compute_mask, zori_col] / (
            result.loc[compute_mask, income_col] / 12.0
        )

    # Log summary
    computed_count = compute_mask.sum()
    logger.info(
        f"Computed rent_to_income for {computed_count} rows "
        f"({100 * computed_count / len(result):.1f}% of panel)"
    )

    # Check for null income among eligible rows
    null_income_eligible = eligible_mask & zori_valid & ~income_valid
    if null_income_eligible.any():
        logger.warning(
            f"{null_income_eligible.sum()} ZORI-eligible rows have null/zero income "
            f"and cannot compute rent_to_income"
        )

    return result


def add_provenance_columns(
    df: pd.DataFrame,
    provenance: ZoriProvenance,
) -> pd.DataFrame:
    """Add ZORI provenance columns to a panel DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame.
    provenance : ZoriProvenance
        Provenance metadata to embed.

    Returns
    -------
    pd.DataFrame
        DataFrame with provenance columns added:
        - rent_metric: "ZORI"
        - rent_alignment: e.g., "pit_january"
        - zori_min_coverage: coverage threshold used
    """
    result = df.copy()

    result["rent_metric"] = provenance.rent_metric
    result["rent_alignment"] = provenance.rent_alignment
    result["zori_min_coverage"] = provenance.zori_min_coverage

    return result


def summarize_zori_eligibility(df: pd.DataFrame) -> dict[str, Any]:
    """Generate summary statistics for ZORI eligibility.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame with eligibility columns.

    Returns
    -------
    dict
        Summary statistics including counts and percentages.
    """
    total_rows = len(df)

    if "zori_is_eligible" not in df.columns:
        return {
            "total_rows": total_rows,
            "zori_integrated": False,
        }

    eligible_count = df["zori_is_eligible"].sum()
    ineligible_count = total_rows - eligible_count

    # Count by exclusion reason
    reason_counts = {}
    if "zori_excluded_reason" in df.columns:
        reason_df = df[~df["zori_is_eligible"]]["zori_excluded_reason"].value_counts()
        reason_counts = reason_df.to_dict()

    # rent_to_income stats
    rti_stats = {}
    if "rent_to_income" in df.columns:
        rti = df["rent_to_income"].dropna()
        if len(rti) > 0:
            rti_stats = {
                "rent_to_income_count": len(rti),
                "rent_to_income_mean": float(rti.mean()),
                "rent_to_income_median": float(rti.median()),
                "rent_to_income_min": float(rti.min()),
                "rent_to_income_max": float(rti.max()),
            }

    return {
        "total_rows": total_rows,
        "zori_integrated": True,
        "zori_eligible_count": int(eligible_count),
        "zori_eligible_pct": 100 * eligible_count / total_rows if total_rows > 0 else 0,
        "zori_ineligible_count": int(ineligible_count),
        "exclusion_reasons": reason_counts,
        **rti_stats,
    }


def get_zori_panel_columns() -> list[str]:
    """Return the canonical column names for ZORI integration.

    Returns
    -------
    list[str]
        Column names in canonical order.
    """
    return [
        "zori_coc",
        "zori_coverage_ratio",
        "zori_is_eligible",
        "zori_excluded_reason",
        "rent_to_income",
        "rent_metric",
        "rent_alignment",
        "zori_min_coverage",
    ]
