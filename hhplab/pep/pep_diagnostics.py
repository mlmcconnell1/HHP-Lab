"""Diagnostics and reporting for CoC-level PEP population data.

Provides per-CoC diagnostic summaries for PEP aggregation results,
analogous to ``hhplab.rents.zori_diagnostics`` for ZORI.

Diagnostics Output Schema
-------------------------
- coc_id: CoC identifier
- years_total: Total number of years in the data
- years_covered: Number of years with valid population (coverage >= threshold)
- coverage_ratio_mean: Mean coverage ratio across years
- flag_low_coverage: True if mean coverage < threshold
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

logger = logging.getLogger(__name__)

# PEP uses a higher coverage threshold because county data is near-complete
DEFAULT_MIN_COVERAGE = 0.95


def compute_coc_diagnostics(
    pep_coc_df: pd.DataFrame,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compute per-CoC diagnostics for PEP aggregation results.

    Parameters
    ----------
    pep_coc_df : pd.DataFrame
        CoC-level PEP data with columns: ``geo_id_col``, year, population,
        coverage_ratio.
    min_coverage : float
        Minimum coverage ratio threshold for flagging.
    geo_id_col : str
        Geography identifier column name.

    Returns
    -------
    pd.DataFrame
        Diagnostics with one row per CoC.
    """
    if geo_id_col not in pep_coc_df.columns:
        raise ValueError(f"Missing required column: {geo_id_col}")

    coverage_col = "coverage_ratio"
    has_coverage = coverage_col in pep_coc_df.columns

    results = []
    for geo_id, group in pep_coc_df.groupby(geo_id_col):
        years_total = group["year"].nunique() if "year" in group.columns else len(group)

        if has_coverage:
            years_covered = (group[coverage_col] >= min_coverage).sum()
            coverage_mean = group[coverage_col].mean()
        else:
            years_covered = years_total
            coverage_mean = 1.0

        results.append(
            {
                geo_id_col: geo_id,
                "years_total": years_total,
                "years_covered": int(years_covered),
                "coverage_ratio_mean": coverage_mean,
                "flag_low_coverage": coverage_mean < min_coverage,
            }
        )

    return pd.DataFrame(results).sort_values(geo_id_col).reset_index(drop=True)


def generate_text_summary(
    diagnostics_df: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> str:
    """Generate a human-readable text summary from PEP diagnostics.

    Parameters
    ----------
    diagnostics_df : pd.DataFrame
        Output from :func:`compute_coc_diagnostics`.
    geo_id_col : str
        Geography identifier column name.

    Returns
    -------
    str
        Multi-line text summary.
    """
    n_geos = len(diagnostics_df)
    if n_geos == 0:
        return "PEP Diagnostics: No geographies found."

    lines = [
        "PEP Population Diagnostics",
        "=" * 40,
        f"Total geographies: {n_geos}",
    ]

    if "years_total" in diagnostics_df.columns:
        years_range = diagnostics_df["years_total"].iloc[0]
        lines.append(f"Years per geography: {years_range}")

    if "flag_low_coverage" in diagnostics_df.columns:
        n_low = diagnostics_df["flag_low_coverage"].sum()
        lines.append(f"Low coverage flagged: {n_low}")

    if "coverage_ratio_mean" in diagnostics_df.columns:
        mean_cov = diagnostics_df["coverage_ratio_mean"].mean()
        lines.append(f"Mean coverage ratio: {mean_cov:.3f}")

    return "\n".join(lines)


def run_pep_diagnostics(
    pep_coc_path: Path | str,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    output_path: Path | str | None = None,
    *,
    geo_id_col: str = "coc_id",
) -> tuple[str, pd.DataFrame]:
    """Run PEP diagnostics on a CoC-level PEP parquet file.

    Parameters
    ----------
    pep_coc_path : Path or str
        Path to CoC-level PEP parquet file.
    min_coverage : float
        Minimum coverage ratio threshold.
    output_path : Path or str, optional
        If provided, save diagnostics to this path.
    geo_id_col : str
        Geography identifier column name.

    Returns
    -------
    tuple[str, pd.DataFrame]
        Text summary and diagnostics DataFrame.
    """
    pep_coc_path = Path(pep_coc_path)
    if not pep_coc_path.exists():
        raise FileNotFoundError(f"PEP CoC file not found: {pep_coc_path}")

    df = pd.read_parquet(pep_coc_path)
    diagnostics = compute_coc_diagnostics(df, min_coverage, geo_id_col=geo_id_col)
    summary = generate_text_summary(diagnostics, geo_id_col=geo_id_col)

    if output_path is not None:
        output_path = Path(output_path)
        provenance = ProvenanceBlock(
            extra={
                "dataset_type": "pep_diagnostics",
                "min_coverage": min_coverage,
                "source_path": str(pep_coc_path),
            },
        )
        write_parquet_with_provenance(diagnostics, output_path, provenance)
        logger.info(f"Saved PEP diagnostics to {output_path}")

    return summary, diagnostics
