"""Shared panel finalization layer.

Canonical panel shaping, dtype enforcement, boundary-change derivation,
source labeling, and column ordering live here so both the legacy
``build_panel`` path and the recipe executor share a single
implementation.

Typical usage::

    from hhplab.panel.finalize import finalize_panel

    panel = finalize_panel(df, geo_type="coc")
"""

from __future__ import annotations

import logging

import pandas as pd

from hhplab.analysis_geo import GEO_TYPE_METRO, GEO_TYPE_MSA

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical column ordering
# ---------------------------------------------------------------------------

COC_PANEL_COLUMNS: list[str] = [
    "coc_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "unemployment_rate",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

METRO_PANEL_COLUMNS: list[str] = [
    "metro_id",
    "metro_name",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "unemployment_rate_acs1",
    "labor_force",
    "employed",
    "unemployed",
    "unemployment_rate",
    "coverage_ratio",
    "boundary_changed",
    "acs1_vintage_used",
    "laus_vintage_used",
    "source",
]

MSA_PANEL_COLUMNS: list[str] = [
    "msa_id",
    "cbsa_code",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
    "acs5_vintage_used",
    "tract_vintage_used",
    "alignment_type",
    "weighting_method",
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "population",
    "coverage_ratio",
    "boundary_changed",
    "source",
]

ZORI_COLUMNS: list[str] = [
    "zori_coc",
    "zori_coverage_ratio",
    "zori_is_eligible",
    "zori_excluded_reason",
    "rent_to_income",
]

ZORI_PROVENANCE_COLUMNS: list[str] = [
    "rent_metric",
    "rent_alignment",
    "zori_min_coverage",
]


# ---------------------------------------------------------------------------
# Preferred column aliases for recipe outputs (coclab-t9rp)
# ---------------------------------------------------------------------------
# When a panel mixes data from multiple providers, these aliases make the
# provenance of each column explicit.

RECIPE_COLUMN_ALIASES: dict[str, str] = {
    "total_population": "total_population_acs5",
    "adult_population": "adult_population_acs5",
    "population_below_poverty": "population_below_poverty_acs5",
    "median_household_income": "median_household_income_acs5",
    "median_gross_rent": "median_gross_rent_acs5",
    "population": "pep_population",
    "zori_coc": "zori",
}

# Reverse mapping for legacy compatibility checks.
_ALIAS_REVERSE: dict[str, str] = {v: k for k, v in RECIPE_COLUMN_ALIASES.items()}


# ---------------------------------------------------------------------------
# Dtype specification
# ---------------------------------------------------------------------------
# Maps column names to their canonical pandas dtype.  Columns marked
# ``"string"`` use pandas' nullable StringDtype; ``"Int64"`` is nullable
# integer.  Plain ``str`` / ``int`` / ``bool`` use numpy-backed types.

_PANEL_DTYPE_SPEC: dict[str, str | type] = {
    # Geo identifiers
    "coc_id": "str",
    "metro_id": "str",
    "msa_id": "str",
    "geo_id": "str",
    "geo_type": "str",
    "metro_name": "str",
    "cbsa_code": "str",
    # Temporal
    "year": "int",
    # PIT counts
    "pit_total": "int",
    "pit_sheltered": "Int64",
    "pit_unsheltered": "Int64",
    # Vintage metadata
    "boundary_vintage_used": "str",
    "definition_version_used": "str",
    "acs5_vintage_used": "str",
    "tract_vintage_used": "string",
    "alignment_type": "string",
    "weighting_method": "str",
    # Derived flags
    "boundary_changed": "bool",
    # Source label
    "source": "str",
    # ACS1 metro extras
    "acs1_vintage_used": "string",
    "laus_vintage_used": "string",
}


# ---------------------------------------------------------------------------
# Boundary-change detection
# ---------------------------------------------------------------------------

def detect_boundary_changes(
    df: pd.DataFrame,
    *,
    geo_col: str = "coc_id",
    vintage_col: str = "boundary_vintage_used",
) -> pd.Series:
    """Detect vintage changes between consecutive years for each geo unit.

    Returns a boolean Series: ``True`` when the vintage column value
    differs from the prior year for the same geography.  The first
    year for each geography is always ``False``.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    if geo_col not in df.columns or vintage_col not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)

    sorted_df = df.sort_values([geo_col, "year"]).copy()
    sorted_df["_prior_vintage"] = sorted_df.groupby(geo_col)[vintage_col].shift(1)
    boundary_changed = sorted_df["_prior_vintage"].notna() & (
        sorted_df[vintage_col] != sorted_df["_prior_vintage"]
    )
    return boundary_changed.reindex(df.index)


# ---------------------------------------------------------------------------
# Alignment-type classification
# ---------------------------------------------------------------------------

def determine_alignment_type(pit_year: int, boundary_vintage: str) -> str:
    """Classify alignment type for a PIT year and boundary vintage.

    Returns ``"period_faithful"`` when they match, ``"retrospective"``
    when the boundary vintage is newer, or ``"custom"`` otherwise.
    """
    if boundary_vintage == str(pit_year):
        return "period_faithful"
    try:
        boundary_year = int(boundary_vintage)
    except (TypeError, ValueError):
        return "custom"
    if boundary_year >= pit_year:
        return "retrospective"
    return "custom"


# ---------------------------------------------------------------------------
# Panel finalization
# ---------------------------------------------------------------------------

def _resolve_column_order(
    geo_type: str,
    include_zori: bool,
    extra_columns: list[str] | None,
    canonical_columns: list[str] | None = None,
) -> list[str]:
    """Return the canonical column list for the given panel type."""
    if canonical_columns is not None:
        columns = list(canonical_columns)
    elif geo_type == GEO_TYPE_METRO:
        columns = list(METRO_PANEL_COLUMNS)
    elif geo_type == GEO_TYPE_MSA:
        columns = list(MSA_PANEL_COLUMNS)
    else:
        columns = list(COC_PANEL_COLUMNS)

    if include_zori and canonical_columns is None:
        columns += ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS

    if extra_columns:
        for col in extra_columns:
            if col not in columns:
                columns.append(col)

    return columns


def _apply_dtype_spec(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their canonical dtypes where the column exists."""
    for col, dtype in _PANEL_DTYPE_SPEC.items():
        if col not in df.columns:
            continue
        try:
            df[col] = df[col].astype(dtype)
        except (ValueError, TypeError):
            logger.debug("Could not cast %s to %s", col, dtype)
    return df


def _default_source_label(geo_type: str) -> str:
    """Return the default source label for a geo type."""
    if geo_type == GEO_TYPE_METRO:
        return "metro_panel"
    if geo_type == GEO_TYPE_MSA:
        return "msa_panel"
    return "hhplab_panel"


def finalize_panel(
    df: pd.DataFrame,
    *,
    geo_type: str,
    include_zori: bool = False,
    source_label: str | None = None,
    add_boundary_changed: bool = True,
    column_aliases: dict[str, str] | None = None,
    extra_columns: list[str] | None = None,
    canonical_columns: list[str] | None = None,
    ensure_canonical_columns: bool = True,
) -> pd.DataFrame:
    """Apply canonical panel finalization.

    This is the single shared path for panel shaping used by both the
    legacy ``build_panel`` helper and the recipe executor.  It performs:

    1. Boundary-change detection (unless already present or disabled).
    2. Source labeling (fills ``source`` column if not already set).
    3. Ensures all canonical columns exist (fills missing with ``NA``).
    4. Column ordering to canonical order.
    5. Dtype enforcement.
    6. Column alias renaming (optional, for recipe outputs).

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame to finalize.
    geo_type : str
        ``"coc"`` or ``"metro"``.
    include_zori : bool
        Whether ZORI columns should be included in the output.
    source_label : str, optional
        Override the default source label.
    add_boundary_changed : bool
        If True and ``boundary_changed`` is not already in the
        DataFrame, detect and add it.
    column_aliases : dict[str, str], optional
        Column rename mapping applied after all other finalization.
        Keys are current column names, values are new names.
    extra_columns : list[str], optional
        Additional columns to preserve beyond the canonical set
        (e.g. ``"zori_max_geo_contribution"``).
    canonical_columns : list[str], optional
        Override the default canonical ordering for the given
        ``geo_type``. Useful for recipe outputs that want a preferred
        order without inheriting the legacy union schema.
    ensure_canonical_columns : bool
        When True (default), create any missing canonical columns and
        fill them with ``NA``. When False, only existing columns are
        reordered.

    Returns
    -------
    pd.DataFrame
        Finalized panel with canonical column ordering and dtypes.
    """
    result = df.copy()
    if geo_type == GEO_TYPE_METRO:
        geo_col = "metro_id"
    elif geo_type == GEO_TYPE_MSA:
        geo_col = "msa_id"
    else:
        geo_col = "coc_id"
    vintage_col = (
        "definition_version_used"
        if geo_type in {GEO_TYPE_METRO, GEO_TYPE_MSA}
        else "boundary_vintage_used"
    )

    # 1. Boundary-change detection
    if add_boundary_changed and "boundary_changed" not in result.columns:
        result["boundary_changed"] = detect_boundary_changes(
            result, geo_col=geo_col, vintage_col=vintage_col,
        )

    # 2. Source labeling
    if "source" not in result.columns or result["source"].isna().all():
        result["source"] = source_label or _default_source_label(geo_type)

    # 3. Resolve canonical column order and ensure all exist
    canonical = _resolve_column_order(
        geo_type,
        include_zori,
        extra_columns,
        canonical_columns=canonical_columns,
    )
    if ensure_canonical_columns:
        for col in canonical:
            if col not in result.columns:
                result[col] = pd.NA

    # 4. Reorder: canonical columns first, then any remaining columns
    canonical_present = [col for col in canonical if col in result.columns]
    remaining = [col for col in result.columns if col not in canonical_present]
    result = result[canonical_present + remaining].copy()

    # 5. Dtype enforcement
    result = _apply_dtype_spec(result)

    # 6. Column aliases
    if column_aliases:
        rename_map = {k: v for k, v in column_aliases.items() if k in result.columns}
        if rename_map:
            result = result.rename(columns=rename_map)

    return result
