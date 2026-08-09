"""Panel and measure artifact naming."""

from __future__ import annotations

from pathlib import Path

from .shared import (
    _normalize_acs_vintage,
    _normalize_definition_version,
)

__all__ = [
    "county_panel_filename",
    "geo_map_filename",
    "geo_panel_filename",
    "measures_filename",
    "measures_path",
    "metro_measures_acs1_filename",
    "metro_measures_acs1_path",
    "metro_measures_filename",
    "metro_panel_filename",
    "msa_coc_panel_filename",
    "msa_measures_filename",
    "msa_panel_filename",
    "panel_filename",
    "panel_path",
    "recipe_transform_filename",
    "sanctuary_msa_panel_covariate_filename",
    "sanctuary_msa_panel_covariate_path",
]

def county_panel_filename(
    start_year: int,
    end_year: int,
    county_vintage: str | int,
) -> str:
    """Generate filename for county-scoped panel.

    Pattern: ``panel__county__Y{start}-{end}@C{county}.parquet``
    """
    return f"panel__county__Y{start_year}-{end_year}@C{county_vintage}.parquet"

def geo_map_filename(
    start_year: int,
    end_year: int,
    *,
    geo_type: str = "coc",
    boundary_vintage: str | None = None,
    definition_version: str | None = None,
    profile_definition_version: str | None = None,
) -> str:
    """Return the HTML map filename for any supported analysis geography."""
    panel_name = geo_panel_filename(
        start_year,
        end_year,
        geo_type=geo_type,
        boundary_vintage=boundary_vintage,
        definition_version=definition_version,
        profile_definition_version=profile_definition_version,
    )
    return panel_name.replace("panel__", "map__", 1).replace(".parquet", ".html")

def geo_panel_filename(
    start_year: int,
    end_year: int,
    *,
    geo_type: str = "coc",
    boundary_vintage: str | None = None,
    definition_version: str | None = None,
    profile_definition_version: str | None = None,
) -> str:
    """Return the panel filename for any supported analysis geography.

    For ``geo_type="coc"``, delegates to :func:`panel_filename`.
    For ``geo_type="metro"``, delegates to :func:`metro_panel_filename`.
    For ``geo_type="msa"``, delegates to :func:`msa_panel_filename`.
    For ``geo_type="county"``, delegates to :func:`county_panel_filename`.
    """
    if geo_type == "coc":
        if boundary_vintage is None:
            raise ValueError("boundary_vintage is required for geo_type='coc'")
        return panel_filename(start_year, end_year, boundary_vintage)
    if geo_type == "metro":
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='metro'")
        return metro_panel_filename(
            start_year,
            end_year,
            definition_version,
            profile_definition_version=profile_definition_version,
        )
    if geo_type == "msa":
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='msa'")
        return msa_panel_filename(start_year, end_year, definition_version)
    if geo_type == "county":
        if boundary_vintage is None:
            raise ValueError("boundary_vintage is required for geo_type='county'")
        return county_panel_filename(start_year, end_year, boundary_vintage)
    raise ValueError(f"Unsupported geo_type: {geo_type!r}")

def measures_filename(
    acs_vintage: str,
    boundary_vintage: str,
    tract_vintage: str | int | None = None,
    alignment_year: int | None = None,
) -> str:
    """Generate filename for CoC measures dataset.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        tract_vintage: Optional tract vintage used in crosswalk
        alignment_year: Optional alignment year for window_center_year mode.
            When the ACS end year differs from the boundary year, shows
            which hub year the ACS vintage was aligned to.
            E.g., ``measures__A2015(2013)@B2013xT2010.parquet``

    Returns:
        Filename like 'measures__A2023@B2025.parquet' or
        'measures__A2023@B2025xT2023.parquet' if tract_vintage specified

    Note:
        The ACS vintage is normalized to just the end year (e.g., "2019-2023" -> "A2023")
    """
    # Normalize ACS vintage to end year
    acs_year = _normalize_acs_vintage(acs_vintage)

    acs_part = f"A{acs_year}"
    if alignment_year is not None:
        acs_part += f"({alignment_year})"

    if tract_vintage is not None:
        return f"measures__{acs_part}@B{boundary_vintage}xT{tract_vintage}.parquet"
    return f"measures__{acs_part}@B{boundary_vintage}.parquet"

def measures_path(
    acs_vintage: str,
    boundary_vintage: str,
    tract_vintage: str | int | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for measures file.

    Args:
        acs_vintage: ACS vintage
        boundary_vintage: CoC boundary vintage
        tract_vintage: Optional tract vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/measures/measures__A2023@B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / measures_filename(acs_vintage, boundary_vintage, tract_vintage)
    )

def metro_measures_acs1_filename(
    acs1_vintage: int,
    definition_version: str,
) -> str:
    """Generate filename for metro ACS1 measures artifact (post-aggregation).

    The ``__acs1__`` segment prevents collision with ACS5 metro measures files.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")

    Returns:
        Filename like 'measures__metro__acs1__A2023@Dglynnfoxv1.parquet'
    """
    defn = _normalize_definition_version(definition_version)
    return f"measures__metro__acs1__A{acs1_vintage}@D{defn}.parquet"

def metro_measures_acs1_path(
    acs1_vintage: int,
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for metro ACS1 measures artifact.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/measures/measures__metro__acs1__A2023@Dglynnfoxv1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / metro_measures_acs1_filename(acs1_vintage, definition_version)
    )

def metro_measures_filename(
    acs_vintage: str,
    definition_version: str,
    tract_vintage: str | int | None = None,
) -> str:
    """Generate filename for metro-scoped ACS measures.

    Pattern: ``measures__metro__A{acs}@D{def}xT{tract}.parquet``

    The ``metro`` segment prevents collision with CoC measures files.
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    defn = _normalize_definition_version(definition_version)
    if tract_vintage is not None:
        return f"measures__metro__A{acs_year}@D{defn}xT{tract_vintage}.parquet"
    return f"measures__metro__A{acs_year}@D{defn}.parquet"

def metro_panel_filename(
    start_year: int,
    end_year: int,
    definition_version: str,
    profile_definition_version: str | None = None,
) -> str:
    """Generate filename for metro-scoped panel.

    Pattern: ``panel__metro__Y{start}-{end}@D{def}[xS{subset}].parquet``
    """
    defn = _normalize_definition_version(definition_version)
    subset = ""
    if profile_definition_version is not None:
        subset = f"xS{_normalize_definition_version(profile_definition_version)}"
    return f"panel__metro__Y{start_year}-{end_year}@D{defn}{subset}.parquet"

def msa_coc_panel_filename(
    start_year: int,
    end_year: int,
    coc_boundary_vintage: str | int,
    msa_definition_version: str,
) -> str:
    """Generate filename for MSA-to-CoC containment panels.

    Pattern: ``panel__msa-coc__Y{start}-{end}@B{boundary}xM{def}.parquet``
    """
    defn = _normalize_definition_version(msa_definition_version)
    return f"panel__msa-coc__Y{start_year}-{end_year}@B{coc_boundary_vintage}xM{defn}.parquet"

def msa_measures_filename(
    acs_vintage: str,
    definition_version: str,
    tract_vintage: str | int | None = None,
) -> str:
    """Generate filename for MSA-scoped ACS5 measures.

    Pattern: ``measures__msa__A{acs}@M{def}xT{tract}.parquet``.
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    defn = _normalize_definition_version(definition_version)
    if tract_vintage is not None:
        return f"measures__msa__A{acs_year}@M{defn}xT{tract_vintage}.parquet"
    return f"measures__msa__A{acs_year}@M{defn}.parquet"

def msa_panel_filename(
    start_year: int,
    end_year: int,
    definition_version: str,
) -> str:
    """Generate filename for MSA-scoped panel.

    Pattern: ``panel__msa__Y{start}-{end}@M{def}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return f"panel__msa__Y{start_year}-{end_year}@M{defn}.parquet"

def panel_filename(
    start_year: int,
    end_year: int,
    boundary_vintage: str,
) -> str:
    """Generate filename for CoC panel dataset.

    Args:
        start_year: First year in panel (e.g., 2015)
        end_year: Last year in panel (e.g., 2024)
        boundary_vintage: Target CoC boundary vintage (e.g., "2025")

    Returns:
        Filename like 'panel__Y2015-2024@B2025.parquet'
    """
    return f"panel__Y{start_year}-{end_year}@B{boundary_vintage}.parquet"

def panel_path(
    start_year: int,
    end_year: int,
    boundary_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for panel file.

    Args:
        start_year: First panel year
        end_year: Last panel year
        boundary_vintage: Target CoC boundary vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/panel/panel__Y2015-2024@B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "panel" / panel_filename(start_year, end_year, boundary_vintage)

def recipe_transform_filename(
    transform_id: str,
    base_geo_type: str,
    definition_version: str,
    *,
    base_vintage: str | int | None = None,
    subset_definition_version: str | None = None,
) -> str:
    """Return the filename for recipe-cache generated transform artifacts."""
    base_suffix = base_geo_type
    if base_vintage is not None:
        base_suffix = f"{base_suffix}_{base_vintage}"
    definition = definition_version
    if subset_definition_version:
        definition = f"{definition}__subset_{subset_definition_version}"
    return f"{transform_id}__{base_suffix}__{definition}.parquet"

def sanctuary_msa_panel_covariate_filename(
    source_date: str,
    msa_definition_version: str,
) -> str:
    """Canonical filename for panel-ready DOJ sanctuary MSA covariates."""
    compact_date = source_date.replace("-", "")
    return f"sanctuary_msa_panel__D{compact_date}xM{msa_definition_version}.parquet"

def sanctuary_msa_panel_covariate_path(
    source_date: str,
    msa_definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for panel-ready DOJ sanctuary MSA covariates."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "sanctuary"
        / sanctuary_msa_panel_covariate_filename(source_date, msa_definition_version)
    )


# =============================================================================
# BLS LAUS metro artifact filenames
# =============================================================================

