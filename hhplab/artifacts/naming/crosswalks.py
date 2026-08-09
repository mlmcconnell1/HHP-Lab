"""Crosswalk, containment, and geometry-join artifact naming."""

from __future__ import annotations

import re
from pathlib import Path

from .shared import (
    _abbreviate_weighting,
    _normalize_acs_vintage,
    _normalize_definition_version,
)

__all__ = [
    "coc_urban_area_detail_filename",
    "coc_urban_area_detail_path",
    "coc_urban_fraction_filename",
    "coc_urban_fraction_path",
    "containment_filename",
    "county_weights_filename",
    "county_weights_path",
    "county_xwalk_filename",
    "county_xwalk_path",
    "msa_coc_block_population_xwalk_filename",
    "msa_coc_block_population_xwalk_path",
    "msa_coc_coverage_filename",
    "msa_coc_coverage_path",
    "msa_coc_xwalk_filename",
    "msa_coc_xwalk_path",
    "msa_fractional_rollup_filename",
    "msa_fractional_rollup_path",
    "tract_mediated_county_xwalk_filename",
    "tract_mediated_county_xwalk_path",
    "tract_relationship_filename",
    "tract_xwalk_filename",
    "tract_xwalk_path",
]

def _containment_geometry_token(
    geometry_type: str,
    *,
    vintage: str | int | None,
    definition_version: str | None,
) -> str:
    if geometry_type == "coc":
        if vintage is None:
            raise ValueError("CoC containment filenames require a boundary vintage.")
        return f"B{vintage}"
    if geometry_type == "county":
        if vintage is None:
            raise ValueError("County containment filenames require a county vintage.")
        return f"C{vintage}"
    if geometry_type == "msa":
        if definition_version:
            return f"M{definition_version}"
        if vintage is None:
            raise ValueError("MSA containment filenames require a definition version or vintage.")
        return f"M{vintage}"
    raise ValueError(
        f"Unsupported containment geometry type '{geometry_type}'. "
        "Supported types: coc, county, msa."
    )

def _overlap_basis_token(overlap_bases: str | list[str] | tuple[str, ...]) -> str:
    if isinstance(overlap_bases, str):
        bases = [part for part in re.split(r"[-,+]", overlap_bases) if part]
    else:
        bases = list(overlap_bases)
    if not bases:
        raise ValueError("MSA-CoC coverage filenames require at least one overlap basis.")
    invalid = sorted(set(bases) - {"area", "population"})
    if invalid:
        raise ValueError(
            f"Unsupported MSA-CoC overlap basis values {invalid}; expected area and/or population."
        )
    ordered = [basis for basis in ("area", "population") if basis in set(bases)]
    return "-".join(ordered)

def _slug_output_id(output_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", output_id.strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-.")
    return normalized or "containment"

def _tract_mediated_denominator_token(
    *,
    acs_vintage: str | int | None,
    denominator_source: str,
    denominator_vintage: str | int | None,
) -> str:
    """Return the filename token for a tract-mediated denominator artifact."""
    if denominator_source == "acs":
        vintage = denominator_vintage if denominator_vintage is not None else acs_vintage
        if vintage is None:
            raise ValueError("ACS tract-mediated denominators require acs_vintage.")
        return f"A{_normalize_acs_vintage(str(vintage))}"
    if denominator_source == "decennial":
        if denominator_vintage is None:
            raise ValueError("Decennial tract-mediated denominators require denominator_vintage.")
        return f"N{denominator_vintage}"
    raise ValueError(
        "Unsupported tract-mediated denominator_source "
        f"{denominator_source!r}; expected 'acs' or 'decennial'."
    )

def coc_urban_area_detail_filename(
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
) -> str:
    """Generate filename for optional CoC-by-Urban-Area detail artifacts."""
    return (
        f"coc_urban_area_detail__N{decennial_vintage}@B{boundary_vintage}"
        f"xU{urban_area_vintage}xK{block_vintage}.parquet"
    )


# =============================================================================
# Crosswalks (join two geometry vintages)
# =============================================================================

def coc_urban_area_detail_path(
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for optional CoC-by-Urban-Area detail artifacts."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / coc_urban_area_detail_filename(
            boundary_vintage,
            urban_area_vintage,
            block_vintage,
            decennial_vintage,
        )
    )

def coc_urban_fraction_filename(
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
) -> str:
    """Generate filename for CoC-level urban population fractions.

    The ``U`` Urban Area token keeps this artifact distinct from ``B`` CoC
    boundary files while the ``N`` and ``K`` tokens name the population and
    block vintages used in the denominator.
    """
    return (
        f"coc_urban_fraction__N{decennial_vintage}@B{boundary_vintage}"
        f"xU{urban_area_vintage}xK{block_vintage}.parquet"
    )

def coc_urban_fraction_path(
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for CoC-level urban population fractions."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / coc_urban_fraction_filename(
            boundary_vintage,
            urban_area_vintage,
            block_vintage,
            decennial_vintage,
        )
    )

def containment_filename(
    *,
    container_type: str,
    candidate_type: str,
    output_id: str,
    container_vintage: str | int | None = None,
    candidate_vintage: str | int | None = None,
    definition_version: str | None = None,
) -> str:
    """Generate filename for a recipe containment-list output."""
    container_token = _containment_geometry_token(
        container_type,
        vintage=container_vintage,
        definition_version=definition_version,
    )
    candidate_token = _containment_geometry_token(
        candidate_type,
        vintage=candidate_vintage,
        definition_version=None,
    )
    return f"containment__{container_token}x{candidate_token}__{_slug_output_id(output_id)}.parquet"

def county_weights_filename(acs_vintage: str, weighting: str) -> str:
    """Generate filename for county-level ACS weights.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        weighting: Weighting method (e.g., "renter_households")

    Returns:
        Filename like 'county_weights__A2023__wrenter.parquet'
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    weight_abbrev = _abbreviate_weighting(weighting)
    return f"county_weights__A{acs_year}__w{weight_abbrev}.parquet"


# =============================================================================
# Path helpers (combine filename with directory)
# =============================================================================

def county_weights_path(
    acs_vintage: str,
    weighting: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for county weights file.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        weighting: Weighting method (e.g., "renter_households")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/acs/county_weights__A2023__wrenter.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "acs" / county_weights_filename(acs_vintage, weighting)


# =============================================================================
# Helper functions
# =============================================================================

def county_xwalk_filename(boundary_vintage: str, county_vintage: str | int) -> str:
    """Generate filename for CoC-to-county crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        county_vintage: County geometry vintage (e.g., 2023)

    Returns:
        Filename like 'xwalk__B2025xC2023.parquet'
    """
    return f"xwalk__B{boundary_vintage}xC{county_vintage}.parquet"

def county_xwalk_path(
    boundary_vintage: str,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for county crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        county_vintage: County geometry vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/xwalks/xwalk__B2025xC2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "xwalks" / county_xwalk_filename(boundary_vintage, county_vintage)

def msa_coc_block_population_xwalk_filename(
    boundary_vintage: str | int,
    definition_version: str,
    county_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
) -> str:
    """Generate filename for a block-population CoC-to-MSA crosswalk."""
    return (
        f"msa_coc_xwalk__N{decennial_vintage}@B{boundary_vintage}"
        f"xM{definition_version}xC{county_vintage}xK{block_vintage}"
        "__basis-block_population.parquet"
    )

def msa_coc_block_population_xwalk_path(
    boundary_vintage: str | int,
    definition_version: str,
    county_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for a block-population CoC-to-MSA crosswalk."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "xwalks"
        / msa_coc_block_population_xwalk_filename(
            boundary_vintage,
            definition_version,
            county_vintage,
            block_vintage,
            decennial_vintage,
        )
    )

def msa_coc_coverage_filename(
    year: str | int,
    boundary_vintage: str | int,
    definition_version: str,
    county_vintage: str | int,
    top_n: str | int,
    overlap_bases: str | list[str] | tuple[str, ...],
) -> str:
    """Generate filename for MSA-CoC overlap coverage artifacts.

    The basis token is explicit so area-only and population-enabled artifacts
    cannot be confused during agent discovery.
    """
    basis_token = _overlap_basis_token(overlap_bases)
    return (
        f"msa_coc_coverage__Y{year}@B{boundary_vintage}xM{definition_version}"
        f"xC{county_vintage}__top{top_n}__basis-{basis_token}.parquet"
    )

def msa_coc_coverage_path(
    year: str | int,
    boundary_vintage: str | int,
    definition_version: str,
    county_vintage: str | int,
    top_n: str | int,
    overlap_bases: str | list[str] | tuple[str, ...],
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for an MSA-CoC overlap coverage artifact."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "msa"
        / msa_coc_coverage_filename(
            year,
            boundary_vintage,
            definition_version,
            county_vintage,
            top_n,
            overlap_bases,
        )
    )

def msa_coc_xwalk_filename(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str | int,
) -> str:
    """Generate filename for CoC-to-MSA crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        definition_version: MSA definition version (e.g., "census_msa_2023")
        county_vintage: County geometry vintage used to derive the overlap

    Returns:
        Filename like
        ``msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet``
    """
    return f"msa_coc_xwalk__B{boundary_vintage}xM{definition_version}xC{county_vintage}.parquet"

def msa_coc_xwalk_path(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for CoC-to-MSA crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        definition_version: MSA definition version
        county_vintage: County geometry vintage used to derive the overlap
        base_dir: Base data directory (defaults to ``data``)

    Returns:
        Path like
        ``data/curated/xwalks/msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet``
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "xwalks"
        / msa_coc_xwalk_filename(boundary_vintage, definition_version, county_vintage)
    )

def msa_fractional_rollup_filename(
    start_year: int,
    end_year: int,
    measure_set_id: str,
    allocation_basis: str,
    coc_boundary_vintage: str | int,
    msa_definition_version: str,
    county_vintage: str | int,
    block_vintage: str | int | None = None,
    decennial_vintage: str | int | None = None,
) -> str:
    """Generate filename for CoC-to-MSA fractional rollup panels.

    Pattern:
    ``panel__msa-rollup-{measures}__Y{start}-{end}__basis-{basis}@B{boundary}xM{def}xC{county}[xK{block}xN{decennial}].parquet``
    """
    defn = _normalize_definition_version(msa_definition_version)
    measure_token = _normalize_definition_version(measure_set_id)
    basis_token = allocation_basis.replace("_", "-")
    suffix = f"@B{coc_boundary_vintage}xM{defn}xC{county_vintage}"
    if block_vintage is not None:
        suffix += f"xK{block_vintage}"
    if decennial_vintage is not None:
        suffix += f"xN{decennial_vintage}"
    return (
        f"panel__msa-rollup-{measure_token}__Y{start_year}-{end_year}"
        f"__basis-{basis_token}{suffix}.parquet"
    )

def msa_fractional_rollup_path(
    start_year: int,
    end_year: int,
    measure_set_id: str,
    allocation_basis: str,
    coc_boundary_vintage: str | int,
    msa_definition_version: str,
    county_vintage: str | int,
    block_vintage: str | int | None = None,
    decennial_vintage: str | int | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for a CoC-to-MSA fractional rollup panel."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "panel"
        / msa_fractional_rollup_filename(
            start_year,
            end_year,
            measure_set_id,
            allocation_basis,
            coc_boundary_vintage,
            msa_definition_version,
            county_vintage,
            block_vintage,
            decennial_vintage,
        )
    )

def tract_mediated_county_xwalk_filename(
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int | None = None,
    *,
    denominator_source: str = "acs",
    denominator_vintage: str | int | None = None,
) -> str:
    """Generate filename for tract-mediated county-to-CoC weights.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        county_vintage: County geometry vintage represented by county FIPS
        tract_vintage: Census tract geometry vintage used by the tract crosswalk
        acs_vintage: ACS denominator vintage (range or end year). Retained for
            backward-compatible callers using ACS denominators.
        denominator_source: Denominator source, either ``"acs"`` or ``"decennial"``.
        denominator_vintage: Explicit denominator vintage. Required for decennial
            denominators and optional for ACS denominators.

    Returns:
        Filename like ``xwalk_tract_mediated_county__A2023@B2025xC2023xT2020.parquet``
    """
    denominator_token = _tract_mediated_denominator_token(
        acs_vintage=acs_vintage,
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
    )
    return (
        f"xwalk_tract_mediated_county__{denominator_token}@B{boundary_vintage}"
        f"xC{county_vintage}xT{tract_vintage}.parquet"
    )

def tract_mediated_county_xwalk_path(
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int | None = None,
    base_dir: Path | str | None = None,
    *,
    denominator_source: str = "acs",
    denominator_vintage: str | int | None = None,
) -> Path:
    """Get canonical path for tract-mediated county-to-CoC weights.

    Args:
        boundary_vintage: CoC boundary vintage
        county_vintage: County vintage encoded by county FIPS
        tract_vintage: Tract geometry vintage used by the tract crosswalk
        acs_vintage: ACS denominator vintage or range
        base_dir: Base data directory (defaults to "data")
        denominator_source: Denominator source, either ``"acs"`` or ``"decennial"``.
        denominator_vintage: Explicit denominator vintage.

    Returns:
        Path like
        data/curated/xwalks/xwalk_tract_mediated_county__A2023@B2025xC2020xT2020.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "xwalks"
        / tract_mediated_county_xwalk_filename(
            boundary_vintage,
            county_vintage,
            tract_vintage,
            acs_vintage,
            denominator_source=denominator_source,
            denominator_vintage=denominator_vintage,
        )
    )

def tract_relationship_filename(
    from_vintage: str | int = 2010,
    to_vintage: str | int = 2020,
) -> str:
    """Generate filename for Census tract relationship file.

    Args:
        from_vintage: Source tract vintage (e.g., 2010)
        to_vintage: Target tract vintage (e.g., 2020)

    Returns:
        Filename like 'tract_relationship__T2010xT2020.parquet'
    """
    return f"tract_relationship__T{from_vintage}xT{to_vintage}.parquet"


# =============================================================================
# Derived datasets (compound notation)
# =============================================================================

def tract_xwalk_filename(boundary_vintage: str, tract_vintage: str | int) -> str:
    """Generate filename for CoC-to-tract crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        tract_vintage: Tract geometry vintage (e.g., 2023)

    Returns:
        Filename like 'xwalk__B2025xT2023.parquet'
    """
    return f"xwalk__B{boundary_vintage}xT{tract_vintage}.parquet"

def tract_xwalk_path(
    boundary_vintage: str,
    tract_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for tract crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        tract_vintage: Tract geometry vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/xwalks/xwalk__B2025xT2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "xwalks" / tract_xwalk_filename(boundary_vintage, tract_vintage)

