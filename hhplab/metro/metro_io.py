"""Read and write curated metro definition artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import hhplab.naming as naming
from hhplab.metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
    PROFILE_NAME,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
    build_glynn_fox_subset_profile_df,
    build_metro_universe_df,
)
from hhplab.metro.metro_validate import (
    MetroValidationResult,
    validate_metro_artifacts,
    validate_metro_universe_artifacts,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance


def write_metro_artifacts(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
    *,
    validate: bool = True,
) -> tuple[Path, Path, Path]:
    """Generate and write curated metro definition parquet files.

    Builds all three tables from the in-code constants, validates them,
    and writes to the canonical paths under ``data/curated/metro/``.

    Parameters
    ----------
    definition_version : str
        Definition version to write (default: current version).
    base_dir : Path or str, optional
        Root data directory (defaults to ``data/``).
    validate : bool
        If True, validate before writing and raise on errors.

    Returns
    -------
    tuple[Path, Path, Path]
        Paths to (definitions, coc_membership, county_membership) files.

    Raises
    ------
    ValueError
        If validation fails and ``validate=True``.
    """
    defs_df = build_definitions_df()
    coc_df = build_coc_membership_df()
    county_df = build_county_membership_df()

    if validate:
        result = validate_metro_artifacts(defs_df, coc_df, county_df)
        if not result.passed:
            raise ValueError(f"Metro definition validation failed:\n{result.summary()}")

    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=definition_version,
        extra={
            "dataset_type": "metro_definition",
            "source": "glynn_fox_2019",
            "source_ref": "Glynn and Fox (2019), Table 1, p. 577",
        },
    )

    defs_path = naming.metro_definitions_path(definition_version, base_dir)
    coc_path = naming.metro_coc_membership_path(definition_version, base_dir)
    county_path = naming.metro_county_membership_path(definition_version, base_dir)

    write_parquet_with_provenance(defs_df, defs_path, provenance)
    write_parquet_with_provenance(coc_df, coc_path, provenance)
    write_parquet_with_provenance(county_df, county_path, provenance)

    return defs_path, coc_path, county_path


def read_metro_definitions(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read metro definitions from the curated parquet file."""
    path = naming.metro_definitions_path(definition_version, base_dir)
    return pd.read_parquet(path)


def read_metro_coc_membership(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read metro-to-CoC membership from the curated parquet file."""
    path = naming.metro_coc_membership_path(definition_version, base_dir)
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Metro membership artifact not found at {path}. "
            f"Run: hhplab generate metro --definition-version {definition_version}"
        ) from None


def read_metro_county_membership(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read metro-to-county membership from the curated parquet file."""
    path = naming.metro_county_membership_path(definition_version, base_dir)
    return pd.read_parquet(path)


def write_metro_universe_artifacts(
    metro_definition_version: str = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    profile_definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
    *,
    validate: bool = True,
    msa_definitions_df: pd.DataFrame | None = None,
) -> tuple[Path, Path]:
    """Generate and write canonical metro-universe and subset-profile artifacts."""
    if msa_definitions_df is None:
        from hhplab.msa.msa_io import read_msa_definitions

        try:
            msa_definitions_df = read_msa_definitions(metro_definition_version, base_dir)
        except FileNotFoundError:
            path = naming.msa_definitions_path(metro_definition_version, base_dir)
            raise FileNotFoundError(
                f"MSA definitions artifact not found at {path}. "
                "Run: hhplab generate msa "
                f"--definition-version {metro_definition_version}"
            ) from None

    universe_df = build_metro_universe_df(msa_definitions_df)
    subset_df = build_glynn_fox_subset_profile_df(msa_definitions_df)

    if validate:
        result = validate_metro_universe_artifacts(universe_df, subset_df)
        if not result.passed:
            raise ValueError(f"Metro-universe validation failed:\n{result.summary()}")

    universe_provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=metro_definition_version,
        extra={
            "dataset_type": "metro_universe",
            "source_geo_type": "msa",
            "source_definition_version": metro_definition_version,
        },
    )
    subset_provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=profile_definition_version,
        extra={
            "dataset_type": "metro_subset_profile",
            "profile": PROFILE_NAME,
            "metro_definition_version": metro_definition_version,
        },
    )

    universe_path = naming.metro_universe_path(metro_definition_version, base_dir)
    subset_path = naming.metro_subset_membership_path(
        profile_definition_version,
        metro_definition_version,
        base_dir,
    )
    write_parquet_with_provenance(universe_df, universe_path, universe_provenance)
    write_parquet_with_provenance(subset_df, subset_path, subset_provenance)
    return universe_path, subset_path


def read_metro_universe(
    definition_version: str = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read canonical metro-universe definitions from the curated parquet file."""
    path = naming.metro_universe_path(definition_version, base_dir)
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Metro-universe artifact not found at {path}. "
            "Run: hhplab generate metro-universe "
            f"--definition-version {definition_version}"
        ) from None


def read_metro_subset_membership(
    profile_definition_version: str = DEFINITION_VERSION,
    metro_definition_version: str = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read metro subset-profile membership from the curated parquet file."""
    path = naming.metro_subset_membership_path(
        profile_definition_version,
        metro_definition_version,
        base_dir,
    )
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Metro subset-profile artifact not found at {path}. "
            "Run: hhplab generate metro-universe "
            f"--definition-version {metro_definition_version}"
        ) from None


def validate_curated_metro_universe(
    metro_definition_version: str = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    profile_definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> MetroValidationResult:
    """Load curated metro-universe artifacts and validate them."""
    universe_df = read_metro_universe(metro_definition_version, base_dir)
    subset_df = read_metro_subset_membership(
        profile_definition_version,
        metro_definition_version,
        base_dir,
    )
    return validate_metro_universe_artifacts(universe_df, subset_df)


def validate_curated_metro(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> MetroValidationResult:
    """Load curated metro artifacts and validate them.

    Convenience function for validating already-written artifacts.
    """
    defs_df = read_metro_definitions(definition_version, base_dir)
    coc_df = read_metro_coc_membership(definition_version, base_dir)
    county_df = read_metro_county_membership(definition_version, base_dir)
    return validate_metro_artifacts(defs_df, coc_df, county_df)


def read_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    county_vintage: str | int = 2020,
    base_dir: Path | str | None = None,
):
    """Read materialized metro boundary polygons from the curated artifact."""
    from hhplab.metro.metro_boundaries import read_metro_boundaries as _read

    return _read(
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )


def validate_curated_metro_boundaries(
    definition_version: str = DEFINITION_VERSION,
    *,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
):
    """Load curated metro boundaries and validate them."""
    from hhplab.metro.metro_boundaries import validate_curated_metro_boundaries as _validate

    return _validate(
        definition_version=definition_version,
        county_vintage=county_vintage,
        base_dir=base_dir,
    )
