"""Read and write curated metro definition artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from coclab import naming
from coclab.metro.definitions import (
    DEFINITION_VERSION,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
)
from coclab.metro.validate import MetroValidationResult, validate_metro_artifacts
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance


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
            raise ValueError(
                f"Metro definition validation failed:\n{result.summary()}"
            )

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
            f"Run: coclab generate metro --definition-version {definition_version}"
        ) from None


def read_metro_county_membership(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read metro-to-county membership from the curated parquet file."""
    path = naming.metro_county_membership_path(definition_version, base_dir)
    return pd.read_parquet(path)


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
