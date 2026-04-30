"""Read and write curated Census MSA definition artifacts."""

from __future__ import annotations

from pathlib import Path

import httpx
import pandas as pd

import hhplab.naming as naming
from hhplab.msa.definitions import (
    DEFINITION_VERSION,
    DELINEATION_FILE_YEAR,
    SOURCE_NAME,
    SOURCE_REF,
    WORKBOOK_FILENAME,
    build_county_membership_df,
    build_definitions_df,
    parse_delineation_workbook,
)
from hhplab.msa.validate import MSAValidationResult, validate_msa_artifacts
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.raw_snapshot import persist_file_snapshot
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import CENSUS_MSA_DELINEATION_FILE_2023


def download_delineation_rows(
    raw_root: Path | None = None,
) -> tuple[pd.DataFrame, str, int, Path]:
    """Download and parse the official Census July 2023 delineation workbook."""
    with httpx.Client(timeout=300.0) as client:
        response = client.get(CENSUS_MSA_DELINEATION_FILE_2023, follow_redirects=True)
        response.raise_for_status()
        raw_content = response.content

    raw_path, content_sha256, content_size = persist_file_snapshot(
        raw_content,
        "census_cbsa",
        WORKBOOK_FILENAME,
        subdirs=(str(DELINEATION_FILE_YEAR),),
        raw_root=raw_root,
    )

    changed, details = check_source_changed(
        source_type="census_cbsa",
        source_url=CENSUS_MSA_DELINEATION_FILE_2023,
        current_sha256=content_sha256,
    )
    if changed:
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(
            "UPSTREAM DATA CHANGED: Census MSA delineation workbook changed since "
            "last download. Previous hash: %s... Current hash: %s... Last ingested: %s",
            details["previous_sha256"][:16],
            content_sha256[:16],
            details["previous_ingested_at"],
        )

    return parse_delineation_workbook(raw_content), content_sha256, content_size, raw_path


def write_msa_artifacts(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
    *,
    validate: bool = True,
    raw_root: Path | None = None,
    delineation_df: pd.DataFrame | None = None,
) -> tuple[Path, Path]:
    """Generate and write curated MSA definition parquet files."""
    downloaded_source = delineation_df is None
    if downloaded_source:
        delineation_df, content_sha256, content_size, raw_path = download_delineation_rows(
            raw_root=raw_root
        )
    else:
        content_sha256 = ""
        content_size = 0
        raw_path = Path("")

    defs_df = build_definitions_df(delineation_df)
    county_df = build_county_membership_df(delineation_df)

    if validate:
        result = validate_msa_artifacts(defs_df, county_df)
        if not result.passed:
            raise ValueError(f"MSA definition validation failed:\n{result.summary()}")

    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=definition_version,
        extra={
            "dataset_type": "msa_definition",
            "source": "census_msa_delineation_2023",
            "source_ref": SOURCE_REF,
        },
    )

    defs_path = naming.msa_definitions_path(definition_version, base_dir)
    county_path = naming.msa_county_membership_path(definition_version, base_dir)
    write_parquet_with_provenance(defs_df, defs_path, provenance)
    write_parquet_with_provenance(county_df, county_path, provenance)

    if downloaded_source:
        register_source(
            source_type="census_cbsa",
            source_url=CENSUS_MSA_DELINEATION_FILE_2023,
            source_name=SOURCE_NAME,
            raw_sha256=content_sha256,
            file_size=content_size,
            local_path=str(raw_path),
            metadata={
                "definition_version": definition_version,
                "curated_paths": [str(defs_path), str(county_path)],
                "msa_count": len(defs_df),
                "county_membership_rows": len(county_df),
            },
        )

    return defs_path, county_path


def read_msa_definitions(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read MSA definitions from the curated parquet file."""
    return pd.read_parquet(naming.msa_definitions_path(definition_version, base_dir))


def read_msa_county_membership(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read MSA-to-county membership from the curated parquet file."""
    path = naming.msa_county_membership_path(definition_version, base_dir)
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"MSA county membership artifact not found at {path}. "
            f"Run: hhplab generate msa --definition-version {definition_version}"
        ) from None


def validate_curated_msa(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
) -> MSAValidationResult:
    """Load curated MSA artifacts and validate them."""
    defs_df = read_msa_definitions(definition_version, base_dir)
    county_df = read_msa_county_membership(definition_version, base_dir)
    return validate_msa_artifacts(defs_df, county_df)
