"""Read and write curated Census MSA definition artifacts."""

from __future__ import annotations

from pathlib import Path

import httpx
import pandas as pd

import hhplab.artifacts.naming.naming as naming
from hhplab.geographies.msa.msa_definitions import (
    DEFINITION_VERSION,
    HISTORICAL_DEFINITION_VERSION,
    HISTORICAL_OMB_REF,
    HISTORICAL_STANDARDS_BASIS,
    build_county_membership_df,
    build_definitions_df,
    get_msa_source_spec,
    parse_delineation_source,
)
from hhplab.geographies.msa.msa_validate import MSAValidationResult, validate_msa_artifacts
from hhplab.registry.source_registry import check_source_changed, register_source
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.storage.raw_snapshot import persist_file_snapshot


def download_delineation_rows(
    definition_version: str = DEFINITION_VERSION,
    raw_root: Path | None = None,
    source_file: Path | str | None = None,
) -> tuple[pd.DataFrame, str, int, Path]:
    """Download or read and parse the selected Census MSA delineation source."""
    spec = get_msa_source_spec(definition_version)
    if source_file is not None:
        source_path = Path(source_file)
        raw_content = source_path.read_bytes()
        source_url = spec.source_url
    else:
        source_url = spec.source_url
        with httpx.Client(timeout=300.0) as client:
            response = client.get(source_url, follow_redirects=True)
            response.raise_for_status()
            raw_content = response.content

    raw_path, content_sha256, content_size = persist_file_snapshot(
        raw_content,
        "census_cbsa",
        spec.workbook_filename,
        subdirs=(str(spec.delineation_file_year),),
        raw_root=raw_root,
    )

    changed, details = check_source_changed(
        source_type="census_cbsa",
        source_url=source_url,
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

    delineation_rows = parse_delineation_source(str(raw_path), definition_version)
    return delineation_rows, content_sha256, content_size, raw_path


def write_msa_artifacts(
    definition_version: str = DEFINITION_VERSION,
    base_dir: Path | str | None = None,
    *,
    validate: bool = True,
    raw_root: Path | None = None,
    delineation_df: pd.DataFrame | None = None,
    source_file: Path | str | None = None,
) -> tuple[Path, Path]:
    """Generate and write curated MSA definition parquet files."""
    downloaded_source = delineation_df is None
    if downloaded_source:
        delineation_df, content_sha256, content_size, raw_path = download_delineation_rows(
            definition_version=definition_version,
            raw_root=raw_root,
            source_file=source_file,
        )
    else:
        content_sha256 = ""
        content_size = 0
        raw_path = Path("")

    spec = get_msa_source_spec(definition_version)
    defs_df = build_definitions_df(delineation_df, definition_version)
    county_df = build_county_membership_df(delineation_df, definition_version)

    if validate:
        result = validate_msa_artifacts(defs_df, county_df, definition_version)
        if not result.passed:
            raise ValueError(f"MSA definition validation failed:\n{result.summary()}")

    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=definition_version,
        extra={
            "dataset_type": "msa_definition",
            "source": spec.source_slug,
            "source_ref": spec.source_ref,
            "source_url": spec.source_url,
            "source_release_date": spec.source_release_date,
            "definition_calendar_year": spec.geometry_vintage,
            "county_boundary_vintage": spec.geometry_vintage,
            "raw_sha256": content_sha256 or None,
            "raw_file_size": content_size or None,
            "raw_snapshot_path": str(raw_path) if downloaded_source else None,
            **(
                {
                    "standards_basis": HISTORICAL_STANDARDS_BASIS,
                    "omb_bulletin_ref": HISTORICAL_OMB_REF,
                }
                if definition_version == HISTORICAL_DEFINITION_VERSION
                else {}
            ),
        },
    )

    defs_path = naming.msa_definitions_path(definition_version, base_dir)
    county_path = naming.msa_county_membership_path(definition_version, base_dir)
    write_parquet_with_provenance(defs_df, defs_path, provenance)
    write_parquet_with_provenance(county_df, county_path, provenance)

    if downloaded_source:
        register_source(
            source_type="census_cbsa",
            source_url=spec.source_url,
            source_name=spec.source_name,
            raw_sha256=content_sha256,
            file_size=content_size,
            local_path=str(raw_path),
            metadata={
                "definition_version": definition_version,
                "source_release_date": spec.source_release_date,
                "county_boundary_vintage": spec.geometry_vintage,
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
    path = naming.msa_definitions_path(definition_version, base_dir)
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"MSA definitions artifact not found at {path}. "
            f"Run: hhplab generate msa --definition-version {definition_version}"
        ) from None


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
    return validate_msa_artifacts(defs_df, county_df, definition_version)
