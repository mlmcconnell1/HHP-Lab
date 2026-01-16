"""Validation functions for export bundle generation."""

from pathlib import Path

import pyarrow.parquet as pq

from coclab.export.types import BundleConfig, SelectionPlan


class ExportValidationError(Exception):
    """Raised when export validation fails."""

    pass


# Default expected columns for standard panel
DEFAULT_PANEL_COLUMNS = ["coc_id", "year", "pit_total"]

# Additional columns expected for ZORI-enhanced panels
ZORI_PANEL_COLUMNS = ["rent_to_income", "zori_is_eligible"]


def validate_panel_exists(path: Path) -> None:
    """
    Validate panel file exists and is readable.

    Args:
        path: Path to the panel parquet file.

    Raises:
        ExportValidationError: If file does not exist or is not readable.
    """
    if not path.exists():
        raise ExportValidationError(f"Panel file does not exist: {path}")

    if not path.is_file():
        raise ExportValidationError(f"Panel path is not a file: {path}")

    # Try to read the schema to verify it's a valid parquet file
    try:
        pq.read_schema(path)
    except Exception as e:
        raise ExportValidationError(
            f"Panel file is not readable as parquet: {path}. Error: {e}"
        ) from e


def _is_zori_panel(path: Path, columns: list[str]) -> bool:
    """
    Detect if a panel is a ZORI-enhanced panel.

    Detection is based on:
    - Filename containing 'zori' (case-insensitive)
    - Presence of ZORI-specific columns

    Args:
        path: Path to the panel file.
        columns: List of column names in the panel.

    Returns:
        True if this appears to be a ZORI panel.
    """
    # Check filename
    if "zori" in path.name.lower():
        return True

    # Check for ZORI-specific columns
    return any(col in columns for col in ZORI_PANEL_COLUMNS)


def validate_panel_schema(path: Path, expected_cols: list[str] | None = None) -> None:
    """
    Validate panel has expected columns.

    Default expected columns: coc_id, year, pit_total
    If ZORI panel (detected by filename or columns): also expect rent_to_income, zori_is_eligible

    Args:
        path: Path to the panel parquet file.
        expected_cols: Optional list of expected column names. If None, uses defaults.

    Raises:
        ExportValidationError: If expected columns are missing.
    """
    try:
        schema = pq.read_schema(path)
        columns = schema.names
    except Exception as e:
        raise ExportValidationError(f"Failed to read panel schema from {path}. Error: {e}") from e

    # Determine expected columns
    if expected_cols is not None:
        required_cols = expected_cols
    else:
        required_cols = list(DEFAULT_PANEL_COLUMNS)

        # If ZORI panel, add ZORI columns to requirements
        if _is_zori_panel(path, columns):
            required_cols.extend(ZORI_PANEL_COLUMNS)

    # Check for missing columns
    missing_cols = [col for col in required_cols if col not in columns]

    if missing_cols:
        raise ExportValidationError(
            f"Panel is missing expected columns: {missing_cols}. Available columns: {columns}"
        )


def validate_vintage_compatibility(panel_path: Path, config: BundleConfig) -> list[str]:
    """
    Best-effort check that panel vintages match config.

    Reads panel parquet metadata to check boundary_vintage, acs_vintage if present.

    Args:
        panel_path: Path to the panel parquet file.
        config: Bundle configuration with vintage settings.

    Returns:
        List of warning messages (empty if no compatibility issues found).
    """
    warnings: list[str] = []

    try:
        parquet_file = pq.ParquetFile(panel_path)
        metadata = parquet_file.schema_arrow.metadata
    except Exception:
        # If we can't read metadata, return a warning but don't fail
        warnings.append(f"Could not read parquet metadata from {panel_path} for vintage validation")
        return warnings

    if metadata is None:
        return warnings

    # Check boundary_vintage
    if config.boundary_vintage:
        panel_boundary_vintage = metadata.get(b"boundary_vintage")
        if panel_boundary_vintage:
            panel_value = panel_boundary_vintage.decode("utf-8")
            if panel_value != config.boundary_vintage:
                warnings.append(
                    f"Panel boundary_vintage ({panel_value}) does not match "
                    f"config boundary_vintage ({config.boundary_vintage})"
                )

    # Check acs_vintage
    if config.acs_vintage:
        panel_acs_vintage = metadata.get(b"acs_vintage")
        if panel_acs_vintage:
            panel_value = panel_acs_vintage.decode("utf-8")
            if panel_value != config.acs_vintage:
                warnings.append(
                    f"Panel acs_vintage ({panel_value}) does not match "
                    f"config acs_vintage ({config.acs_vintage})"
                )

    # Check tract_vintage
    if config.tract_vintage:
        panel_tract_vintage = metadata.get(b"tract_vintage")
        if panel_tract_vintage:
            panel_value = panel_tract_vintage.decode("utf-8")
            if panel_value != config.tract_vintage:
                warnings.append(
                    f"Panel tract_vintage ({panel_value}) does not match "
                    f"config tract_vintage ({config.tract_vintage})"
                )

    # Check county_vintage
    if config.county_vintage:
        panel_county_vintage = metadata.get(b"county_vintage")
        if panel_county_vintage:
            panel_value = panel_county_vintage.decode("utf-8")
            if panel_value != config.county_vintage:
                warnings.append(
                    f"Panel county_vintage ({panel_value}) does not match "
                    f"config county_vintage ({config.county_vintage})"
                )

    return warnings


def validate_selection_plan(plan: SelectionPlan) -> list[str]:
    """
    Validate selection plan.

    Checks:
    - At least one panel artifact if 'panel' role is expected
    - All source paths exist

    Args:
        plan: The selection plan to validate.

    Returns:
        List of error messages (empty if valid).
    """
    errors: list[str] = []

    # Check that at least one panel artifact exists
    if not plan.panel_artifacts:
        errors.append("Selection plan has no panel artifacts")

    # Gather all artifacts to check
    all_artifacts = (
        plan.panel_artifacts
        + plan.input_artifacts
        + plan.derived_artifacts
        + plan.diagnostic_artifacts
        + plan.codebook_artifacts
    )

    # Check that all source paths exist
    for artifact in all_artifacts:
        if not artifact.source_path.exists():
            errors.append(
                f"Artifact source path does not exist: {artifact.source_path} "
                f"(role: {artifact.role})"
            )

    return errors


def run_all_validations(plan: SelectionPlan, config: BundleConfig) -> tuple[list[str], list[str]]:
    """
    Run all validations.

    Args:
        plan: The selection plan to validate.
        config: The bundle configuration.

    Returns:
        Tuple of (errors, warnings).
        Errors should fail the export, warnings are informational.
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Validate selection plan
    plan_errors = validate_selection_plan(plan)
    errors.extend(plan_errors)

    # Validate each panel artifact
    for artifact in plan.panel_artifacts:
        panel_path = artifact.source_path

        # Validate panel exists
        try:
            validate_panel_exists(panel_path)
        except ExportValidationError as e:
            errors.append(str(e))
            continue  # Skip further validation for this panel

        # Validate panel schema
        try:
            validate_panel_schema(panel_path)
        except ExportValidationError as e:
            errors.append(str(e))

        # Check vintage compatibility (warnings only)
        vintage_warnings = validate_vintage_compatibility(panel_path, config)
        warnings.extend(vintage_warnings)

    return errors, warnings
