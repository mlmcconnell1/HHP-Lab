"""Built-in dataset adapters for recipe validation."""

from __future__ import annotations

from coclab.recipe.adapters import (
    DatasetAdapterRegistry,
    ValidationDiagnostic,
)
from coclab.recipe.recipe_schema import DatasetSpec


def _uses_materialized_artifact(spec: DatasetSpec) -> bool:
    """Whether the recipe points at a concrete on-disk artifact."""
    return spec.path is not None or spec.file_set is not None


def _validate_hud_pit(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate HUD PIT dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/pit: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "coc" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/pit: expected native_geometry type 'coc', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"vintage", "align"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"hud/pit: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params:
        valid_aligns = ("point_in_time_jan", "to_calendar_year")
        if spec.params["align"] not in valid_aligns:
            diags.append(
                ValidationDiagnostic(
                    "warning",
                    f"hud/pit: unknown align mode '{spec.params['align']}'; "
                    f"expected one of {valid_aligns}.",
                )
            )
    return diags


def _validate_census_acs5(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census ACS5 dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    return diags


def _validate_census_acs(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census ACS dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    return diags


def _validate_census_pep(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census PEP dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/pep: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/pep: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"series", "vintage", "align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/pep: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def _validate_zillow_zori(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Zillow ZORI dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"zillow/zori: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"zillow/zori: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"align", "series", "vintage"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"zillow/zori: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def register_dataset_defaults(registry: DatasetAdapterRegistry) -> None:
    """Register built-in dataset adapters."""
    registry.register("hud", "pit", _validate_hud_pit)
    registry.register("census", "acs5", _validate_census_acs5)
    registry.register("census", "acs", _validate_census_acs)
    registry.register("census", "pep", _validate_census_pep)
    registry.register("zillow", "zori", _validate_zillow_zori)
