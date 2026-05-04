"""Built-in geometry adapters for recipe validation."""

from __future__ import annotations

from hhplab.recipe.adapters import (
    GeometryAdapterRegistry,
    ValidationDiagnostic,
)
from hhplab.recipe.schema_common import GeometryRef


def _validate_coc(ref: GeometryRef) -> list[ValidationDiagnostic]:
    """Validate a CoC geometry reference."""
    diags: list[ValidationDiagnostic] = []
    if ref.vintage is not None and ref.vintage < 2000:
        diags.append(
            ValidationDiagnostic(
                "warning", f"CoC vintage {ref.vintage} is unusually early."
            )
        )
    return diags


def _validate_tract(ref: GeometryRef) -> list[ValidationDiagnostic]:
    """Validate a Census tract geometry reference."""
    diags: list[ValidationDiagnostic] = []
    if ref.vintage is not None and ref.vintage % 10 != 0:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"Tract vintage {ref.vintage} is not a decennial year.",
            )
        )
    return diags


def _validate_county(ref: GeometryRef) -> list[ValidationDiagnostic]:
    """Validate a county geometry reference."""
    diags: list[ValidationDiagnostic] = []
    if ref.vintage is not None and ref.vintage % 10 != 0:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"County vintage {ref.vintage} is not a decennial year.",
            )
        )
    return diags


def _validate_metro(ref: GeometryRef) -> list[ValidationDiagnostic]:
    """Validate a metro geometry reference."""
    diags: list[ValidationDiagnostic] = []
    if not ref.source:
        diags.append(
            ValidationDiagnostic(
                "error",
                "Metro geometry must set source to the definition version "
                "(for example 'glynn_fox_v1').",
            )
        )
    if ref.vintage is not None:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"Metro geometry ignores vintage={ref.vintage}; "
                "use source for the synthetic geography definition version.",
            )
        )
    if (
        ref.subset_profile is None
        and ref.subset_profile_definition_version is not None
    ):
        diags.append(
            ValidationDiagnostic(
                "warning",
                "Metro geometry sets subset_profile_definition_version without "
                "subset_profile. The subset artifact will still resolve, but "
                "including subset_profile makes the recipe intent explicit.",
            )
        )
    return diags


def _validate_msa(ref: GeometryRef) -> list[ValidationDiagnostic]:
    """Validate an MSA geometry reference."""
    diags: list[ValidationDiagnostic] = []
    if not ref.source:
        diags.append(
            ValidationDiagnostic(
                "error",
                "MSA geometry must set source to the definition version "
                "(for example 'census_msa_2023').",
            )
        )
    if ref.vintage is not None:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"MSA geometry ignores vintage={ref.vintage}; "
                "use source for the MSA delineation version.",
            )
        )
    return diags


def register_geometry_defaults(registry: GeometryAdapterRegistry) -> None:
    """Register built-in geometry adapters."""
    registry.register("coc", _validate_coc)
    registry.register("tract", _validate_tract)
    registry.register("county", _validate_county)
    registry.register("metro", _validate_metro)
    registry.register("msa", _validate_msa)
