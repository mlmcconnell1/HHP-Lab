"""Built-in geometry adapters for recipe validation."""

from __future__ import annotations

from coclab.recipe.adapters import (
    GeometryAdapterRegistry,
    ValidationDiagnostic,
)
from coclab.recipe.recipe_schema import GeometryRef


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
    return diags


def register_geometry_defaults(registry: GeometryAdapterRegistry) -> None:
    """Register built-in geometry adapters."""
    registry.register("coc", _validate_coc)
    registry.register("tract", _validate_tract)
    registry.register("county", _validate_county)
    registry.register("metro", _validate_metro)
