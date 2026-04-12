"""Runtime adapter registries for recipe semantic validation.

The recipe schema (recipe_schema.py) handles *structural* validation.
These registries handle *semantic* validation — checking whether
referenced geometry types and dataset providers actually have
adapters that know how to process them.

Adapters are registered as callables that accept a GeometryRef or
DatasetSpec and return a list of diagnostic strings (empty = valid).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef, RecipeV1


@dataclass
class ValidationDiagnostic:
    """A single validation finding."""

    level: str  # "error" or "warning"
    message: str


# Type aliases for adapter callables
GeometryAdapter = Callable[[GeometryRef], list[ValidationDiagnostic]]
DatasetAdapter = Callable[[DatasetSpec], list[ValidationDiagnostic]]


class GeometryAdapterRegistry:
    """Registry for geometry type validation adapters.

    Each registered adapter handles a specific geometry type string
    (e.g., "coc", "tract", "county"). When validate() is called,
    the registry looks up the adapter for the ref's type and delegates.

    If no adapter is registered for a type, validation returns an error
    diagnostic indicating the type is unknown.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, GeometryAdapter] = {}

    def register(self, geometry_type: str, adapter: GeometryAdapter) -> None:
        """Register an adapter for a geometry type."""
        self._adapters[geometry_type] = adapter

    def validate(self, ref: GeometryRef) -> list[ValidationDiagnostic]:
        """Validate a GeometryRef against the registered adapter."""
        adapter = self._adapters.get(ref.type)
        if adapter is None:
            return [
                ValidationDiagnostic(
                    level="error",
                    message=f"No geometry adapter registered for type '{ref.type}'.",
                )
            ]
        return adapter(ref)

    def registered_types(self) -> list[str]:
        """Return sorted list of registered geometry types."""
        return sorted(self._adapters.keys())

    def reset(self) -> None:
        """Clear all registered adapters (useful for testing)."""
        self._adapters.clear()


class DatasetAdapterRegistry:
    """Registry for dataset validation adapters.

    Each registered adapter handles a specific (provider, product) pair.
    When validate() is called, the registry looks up the adapter and
    delegates semantic validation of version, params, native_geometry, etc.

    If no adapter is registered for a (provider, product) pair, validation
    returns an error diagnostic.
    """

    def __init__(self) -> None:
        self._adapters: dict[tuple[str, str], DatasetAdapter] = {}

    def register(self, provider: str, product: str, adapter: DatasetAdapter) -> None:
        """Register an adapter for a (provider, product) pair."""
        self._adapters[(provider, product)] = adapter

    def validate(self, spec: DatasetSpec) -> list[ValidationDiagnostic]:
        """Validate a DatasetSpec against the registered adapter."""
        key = (spec.provider, spec.product)
        adapter = self._adapters.get(key)
        if adapter is None:
            return [
                ValidationDiagnostic(
                    level="error",
                    message=(
                        f"No dataset adapter registered for "
                        f"provider='{spec.provider}', product='{spec.product}'."
                    ),
                )
            ]
        return adapter(spec)

    def registered_products(self) -> list[tuple[str, str]]:
        """Return sorted list of registered (provider, product) pairs."""
        return sorted(self._adapters.keys())

    def reset(self) -> None:
        """Clear all registered adapters (useful for testing)."""
        self._adapters.clear()


def validate_recipe_adapters(
    recipe: RecipeV1,
    geometry_registry: GeometryAdapterRegistry,
    dataset_registry: DatasetAdapterRegistry,
) -> list[ValidationDiagnostic]:
    """Validate all geometry refs and dataset specs in a recipe.

    Collects diagnostics from both registries for every geometry
    reference and dataset specification in the recipe.

    Parameters
    ----------
    recipe : RecipeV1
        A structurally valid recipe (already passed Pydantic validation).
    geometry_registry : GeometryAdapterRegistry
        Registry with geometry adapters.
    dataset_registry : DatasetAdapterRegistry
        Registry with dataset adapters.

    Returns
    -------
    list[ValidationDiagnostic]
        All diagnostics found. Empty list means fully valid.
    """
    diagnostics: list[ValidationDiagnostic] = []

    # Validate all geometry refs found in targets
    for target in recipe.targets:
        diagnostics.extend(geometry_registry.validate(target.geometry))

    # Validate geometry refs in transforms (from and to)
    for transform in recipe.transforms:
        diagnostics.extend(geometry_registry.validate(transform.from_))
        diagnostics.extend(geometry_registry.validate(transform.to))

    # Validate dataset specs
    for _dataset_id, dataset_spec in recipe.datasets.items():
        diagnostics.extend(geometry_registry.validate(dataset_spec.native_geometry))
        diagnostics.extend(dataset_registry.validate(dataset_spec))

    return diagnostics


# Module-level default registries (can be used as singletons or replaced in tests)
geometry_registry = GeometryAdapterRegistry()
dataset_registry = DatasetAdapterRegistry()
