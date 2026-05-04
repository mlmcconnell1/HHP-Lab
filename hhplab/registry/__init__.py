"""Registry module for tracking boundary vintages."""

from hhplab.registry.boundary_registry import (
    RegistryHealthIssue,
    RegistryHealthReport,
    check_registry_health,
    latest_vintage,
    list_boundaries,
    register_vintage,
)
from hhplab.registry.schema import RegistryEntry

__all__ = [
    "RegistryEntry",
    "RegistryHealthIssue",
    "RegistryHealthReport",
    "check_registry_health",
    "latest_vintage",
    "list_boundaries",
    "register_vintage",
]
