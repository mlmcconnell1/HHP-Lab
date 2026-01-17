"""Registry module for tracking boundary vintages."""

from coclab.registry.registry import (
    RegistryHealthIssue,
    RegistryHealthReport,
    check_registry_health,
    latest_vintage,
    list_boundaries,
    register_vintage,
)
from coclab.registry.schema import RegistryEntry

__all__ = [
    "RegistryEntry",
    "RegistryHealthIssue",
    "RegistryHealthReport",
    "check_registry_health",
    "latest_vintage",
    "list_boundaries",
    "register_vintage",
]
