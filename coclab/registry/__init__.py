"""Registry module for tracking boundary vintages."""

from coclab.registry.registry import (
    latest_vintage,
    list_vintages,
    register_vintage,
)
from coclab.registry.schema import RegistryEntry

__all__ = [
    "RegistryEntry",
    "latest_vintage",
    "list_vintages",
    "register_vintage",
]
