"""Panel assembly policies for CoC Lab Phase 3.

This module defines explicit alignment rules for constructing CoC x year panels.
These policies determine how PIT years align with boundary vintages and ACS vintages.

Policies are implemented as pure functions with no side effects, making them
suitable for recording in provenance metadata and ensuring reproducibility.

Key Concepts
------------
- **PIT year**: The calendar year of the Point-in-Time homeless count.
- **Boundary vintage**: The CoC boundary geometry version (typically matches PIT year).
- **ACS vintage**: The American Community Survey 5-year estimate end year.

Default Alignment Rules
-----------------------
1. PIT year Y -> boundary vintage "Y"
   - PIT counts are matched to same-year boundary definitions.

2. PIT year Y -> ACS vintage "Y-1"
   - ACS 5-year estimates are released with a 1-year lag.
   - Example: PIT 2024 uses ACS vintage "2023" (2019-2023 5-year estimates).

Usage
-----
    from coclab.panel.policies import DEFAULT_POLICY, AlignmentPolicy

    # Use default policy
    boundary = DEFAULT_POLICY.boundary_vintage_func(2024)  # "2024"
    acs = DEFAULT_POLICY.acs_vintage_func(2024)            # "2023"

    # Create custom policy for research
    custom_policy = AlignmentPolicy(
        boundary_vintage_func=lambda y: str(y - 1),  # Use prior year boundaries
        acs_vintage_func=lambda y: str(y - 2),       # Use 2-year lag for ACS
        weighting_method="area",
    )
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal


def default_boundary_vintage(pit_year: int) -> str:
    """Map PIT year to boundary vintage.

    The default policy uses the same year for boundaries as the PIT count,
    since HUD releases CoC boundaries annually and the PIT count is conducted
    using the boundary definitions in effect for that year.

    Parameters
    ----------
    pit_year : int
        The calendar year of the PIT count.

    Returns
    -------
    str
        The boundary vintage string (e.g., "2024").
    """
    return str(pit_year)


def default_acs_vintage(pit_year: int) -> str:
    """Map PIT year to ACS vintage (one year lag).

    The default policy uses ACS vintage Y-1 for PIT year Y because the
    American Community Survey 5-year estimates are released with approximately
    a one-year lag. For example:

    - ACS 2019-2023 5-year estimates are released in December 2024
    - These would be the latest available for a January 2025 PIT count
    - Therefore, PIT 2025 uses ACS vintage "2024" (2020-2024 estimates)

    Note: The ACS vintage string represents the end year of the 5-year period.
    ACS vintage "2023" means the 2019-2023 5-year estimates.

    Parameters
    ----------
    pit_year : int
        The calendar year of the PIT count.

    Returns
    -------
    str
        The ACS vintage string (e.g., "2023" for PIT year 2024).
    """
    return str(pit_year - 1)


@dataclass
class AlignmentPolicy:
    """Encapsulates all alignment choices for a panel build.

    This class bundles the policy functions and parameters needed to construct
    a CoC x year panel. It is designed to be serializable for provenance
    embedding, ensuring reproducibility of panel construction.

    Attributes
    ----------
    boundary_vintage_func : Callable[[int], str]
        Function mapping PIT year to boundary vintage string.
    acs_vintage_func : Callable[[int], str]
        Function mapping PIT year to ACS vintage string.
    weighting_method : Literal["area", "population"]
        Method for apportioning tract-level ACS measures to CoCs.
        - "area": Weight by geographic area overlap.
        - "population": Weight by population in overlapping areas.
    """

    boundary_vintage_func: Callable[[int], str]
    acs_vintage_func: Callable[[int], str]
    weighting_method: Literal["area", "population"]

    def to_dict(self) -> dict:
        """Serialize for provenance embedding.

        Converts the policy to a dictionary representation suitable for
        embedding in Parquet metadata. Function references are stored by
        their qualified names since functions themselves are not JSON
        serializable.

        Returns
        -------
        dict
            Dictionary with string representations of all policy parameters.
        """
        return {
            "boundary_vintage_func": _func_name(self.boundary_vintage_func),
            "acs_vintage_func": _func_name(self.acs_vintage_func),
            "weighting_method": self.weighting_method,
        }

    @classmethod
    def from_dict(cls, data: dict) -> AlignmentPolicy:
        """Deserialize from provenance dictionary.

        Reconstructs an AlignmentPolicy from a dictionary representation.
        Only supports the built-in policy functions (default_boundary_vintage
        and default_acs_vintage). Custom functions will need to be passed
        directly to the constructor.

        Parameters
        ----------
        data : dict
            Dictionary containing policy parameters.

        Returns
        -------
        AlignmentPolicy
            Reconstructed policy object.

        Raises
        ------
        ValueError
            If an unknown function name is encountered.
        """
        boundary_func = _resolve_func(data.get("boundary_vintage_func", ""))
        acs_func = _resolve_func(data.get("acs_vintage_func", ""))

        return cls(
            boundary_vintage_func=boundary_func,
            acs_vintage_func=acs_func,
            weighting_method=data.get("weighting_method", "population"),
        )


def _func_name(func: Callable) -> str:
    """Extract a qualified name for a function."""
    if hasattr(func, "__name__"):
        return func.__name__
    elif hasattr(func, "__qualname__"):
        return func.__qualname__
    else:
        return repr(func)


def _resolve_func(name: str) -> Callable[[int], str]:
    """Resolve a function name to the actual function."""
    known_funcs = {
        "default_boundary_vintage": default_boundary_vintage,
        "default_acs_vintage": default_acs_vintage,
    }

    simple_name = name.split(".")[-1] if "." in name else name

    if simple_name in known_funcs:
        return known_funcs[simple_name]

    raise ValueError(
        f"Unknown function name: {name!r}. Known functions: {list(known_funcs.keys())}"
    )


# Default policy instance using standard alignment rules
DEFAULT_POLICY = AlignmentPolicy(
    boundary_vintage_func=default_boundary_vintage,
    acs_vintage_func=default_acs_vintage,
    weighting_method="population",
)
