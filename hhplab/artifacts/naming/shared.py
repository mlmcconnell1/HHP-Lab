"""Shared naming token normalization helpers."""

from __future__ import annotations

__all__ = [
    "expand_acs_vintage",
]

def _abbreviate_weighting(weighting: str) -> str:
    """Abbreviate weighting method for filename.

    Args:
        weighting: Full weighting name like "renter_households"

    Returns:
        Abbreviated form like "renter"
    """
    # Common abbreviations
    abbreviations = {
        "renter_households": "renter",
        "total_population": "pop",
        "area": "area",
    }
    return abbreviations.get(weighting, weighting)


# ---------------------------------------------------------------------------
# PEP (Population Estimates Program) filenames
# ---------------------------------------------------------------------------

def _normalize_acs_vintage(acs_vintage: str) -> str:
    """Normalize ACS vintage to just the end year.

    Args:
        acs_vintage: ACS vintage like "2019-2023" or "2023"

    Returns:
        End year as string, e.g., "2023"
    """
    if "-" in acs_vintage:
        # Format like "2019-2023", extract end year
        return acs_vintage.split("-")[1]
    return acs_vintage

def _normalize_definition_version(definition_version: str) -> str:
    """Normalize a definition version string for use in filenames.

    Strips non-alphanumeric characters (except underscores) and
    lowercases. Example: ``"glynn_fox_v1"`` -> ``"glynnfoxv1"``.
    """
    return "".join(c for c in definition_version.lower() if c.isalnum())


# =============================================================================
# Metro (geography-scoped) filenames
# =============================================================================

def expand_acs_vintage(acs_vintage: str) -> str:
    """Expand ACS end year to full 5-year range for display.

    Args:
        acs_vintage: ACS vintage as end year ("2023") or range ("2019-2023")

    Returns:
        Full 5-year range, e.g., "2019-2023"
    """
    if "-" in acs_vintage:
        # Already a range
        return acs_vintage
    # Single year - expand to 5-year range
    end_year = int(acs_vintage)
    start_year = end_year - 4
    return f"{start_year}-{end_year}"

