"""Shared recipe schema value objects.

These contracts are imported by adapters, planning, and execution helpers that
do not need the full top-level ``RecipeV1`` model.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class YearSpec(BaseModel):
    """
    Represents a year domain.
    Exactly one of:
      - range: "2018-2024"
      - years: [2018, 2019, ...]
    Also accepts a bare string like "2018-2024" as shorthand for {range: "2018-2024"}.
    """

    model_config = ConfigDict(extra="forbid")

    range: str | None = Field(
        default=None,
        description="Year range in inclusive form 'YYYY-YYYY'.",
        examples=["2018-2024"],
    )
    years: list[int] | None = Field(
        default=None,
        description="Explicit list of years.",
        examples=[[2018, 2019, 2020]],
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_bare_string(cls, data: Any) -> Any:
        if isinstance(data, str):
            return {"range": data}
        return data

    @model_validator(mode="after")
    def _validate_one_of(self) -> YearSpec:
        if (self.range is None) == (self.years is None):
            raise ValueError("YearSpec must set exactly one of 'range' or 'years'.")
        return self


def expand_year_spec(spec: YearSpec | str | list[int]) -> list[int]:
    """Expand a YearSpec (or shorthand) to a sorted list of year ints.

    Accepts:
      - YearSpec model instance
      - str range like "2018-2024"
      - explicit list of ints
    """
    if isinstance(spec, list):
        return sorted(spec)
    if isinstance(spec, str):
        spec = YearSpec(range=spec)
    if spec.years is not None:
        return sorted(spec.years)
    if spec.range is not None:
        parts = spec.range.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid year range format: '{spec.range}'. Expected 'YYYY-YYYY'.")
        start, end = int(parts[0]), int(parts[1])
        if start > end:
            raise ValueError(
                f"Invalid year range '{spec.range}': start ({start}) > end ({end}). "
                f"Did you mean '{end}-{start}'?"
            )
        return list(range(start, end + 1))
    raise ValueError("YearSpec has neither 'range' nor 'years'.")


class GeometryRef(BaseModel):
    """
    Reference to a geometry universe.
    type: open string set (e.g., 'coc', 'tract', 'county', 'state', 'zcta', 'zip')
    vintage: required for most polygonal types in practice, but schema keeps it optional
            and lets runtime adapters enforce requirements.
    source: optional provenance hint (e.g., 'hud_exchange', 'tiger', 'nhgis').
    """

    model_config = ConfigDict(extra="forbid")

    type: str = Field(
        ...,
        description="Geometry type identifier (open set).",
        examples=["coc", "county", "state", "zcta"],
    )
    vintage: int | None = Field(
        default=None,
        description="Vintage year for geometry (if applicable).",
        examples=[2025, 2023, 2020],
    )
    source: str | None = Field(
        default=None,
        description="Geometry source hint (optional).",
        examples=["hud_exchange", "tiger", "nhgis"],
    )
    subset_profile: str | None = Field(
        default=None,
        description=(
            "Optional subset/profile name layered over a canonical geometry "
            "universe. Primarily used for metro targets."
        ),
        examples=["glynn_fox"],
    )
    subset_profile_definition_version: str | None = Field(
        default=None,
        description=(
            "Optional version token for the selected subset/profile. "
            "Primarily used for metro targets."
        ),
        examples=["glynn_fox_v1"],
    )

    def resolved_metro_definition_version(self) -> str | None:
        """Return the runtime metro-universe definition version.

        Legacy metro recipes use ``source='glynn_fox_v1'`` to mean the
        historical Glynn/Fox metro family. Runtime metro-universe execution
        treats that as the canonical metro universe filtered through the
        Glynn/Fox subset profile, while preserving the original recipe value
        for naming/backward compatibility elsewhere.
        """
        if self.type != "metro":
            return self.source
        if self.source is None:
            return None

        from hhplab.metro.metro_definitions import (
            CANONICAL_UNIVERSE_DEFINITION_VERSION,
        )
        from hhplab.metro.metro_definitions import (
            DEFINITION_VERSION as GLYNN_FOX_DEFINITION_VERSION,
        )

        if (
            self.source == GLYNN_FOX_DEFINITION_VERSION
            and self.subset_profile is None
            and self.subset_profile_definition_version is None
        ):
            return CANONICAL_UNIVERSE_DEFINITION_VERSION
        return self.source

    def resolved_metro_subset_profile(self) -> str | None:
        """Return the runtime metro subset/profile name, if any."""
        if self.type != "metro":
            return None
        if self.subset_profile is not None:
            return self.subset_profile

        from hhplab.metro.metro_definitions import (
            DEFINITION_VERSION as GLYNN_FOX_DEFINITION_VERSION,
        )
        from hhplab.metro.metro_definitions import (
            PROFILE_NAME,
        )

        if (
            self.source == GLYNN_FOX_DEFINITION_VERSION
            and self.subset_profile_definition_version is None
        ):
            return PROFILE_NAME
        return None

    def resolved_metro_subset_definition_version(self) -> str | None:
        """Return the runtime metro subset/profile definition version, if any."""
        if self.type != "metro":
            return None
        if self.subset_profile_definition_version is not None:
            return self.subset_profile_definition_version

        from hhplab.metro.metro_definitions import (
            DEFINITION_VERSION as GLYNN_FOX_DEFINITION_VERSION,
        )

        if self.source == GLYNN_FOX_DEFINITION_VERSION and self.subset_profile is None:
            return GLYNN_FOX_DEFINITION_VERSION
        return None


class VintageSetRule(BaseModel):
    """A rule within a vintage set that maps years to dimension values."""

    model_config = ConfigDict(extra="forbid")

    years: YearSpec
    constants: dict[str, str | int] = Field(
        default_factory=dict,
        description="Fixed dimension values for this rule's year band.",
    )
    year_offsets: dict[str, int] = Field(
        default_factory=dict,
        description="Dimension values derived from analysis year (value = year + offset).",
    )


class VintageSetSpec(BaseModel):
    """A named set of vintage tuples, expanded from compact range rules."""

    model_config = ConfigDict(extra="forbid")

    dimensions: list[str] = Field(
        ...,
        min_length=1,
        description="Ordered list of dimension names in each tuple.",
    )
    rules: list[VintageSetRule] = Field(
        ...,
        min_length=1,
        description="Year-banded rules that expand into tuples.",
    )
