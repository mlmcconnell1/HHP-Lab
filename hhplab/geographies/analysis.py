"""Contracts for the geography used as a unit of analysis.

This module owns the low-level analysis-geography model and DataFrame helpers.
The historical :mod:`hhplab.analysis_geo` module re-exports this API for
callers that have not migrated yet.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# Canonical column names and supported geography values.
GEO_TYPE_COL: str = "geo_type"
GEO_ID_COL: str = "geo_id"
GEO_TYPE_COC: str = "coc"
GEO_TYPE_METRO: str = "metro"
GEO_TYPE_MSA: str = "msa"
VALID_GEO_TYPES: tuple[str, ...] = (GEO_TYPE_COC, GEO_TYPE_METRO, GEO_TYPE_MSA)
COC_ID_COL: str = "coc_id"
METRO_ID_COL: str = "metro_id"
MSA_ID_COL: str = "msa_id"


@dataclass(frozen=True)
class AnalysisGeometryRef:
    """Identify a specific analysis geography for a build or dataset."""

    geo_type: str
    boundary_vintage: str | None = None
    definition_version: str | None = None

    def __post_init__(self) -> None:
        if self.geo_type not in VALID_GEO_TYPES:
            raise ValueError(
                f"Unknown geo_type '{self.geo_type}'; expected one of {VALID_GEO_TYPES}"
            )

    @property
    def is_coc(self) -> bool:
        return self.geo_type == GEO_TYPE_COC

    @property
    def is_metro(self) -> bool:
        return self.geo_type == GEO_TYPE_METRO

    @property
    def is_msa(self) -> bool:
        return self.geo_type == GEO_TYPE_MSA

    def to_dict(self) -> dict[str, str | None]:
        result: dict[str, str | None] = {GEO_TYPE_COL: self.geo_type}
        if self.boundary_vintage is not None:
            result["boundary_vintage"] = self.boundary_vintage
        if self.definition_version is not None:
            result["definition_version"] = self.definition_version
        return result

    @classmethod
    def coc(cls, boundary_vintage: str) -> AnalysisGeometryRef:
        """Construct a CoC geography reference."""
        return cls(geo_type=GEO_TYPE_COC, boundary_vintage=boundary_vintage)

    @classmethod
    def metro(cls, definition_version: str) -> AnalysisGeometryRef:
        """Construct a synthetic metro geography reference."""
        return cls(geo_type=GEO_TYPE_METRO, definition_version=definition_version)

    @classmethod
    def msa(cls, definition_version: str) -> AnalysisGeometryRef:
        """Construct an MSA geography reference."""
        return cls(geo_type=GEO_TYPE_MSA, definition_version=definition_version)


def resolve_geo_col(df: pd.DataFrame) -> str:
    """Return the geography-ID column present in *df*."""
    for column in (COC_ID_COL, METRO_ID_COL, MSA_ID_COL, GEO_ID_COL):
        if column in df.columns:
            return column
    raise KeyError(
        f"DataFrame has neither '{COC_ID_COL}', '{METRO_ID_COL}', '{MSA_ID_COL}', "
        f"nor '{GEO_ID_COL}' column. Available columns: {list(df.columns)}"
    )


def infer_geo_type(df: pd.DataFrame) -> str:
    """Infer the geography type from canonical values or ID column names."""
    if GEO_TYPE_COL in df.columns:
        types = df[GEO_TYPE_COL].dropna().unique()
        if len(types) == 1:
            value = str(types[0])
            if value not in VALID_GEO_TYPES:
                raise ValueError(
                    f"Unsupported geo_type '{value}' in data; expected one of {VALID_GEO_TYPES}"
                )
            return value
        if len(types) > 1:
            raise ValueError(f"DataFrame contains multiple geo_type values: {list(types)}")
    for column, geo_type in (
        (COC_ID_COL, GEO_TYPE_COC),
        (METRO_ID_COL, GEO_TYPE_METRO),
        (MSA_ID_COL, GEO_TYPE_MSA),
    ):
        if column in df.columns:
            return geo_type
    raise ValueError(
        f"Cannot infer geo_type: DataFrame has neither '{GEO_TYPE_COL}', "
        f"'{COC_ID_COL}', '{METRO_ID_COL}', nor '{MSA_ID_COL}' columns"
    )


def ensure_canonical_geo_columns(
    df: pd.DataFrame,
    geo_type: str,
    *,
    geo_id_source_col: str | None = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """Add canonical ``geo_type`` and ``geo_id`` columns to a DataFrame."""
    if not inplace:
        df = df.copy()

    if geo_id_source_col is None:
        if GEO_ID_COL in df.columns:
            geo_id_source_col = GEO_ID_COL
        elif geo_type == GEO_TYPE_MSA and MSA_ID_COL in df.columns:
            geo_id_source_col = MSA_ID_COL
        elif COC_ID_COL in df.columns:
            geo_id_source_col = COC_ID_COL
        else:
            raise KeyError(
                "Cannot determine geo_id source column. "
                f"Provide geo_id_source_col or ensure '{COC_ID_COL}' or "
                f"'{GEO_ID_COL}' exists. For geo_type='{GEO_TYPE_MSA}', "
                f"ensure '{MSA_ID_COL}' exists."
            )

    if geo_id_source_col != GEO_ID_COL:
        df[GEO_ID_COL] = df[geo_id_source_col]
    df[GEO_TYPE_COL] = geo_type
    return df


__all__ = [
    "AnalysisGeometryRef",
    "COC_ID_COL",
    "GEO_ID_COL",
    "GEO_TYPE_COC",
    "GEO_TYPE_COL",
    "GEO_TYPE_METRO",
    "GEO_TYPE_MSA",
    "METRO_ID_COL",
    "MSA_ID_COL",
    "VALID_GEO_TYPES",
    "ensure_canonical_geo_columns",
    "infer_geo_type",
    "resolve_geo_col",
]
