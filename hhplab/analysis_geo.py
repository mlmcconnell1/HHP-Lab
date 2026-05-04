"""Analysis geography model for HHP-Lab.

This module introduces the canonical analysis geography abstraction used
across build, aggregate, panel, and conformance layers. It separates the
concept of an *analysis geography* (the unit of observation in derived
outputs) from *source geometries* (the native resolution of input data).

Supported analysis geography types:

- ``coc``: HUD Continuum of Care boundaries (polygonal, vintaged).
- ``metro``: Synthetic metro areas defined by researcher membership
  rules (e.g., Glynn/Fox metros). These use a ``definition_version``
  rather than a boundary vintage.
- ``msa``: Census metropolitan statistical areas keyed by 5-digit
  CBSA/MSA codes and a delineation version.

Future: ``county`` is anticipated but out of scope for this phase.

Canonical column contract for derived datasets:

- ``geo_type``: string, one of the supported geography types.
- ``geo_id``: string, canonical identifier within the geography family.
- ``year``: int, observation year.

Backward-compatibility rule: CoC outputs may retain ``coc_id`` as an
alias during transition. Metro outputs must use ``geo_id`` and never
invent a fake ``coc_id``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Canonical column name for the analysis geography type.
GEO_TYPE_COL: str = "geo_type"

#: Canonical column name for the analysis geography identifier.
GEO_ID_COL: str = "geo_id"

#: Supported analysis geography type values.
GEO_TYPE_COC: str = "coc"
GEO_TYPE_METRO: str = "metro"
GEO_TYPE_MSA: str = "msa"

#: All currently supported geo types (ordered for display).
VALID_GEO_TYPES: tuple[str, ...] = (GEO_TYPE_COC, GEO_TYPE_METRO, GEO_TYPE_MSA)

#: Legacy column name used in CoC-centered outputs.
COC_ID_COL: str = "coc_id"

#: Column name used in metro outputs.
METRO_ID_COL: str = "metro_id"

#: Column name used in MSA outputs.
MSA_ID_COL: str = "msa_id"


# ---------------------------------------------------------------------------
# Analysis geometry reference
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AnalysisGeometryRef:
    """Identifies a specific analysis geography for a build or dataset.

    Attributes
    ----------
    geo_type : str
        Geography family (``"coc"``, ``"metro"``, or ``"msa"``).
    boundary_vintage : str | None
        For polygonal families (CoC), the boundary vintage year.
    definition_version : str | None
        For synthetic families (metro), the definition version
        (e.g., ``"glynn_fox_v1"``).
    """

    geo_type: str
    boundary_vintage: str | None = None
    definition_version: str | None = None

    def __post_init__(self) -> None:
        if self.geo_type not in VALID_GEO_TYPES:
            raise ValueError(
                f"Unknown geo_type '{self.geo_type}'; "
                f"expected one of {VALID_GEO_TYPES}"
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
        d: dict[str, str | None] = {"geo_type": self.geo_type}
        if self.boundary_vintage is not None:
            d["boundary_vintage"] = self.boundary_vintage
        if self.definition_version is not None:
            d["definition_version"] = self.definition_version
        return d

    @classmethod
    def coc(cls, boundary_vintage: str) -> AnalysisGeometryRef:
        """Convenience constructor for a CoC geography."""
        return cls(
            geo_type=GEO_TYPE_COC,
            boundary_vintage=boundary_vintage,
        )

    @classmethod
    def metro(cls, definition_version: str) -> AnalysisGeometryRef:
        """Convenience constructor for a metro geography."""
        return cls(
            geo_type=GEO_TYPE_METRO,
            definition_version=definition_version,
        )

    @classmethod
    def msa(cls, definition_version: str) -> AnalysisGeometryRef:
        """Convenience constructor for an MSA geography."""
        return cls(
            geo_type=GEO_TYPE_MSA,
            definition_version=definition_version,
        )


# ---------------------------------------------------------------------------
# DataFrame helpers
# ---------------------------------------------------------------------------


def resolve_geo_col(df: pd.DataFrame) -> str:
    """Return the geo-ID column name present in *df*.

    Checks for ``coc_id`` first (backward compatibility), then
    ``metro_id``, then ``msa_id``, then ``geo_id``.
    Raises ``KeyError`` if none is found.
    """
    if COC_ID_COL in df.columns:
        return COC_ID_COL
    if METRO_ID_COL in df.columns:
        return METRO_ID_COL
    if MSA_ID_COL in df.columns:
        return MSA_ID_COL
    if GEO_ID_COL in df.columns:
        return GEO_ID_COL
    raise KeyError(
        f"DataFrame has neither '{COC_ID_COL}', '{METRO_ID_COL}', '{MSA_ID_COL}', "
        f"nor '{GEO_ID_COL}' column. "
        f"Available columns: {list(df.columns)}"
    )


def infer_geo_type(df: pd.DataFrame) -> str:
    """Infer the geography type from a DataFrame.

    Uses the ``geo_type`` column if present, otherwise infers from
    column names (``coc_id`` implies ``coc``).
    """
    if GEO_TYPE_COL in df.columns:
        types = df[GEO_TYPE_COL].dropna().unique()
        if len(types) == 1:
            value = str(types[0])
            if value not in VALID_GEO_TYPES:
                raise ValueError(
                    f"Unsupported geo_type '{value}' in data; "
                    f"expected one of {VALID_GEO_TYPES}"
                )
            return value
        if len(types) > 1:
            raise ValueError(
                f"DataFrame contains multiple geo_type values: {list(types)}"
            )
    # Fall back to column heuristic
    if COC_ID_COL in df.columns:
        return GEO_TYPE_COC
    if METRO_ID_COL in df.columns:
        return GEO_TYPE_METRO
    if MSA_ID_COL in df.columns:
        return GEO_TYPE_MSA
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
    """Add canonical ``geo_type`` and ``geo_id`` columns to a DataFrame.

    If ``geo_id`` already exists, only ``geo_type`` is added (if missing).
    For CoC data, ``coc_id`` is preserved as an alias alongside ``geo_id``.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    geo_type : str
        The geography type to set.
    geo_id_source_col : str, optional
        Column to copy into ``geo_id``. Defaults to ``coc_id`` for CoC
        or ``geo_id`` if already present.
    inplace : bool
        If True, modify *df* in place; otherwise return a copy.

    Returns
    -------
    pd.DataFrame
        DataFrame with ``geo_type`` and ``geo_id`` columns.
    """
    if not inplace:
        df = df.copy()

    # Determine source column for geo_id
    if geo_id_source_col is None:
        if GEO_ID_COL in df.columns:
            geo_id_source_col = GEO_ID_COL
        elif geo_type == GEO_TYPE_MSA and MSA_ID_COL in df.columns:
            geo_id_source_col = MSA_ID_COL
        elif COC_ID_COL in df.columns:
            geo_id_source_col = COC_ID_COL
        else:
            raise KeyError(
                f"Cannot determine geo_id source column. "
                f"Provide geo_id_source_col or ensure '{COC_ID_COL}' "
                f"or '{GEO_ID_COL}' exists. For geo_type='{GEO_TYPE_MSA}', "
                f"ensure '{MSA_ID_COL}' exists."
            )

    # Set geo_id from source column (no-op if same column)
    if geo_id_source_col != GEO_ID_COL:
        df[GEO_ID_COL] = df[geo_id_source_col]

    # Set geo_type
    df[GEO_TYPE_COL] = geo_type

    return df
