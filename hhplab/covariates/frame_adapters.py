"""Source-owned normalization adapters for tabular covariate frames.

Adapters contain provider-specific parsing or derived-measure semantics only.
The ingest pipeline retains shared responsibility for schema validation,
provenance, source registration, canonical naming, and persistence.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from hhplab.covariates.temperature import derive_prism_temperature_basis

FrameTransform = Callable[[pd.DataFrame], pd.DataFrame]


def _identity(rows: pd.DataFrame) -> pd.DataFrame:
    return rows


@dataclass(frozen=True)
class CovariateFrameAdapter:
    """Provider-specific hooks around shared canonical-column normalization."""

    source_id: str
    prepare_raw: FrameTransform = _identity
    derive_measures: FrameTransform = _identity


def _prepare_eviction_lab_national(rows: pd.DataFrame) -> pd.DataFrame:
    """Map Eviction Lab's public county-estimate extract to canonical measures."""
    normalized = rows.copy()
    if "eviction_filings" not in normalized.columns and "filings_estimate" in normalized.columns:
        normalized["eviction_filings"] = normalized["filings_estimate"]
    if "eviction_rate" not in normalized.columns and {
        "filings_estimate",
        "renting_hh",
    }.issubset(normalized.columns):
        filings = pd.to_numeric(normalized["filings_estimate"], errors="coerce")
        renting_households = pd.to_numeric(normalized["renting_hh"], errors="coerce")
        normalized["eviction_rate"] = (
            filings / renting_households.where(renting_households > 0) * 100.0
        )
    return normalized


_FRAME_ADAPTERS: dict[str, CovariateFrameAdapter] = {
    adapter.source_id: adapter
    for adapter in (
        CovariateFrameAdapter(
            source_id="eviction_lab_national",
            prepare_raw=_prepare_eviction_lab_national,
        ),
        CovariateFrameAdapter(
            source_id="prism_tmin_january",
            derive_measures=derive_prism_temperature_basis,
        ),
    )
}


def covariate_frame_adapter(source_id: str) -> CovariateFrameAdapter:
    """Return the registered source adapter or a no-op generic adapter."""
    return _FRAME_ADAPTERS.get(source_id, CovariateFrameAdapter(source_id=source_id))


def registered_covariate_frame_adapters() -> tuple[str, ...]:
    """Return source IDs with provider-specific frame semantics."""
    return tuple(sorted(_FRAME_ADAPTERS))
