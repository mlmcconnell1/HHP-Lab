"""Stable analysis result and error contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd


class AnalysisError(ValueError):
    """Raised when an analysis request is invalid for the input panel."""


@dataclass(frozen=True)
class AnalysisResult:
    """A persisted analysis result plus JSON-ready summary metadata."""

    table: pd.DataFrame
    output_path: Path
    manifest_path: Path
    metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return _json_safe(
            {
                "status": "ok",
                "output_path": str(self.output_path),
                "manifest_path": str(self.manifest_path),
                **self.metadata,
                "records": self.table.to_dict(orient="records"),
            }
        )


InferenceMethod = Literal["none", "wild-cluster", "permutation"]


@dataclass(frozen=True)
class _RegressionFit:
    beta: np.ndarray
    fitted: np.ndarray
    residuals: np.ndarray
    std_errors: np.ndarray
    std_error_type: str
    t_stats: np.ndarray
    p_values: np.ndarray
    r_squared: float

def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if pd.isna(value) and not isinstance(value, (list, tuple, dict)):
        return None
    return value
