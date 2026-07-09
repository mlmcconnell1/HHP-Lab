"""Shared repository-relative paths for result workflow modules."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = REPO_ROOT / "data"
OUTPUTS_ROOT = REPO_ROOT / "outputs"


def _infer_result_artifact_role(path: Path) -> str:
    stem = path.stem
    for suffix in (
        "_levels",
        "_fd",
        "_regressions",
        "_robustness",
        "_longdiff",
        "_ppi_benchmark",
        "_distinctness",
    ):
        if stem.endswith(suffix):
            return suffix.removeprefix("_")
    return stem


def result_workflow_provenance(
    df: pd.DataFrame,
    path: Path | str,
    *,
    artifact_role: str | None = None,
    extra: dict[str, Any] | None = None,
) -> ProvenanceBlock:
    """Build canonical provenance for result workflow parquet artifacts."""
    path = Path(path)
    try:
        relative_output_path = str(path.relative_to(REPO_ROOT))
    except ValueError:
        relative_output_path = str(path)
    payload: dict[str, Any] = {
        "dataset_type": "result_workflow_artifact",
        "workflow_id": path.parent.name,
        "artifact_name": path.stem,
        "artifact_role": artifact_role or _infer_result_artifact_role(path),
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "relative_output_path": relative_output_path,
    }
    if extra:
        payload.update(extra)
    return ProvenanceBlock(extra=payload)


def write_result_parquet(
    df: pd.DataFrame,
    path: Path | str,
    *,
    artifact_role: str | None = None,
    extra: dict[str, Any] | None = None,
    index: bool | None = None,
) -> Path:
    """Write a result workflow artifact with embedded lineage metadata."""
    if index:
        raise ValueError("Result workflow parquet provenance writer does not preserve indexes.")
    provenance = result_workflow_provenance(
        df,
        path,
        artifact_role=artifact_role,
        extra=extra,
    )
    return write_parquet_with_provenance(df, path, provenance)


__all__ = [
    "DATA_ROOT",
    "OUTPUTS_ROOT",
    "REPO_ROOT",
    "result_workflow_provenance",
    "write_result_parquet",
]
