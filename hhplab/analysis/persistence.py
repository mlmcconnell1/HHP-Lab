"""Analysis artifact persistence and manifest ledger helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.artifacts.naming.naming import analysis_manifest_path, analysis_output_path
from hhplab.storage.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)

from .contracts import AnalysisError, AnalysisResult, _json_safe


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_panel(panel_path: Path) -> pd.DataFrame:
    if not panel_path.exists():
        raise AnalysisError(f"Panel parquet not found: {panel_path}")
    return pd.read_parquet(panel_path)


def _require_columns(df: pd.DataFrame, columns: list[str], *, context: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise AnalysisError(
            f"{context} references missing panel columns {missing}. "
            f"Available columns: {sorted(df.columns.tolist())}"
        )

def _analysis_provenance(
    *,
    analysis_type: str,
    panel_path: Path,
    parameters: dict[str, Any],
) -> ProvenanceBlock:
    input_provenance = read_provenance(panel_path)
    return ProvenanceBlock(
        extra={
            "dataset_type": "analysis_result",
            "analysis_type": analysis_type,
            "input_panel": str(panel_path),
            "input_panel_provenance": (
                input_provenance.to_dict() if input_provenance is not None else None
            ),
            "parameters": parameters,
        }
    )

def _result_summary(table: pd.DataFrame, metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_count": int(len(table)),
        "columns": [str(column) for column in table.columns],
        "metadata": _json_safe(metadata),
    }


def _write_analysis_manifest(
    *,
    panel_path: Path,
    output_path: Path,
    analysis_type: str,
    parameters: dict[str, Any],
    metadata: dict[str, Any],
    table: pd.DataFrame,
) -> Path:
    input_provenance = read_provenance(panel_path)
    manifest_path = analysis_manifest_path(output_path)
    manifest = {
        "manifest_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "analysis_type": analysis_type,
        "specification": {
            "analysis_type": analysis_type,
            "parameters": _json_safe(parameters),
        },
        "panel": {
            "path": str(panel_path),
            "name": panel_path.stem,
            "sha256": _sha256(panel_path),
            "provenance": input_provenance.to_dict() if input_provenance else None,
        },
        "output": {
            "path": str(output_path),
            "manifest_path": str(manifest_path),
            "sha256": _sha256(output_path),
        },
        "result_summary": _result_summary(table, metadata),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def _persist_result(
    table: pd.DataFrame,
    *,
    panel_path: Path,
    output_path: Path | None,
    analysis_type: str,
    parameters: dict[str, Any],
    metadata: dict[str, Any],
) -> AnalysisResult:
    resolved_output = output_path or analysis_output_path(panel_path, analysis_type)
    provenance = _analysis_provenance(
        analysis_type=analysis_type,
        panel_path=panel_path,
        parameters=parameters,
    )
    write_parquet_with_provenance(table, resolved_output, provenance)
    manifest_path = _write_analysis_manifest(
        panel_path=panel_path,
        output_path=resolved_output,
        analysis_type=analysis_type,
        parameters=parameters,
        metadata=metadata,
        table=table,
    )
    return AnalysisResult(
        table=table,
        output_path=resolved_output,
        manifest_path=manifest_path,
        metadata=metadata,
    )

def read_analysis_manifest(path: Path) -> dict[str, Any]:
    """Read an analysis manifest sidecar."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise AnalysisError(f"Analysis manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise AnalysisError(f"Invalid analysis manifest JSON: {path}") from exc


def list_analysis_manifests(
    directory: Path,
    *,
    analysis_type: str | None = None,
    panel: str | None = None,
) -> list[dict[str, Any]]:
    """List analysis manifest summaries under a directory."""
    if not directory.exists():
        raise AnalysisError(f"Analysis manifest directory not found: {directory}")
    rows: list[dict[str, Any]] = []
    for manifest_path in sorted(directory.rglob("*.manifest.json")):
        try:
            manifest = read_analysis_manifest(manifest_path)
        except AnalysisError:
            continue
        if manifest.get("manifest_version") != 1 or "analysis_type" not in manifest:
            continue
        manifest_type = str(manifest.get("analysis_type"))
        panel_path = str(manifest.get("panel", {}).get("path", ""))
        panel_name = str(manifest.get("panel", {}).get("name", ""))
        if analysis_type is not None and manifest_type != analysis_type:
            continue
        if panel is not None and panel not in {panel_path, panel_name}:
            continue
        rows.append(
            {
                "manifest_path": str(manifest_path),
                "created_at": manifest.get("created_at"),
                "analysis_type": manifest_type,
                "panel_path": panel_path,
                "panel_name": panel_name,
                "output_path": manifest.get("output", {}).get("path"),
                "parameters": manifest.get("specification", {}).get("parameters", {}),
                "result_summary": manifest.get("result_summary", {}),
            }
        )
    return rows
