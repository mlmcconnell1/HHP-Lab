"""Analysis output artifact naming."""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "analysis_manifest_filename",
    "analysis_manifest_path",
    "analysis_output_filename",
    "analysis_output_path",
]

def analysis_manifest_filename(output_filename: str) -> str:
    """Return the manifest sidecar filename for an analysis artifact."""
    return Path(output_filename).with_suffix(".manifest.json").name

def analysis_manifest_path(output_path: Path | str) -> Path:
    """Return the manifest sidecar path for an analysis artifact."""
    output_path = Path(output_path)
    return output_path.with_name(analysis_manifest_filename(output_path.name))

def analysis_output_filename(panel_filename: str, analysis_type: str) -> str:
    """Return the canonical filename for an analysis artifact derived from a panel."""
    panel_path = Path(panel_filename)
    return f"{panel_path.stem}__analysis_{analysis_type}.parquet"

def analysis_output_path(panel_path: Path | str, analysis_type: str) -> Path:
    """Return the canonical analysis artifact path beside the source panel."""
    panel_path = Path(panel_path)
    return panel_path.with_name(analysis_output_filename(panel_path.name, analysis_type))

