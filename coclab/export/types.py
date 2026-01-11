"""Core types for export bundle generation."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass
class ArtifactRecord:
    """Record of an artifact included in export bundle."""

    role: Literal["panel", "input", "derived", "diagnostic", "codebook"]
    source_path: Path
    dest_path: str  # relative path within bundle
    sha256: str | None = None
    bytes: int | None = None
    rows: int | None = None
    columns: int | None = None
    key_columns: list[str] = field(default_factory=list)
    provenance: dict | None = None


@dataclass
class SelectionPlan:
    """Plan of artifacts to include in export bundle."""

    panel_artifacts: list[ArtifactRecord]
    input_artifacts: list[ArtifactRecord]
    derived_artifacts: list[ArtifactRecord]
    diagnostic_artifacts: list[ArtifactRecord]
    codebook_artifacts: list[ArtifactRecord]
    inferred_selections: dict[str, str] = field(default_factory=dict)


@dataclass
class BundleConfig:
    """Configuration for export bundle generation."""

    name: str
    out_dir: Path
    panel_path: Path | None = None
    include: set[str] = field(
        default_factory=lambda: {"panel", "manifest", "codebook", "diagnostics"}
    )
    boundary_vintage: str | None = None
    tract_vintage: str | None = None
    county_vintage: str | None = None
    acs_vintage: str | None = None
    years: str | None = None
    copy_mode: Literal["copy", "hardlink", "symlink"] = "copy"
    compress: bool = False
    force: bool = False


@dataclass
class ManifestSchema:
    """Schema for MANIFEST.json."""

    bundle_name: str
    export_id: str
    created_at_utc: str
    coclab: dict
    parameters: dict
    artifacts: list[dict]
    sources: list[dict]
    notes: str = ""
