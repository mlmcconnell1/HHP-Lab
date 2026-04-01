"""Recipe-level provenance manifest and replication export.

Records exactly which base assets (datasets, crosswalks) were consumed
during a recipe execution, with full metadata (paths, SHA-256 hashes,
sizes).  Provides the ability to export a self-contained bundle that a
replicator can use to reproduce the build.

Asset records carry a ``root`` field that identifies the logical storage
root the ``path`` is relative to:

- ``"asset_store"`` — reusable internal asset (resolved via
  ``StorageConfig.asset_store_root``)
- ``"output"`` — downstream-consumable product (resolved via
  ``StorageConfig.output_root``)
- ``None`` — legacy project-relative path (resolved via
  ``project_root``)
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from coclab.config import StorageConfig, load_config

logger = logging.getLogger(__name__)

# Recognised logical root names
ROOT_ASSET_STORE = "asset_store"
ROOT_OUTPUT = "output"


@dataclass
class AssetRecord:
    """Record of a single asset consumed during recipe execution.

    Attributes
    ----------
    root : str or None
        Logical storage root (``"asset_store"`` or ``"output"``).
        ``None`` for legacy manifests where ``path`` is project-relative.
    path : str
        Path relative to the logical root (or project root for legacy).
    """

    role: str  # "dataset" or "crosswalk"
    path: str  # Root-relative path (or project-relative for legacy)
    sha256: str
    size: int
    root: str | None = None  # "asset_store", "output", or None (legacy)
    dataset_id: str | None = None
    transform_id: str | None = None


@dataclass
class RecipeManifest:
    """Full provenance manifest for a recipe execution.

    Contains the recipe identity, execution timestamp, and a complete
    list of every file consumed during the build — enough information
    to verify reproducibility or bundle assets for replication.
    """

    recipe_name: str
    recipe_version: int
    pipeline_id: str
    executed_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
    )
    assets: list[AssetRecord] = field(default_factory=list)
    datasets: dict[str, dict] = field(default_factory=dict)
    transforms: dict[str, str] = field(default_factory=dict)
    output_path: str | None = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict) -> RecipeManifest:
        """Deserialize from a dictionary."""
        assets = [AssetRecord(**a) for a in data.get("assets", [])]
        return cls(
            recipe_name=data["recipe_name"],
            recipe_version=data["recipe_version"],
            pipeline_id=data["pipeline_id"],
            executed_at=data.get(
                "executed_at", datetime.now(UTC).isoformat(),
            ),
            assets=assets,
            datasets=data.get("datasets", {}),
            transforms=data.get("transforms", {}),
            output_path=data.get("output_path"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> RecipeManifest:
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(json_str))


def write_manifest(manifest: RecipeManifest, path: Path) -> Path:
    """Write a manifest JSON file to disk."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest.to_json(), encoding="utf-8")
    return path


def read_manifest(path: Path) -> RecipeManifest:
    """Read a manifest JSON file from disk."""
    return RecipeManifest.from_json(Path(path).read_text(encoding="utf-8"))


def _resolve_asset_source(
    asset: AssetRecord,
    project_root: Path,
    storage_config: StorageConfig | None,
) -> Path:
    """Resolve an asset record to its absolute source path on disk.

    For root-aware assets, resolves from the appropriate storage root.
    For legacy assets (root=None), resolves from project_root.
    """
    rel = Path(asset.path)
    if rel.is_absolute():
        msg = f"Absolute asset path rejected: {asset.path}"
        raise ValueError(msg)

    if asset.root is None:
        # Legacy: project-relative
        return project_root / rel

    cfg = storage_config or load_config(project_root=project_root)
    if asset.root == ROOT_ASSET_STORE:
        return cfg.asset_store_root / rel
    if asset.root == ROOT_OUTPUT:
        return cfg.output_root / rel

    msg = f"Unknown asset root '{asset.root}' for {asset.path}"
    raise ValueError(msg)


def _bundle_dest_path(
    asset: AssetRecord,
    bundle_root: Path,
) -> Path:
    """Compute the destination path inside the export bundle.

    Root-aware assets are namespaced by root:
    ``<bundle_root>/<root>/<path>``.  Legacy assets go under
    ``<bundle_root>/assets/<path>``.
    """
    rel = Path(asset.path)
    if asset.root is not None:
        return bundle_root / asset.root / rel
    return bundle_root / "assets" / rel


def export_bundle(
    manifest: RecipeManifest,
    project_root: Path,
    destination: Path,
    *,
    storage_config: StorageConfig | None = None,
) -> Path:
    """Copy all consumed assets into a self-contained replication bundle.

    Creates *destination* with:
    - ``manifest.json`` — the provenance manifest
    - ``asset_store/`` — copies of asset-store files, preserving layout
    - ``output/`` — copies of output files, preserving layout
    - ``assets/`` — copies of legacy project-relative files (if any)

    Parameters
    ----------
    manifest : RecipeManifest
        The provenance manifest to export.
    project_root : Path
        Project root (used for legacy manifests without root metadata).
    destination : Path
        Destination directory for the bundle.
    storage_config : StorageConfig, optional
        Storage root configuration for resolving root-aware paths.

    Returns
    -------
    Path
        The destination directory.
    """
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)

    for asset in manifest.assets:
        src = _resolve_asset_source(asset, project_root, storage_config)
        src = src.resolve()

        # Security: for legacy (root=None) paths, verify they stay within
        # the project root to prevent path traversal attacks.
        if asset.root is None:
            if not src.is_relative_to(project_root.resolve()):
                msg = f"Asset path escapes project root: {asset.path}"
                raise ValueError(msg)

        if not src.exists():
            logger.warning("export_bundle: skipping missing asset %s", asset.path)
            continue

        dst = _bundle_dest_path(asset, destination)
        dst = dst.resolve()
        # Security: ensure destination stays within the bundle
        if not dst.is_relative_to(destination.resolve()):
            msg = f"Asset path escapes bundle directory: {asset.path}"
            raise ValueError(msg)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    write_manifest(manifest, destination / "manifest.json")
    return destination
