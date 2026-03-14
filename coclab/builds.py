"""Helpers for named build directories."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_BUILDS_DIR = Path("builds")
DEFAULT_CATALOG_PATH = Path("data/registry/base_assets.json")


def resolve_build_dir(name: str, builds_dir: Path | None = None) -> Path:
    """Resolve the root directory for a named build."""
    base = builds_dir if builds_dir is not None else DEFAULT_BUILDS_DIR
    return Path(base) / name


def build_curated_dir(build_dir: Path) -> Path:
    """Return the curated data directory for a build."""
    return build_dir / "data" / "curated"


def build_raw_dir(build_dir: Path) -> Path:
    """Return the raw data directory for a build."""
    return build_dir / "data" / "raw"


def build_base_dir(build_dir: Path) -> Path:
    """Return the base directory for a build."""
    return build_dir / "base"


def build_hub_dir(build_dir: Path) -> Path:
    """Return the base directory for a build.

    .. deprecated::
        Use :func:`build_base_dir` instead.
    """
    return build_base_dir(build_dir)


def build_manifest_path(build_dir: Path) -> Path:
    """Return the manifest.json path for a build."""
    return build_dir / "manifest.json"


# ---------------------------------------------------------------------------
# File hashing
# ---------------------------------------------------------------------------


def _compute_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest for a file."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# ---------------------------------------------------------------------------
# Base asset catalog (optional)
# ---------------------------------------------------------------------------


def read_base_catalog(catalog_path: Path | None = None) -> dict:
    """Read the global base asset catalog, returning an empty structure if absent.

    The catalog at ``data/registry/base_assets.json`` is an optional inventory
    of available base assets.  When present, :func:`_resolve_boundary_source`
    consults it before falling back to filesystem discovery.
    """
    path = catalog_path or DEFAULT_CATALOG_PATH
    if not path.exists():
        return {"schema_version": 1, "assets": []}
    return json.loads(path.read_text())


def write_base_catalog(
    assets: list[dict],
    catalog_path: Path | None = None,
) -> Path:
    """Write (or overwrite) the global base asset catalog.

    Args:
        assets: List of asset dicts with keys
            ``asset_type``, ``year``, ``path``, ``sha256``.
        catalog_path: Override for the default catalog location.

    Returns:
        Path to the written catalog file.
    """
    path = catalog_path or DEFAULT_CATALOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    catalog = {"schema_version": 1, "assets": assets}
    path.write_text(json.dumps(catalog, indent=2) + "\n")
    return path


def scan_boundary_assets(data_dir: Path | None = None) -> list[dict]:
    """Scan curated boundary files and return catalog entries.

    Discovers all ``coc__B*.parquet`` files under the curated boundary
    directory and computes SHA-256 hashes for each.

    Args:
        data_dir: Root data directory (defaults to ``data/``).

    Returns:
        Sorted list of asset dicts.
    """
    base = Path(data_dir) if data_dir else Path("data")
    boundary_dir = base / "curated" / "coc_boundaries"
    if not boundary_dir.exists():
        return []

    assets: list[dict] = []
    for p in sorted(boundary_dir.glob("coc__B*.parquet")):
        # Extract year from filename like coc__B2024.parquet
        stem = p.stem  # e.g. "coc__B2024"
        year_str = stem.split("__B")[-1] if "__B" in stem else None
        if year_str is None or not year_str.isdigit():
            continue
        assets.append({
            "asset_type": "coc_boundary",
            "year": int(year_str),
            "path": p.as_posix(),
            "sha256": _compute_sha256(p),
        })
    return assets


def _catalog_lookup(year: int, catalog_path: Path | None = None) -> Path | None:
    """Look up a boundary asset for *year* in the catalog.

    Returns the path if found and the file exists, otherwise ``None``.
    """
    catalog = read_base_catalog(catalog_path)
    for asset in catalog.get("assets", []):
        if asset.get("asset_type") == "coc_boundary" and asset.get("year") == year:
            p = Path(asset["path"])
            if p.exists():
                return p
    return None


# ---------------------------------------------------------------------------
# Base asset pinning
# ---------------------------------------------------------------------------


def _resolve_boundary_source(year: int, data_dir: Path | None = None) -> Path:
    """Resolve a curated boundary file for *year*.

    Checks the optional base asset catalog first, then falls back to the
    multi-scheme filesystem resolver (``coc__B``, ``boundaries__B``,
    legacy ``coc_boundaries__``).

    Raises:
        FileNotFoundError: if no boundary file is found for *year*.
    """
    # Try catalog first (fast path)
    catalog_hit = _catalog_lookup(year)
    if catalog_hit is not None:
        return catalog_hit

    from coclab.geo.io import resolve_curated_boundary_path

    return resolve_curated_boundary_path(str(year), base_dir=data_dir)


def populate_base_assets(
    build_dir: Path,
    years: list[int],
    *,
    data_dir: Path | None = None,
) -> list[dict]:
    """Discover, copy, and pin boundary assets into a build's ``base/`` tree.

    For each year in *years*, the corresponding curated boundary file is
    resolved, copied into ``builds/<build>/base/``,
    and its SHA-256 hash is recorded.

    Args:
        build_dir: Root of the build directory.
        years: Sorted list of years to pin.
        data_dir: Root data directory (defaults to ``data/``).

    Returns:
        A list of base-asset dicts ready for the manifest.

    Raises:
        FileNotFoundError: If a boundary file is missing for any year.
    """
    base = build_base_dir(build_dir)
    assets: list[dict] = []

    for year in years:
        source_path = _resolve_boundary_source(year, data_dir=data_dir)

        base.mkdir(parents=True, exist_ok=True)
        dest_path = base / source_path.name

        shutil.copy2(source_path, dest_path)

        rel_path = dest_path.relative_to(build_dir).as_posix()
        sha256 = _compute_sha256(dest_path)

        assets.append(
            {
                "asset_type": "coc_boundary",
                "year": year,
                "source": source_path.as_posix(),
                "relative_path": rel_path,
                "sha256": sha256,
            }
        )

    return assets


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def write_build_manifest(
    build_dir: Path,
    name: str,
    years: list[int],
    base_assets: list[dict],
    *,
    geo_type: str | None = None,
    definition_version: str | None = None,
) -> Path:
    """Write (or overwrite) the build manifest.

    Args:
        build_dir: Root of the build directory.
        name: Build name.
        years: Normalized/sorted year list.
        base_assets: Asset dicts returned by :func:`populate_base_assets`.
        geo_type: Optional analysis geography type (``"coc"`` or ``"metro"``).
            When provided, the manifest records the target geography.
        definition_version: Optional synthetic geography definition version
            (e.g., ``"glynn_fox_v1"``). Used for metro builds.

    Returns:
        Path to the written manifest.json.
    """
    build_block: dict = {
        "name": name,
        "created_at": datetime.now(UTC).isoformat(),
        "years": years,
    }
    if geo_type is not None:
        build_block["geo_type"] = geo_type
    if definition_version is not None:
        build_block["definition_version"] = definition_version

    manifest = {
        "schema_version": 1,
        "build": build_block,
        "base_assets": base_assets,
        "aggregate_runs": [],
    }

    manifest_path = build_manifest_path(build_dir)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


# ---------------------------------------------------------------------------
# Build scaffold
# ---------------------------------------------------------------------------


def ensure_build_dir(
    name: str,
    builds_dir: Path | None = None,
    *,
    years: list[int] | None = None,
    data_dir: Path | None = None,
    geo_type: str | None = None,
    definition_version: str | None = None,
) -> tuple[Path, list[dict]]:
    """Create a named build directory scaffold and pin base assets.

    When *years* is provided, boundary assets for each year are resolved,
    copied into the build's ``base/`` tree, and recorded in a full v1
    manifest.  Without *years*, only the directory scaffold and a minimal
    manifest are created (legacy behaviour).

    Returns:
        A ``(build_dir, base_assets)`` tuple.

    Raises:
        FileNotFoundError: If *years* is provided and a boundary file
            is missing for any requested year.
    """
    build_dir = resolve_build_dir(name, builds_dir=builds_dir)
    curated_dir = build_curated_dir(build_dir)
    raw_dir = build_raw_dir(build_dir)
    base_dir = build_base_dir(build_dir)

    curated_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    base_dir.mkdir(parents=True, exist_ok=True)

    base_assets: list[dict] = []

    if years is not None:
        # Metro builds are definition-fixed: geometry comes from the metro
        # definition version, not from yearly CoC boundary files.
        if geo_type != "metro":
            base_assets = populate_base_assets(
                build_dir, years, data_dir=data_dir,
            )
        write_build_manifest(
            build_dir,
            name,
            years,
            base_assets,
            geo_type=geo_type,
            definition_version=definition_version,
        )
    else:
        manifest = build_manifest_path(build_dir)
        if not manifest.exists():
            manifest_data: dict[str, object] = {"schema_version": 1}
            if geo_type is not None or definition_version is not None:
                build_block: dict[str, object] = {"name": name}
                if geo_type is not None:
                    build_block["geo_type"] = geo_type
                if definition_version is not None:
                    build_block["definition_version"] = definition_version
                manifest_data["build"] = build_block
            manifest.write_text(json.dumps(manifest_data, indent=2) + "\n")

    return build_dir, base_assets


def record_aggregate_run(
    build_dir: Path,
    *,
    dataset: str,
    alignment: str,
    years_requested: list[int],
    years_materialized: list[int] | None = None,
    alignment_params: dict | None = None,
    outputs: list[str] | None = None,
    status: str = "success",
    error: str | None = None,
) -> dict:
    """Append an aggregate-run entry to the build manifest.

    Args:
        build_dir: Root of the build directory.
        dataset: Dataset identifier (acs, zori, pep, pit).
        alignment: Alignment mode used.
        years_requested: Years the user requested.
        years_materialized: Years actually produced (defaults to requested).
        alignment_params: Extra alignment parameters (e.g. lag_months).
        outputs: List of output file paths (relative to build dir).
        status: "success" or "failed".
        error: Error message if status is "failed".

    Returns:
        The recorded run entry dict.
    """
    import uuid

    manifest = read_build_manifest(build_dir)

    run_entry: dict = {
        "run_id": uuid.uuid4().hex[:12],
        "dataset": dataset,
        "invoked_at": datetime.now(UTC).isoformat(),
        "years_requested": years_requested,
        "years_materialized": years_materialized or years_requested,
        "alignment": {"mode": alignment},
        "outputs": outputs or [],
        "status": status,
    }
    if alignment_params:
        run_entry["alignment"].update(alignment_params)
    if error:
        run_entry["error"] = error

    manifest.setdefault("aggregate_runs", []).append(run_entry)

    manifest_path = build_manifest_path(build_dir)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    return run_entry


def read_build_manifest(build_dir: Path) -> dict:
    """Read and return the build manifest as a dict.

    Raises:
        FileNotFoundError: if manifest.json is missing.
        json.JSONDecodeError: if manifest is invalid JSON.
    """
    manifest_path = build_manifest_path(build_dir)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text())


def get_build_years(build_dir: Path) -> list[int]:
    """Return the sorted year list from a build manifest."""
    manifest = read_build_manifest(build_dir)
    return manifest.get("build", {}).get("years", [])


def list_builds(builds_dir: Path | None = None) -> list[Path]:
    """List named build directories."""
    base = builds_dir if builds_dir is not None else DEFAULT_BUILDS_DIR
    base = Path(base)
    if not base.exists():
        return []
    return sorted([path for path in base.iterdir() if path.is_dir()])


def require_build_dir(name: str, builds_dir: Path | None = None) -> Path:
    """Resolve a named build directory, raising if it does not exist."""
    build_dir = resolve_build_dir(name, builds_dir=builds_dir)
    if not build_dir.exists():
        raise FileNotFoundError(build_dir)
    return build_dir
