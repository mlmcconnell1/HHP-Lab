"""Helpers for named build directories."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_BUILDS_DIR = Path("builds")


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
# Base asset pinning
# ---------------------------------------------------------------------------


def _resolve_boundary_source(year: int, data_dir: Path | None = None) -> Path:
    """Resolve a curated boundary file for *year*.

    Uses the multi-scheme resolver so that ``coc__B``, ``boundaries__B``,
    and legacy ``coc_boundaries__`` filenames are all accepted.

    Raises:
        FileNotFoundError: if no boundary file is found for *year*.
    """
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
) -> Path:
    """Write (or overwrite) the build manifest.

    Args:
        build_dir: Root of the build directory.
        name: Build name.
        years: Normalized/sorted year list.
        base_assets: Asset dicts returned by :func:`populate_base_assets`.

    Returns:
        Path to the written manifest.json.
    """
    manifest = {
        "schema_version": 1,
        "build": {
            "name": name,
            "created_at": datetime.now(UTC).isoformat(),
            "years": years,
        },
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
        base_assets = populate_base_assets(
            build_dir, years, data_dir=data_dir,
        )
        write_build_manifest(build_dir, name, years, base_assets)
    else:
        manifest = build_manifest_path(build_dir)
        if not manifest.exists():
            manifest.write_text(
                json.dumps({"schema_version": 1}, indent=2) + "\n"
            )

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
        alignment_params: Extra alignment parameters (e.g. lag_years).
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
