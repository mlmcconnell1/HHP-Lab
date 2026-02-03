"""Helpers for named build directories."""

from __future__ import annotations

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


def build_hub_dir(build_dir: Path) -> Path:
    """Return the hub directory for a build."""
    return build_dir / "hub"


def ensure_build_dir(name: str, builds_dir: Path | None = None) -> Path:
    """Create a named build directory scaffold if missing."""
    build_dir = resolve_build_dir(name, builds_dir=builds_dir)
    curated_dir = build_curated_dir(build_dir)
    raw_dir = build_raw_dir(build_dir)
    hub_dir = build_hub_dir(build_dir)

    curated_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    hub_dir.mkdir(parents=True, exist_ok=True)

    return build_dir


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
