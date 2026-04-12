"""Centralized path resolution for storage roots.

Provides typed directory helpers that resolve against a
:class:`~coclab.config.StorageConfig`.  All code that needs a data
directory should call these helpers instead of constructing
``Path("data/...")`` directly.

The internal layout beneath each root follows the existing convention:

Asset store (``asset_store_root``)::

    <asset_store_root>/
        raw/            # raw snapshots
        curated/        # curated ingests, crosswalks, registries, aggregates
            acs/
            pit/
            tiger/
            xwalks/
            zori/
            pep/
            measures/
            maps/
            ...

Output root (``output_root``)::

    <output_root>/
        <recipe-name>/  # recipe-built panels + sidecars
            panel__...parquet
            panel__...manifest.json
            panel__...__diagnostics.json
"""

from __future__ import annotations

from pathlib import Path

from coclab.config import StorageConfig, load_config

# ---------------------------------------------------------------------------
# Asset store helpers
# ---------------------------------------------------------------------------


def asset_store_root(config: StorageConfig | None = None) -> Path:
    """Return the asset store root directory."""
    cfg = config or load_config()
    return cfg.asset_store_root


def raw_root(config: StorageConfig | None = None) -> Path:
    """Return the raw data root (``<asset_store_root>/raw``)."""
    return asset_store_root(config) / "raw"


def curated_root(config: StorageConfig | None = None) -> Path:
    """Return the curated data root (``<asset_store_root>/curated``)."""
    return asset_store_root(config) / "curated"


def curated_dir(kind: str, config: StorageConfig | None = None) -> Path:
    """Return a curated subdirectory for a given data kind.

    Parameters
    ----------
    kind : str
        Subdirectory name under ``curated/`` (e.g. ``"acs"``, ``"pit"``,
        ``"xwalks"``, ``"tiger"``, ``"zori"``, ``"pep"``, ``"measures"``,
        ``"maps"``).
    config : StorageConfig, optional
        Storage configuration.  Defaults to ``load_config()``.
    """
    return curated_root(config) / kind


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def output_root(config: StorageConfig | None = None) -> Path:
    """Return the output root directory."""
    cfg = config or load_config()
    return cfg.output_root


def output_dir(kind: str, config: StorageConfig | None = None) -> Path:
    """Return an output subdirectory for a given output kind.

    Parameters
    ----------
    kind : str
        Subdirectory name under ``output_root/`` for non-recipe outputs or
        higher-level workflow grouping.
    config : StorageConfig, optional
        Storage configuration.  Defaults to ``load_config()``.
    """
    return output_root(config) / kind
