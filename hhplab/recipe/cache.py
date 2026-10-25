"""Recipe-level asset caching.

Provides in-memory DataFrame caching within a single recipe execution,
avoiding redundant Parquet reads when the same file is accessed for
multiple years.  Also computes and caches file identity (SHA-256) for
provenance tracking.
"""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from hhplab.schema.contracts import frame_annotation_contract
from hhplab.storage.provenance import read_provenance


@dataclass(frozen=True)
class FileIdentity:
    """Content-addressed identity of a file on disk.

    Note: ``path`` stores the string representation of the path passed to
    :meth:`RecipeCache.file_identity` and may be absolute.  Consumers
    needing project-relative paths (e.g. for :class:`AssetRecord`) should
    call ``relative_to(project_root)`` on the *original* ``Path`` object,
    not read this field.
    """

    path: str
    sha256: str
    size: int


class RecipeCache:
    """In-memory cache for recipe execution.

    Caches DataFrame reads keyed by absolute file path, avoiding
    redundant reads when the same Parquet file is accessed for
    multiple years.  Also computes and caches file identity (SHA-256)
    for provenance tracking.

    Parameters
    ----------
    enabled : bool
        When False, all reads go directly to disk and no caching
        occurs.  File identity tracking still works.
    """

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self._frames: dict[str, pd.DataFrame] = {}
        self._identities: dict[str, FileIdentity] = {}

    def read_parquet(self, path: Path, *, reject_annotations: bool = False) -> pd.DataFrame:
        """Read a Parquet file, returning a cached copy when available.

        Embeds ``results_panel_provenance`` (when the file carries one) into
        the returned frame's ``attrs`` so annotation and lineage metadata
        survives cache reads. Cache entries are keyed by resolved path plus
        content SHA-256 so metadata-only edits cannot silently reuse stale notes,
        including replacements with unchanged size and modification time.

        Returns a *copy* of the cached DataFrame so callers can freely
        filter/mutate without corrupting the cache.
        """
        resolved = path.resolve()
        key = f"{resolved}:{_sha256_file(path)}"
        if self.enabled and key in self._frames:
            df = copy.deepcopy(self._frames[key])
        else:
            df = pd.read_parquet(path)
            provenance = read_provenance(path)
            if provenance is not None:
                df.attrs["results_panel_provenance"] = provenance.to_dict()
            if frame_annotation_contract(df) is not None:
                source_bytes = path.read_bytes()
                df.attrs["results_source_parquet_bytes"] = source_bytes
                df.attrs["results_content_sha256"] = hashlib.sha256(source_bytes).hexdigest()
            if self.enabled:
                self._frames[key] = copy.deepcopy(df)
        contract = frame_annotation_contract(df)
        if reject_annotations and contract is not None:
            raise ValueError(
                f"Annotated input {path.name!r}: this v1 preparation path cannot preserve "
                "bindings through joins/aggregations. Use a saved annotated panel in a v2 "
                "results recipe or the supported site-year assembly."
            )
        return df

    def file_identity(self, path: Path) -> FileIdentity:
        """Get or compute file identity (SHA-256, size)."""
        sha256 = _sha256_file(path)
        key = f"{path.resolve()}:{sha256}"
        if key in self._identities:
            return self._identities[key]

        stat = path.stat()
        identity = FileIdentity(
            path=str(path),
            sha256=sha256,
            size=stat.st_size,
        )
        self._identities[key] = identity
        return identity

    @property
    def cached_count(self) -> int:
        """Number of DataFrames currently cached."""
        return len(self._frames)


def _sha256_file(path: Path, chunk_size: int = 65536) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
