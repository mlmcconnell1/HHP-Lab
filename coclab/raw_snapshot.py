"""Shared raw snapshot utilities for data retention compliance.

Provides helpers for persisting canonical raw snapshots under
``data/raw/<source_type>/...`` for both file-based and API-based sources,
per the raw-data-retention-policy.

File-based sources
    Use :func:`persist_file_snapshot` to write a downloaded artifact
    (ZIP, CSV, etc.) and get back its path, SHA-256 hash, and size.

API-based sources
    Use :func:`write_api_snapshot` to persist paginated API responses as
    ``response.ndjson`` + ``request.json`` + ``manifest.json``, with
    deterministic serialisation for reproducible hashes.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import zipfile
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

RAW_DATA_ROOT = Path("data/raw")


# ---------------------------------------------------------------------------
# File-based raw snapshots
# ---------------------------------------------------------------------------


def hash_zip_contents(raw_content: bytes) -> str:
    """Compute a content-stable SHA-256 from a ZIP's decompressed entries.

    Hashes the extracted file contents (in sorted filename order) rather than
    the ZIP container bytes.  This makes the hash stable across re-compression
    — different DEFLATE implementations produce different byte streams for the
    same input data, but the decompressed contents remain identical.

    Each entry contributes its filename (UTF-8 encoded) followed by its
    decompressed bytes, so file renames are also detected.

    Parameters
    ----------
    raw_content : bytes
        Raw ZIP file bytes.

    Returns
    -------
    str
        Lowercase hex SHA-256 digest.
    """
    hasher = hashlib.sha256()
    with zipfile.ZipFile(io.BytesIO(raw_content)) as zf:
        for name in sorted(zf.namelist()):
            info = zf.getinfo(name)
            if not info.is_dir():
                hasher.update(name.encode("utf-8"))
                hasher.update(zf.read(name))
    return hasher.hexdigest()


def persist_file_snapshot(
    raw_content: bytes,
    source_type: str,
    filename: str,
    *,
    subdirs: tuple[str, ...] = (),
    raw_root: Path | None = None,
) -> tuple[Path, str, int]:
    """Persist a file-based raw snapshot and return its hash.

    Writes *raw_content* to ``data/raw/<source_type>[/subdirs...]/<filename>``.
    The SHA-256 hash and byte-size are computed from the persisted file to
    ensure the hash always matches what is on disk.

    Parameters
    ----------
    raw_content : bytes
        Raw downloaded bytes (ZIP, CSV, …).
    source_type : str
        Top-level subdirectory under ``data/raw/`` (e.g. ``"census"``).
    filename : str
        Leaf filename (e.g. ``"tl_2023_06_tract.zip"``).
    subdirs : tuple[str, ...], optional
        Additional path segments between *source_type* and *filename*.
    raw_root : Path, optional
        Override the default ``data/raw`` root.

    Returns
    -------
    tuple[Path, str, int]
        ``(persisted_path, sha256_hex, byte_size)``
    """
    root = raw_root or RAW_DATA_ROOT
    dest_dir = root / source_type
    for seg in subdirs:
        dest_dir = dest_dir / seg
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_path = dest_dir / filename
    dest_path.write_bytes(raw_content)

    if filename.lower().endswith(".zip"):
        sha256_hex = hash_zip_contents(raw_content)
    else:
        sha256_hex = hashlib.sha256(raw_content).hexdigest()
    byte_size = len(raw_content)

    logger.debug(
        "Persisted raw snapshot: %s (%d bytes, sha256=%s…)",
        dest_path, byte_size, sha256_hex[:12],
    )
    return dest_path, sha256_hex, byte_size


# ---------------------------------------------------------------------------
# API-based raw snapshots
# ---------------------------------------------------------------------------


def write_api_snapshot(
    response_payloads: list[bytes],
    source_type: str,
    *,
    snapshot_id: str,
    request_metadata: dict | None = None,
    record_count: int | None = None,
    raw_root: Path | None = None,
) -> tuple[Path, str, int]:
    """Persist a canonical API raw snapshot.

    Writes three files into ``data/raw/<source_type>/<snapshot_id>/``:

    * **response.ndjson** — one line per response payload (deterministic
      serialisation via ``json.dumps(sort_keys=True)``).
    * **request.json** — capture of URL, params, headers used.
    * **manifest.json** — pagination metadata, timestamps, row counts,
      content hash.

    The SHA-256 hash is computed from the persisted ``response.ndjson`` so
    it always matches what is on disk.

    Parameters
    ----------
    response_payloads : list[bytes]
        Raw HTTP response bodies (one entry per page/request).  Each entry
        is expected to be valid JSON bytes.
    source_type : str
        Top-level subdirectory under ``data/raw/`` (e.g. ``"hud_opendata"``).
    snapshot_id : str
        Sub-directory name that gives temporal identity to the snapshot
        (e.g. a date string ``"2026-02-07"`` or ``"A2023"``).
    request_metadata : dict, optional
        Dict with keys like ``url``, ``params``, ``headers`` that describe
        how the data was fetched.
    record_count : int, optional
        Total number of records/features across all pages.
    raw_root : Path, optional
        Override the default ``data/raw`` root.

    Returns
    -------
    tuple[Path, str, int]
        ``(snapshot_dir, sha256_hex, ndjson_byte_size)`` where the hash and
        size are derived from the persisted ``response.ndjson``.
    """
    root = raw_root or RAW_DATA_ROOT
    snap_dir = root / source_type / snapshot_id
    snap_dir.mkdir(parents=True, exist_ok=True)

    # ---- response.ndjson ------------------------------------------------
    ndjson_path = snap_dir / "response.ndjson"
    lines: list[bytes] = []
    for payload in response_payloads:
        # Re-serialise for deterministic ordering
        obj = json.loads(payload)
        lines.append(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8"))

    ndjson_content = b"\n".join(lines) + b"\n" if lines else b""
    ndjson_path.write_bytes(ndjson_content)

    # Hash from persisted file
    sha256_hex = hashlib.sha256(ndjson_content).hexdigest()
    ndjson_size = len(ndjson_content)

    # ---- request.json ---------------------------------------------------
    if request_metadata:
        request_path = snap_dir / "request.json"
        request_path.write_text(
            json.dumps(request_metadata, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # ---- manifest.json --------------------------------------------------
    manifest = {
        "snapshot_id": snapshot_id,
        "source_type": source_type,
        "page_count": len(response_payloads),
        "record_count": record_count,
        "ndjson_sha256": sha256_hex,
        "ndjson_bytes": ndjson_size,
        "retrieved_at": datetime.now(UTC).isoformat(),
    }
    manifest_path = snap_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    logger.debug(
        "Persisted API snapshot: %s (%d pages, %d bytes, sha256=%s…)",
        snap_dir,
        len(response_payloads),
        ndjson_size,
        sha256_hex[:12],
    )
    return snap_dir, sha256_hex, ndjson_size


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def hash_file(path: Path) -> tuple[str, int]:
    """Compute SHA-256 hash and byte-size of a single file.

    Parameters
    ----------
    path : Path
        File to hash.

    Returns
    -------
    tuple[str, int]
        ``(sha256_hex, byte_size)``
    """
    content = path.read_bytes()
    return hashlib.sha256(content).hexdigest(), len(content)


def hash_directory(directory: Path) -> tuple[str, int]:
    """Compute a combined SHA-256 hash over all files in *directory*.

    Files are hashed in sorted name order so the result is deterministic.

    Parameters
    ----------
    directory : Path
        Directory whose files to hash.

    Returns
    -------
    tuple[str, int]
        ``(combined_sha256_hex, total_byte_size)``
    """
    hasher = hashlib.sha256()
    total_size = 0
    for child in sorted(directory.rglob("*")):
        if child.is_file():
            content = child.read_bytes()
            hasher.update(content)
            total_size += len(content)
    return hasher.hexdigest(), total_size
