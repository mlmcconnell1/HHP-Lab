"""CLI command for rebuilding the source registry."""

import hashlib
import logging
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from coclab.source_registry import (
    DEFAULT_REGISTRY_PATH,
    REGISTRY_COLUMNS,
)

logger = logging.getLogger(__name__)


def _compute_file_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of a file.

    For ZIP files, hashes the decompressed contents (via
    :func:`~coclab.raw_snapshot.hash_zip_contents`) so the hash is
    stable across re-compression by upstream servers.
    """
    content = filepath.read_bytes()
    if filepath.suffix.lower() == ".zip":
        from coclab.raw_snapshot import hash_zip_contents

        return hash_zip_contents(content)
    return hashlib.sha256(content).hexdigest()


def _load_registry(registry_path: Path) -> pd.DataFrame:
    """Load the source registry from disk."""
    if not registry_path.exists():
        return pd.DataFrame(columns=REGISTRY_COLUMNS)
    return pd.read_parquet(registry_path)


def _save_registry(df: pd.DataFrame, registry_path: Path) -> None:
    """Save the source registry to disk."""
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    if "ingested_at" in df.columns:
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    df.to_parquet(registry_path, index=False)


def registry_rebuild(
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Preview changes without modifying the registry.",
        ),
    ] = False,
    on_hash_mismatch: Annotated[
        str,
        typer.Option(
            "--on-hash-mismatch",
            help="Non-interactive policy for hash mismatches: skip, update, remove, error.",
        ),
    ] = "prompt",
    registry_path: Annotated[
        Path,
        typer.Option(
            "--registry",
            "-r",
            help="Path to source registry file.",
        ),
    ] = DEFAULT_REGISTRY_PATH,
) -> None:
    """Rebuild the source registry by validating local files.

    For each registry entry with a local_path:
    - If the file is missing, remove the entry
    - If the file exists and hash matches, keep as-is
    - If the file exists but hash differs, prompt to update or remove

    Only the latest entry per local_path is checked; older entries for the
    same path are removed automatically.

    Examples:

        coclab registry rebuild

        coclab registry rebuild --dry-run
    """
    if not registry_path.exists():
        typer.echo(f"Registry not found at {registry_path}")
        return

    df = _load_registry(registry_path)

    if df.empty:
        typer.echo("Registry is empty. Nothing to rebuild.")
        return

    typer.echo(f"Loaded {len(df)} registry entries from {registry_path}\n")

    # Separate entries with and without local_path
    has_path = df["local_path"].notna() & (df["local_path"] != "")
    entries_with_path = df[has_path].copy()
    entries_without_path = df[~has_path].copy()

    if entries_without_path.empty:
        skipped_count = 0
    else:
        skipped_count = len(entries_without_path)
        typer.echo(f"Skipping {skipped_count} entries without local_path\n")

    if entries_with_path.empty:
        typer.echo("No entries with local_path to check.")
        return

    # Group by local_path, keep only the latest entry per path
    entries_with_path = entries_with_path.sort_values("ingested_at", ascending=False)
    unique_paths = entries_with_path.drop_duplicates(subset=["local_path"], keep="first")
    duplicate_entries = entries_with_path[
        ~entries_with_path.index.isin(unique_paths.index)
    ]

    if not duplicate_entries.empty:
        typer.echo(
            f"Found {len(duplicate_entries)} older duplicate entries "
            f"(same local_path as newer entries)\n"
        )

    # Track actions
    to_keep = []  # indices to keep
    to_remove = []  # indices to remove
    to_update = []  # (index, new_hash) tuples
    missing_count = 0  # count of missing files
    hash_mismatches = 0  # count of hash mismatches (for dry-run display)

    # Always remove duplicates
    to_remove.extend(duplicate_entries.index.tolist())

    # Check each unique path
    typer.echo("Checking files...\n")

    for idx, row in unique_paths.iterrows():
        local_path = Path(row["local_path"])
        source_type = row["source_type"]
        stored_hash = row["raw_sha256"]

        # Display path relative to cwd if possible
        try:
            display_path = local_path.relative_to(Path.cwd())
        except ValueError:
            display_path = local_path

        if not local_path.exists():
            typer.echo(f"  MISSING: {display_path}")
            typer.echo(f"           ({source_type})")
            to_remove.append(idx)
            missing_count += 1
            continue

        # File exists - check hash
        current_hash = _compute_file_hash(local_path)

        if current_hash == stored_hash:
            typer.echo(f"  OK:      {display_path}")
            to_keep.append(idx)
            continue

        # Hash mismatch
        hash_mismatches += 1
        typer.echo(f"  CHANGED: {display_path}")
        typer.echo(f"           ({source_type})")
        typer.echo(f"           Registry: {stored_hash[:16]}...")
        typer.echo(f"           Current:  {current_hash[:16]}...")

        if dry_run:
            typer.echo("           [dry-run: would prompt for action]\n")
            to_keep.append(idx)  # Keep in dry-run mode
            continue

        # Determine action from policy or prompt
        import os

        non_interactive = (
            on_hash_mismatch != "prompt"
            or os.environ.get("COCLAB_NON_INTERACTIVE") == "1"
        )

        if non_interactive:
            policy = on_hash_mismatch if on_hash_mismatch != "prompt" else "skip"
            if policy == "update":
                to_update.append((idx, current_hash))
                typer.echo("           -> Updating hash (policy)\n")
            elif policy == "remove":
                to_remove.append(idx)
                typer.echo("           -> Removing entry (policy)\n")
            elif policy == "error":
                typer.echo(
                    "           -> Hash mismatch with --on-hash-mismatch=error",
                    err=True,
                )
                raise typer.Exit(1)
            else:  # skip
                to_keep.append(idx)
                typer.echo("           -> Skipped (policy)\n")
        else:
            # Prompt user
            typer.echo("")
            action = typer.prompt(
                "           Action: [u]pdate hash, [s]kip, [r]emove entry",
                default="s",
            )

            if action.lower().startswith("u"):
                to_update.append((idx, current_hash))
                typer.echo("           -> Will update hash\n")
            elif action.lower().startswith("r"):
                to_remove.append(idx)
                typer.echo("           -> Will remove entry\n")
            else:
                to_keep.append(idx)
                typer.echo("           -> Skipped\n")

    # Summary
    duplicate_count = len(duplicate_entries)

    typer.echo("\n" + "=" * 50)
    typer.echo("SUMMARY")
    typer.echo("=" * 50)
    typer.echo(f"  Entries without local_path (skipped): {skipped_count}")
    if dry_run:
        typer.echo(f"  Entries OK (hash matches):            {len(to_keep) - hash_mismatches}")
        typer.echo(f"  Entries with hash mismatch:           {hash_mismatches}")
    else:
        typer.echo(f"  Entries OK (hash matches):            {len(to_keep)}")
        typer.echo(f"  Entries to update (hash changed):     {len(to_update)}")
    typer.echo(f"  Duplicate entries to remove:          {duplicate_count}")
    typer.echo(f"  Missing files to remove:              {missing_count}")

    if dry_run:
        typer.echo("\n[dry-run mode - no changes made]")
        return

    if not to_remove and not to_update:
        typer.echo("\nNo changes needed.")
        return

    # Apply changes
    typer.echo("")

    # Remove entries
    if to_remove:
        df = df.drop(index=to_remove)
        typer.echo(f"Removed {len(to_remove)} entries.")

    # Update hashes
    for idx, new_hash in to_update:
        df.loc[idx, "raw_sha256"] = new_hash
    if to_update:
        typer.echo(f"Updated {len(to_update)} hashes.")

    # Save
    _save_registry(df, registry_path)
    typer.echo(f"\nSaved updated registry to {registry_path}")
    typer.echo(f"Final entry count: {len(df)}")
