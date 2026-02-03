"""Export bundle CLI command for CoC Lab.

Creates analysis-ready export bundles with MANIFEST.json for downstream
analysis repositories.
"""

from __future__ import annotations

import re
import tarfile
from pathlib import Path
from typing import Annotated

import typer

from coclab.builds import require_build_dir, resolve_build_dir
from coclab.export.codebook import write_codebook
from coclab.export.copy import copy_artifacts, create_bundle_structure
from coclab.export.manifest import build_manifest, write_manifest
from coclab.export.readme import generate_readme, write_readme
from coclab.export.selection import build_selection_plan
from coclab.export.types import ArtifactRecord, BundleConfig
from coclab.export.validate import run_all_validations

# Exit codes per spec
EXIT_SUCCESS = 0
EXIT_VALIDATION_FAILURE = 2
EXIT_FILESYSTEM_FAILURE = 3
EXIT_MANIFEST_FAILURE = 4


def _find_next_export_number(out_dir: Path) -> int:
    """Find the next export folder number (export-N).

    Scans out_dir for existing export-N folders and returns max(N) + 1,
    or 1 if no existing exports.

    Args:
        out_dir: Directory containing export folders

    Returns:
        Next export number to use
    """
    if not out_dir.exists():
        return 1

    pattern = re.compile(r"^export-(\d+)$")
    max_num = 0

    for item in out_dir.iterdir():
        if item.is_dir():
            match = pattern.match(item.name)
            if match:
                num = int(match.group(1))
                if num > max_num:
                    max_num = num

    return max_num + 1


def _parse_include(include_str: str) -> set[str]:
    """Parse comma-separated include string into set.

    Args:
        include_str: Comma-separated list like "panel,manifest,codebook"

    Returns:
        Set of component names
    """
    return {item.strip() for item in include_str.split(",") if item.strip()}


def _format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    if size_bytes >= 1_000_000_000:
        return f"{size_bytes / 1_000_000_000:.1f} GB"
    elif size_bytes >= 1_000_000:
        return f"{size_bytes / 1_000_000:.0f} MB"
    elif size_bytes >= 1_000:
        return f"{size_bytes / 1_000:.0f} KB"
    else:
        return f"{size_bytes} bytes"


def _create_tarball(bundle_root: Path) -> Path:
    """Create a tarball of the bundle directory.

    Args:
        bundle_root: Path to the bundle directory

    Returns:
        Path to created tarball
    """
    tarball_path = bundle_root.with_suffix(".tar.gz")
    with tarfile.open(tarball_path, "w:gz") as tar:
        tar.add(bundle_root, arcname=bundle_root.name)
    return tarball_path


def export_bundle(
    name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Logical bundle name for metadata and documentation",
        ),
    ],
    out_dir: Annotated[
        Path,
        typer.Option(
            "--out-dir",
            "-o",
            help="Output directory where export-N folders are created",
        ),
    ] = Path("exports"),
    panel: Annotated[
        Path | None,
        typer.Option(
            "--panel",
            "-p",
            help="Explicit panel parquet path (inferred from curated if omitted)",
        ),
    ] = None,
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            help="Named build directory to source panels and artifacts from.",
        ),
    ] = None,
    include: Annotated[
        str,
        typer.Option(
            "--include",
            "-i",
            help="Components to include (comma-separated)",
        ),
    ] = "panel,manifest,codebook,diagnostics",
    boundary_vintage: Annotated[
        str | None,
        typer.Option(
            "--boundary-vintage",
            help="Boundary vintage (e.g., 2025)",
        ),
    ] = None,
    tract_vintage: Annotated[
        str | None,
        typer.Option(
            "--tract-vintage",
            help="Census tract vintage (e.g., 2023)",
        ),
    ] = None,
    county_vintage: Annotated[
        str | None,
        typer.Option(
            "--county-vintage",
            help="County vintage (e.g., 2023)",
        ),
    ] = None,
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs-vintage",
            help="ACS vintage (e.g., 2019-2023)",
        ),
    ] = None,
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year range (e.g., 2011-2024)",
        ),
    ] = None,
    copy_mode: Annotated[
        str,
        typer.Option(
            "--copy-mode",
            help="File copy mode: copy, hardlink, or symlink",
        ),
    ] = "copy",
    compress: Annotated[
        bool,
        typer.Option(
            "--compress",
            help="Create .tar.gz archive of the bundle",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Create bundle even if identical manifest exists",
        ),
    ] = False,
) -> None:
    """Export an analysis-ready bundle with MANIFEST.json.

    Creates a self-contained bundle directory suitable for ingestion into
    downstream analysis repositories (e.g., DVC-tracked repos). The bundle
    includes panels, selected inputs, diagnostics, and a machine-readable
    MANIFEST.json that pins exact content by hash and records provenance.

    Each invocation creates a new export folder (export-1, export-2, ...) so
    prior bundles are not overwritten.

    Examples:

        coclab build export --name my_analysis --panel \
            data/curated/panel/coc_panel__2011_2023.parquet

        coclab build export --name replication --include panel,manifest,codebook,inputs

        coclab build export --name full_export --boundary-vintage 2025 --years 2011-2024

        coclab build export --name demo --build demo
    """
    # Parse include options
    include_set = _parse_include(include)

    base_dir = Path(".")
    if build is not None:
        try:
            base_dir = require_build_dir(build)
        except FileNotFoundError:
            build_path = resolve_build_dir(build)
            typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
            typer.echo("Run: coclab build create --name <build>", err=True)
            raise typer.Exit(EXIT_VALIDATION_FAILURE)

    # Build configuration
    config = BundleConfig(
        name=name,
        out_dir=out_dir,
        panel_path=panel,
        include=include_set,
        boundary_vintage=boundary_vintage,
        tract_vintage=tract_vintage,
        county_vintage=county_vintage,
        acs_vintage=acs_vintage,
        years=years,
        copy_mode=copy_mode,  # type: ignore[arg-type]
        compress=compress,
        force=force,
    )

    # Determine export folder number
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        typer.echo(f"Error: Cannot create output directory: {e}", err=True)
        raise typer.Exit(EXIT_FILESYSTEM_FAILURE) from e

    export_num = _find_next_export_number(out_dir)
    export_id = f"export-{export_num}"
    bundle_root = out_dir / export_id

    typer.echo(f"Creating export bundle: {bundle_root}")
    typer.echo("")

    # Create bundle directory structure
    try:
        bundle_root.mkdir(parents=True, exist_ok=False)
        create_bundle_structure(bundle_root)
    except OSError as e:
        typer.echo(f"Error: Cannot create bundle directory: {e}", err=True)
        raise typer.Exit(EXIT_FILESYSTEM_FAILURE) from e

    # Build selection plan
    typer.echo("Selecting artifacts...")
    try:
        selection_plan = build_selection_plan(config, base_dir=base_dir)
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        # Clean up empty bundle directory
        _cleanup_bundle(bundle_root)
        raise typer.Exit(EXIT_VALIDATION_FAILURE) from e

    typer.echo("")

    # Run validations
    typer.echo("Running validations...")
    errors, warnings = run_all_validations(selection_plan, config)

    # Print warnings
    for warning in warnings:
        typer.echo(f"Warning: {warning}", err=True)

    # Fail on errors
    if errors:
        typer.echo("\nValidation failed:", err=True)
        for error in errors:
            typer.echo(f"  - {error}", err=True)
        # Clean up empty bundle directory
        _cleanup_bundle(bundle_root)
        raise typer.Exit(EXIT_VALIDATION_FAILURE)

    typer.echo("Validations passed.")
    typer.echo("")

    # Copy artifacts
    typer.echo("Copying artifacts...")
    try:
        copied_artifacts = copy_artifacts(selection_plan, bundle_root, copy_mode)
    except (OSError, ValueError) as e:
        typer.echo(f"Error copying artifacts: {e}", err=True)
        _cleanup_bundle(bundle_root)
        raise typer.Exit(EXIT_FILESYSTEM_FAILURE) from e

    # Generate codebook if included
    codebook_files: list[Path] = []
    if "codebook" in include_set and selection_plan.panel_artifacts:
        typer.echo("Generating codebook...")
        try:
            panel_path = selection_plan.panel_artifacts[0].source_path
            codebook_files = write_codebook(bundle_root, panel_path)

            # Add codebook artifacts to the copied list for manifest
            for codebook_file in codebook_files:
                rel_path = codebook_file.relative_to(bundle_root)
                copied_artifacts.append(
                    ArtifactRecord(
                        role="codebook",
                        source_path=codebook_file,
                        dest_path=str(rel_path),
                        bytes=codebook_file.stat().st_size,
                    )
                )
        except (FileNotFoundError, OSError) as e:
            typer.echo(f"Warning: Could not generate codebook: {e}", err=True)

    # Build parameters dict for manifest
    parameters = {
        "boundary_vintage": boundary_vintage,
        "tract_vintage": tract_vintage,
        "county_vintage": county_vintage,
        "acs_vintage": acs_vintage,
        "years": years,
        "copy_mode": copy_mode,
        "include": sorted(include_set),
    }
    # Filter out None values
    parameters = {k: v for k, v in parameters.items() if v is not None}

    # Build and write manifest
    typer.echo("Building manifest...")
    try:
        manifest = build_manifest(
            bundle_root=bundle_root,
            bundle_name=name,
            export_id=export_id,
            artifacts=copied_artifacts,
            parameters=parameters,
            notes="",
        )
        manifest_path = write_manifest(manifest, bundle_root)
    except (OSError, Exception) as e:
        typer.echo(f"Error creating manifest: {e}", err=True)
        _cleanup_bundle(bundle_root)
        raise typer.Exit(EXIT_MANIFEST_FAILURE) from e

    # Generate and write README
    typer.echo("Generating README...")
    try:
        readme_content = generate_readme(manifest, name)
        write_readme(bundle_root, readme_content)
    except OSError as e:
        typer.echo(f"Warning: Could not write README: {e}", err=True)

    # Create tarball if requested
    tarball_path: Path | None = None
    if compress:
        typer.echo("Creating compressed archive...")
        try:
            tarball_path = _create_tarball(bundle_root)
        except OSError as e:
            typer.echo(f"Warning: Could not create tarball: {e}", err=True)

    # Calculate totals for summary
    total_bytes = sum(a.bytes or 0 for a in copied_artifacts)
    total_files = len(copied_artifacts)

    # Count files by role
    role_counts: dict[str, int] = {}
    for artifact in copied_artifacts:
        role = artifact.role
        role_counts[role] = role_counts.get(role, 0) + 1

    # Print summary
    typer.echo("")
    typer.echo(f"Export created: {bundle_root}/")
    for role, count in sorted(role_counts.items()):
        typer.echo(f"  {role}: {count} file{'s' if count != 1 else ''}")
    typer.echo(f"MANIFEST.json: {manifest_path}")
    typer.echo(f"Total: {total_files} files, {_format_size(total_bytes)}")

    if tarball_path:
        typer.echo(f"Archive: {tarball_path}")

    raise typer.Exit(EXIT_SUCCESS)


def _cleanup_bundle(bundle_root: Path) -> None:
    """Remove an incomplete bundle directory.

    Args:
        bundle_root: Path to the bundle directory to remove
    """
    import shutil

    try:
        if bundle_root.exists():
            shutil.rmtree(bundle_root)
    except OSError:
        pass  # Best effort cleanup
