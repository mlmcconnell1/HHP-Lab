"""CLI commands for managing named build directories."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from coclab.builds import DEFAULT_BUILDS_DIR, ensure_build_dir, list_builds


def create_build(
    name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Name of the build directory to create.",
        ),
    ],
    builds_dir: Annotated[
        Path,
        typer.Option(
            "--builds-dir",
            help="Root directory for named builds.",
        ),
    ] = DEFAULT_BUILDS_DIR,
) -> None:
    """Create a named build directory scaffold."""
    build_dir = ensure_build_dir(name, builds_dir=builds_dir)
    typer.echo(f"Created build: {name}")
    typer.echo(f"  Path: {build_dir}")
    typer.echo(f"  Curated: {build_dir / 'data' / 'curated'}")
    typer.echo(f"  Raw: {build_dir / 'data' / 'raw'}")
    typer.echo(f"  Hub: {build_dir / 'hub'}")


def list_builds_cmd(
    builds_dir: Annotated[
        Path,
        typer.Option(
            "--builds-dir",
            help="Root directory for named builds.",
        ),
    ] = DEFAULT_BUILDS_DIR,
) -> None:
    """List available named builds."""
    builds = list_builds(builds_dir=builds_dir)
    if not builds:
        typer.echo(f"No builds found in {builds_dir}.")
        typer.echo("Create one with: coclab build create --name <build>")
        return

    typer.echo(f"Builds in {builds_dir}:")
    for build in builds:
        typer.echo(f"  - {build.name}")
