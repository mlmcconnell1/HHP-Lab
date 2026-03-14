"""CLI commands for managing named build directories."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from coclab.builds import DEFAULT_BUILDS_DIR, ensure_build_dir, list_builds
from coclab.year_spec import parse_year_spec


def create_build(
    name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Name of the build directory to create.",
        ),
    ],
    years: Annotated[
        str,
        typer.Option(
            "--years",
            help=(
                "Year spec: range (2018-2024), list (2018,2019,2020), "
                "or mixed (2018-2020,2022)."
            ),
        ),
    ],
    builds_dir: Annotated[
        Path,
        typer.Option(
            "--builds-dir",
            help="Root directory for named builds.",
        ),
    ] = DEFAULT_BUILDS_DIR,
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir",
            help="Root data directory for resolving base assets (default: data/).",
        ),
    ] = None,
    geo_type: Annotated[
        str,
        typer.Option(
            "--geo-type",
            help="Target analysis geography for this build: 'coc' or 'metro'.",
        ),
    ] = "coc",
    definition_version: Annotated[
        str | None,
        typer.Option(
            "--definition-version",
            help="Synthetic geography definition version required for metro builds.",
        ),
    ] = None,
) -> None:
    """Create a named build directory scaffold with pinned base assets."""
    try:
        parsed_years = parse_year_spec(years)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    if geo_type not in {"coc", "metro"}:
        typer.echo(
            f"Error: Unsupported --geo-type '{geo_type}'. Use 'coc' or 'metro'.",
            err=True,
        )
        raise typer.Exit(code=2)
    if geo_type == "metro" and not definition_version:
        typer.echo(
            "Error: --definition-version is required when --geo-type=metro.",
            err=True,
        )
        raise typer.Exit(code=2)

    try:
        build_dir, base_assets = ensure_build_dir(
            name,
            builds_dir=builds_dir,
            years=parsed_years,
            data_dir=data_dir,
            geo_type=geo_type,
            definition_version=definition_version,
        )
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        typer.echo(
            "Ensure boundary files are ingested for all requested years.",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    typer.echo(f"Created build: {name}")
    typer.echo(f"  Years: {parsed_years}")
    typer.echo(f"  Geo type: {geo_type}")
    if definition_version is not None:
        typer.echo(f"  Definition version: {definition_version}")
    typer.echo(f"  Path: {build_dir}")
    if geo_type == "metro":
        typer.echo("  Base assets: skipped (definition-fixed)")
    else:
        typer.echo(f"  Base assets pinned: {len(base_assets)}")
        for asset in base_assets:
            typer.echo(
                f"    - B{asset['year']}: {asset['sha256'][:12]}..."
            )
    typer.echo(f"  Manifest: {build_dir / 'manifest.json'}")


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


def catalog_cmd(
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir",
            help="Root data directory to scan for boundary assets.",
        ),
    ] = None,
) -> None:
    """Sync the global base asset catalog from curated boundary files.

    Scans ``data/curated/coc_boundaries/`` for boundary files and writes
    an inventory to ``data/registry/base_assets.json``.  The catalog is
    optional and speeds up ``build create`` by avoiding filesystem probing.

    Examples:

        coclab generate catalog
    """
    from coclab.builds import scan_boundary_assets, write_base_catalog

    assets = scan_boundary_assets(data_dir=data_dir)
    if not assets:
        typer.echo("No boundary assets found to catalog.")
        return

    catalog_path = write_base_catalog(assets)
    typer.echo(f"Cataloged {len(assets)} base assets to: {catalog_path}")
    for asset in assets:
        typer.echo(f"  - B{asset['year']}: {asset['sha256'][:12]}...")
