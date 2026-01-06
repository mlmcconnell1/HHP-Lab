"""CLI command for building CoC crosswalks."""

from pathlib import Path
from typing import Annotated

import click
import geopandas as gpd
import typer

from coclab.measures.diagnostics import compute_crosswalk_diagnostics, summarize_diagnostics
from coclab.registry.registry import latest_vintage, list_vintages
from coclab.xwalks.county import build_coc_county_crosswalk, save_county_crosswalk
from coclab.xwalks.tract import build_coc_tract_crosswalk, save_crosswalk


def build_xwalks(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        int,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage year (e.g., 2023).",
        ),
    ] = 2023,
    counties: Annotated[
        int | None,
        typer.Option(
            "--counties",
            "-c",
            help="Census county vintage year. Defaults to same as tracts.",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
) -> None:
    """Build tract and county crosswalks for CoC boundaries.

    Creates area-weighted crosswalks mapping census tracts and counties
    to CoC boundaries. The crosswalks enable aggregating tract-level
    statistics (like ACS data) to CoC level.

    Examples:

        coclab build-xwalks --boundary 2025 --tracts 2023

        coclab build-xwalks --boundary 2025 --tracts 2023 --counties 2023
    """
    # Resolve county vintage
    county_vintage = counties or tracts

    # Resolve boundary vintage from registry
    if boundary is None:
        boundary = latest_vintage()
        if boundary is None:
            typer.echo(
                "Error: No boundary vintages found in registry. "
                "Run 'coclab ingest' first.",
                err=True,
            )
            raise typer.Exit(1)
        typer.echo(f"Using latest boundary vintage: {boundary}")
    else:
        # Verify the boundary vintage exists
        vintages = list_vintages()
        vintage_ids = [v.boundary_vintage for v in vintages]
        if boundary not in vintage_ids:
            typer.echo(
                f"Error: Boundary vintage '{boundary}' not found in registry. "
                f"Available: {vintage_ids}",
                err=True,
            )
            raise typer.Exit(1)

    # Load CoC boundaries from registry
    typer.echo(f"Loading CoC boundaries (vintage: {boundary})...")
    vintages = list_vintages()
    boundary_entry = next(v for v in vintages if v.boundary_vintage == boundary)
    coc_gdf = gpd.read_parquet(boundary_entry.path)

    # Load tract geometries
    tract_path = Path(f"data/curated/census/tracts__{tracts}.parquet")
    if not tract_path.exists():
        typer.echo(
            f"Error: Tract file not found: {tract_path}. "
            f"Run 'coclab ingest-census --year {tracts} --type tracts' first.",
            err=True,
        )
        raise typer.Exit(1)

    typer.echo(f"Loading census tracts (vintage: {tracts})...")
    tract_gdf = gpd.read_parquet(tract_path)

    # Standardize column names for tract crosswalk builder
    # The tract module expects 'GEOID', but tiger_tracts saves as 'geoid'
    if "geoid" in tract_gdf.columns and "GEOID" not in tract_gdf.columns:
        tract_gdf = tract_gdf.rename(columns={"geoid": "GEOID"})

    # Build tract crosswalk with progress bar
    n_cocs = len(coc_gdf)
    with click.progressbar(
        length=n_cocs,
        label="Building tract crosswalk",
        show_pos=True,
    ) as progress:
        def update_progress(completed: int, total: int) -> None:
            progress.update(completed - progress.pos)

        tract_xwalk = build_coc_tract_crosswalk(
            coc_gdf=coc_gdf,
            tract_gdf=tract_gdf,
            boundary_vintage=boundary,
            tract_vintage=str(tracts),
            progress_callback=update_progress,
        )

    # Save tract crosswalk
    tract_output = save_crosswalk(
        crosswalk=tract_xwalk,
        boundary_vintage=boundary,
        tract_vintage=str(tracts),
        output_dir=output_dir,
    )
    typer.echo(f"Saved tract crosswalk to: {tract_output}")

    # Compute and display tract crosswalk diagnostics
    typer.echo("")
    tract_diagnostics = compute_crosswalk_diagnostics(tract_xwalk)
    typer.echo(summarize_diagnostics(tract_diagnostics))

    # Load county geometries
    county_path = Path(f"data/curated/census/counties__{county_vintage}.parquet")
    if not county_path.exists():
        typer.echo(
            f"Warning: County file not found: {county_path}. "
            f"Skipping county crosswalk.",
            err=True,
        )
        return

    typer.echo(f"\nLoading census counties (vintage: {county_vintage})...")
    county_gdf = gpd.read_parquet(county_path)

    # Standardize column names for county crosswalk builder
    if "geoid" in county_gdf.columns and "GEOID" not in county_gdf.columns:
        county_gdf = county_gdf.rename(columns={"geoid": "GEOID"})

    # Build county crosswalk
    typer.echo("Building county crosswalk...")
    county_xwalk = build_coc_county_crosswalk(
        coc_gdf=coc_gdf,
        county_gdf=county_gdf,
        boundary_vintage=boundary,
    )

    # Save county crosswalk
    county_output = save_county_crosswalk(
        crosswalk=county_xwalk,
        boundary_vintage=boundary,
        output_dir=output_dir,
    )
    typer.echo(f"Saved county crosswalk to: {county_output}")

    typer.echo("\nCrosswalk generation complete!")
