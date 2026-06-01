"""CLI commands for ingesting Census geometries."""

import json
from typing import Annotated

import pyarrow.parquet as pq
import typer

from hhplab.census.ingest.tiger_blocks import get_block_geometry_output_path
from hhplab.census.ingest.urban_areas import get_urban_area_output_path
from hhplab.naming import (
    block_geometry_filename,
    county_filename,
    tract_filename,
    urban_area_filename,
)
from hhplab.paths import curated_dir


def _parquet_row_count(path) -> int:
    """Return a Parquet row count without materializing the full artifact."""
    return int(pq.ParquetFile(path).metadata.num_rows)


def ingest_tiger(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="TIGER vintage year (e.g., 2023).",
        ),
    ] = 2023,
    type_: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
        help="What to download: 'tracts', 'counties', 'blocks', 'urban-areas', or 'all'.",
        ),
    ] = "all",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Download TIGER census geometries.

    Downloads census tract, county, and optionally Urban Area shapefiles from the
        US Census Bureau's TIGER/Line files, reprojects to EPSG:4326, and saves as
    GeoParquet files.

    Examples:

        hhplab ingest tiger --year 2023

        hhplab ingest tiger --year 2023 --type tracts

        hhplab ingest tiger --year 2023 --type counties --force
    """
    # Validate type option
    valid_types = {"tracts", "counties", "blocks", "urban-areas", "all"}
    if type_ not in valid_types:
        typer.echo(
            f"Error: Invalid type '{type_}'. Must be one of: {', '.join(sorted(valid_types))}",
            err=True,
        )
        raise typer.Exit(1)

    # Determine what to download
    download_tracts = type_ in ("tracts", "all")
    download_counties = type_ in ("counties", "all")
    download_blocks = type_ == "blocks" or (type_ == "all" and year == 2020)
    download_urban_areas = type_ == "urban-areas"

    # Define output paths using canonical naming helpers
    tracts_path = curated_dir("tiger") / tract_filename(year)
    counties_path = curated_dir("tiger") / county_filename(year)
    blocks_path = curated_dir("tiger") / block_geometry_filename(year)
    urban_areas_path = curated_dir("tiger") / urban_area_filename(year)

    # Track what was downloaded
    downloaded = []

    # Process tracts
    if download_tracts:
        if tracts_path.exists() and not force:
            typer.echo(f"Tracts file already exists: {tracts_path}")
            typer.echo("Use --force to re-download.")
        else:
            if tracts_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {tracts_path}")
            typer.echo(f"Downloading TIGER tracts for {year}...")
            try:
                from hhplab.census.ingest import ingest_tiger_tracts

                output_path = ingest_tiger_tracts(year, show_progress=True)
                typer.echo(f"Saved tracts to: {output_path}")
                downloaded.append(("tracts", output_path))
            except Exception as e:
                typer.echo(f"Error downloading tracts: {e}", err=True)
                if download_counties:
                    typer.echo("Continuing with counties...")
                else:
                    raise typer.Exit(1) from e

    # Process counties
    if download_counties:
        if counties_path.exists() and not force:
            typer.echo(f"Counties file already exists: {counties_path}")
            typer.echo("Use --force to re-download.")
        else:
            if counties_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {counties_path}")
            typer.echo(f"Downloading TIGER counties for {year}...")
            try:
                from hhplab.census.ingest import ingest_tiger_counties

                output_path = ingest_tiger_counties(year)
                typer.echo(f"Saved counties to: {output_path}")
                downloaded.append(("counties", output_path))
            except Exception as e:
                typer.echo(f"Error downloading counties: {e}", err=True)
                if not downloaded:
                    raise typer.Exit(1) from e

    if download_blocks:
        if blocks_path.exists() and not force:
            typer.echo(f"Block geometry file already exists: {blocks_path}")
            typer.echo("Use --force to re-download.")
        else:
            if blocks_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {blocks_path}")
            typer.echo(f"Downloading TIGER tabulation blocks for {year}...")
            try:
                from hhplab.census.ingest import ingest_block_geometry

                output_path = ingest_block_geometry(year, force=force)
                typer.echo(f"Saved block geometry to: {output_path}")
                downloaded.append(("blocks", output_path))
            except Exception as e:
                typer.echo(f"Error downloading block geometry: {e}", err=True)
                if not downloaded:
                    raise typer.Exit(1) from e

    if download_urban_areas:
        if urban_areas_path.exists() and not force:
            typer.echo(f"Urban Areas file already exists: {urban_areas_path}")
            typer.echo("Use --force to re-download.")
        else:
            if urban_areas_path.exists() and force:
                typer.echo(f"Forcing rebuild: removing existing {urban_areas_path}")
            typer.echo(f"Downloading Census Urban Areas for {year}...")
            try:
                from hhplab.census.ingest import ingest_urban_areas

                output_path = ingest_urban_areas(year, force=force)
                typer.echo(f"Saved Urban Areas to: {output_path}")
                downloaded.append(("urban_areas", output_path))
            except Exception as e:
                typer.echo(f"Error downloading Urban Areas: {e}", err=True)
                if not downloaded:
                    raise typer.Exit(1) from e

    # Summary
    if downloaded:
        typer.echo("")
        typer.echo("Census geometry ingestion complete:")
        for name, path in downloaded:
            typer.echo(f"  {name}: {path}")
    elif not (
        tracts_path.exists()
        or counties_path.exists()
        or blocks_path.exists()
        or urban_areas_path.exists()
    ):
        typer.echo("No files were downloaded.")


def ingest_block_geometry_cmd(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="Census tabulation block geometry vintage. Currently supports 2020.",
        ),
    ] = 2020,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Ingest Census tabulation block geometry."""
    from hhplab.census.ingest.tiger_blocks import ingest_block_geometry

    output_path = get_block_geometry_output_path(year)
    if output_path.exists() and not force:
        block_count = _parquet_row_count(output_path)
        if output_json:
            typer.echo(
                json.dumps(
                    {
                        "status": "ok",
                        "cached": True,
                        "block_vintage": year,
                        "output_path": str(output_path),
                        "block_count": block_count,
                    }
                )
            )
            return
        typer.echo(f"Cached file found: {output_path}")
        typer.echo(f"Blocks: {block_count:,}")
        typer.echo("Use --force to re-ingest.")
        return

    try:
        path = ingest_block_geometry(year, force=force)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    block_count = _parquet_row_count(path)
    if output_json:
        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "cached": False,
                    "block_vintage": year,
                    "output_path": str(path),
                    "block_count": block_count,
                }
            )
        )
        return

    typer.echo("Ingested Census tabulation block geometry.")
    typer.echo(f"Output file: {path}")
    typer.echo(f"Blocks: {block_count:,}")


def ingest_urban_areas_cmd(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="Census Urban Area vintage: 2010 or 2020.",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Ingest Census Urban Area geometry for 2010 or 2020."""
    from hhplab.census.ingest.urban_areas import ingest_urban_areas

    output_path = get_urban_area_output_path(year)
    if output_path.exists() and not force:
        urban_area_count = _parquet_row_count(output_path)
        if output_json:
            typer.echo(
                json.dumps(
                    {
                        "status": "ok",
                        "cached": True,
                        "urban_area_vintage": year,
                        "output_path": str(output_path),
                        "urban_area_count": urban_area_count,
                    }
                )
            )
            return
        typer.echo(f"Cached file found: {output_path}")
        typer.echo(f"Urban Areas: {urban_area_count:,}")
        typer.echo("Use --force to re-ingest.")
        return

    try:
        path = ingest_urban_areas(year, force=force)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    urban_area_count = _parquet_row_count(path)
    if output_json:
        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "cached": False,
                    "urban_area_vintage": year,
                    "output_path": str(path),
                    "urban_area_count": urban_area_count,
                }
            )
        )
        return

    typer.echo("Ingested Census Urban Area geometry.")
    typer.echo(f"Output file: {path}")
    typer.echo(f"Urban Areas: {urban_area_count:,}")
