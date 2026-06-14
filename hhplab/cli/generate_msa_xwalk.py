"""CLI command for generating CoC-to-MSA PIT allocation crosswalks."""

from __future__ import annotations

from typing import Annotated, Literal

import geopandas as gpd
import pandas as pd
import typer

from hhplab.geo.geo_io import resolve_curated_boundary_path
from hhplab.msa.msa_definitions import DEFINITION_VERSION, DELINEATION_FILE_YEAR
from hhplab.naming import (
    block_geometry_path,
    county_path,
    msa_coc_block_population_xwalk_path,
    msa_coc_xwalk_path,
    pl_block_population_path,
)
from hhplab.registry.boundary_registry import latest_vintage, list_boundaries


def _resolve_boundary_vintage(boundary: str | None) -> str:
    if boundary is None:
        resolved = latest_vintage()
        if resolved is None:
            raise FileNotFoundError(
                "No boundary vintages found in the registry. "
                "Run: hhplab ingest boundaries --source hud_exchange --vintage <year>"
            )
        return resolved

    available = {entry.boundary_vintage for entry in list_boundaries()}
    if boundary not in available:
        available_list = sorted(available)
        raise FileNotFoundError(
            f"Boundary vintage '{boundary}' not found in registry. "
            f"Available: {available_list}. "
            f"Run: hhplab ingest boundaries --source hud_exchange --vintage {boundary}"
        )
    return boundary


def generate_msa_xwalk(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage. Uses the latest registered vintage when omitted.",
        ),
    ] = None,
    counties: Annotated[
        int,
        typer.Option(
            "--counties",
            "-c",
            help="County geometry vintage used to derive MSA overlaps.",
        ),
    ] = DELINEATION_FILE_YEAR,
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="MSA definition version to use.",
        ),
    ] = DEFINITION_VERSION,
    allocation_basis: Annotated[
        Literal["area", "block_population"],
        typer.Option(
            "--allocation-basis",
            help="Allocation basis for the CoC-to-MSA crosswalk.",
        ),
    ] = "area",
    blocks: Annotated[
        int,
        typer.Option(
            "--blocks",
            help="Block geometry vintage for block-population allocation.",
        ),
    ] = 2020,
    decennial: Annotated[
        int,
        typer.Option(
            "--decennial",
            help="Decennial PL 94-171 population vintage for block-population allocation.",
        ),
    ] = 2020,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite an existing CoC-to-MSA crosswalk artifact.",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Generate the auditable CoC-to-MSA allocation crosswalk used for PIT."""
    import json

    from hhplab.msa.crosswalk import (
        FULL_ALLOCATION_THRESHOLD,
        build_coc_msa_block_population_crosswalk,
        build_coc_msa_crosswalk,
        save_coc_msa_block_population_crosswalk,
        save_coc_msa_crosswalk,
        summarize_coc_msa_allocation,
    )
    from hhplab.msa.msa_io import read_msa_county_membership

    try:
        resolved_boundary = _resolve_boundary_vintage(boundary)
    except FileNotFoundError as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if allocation_basis == "block_population":
        output_path = msa_coc_block_population_xwalk_path(
            resolved_boundary,
            definition_version,
            counties,
            blocks,
            decennial,
        )
    else:
        output_path = msa_coc_xwalk_path(
            resolved_boundary,
            definition_version,
            counties,
        )
    if output_path.exists() and not force:
        payload = {
            "status": "error",
            "error": "artifact_exists",
            "path": str(output_path),
        }
        if json_output:
            typer.echo(json.dumps(payload))
        else:
            typer.echo(
                f"Error: CoC-to-MSA crosswalk already exists at {output_path}. "
                "Use --force to overwrite.",
                err=True,
            )
        raise typer.Exit(1)

    try:
        boundary_path = resolve_curated_boundary_path(resolved_boundary)
    except FileNotFoundError as exc:
        message = (
            f"{exc}. "
            f"Run: hhplab ingest boundaries --source hud_exchange --vintage {resolved_boundary}"
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1) from exc

    county_geometry_path = county_path(counties)
    block_geometry_artifact = block_geometry_path(blocks)
    block_population_artifact = pl_block_population_path(decennial, blocks)

    if not boundary_path.exists():
        message = (
            f"Boundary file not found at {boundary_path}. "
            f"Run: hhplab ingest boundaries --source hud_exchange --vintage {resolved_boundary}"
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1)

    if not county_geometry_path.exists():
        message = (
            f"County geometry file not found at {county_geometry_path}. "
            f"Run: hhplab ingest tiger --year {counties} --type counties"
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1)
    if allocation_basis == "block_population" and not block_geometry_artifact.exists():
        message = (
            f"Block geometry file not found at {block_geometry_artifact}. "
            f"Run: hhplab ingest tiger --year {blocks} --type blocks"
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1)
    if allocation_basis == "block_population" and not block_population_artifact.exists():
        message = (
            f"PL block population file not found at {block_population_artifact}. "
            f"Run: hhplab ingest pl-blocks --decennial {decennial} --blocks {blocks}"
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1)

    try:
        msa_membership = read_msa_county_membership(definition_version)
    except FileNotFoundError as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if not json_output:
        typer.echo(
            "Building CoC-to-MSA crosswalk "
            f"(boundary={resolved_boundary}, msa={definition_version}, counties={counties}, "
            f"allocation_basis={allocation_basis})..."
        )

    try:
        coc_gdf = gpd.read_parquet(boundary_path)
        county_gdf = gpd.read_parquet(county_geometry_path)
        if allocation_basis == "block_population":
            block_gdf = gpd.read_parquet(block_geometry_artifact)
            block_population = pd.read_parquet(block_population_artifact)
            crosswalk = build_coc_msa_block_population_crosswalk(
                coc_gdf,
                county_gdf,
                msa_membership,
                block_gdf,
                block_population,
                boundary_vintage=resolved_boundary,
                county_vintage=str(counties),
                block_vintage=str(blocks),
                decennial_vintage=str(decennial),
                definition_version=definition_version,
            )
        else:
            crosswalk = build_coc_msa_crosswalk(
                coc_gdf,
                county_gdf,
                msa_membership,
                boundary_vintage=resolved_boundary,
                county_vintage=str(counties),
                definition_version=definition_version,
            )
    except (ValueError, OSError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if allocation_basis == "block_population":
        written_path = save_coc_msa_block_population_crosswalk(
            crosswalk,
            boundary_vintage=resolved_boundary,
            county_vintage=str(counties),
            block_vintage=str(blocks),
            decennial_vintage=str(decennial),
            definition_version=definition_version,
        )
    else:
        written_path = save_coc_msa_crosswalk(
            crosswalk,
            boundary_vintage=resolved_boundary,
            county_vintage=str(counties),
            definition_version=definition_version,
        )
    allocation_summary = summarize_coc_msa_allocation(crosswalk)
    partial_allocations = int(
        (allocation_summary["allocation_share_sum"] < FULL_ALLOCATION_THRESHOLD).sum()
    )
    max_unallocated = (
        float(allocation_summary["unallocated_share"].max())
        if not allocation_summary.empty
        else 0.0
    )
    payload = {
        "status": "ok",
        "allocation_basis": allocation_basis,
        "boundary_vintage": resolved_boundary,
        "definition_version": definition_version,
        "county_vintage": str(counties),
        "block_vintage": str(blocks) if allocation_basis == "block_population" else None,
        "decennial_vintage": str(decennial) if allocation_basis == "block_population" else None,
        "rows": int(len(crosswalk)),
        "coc_count": int(crosswalk["coc_id"].nunique()) if not crosswalk.empty else 0,
        "msa_count": int(crosswalk["msa_id"].nunique()) if not crosswalk.empty else 0,
        "partially_allocated_cocs": partial_allocations,
        "max_unallocated_share": max_unallocated,
        "artifact": str(written_path),
    }
    warning = crosswalk.attrs.get("warning")
    if warning:
        payload["warning"] = str(warning)
    if json_output:
        typer.echo(json.dumps(payload))
        return

    typer.echo(f"  Written: {written_path}")
    typer.echo(
        "  Coverage: "
        f"{payload['coc_count']} CoCs across {payload['msa_count']} MSAs; "
        f"{partial_allocations} CoCs have unallocated non-MSA area."
    )
    if warning:
        typer.echo(f"  Warning: {warning}")
