"""CLI command for generating CoC-to-MSA PIT allocation crosswalks."""

from __future__ import annotations

import shutil
from typing import Annotated, Literal

import geopandas as gpd
import pandas as pd
import typer

from hhplab.artifacts.naming.naming import (
    block_geometry_path,
    county_path,
    msa_coc_block_population_xwalk_path,
    msa_coc_xwalk_path,
    pl_block_population_path,
)
from hhplab.geographies.coc.coc_io import resolve_curated_boundary_path
from hhplab.geographies.msa.msa_definitions import DEFINITION_VERSION, DELINEATION_FILE_YEAR
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
    state_shards: Annotated[
        bool,
        typer.Option(
            "--state-shards/--no-state-shards",
            help=(
                "Build block-population crosswalks as deterministic state shards before "
                "concatenating the canonical artifact. Enabled by default for "
                "block_population because national single-pass block overlays can OOM."
            ),
        ),
    ] = True,
    reuse_shards: Annotated[
        bool,
        typer.Option(
            "--reuse-shards",
            help="Reuse existing per-state shard parquet files when --state-shards is enabled.",
        ),
    ] = False,
    cleanup_shards: Annotated[
        bool,
        typer.Option(
            "--cleanup-shards",
            help="Delete per-state shard parquet files after writing the final artifact.",
        ),
    ] = False,
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

    from hhplab.geographies.msa.crosswalk import (
        FULL_ALLOCATION_THRESHOLD,
        build_coc_msa_block_population_crosswalk,
        build_coc_msa_crosswalk,
        save_coc_msa_block_population_crosswalk,
        save_coc_msa_crosswalk,
        summarize_coc_msa_allocation,
    )
    from hhplab.geographies.msa.msa_io import read_msa_county_membership

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
            if state_shards:
                crosswalk = _build_block_population_state_shards(
                    coc_gdf,
                    county_gdf,
                    msa_membership,
                    block_geometry_artifact,
                    block_population_artifact,
                    boundary_vintage=resolved_boundary,
                    county_vintage=str(counties),
                    block_vintage=str(blocks),
                    decennial_vintage=str(decennial),
                    definition_version=definition_version,
                    output_path=output_path,
                    reuse_shards=reuse_shards,
                )
            else:
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
    except MemoryError as exc:
        message = (
            "Block-population MSA crosswalk generation ran out of memory. "
            "The default execution path uses state shards; retry with --reuse-shards "
            "to resume completed shards, or rerun with more memory. Avoid "
            "--no-state-shards for national block-population builds."
        )
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": message}))
        else:
            typer.echo(f"Error: {message}", err=True)
        raise typer.Exit(1) from exc
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
    if allocation_basis == "block_population" and state_shards:
        shard_dir = _state_shard_dir(output_path)
        payload["state_sharded"] = True
        payload["state_shard_dir"] = str(shard_dir)
        payload["state_shard_count"] = len(list(shard_dir.glob("*__state-*.parquet")))
        payload["failed_state_shards"] = []
        payload["reused_state_shards"] = bool(reuse_shards)
        if cleanup_shards and shard_dir.exists():
            shutil.rmtree(shard_dir)
            payload["state_shards_cleaned"] = True
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


def _build_block_population_state_shards(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_membership: pd.DataFrame,
    block_geometry_artifact,
    block_population_artifact,
    *,
    boundary_vintage: str,
    county_vintage: str,
    block_vintage: str,
    decennial_vintage: str,
    definition_version: str,
    output_path,
    reuse_shards: bool,
) -> pd.DataFrame:
    from hhplab.geographies.msa.crosswalk import (
        COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
        build_coc_msa_block_population_crosswalk,
    )

    counties = _county_with_fips(county_gdf)
    membership = msa_membership.copy()
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    shard_dir = _state_shard_dir(output_path)
    shard_dir.mkdir(parents=True, exist_ok=True)

    shard_frames: list[pd.DataFrame] = []
    for state_fips in _state_fips_for_shards(counties, membership):
        shard_path = _state_shard_path(output_path, state_fips)
        if reuse_shards and shard_path.exists():
            shard = pd.read_parquet(shard_path)
        else:
            state_counties = counties[counties["county_fips"].str[:2] == state_fips].copy()
            state_membership = membership[
                membership["county_fips"].isin(state_counties["county_fips"])
            ].copy()
            if state_counties.empty or state_membership.empty:
                continue
            state_cocs = _cocs_intersecting_state(coc_gdf, state_counties)
            if state_cocs.empty:
                continue
            state_blocks = _read_state_block_geometry(block_geometry_artifact, state_fips)
            state_population = _read_state_block_population(
                block_population_artifact,
                state_fips,
            )
            if state_blocks.empty:
                continue
            shard = build_coc_msa_block_population_crosswalk(
                state_cocs,
                state_counties,
                state_membership,
                state_blocks,
                state_population,
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                block_vintage=block_vintage,
                decennial_vintage=decennial_vintage,
                definition_version=definition_version,
            )
            shard.to_parquet(shard_path, index=False)
        if not shard.empty:
            shard = shard.copy()
            shard["_shard_state_fips"] = state_fips
            shard_frames.append(shard)

    if not shard_frames:
        return pd.DataFrame(columns=list(COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS))
    return _concat_block_population_state_shards(shard_frames)


def _state_shard_dir(output_path) -> object:
    return output_path.parent / f"{output_path.stem}__state_shards"


def _state_shard_path(output_path, state_fips: str) -> object:
    return _state_shard_dir(output_path) / f"{output_path.stem}__state-{state_fips}.parquet"


def _county_with_fips(county_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    counties = county_gdf.copy()
    county_col = "GEOID" if "GEOID" in counties.columns else "geoid"
    counties["county_fips"] = counties[county_col].astype(str).str.zfill(5)
    return counties


def _block_with_geoid(block_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    blocks = block_gdf.copy()
    block_col = "block_geoid"
    if block_col not in blocks.columns:
        block_col = "GEOID" if "GEOID" in blocks.columns else "geoid"
    blocks["block_geoid"] = blocks[block_col].astype(str)
    return blocks


def _block_population_with_geoid(block_population: pd.DataFrame) -> pd.DataFrame:
    population = block_population.copy()
    block_col = "block_geoid"
    if block_col not in population.columns:
        block_col = "GEOID" if "GEOID" in population.columns else "geoid"
    population["block_geoid"] = population[block_col].astype(str)
    return population


def _read_state_block_geometry(path, state_fips: str) -> gpd.GeoDataFrame:
    """Read one state's block geometries without materializing the national file."""
    return _block_with_geoid(
        gpd.read_parquet(
            path,
            filters=[("state_fips", "==", state_fips)],
        )
    )


def _read_state_block_population(path, state_fips: str) -> pd.DataFrame:
    """Read one state's PL block population without materializing the national file."""
    return _block_population_with_geoid(
        pd.read_parquet(
            path,
            filters=[("state_fips", "==", state_fips)],
        )
    )


def _state_fips_for_shards(
    counties: pd.DataFrame,
    membership: pd.DataFrame,
) -> list[str]:
    member_counties = set(membership["county_fips"].astype(str))
    available_counties = counties[counties["county_fips"].isin(member_counties)]
    return sorted(set(available_counties["county_fips"].str[:2]))


def _cocs_intersecting_state(
    coc_gdf: gpd.GeoDataFrame,
    state_counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    counties = state_counties
    if counties.crs != coc_gdf.crs:
        counties = counties.to_crs(coc_gdf.crs)
    state_geometry = counties.geometry.union_all()
    mask = coc_gdf.geometry.intersects(state_geometry)
    return coc_gdf.loc[mask].copy()


def _concat_block_population_state_shards(shards: list[pd.DataFrame]) -> pd.DataFrame:
    from hhplab.geographies.msa.crosswalk import (
        COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
        FULL_ALLOCATION_THRESHOLD,
        _coverage_share_from_denominator,
        _validate_coverage_shares,
    )

    rows = pd.concat(shards, ignore_index=True)
    grouped = (
        rows.groupby(["coc_id", "msa_id", "cbsa_code"], as_index=False)
        .agg(
            intersection_population=("intersection_population", "sum"),
            intersection_area=("intersection_area", "sum"),
            block_count=("block_count", "sum"),
            missing_population_block_count=("missing_population_block_count", "sum"),
        )
        .sort_values(["coc_id", "msa_id"])
        .reset_index(drop=True)
    )

    coc_denominators = (
        rows.groupby(["_shard_state_fips", "coc_id"], as_index=False)
        .agg(
            coc_population_denominator=("coc_population_denominator", "first"),
            coc_intersection_area=("coc_intersection_area", "first"),
            coc_missing_population_block_count=(
                "partial_coc_population_coverage",
                lambda s: int(bool(s.any())),
            ),
        )
        .groupby("coc_id", as_index=False)
        .agg(
            coc_population_denominator=("coc_population_denominator", "sum"),
            coc_intersection_area=("coc_intersection_area", "sum"),
            coc_missing_population_block_count=("coc_missing_population_block_count", "sum"),
        )
    )
    msa_denominators = (
        rows.groupby(["_shard_state_fips", "msa_id"], as_index=False)
        .agg(
            msa_population_denominator=("msa_population_denominator", "first"),
            msa_intersection_area=("msa_intersection_area", "first"),
        )
        .groupby("msa_id", as_index=False)
        .agg(
            msa_population_denominator=("msa_population_denominator", "sum"),
            msa_intersection_area=("msa_intersection_area", "sum"),
        )
    )
    grouped = grouped.merge(coc_denominators, on="coc_id", how="left").merge(
        msa_denominators,
        on="msa_id",
        how="left",
    )
    first = rows.iloc[0]
    grouped["boundary_vintage"] = first["boundary_vintage"]
    grouped["county_vintage"] = first["county_vintage"]
    grouped["block_vintage"] = first["block_vintage"]
    grouped["decennial_vintage"] = first["decennial_vintage"]
    grouped["definition_version"] = first["definition_version"]
    grouped["allocation_method"] = "block_population"
    grouped["share_column"] = "allocation_share"
    grouped["share_denominator"] = "coc_population_denominator"
    grouped["zero_population_coc"] = grouped["coc_population_denominator"].fillna(0.0) == 0.0
    grouped["allocation_share"] = (
        grouped["intersection_population"] / grouped["coc_population_denominator"]
    )
    grouped.loc[grouped["zero_population_coc"], "allocation_share"] = 0.0
    grouped["coc_population_containment_share"] = grouped["allocation_share"]
    grouped["msa_population_coverage_share"] = _coverage_share_from_denominator(
        grouped["intersection_population"],
        grouped["msa_population_denominator"],
    )
    grouped = grouped.fillna(
        {
            "allocation_share": 0.0,
            "coc_population_containment_share": 0.0,
            "msa_population_coverage_share": 0.0,
        }
    )
    allocation_totals = grouped.groupby("coc_id")["allocation_share"].transform("sum")
    grouped["partial_coc_population_coverage"] = (
        grouped["coc_missing_population_block_count"] > 0
    ) | (~grouped["zero_population_coc"] & (allocation_totals < FULL_ALLOCATION_THRESHOLD))
    _validate_coverage_shares(
        grouped,
        ("coc_population_containment_share", "msa_population_coverage_share"),
    )
    return grouped.loc[:, COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS]
