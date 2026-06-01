"""CLI command for building CoC Urban Area population fractions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer

import hhplab.naming as naming
from hhplab.census.ingest.decennial_tract_population import STATE_FIPS_CODES
from hhplab.metro.metro_definitions import STATE_ABBREV_TO_FIPS
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import COC_URBAN_AREA_DETAIL_COLUMNS, COC_URBAN_FRACTION_COLUMNS
from hhplab.xwalks.urban_fraction import (
    DEFAULT_ALLOCATION_METHOD,
    DEFAULT_CLASSIFICATION_METHOD,
    build_coc_urban_fraction,
)

MISSING_GEOPARQUET_METADATA_MESSAGE = "Missing geo metadata in Parquet/Feather file."

TERRITORY_STATE_ABBREV_TO_FIPS: dict[str, str] = {
    "AS": "60",
    "GU": "66",
    "MP": "69",
    "VI": "78",
}
STATE_ABBREV_TO_BLOCK_FIPS: dict[str, str] = {
    **STATE_ABBREV_TO_FIPS,
    **TERRITORY_STATE_ABBREV_TO_FIPS,
}


def build_urban_fraction(
    boundary: Annotated[
        str,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage, e.g. 2025.",
        ),
    ],
    urban_area_vintage: Annotated[
        int,
        typer.Option(
            "--urban-area-vintage",
            "--urban",
            help="Census Urban Area vintage: 2010 or 2020.",
        ),
    ],
    decennial: Annotated[
        int,
        typer.Option(
            "--decennial",
            help="PL 94-171 decennial vintage for block population: 2010 or 2020.",
        ),
    ],
    block_vintage: Annotated[
        int | None,
        typer.Option(
            "--block-vintage",
            "--blocks",
            help="Block geometry vintage. Defaults to --decennial.",
        ),
    ] = None,
    block_geometry: Annotated[
        Path | None,
        typer.Option(
            "--block-geometry",
            help=(
                "Optional GeoParquet with block_geoid and geometry when the PL block "
                "population artifact is population-only."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate inputs and planned outputs without writing artifacts.",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing urban fraction artifacts.",
        ),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Build CoC Urban Area population fraction artifacts."""
    resolved_block_vintage = decennial if block_vintage is None else block_vintage
    paths = urban_fraction_paths(
        boundary=boundary,
        urban_area_vintage=urban_area_vintage,
        block_vintage=resolved_block_vintage,
        decennial=decennial,
        block_geometry=block_geometry,
    )
    payload = urban_fraction_preflight_payload(
        boundary=boundary,
        urban_area_vintage=urban_area_vintage,
        block_vintage=resolved_block_vintage,
        decennial=decennial,
        paths=paths,
        dry_run=dry_run,
        force=force,
    )

    missing_inputs = [
        name for name, status in payload["inputs"].items() if not bool(status["exists"])
    ]
    if missing_inputs:
        payload.update(
            {
                "status": "error",
                "error": "missing_inputs",
                "missing_inputs": missing_inputs,
                "commands": _missing_input_commands(
                    boundary=boundary,
                    urban_area_vintage=urban_area_vintage,
                    decennial=decennial,
                    block_vintage=resolved_block_vintage,
                    missing_inputs=missing_inputs,
                ),
            }
        )
        _emit_payload(payload, json_output=output_json)
        raise typer.Exit(1)

    output_exists = paths["summary"].exists() or paths["detail"].exists()
    payload["output_exists"] = output_exists
    if output_exists and not force and not dry_run:
        payload.update({"status": "error", "error": "artifact_exists"})
        _emit_payload(payload, json_output=output_json)
        raise typer.Exit(1)

    if dry_run:
        _emit_payload(payload, json_output=output_json)
        return

    try:
        coc_gdf = gpd.read_parquet(paths["coc_boundaries"])
        urban_area_gdf = gpd.read_parquet(paths["urban_areas"])
        summary, detail, chunk_count = _build_coc_urban_fraction_chunked(
            coc_gdf,
            urban_area_gdf,
            pl_blocks_path=paths["pl_blocks"],
            block_geometry_path=paths.get("block_geometry"),
            boundary_vintage=boundary,
            urban_area_vintage=urban_area_vintage,
            block_vintage=resolved_block_vintage,
            decennial_vintage=decennial,
            progress=not output_json,
        )
    except Exception as exc:
        _emit_error(str(exc), json_output=output_json)
        raise typer.Exit(1) from exc

    provenance = ProvenanceBlock(
        boundary_vintage=str(boundary),
        weighting="block_area_intersection",
        geo_type="coc",
        extra={
            "dataset_type": "coc_urban_fraction",
            "pl_block_population_artifact": str(paths["pl_blocks"]),
            "block_geometry_artifact": payload["inputs"]["block_geometry"].get("path"),
            "urban_area_artifact": str(paths["urban_areas"]),
            "coc_boundary_artifact": str(paths["coc_boundaries"]),
            "urban_area_vintage": str(urban_area_vintage),
            "block_vintage": str(resolved_block_vintage),
            "decennial_vintage": str(decennial),
            "denominator_source": "pl_94_171_block_population",
            "allocation_method": DEFAULT_ALLOCATION_METHOD,
            "classification_method": DEFAULT_CLASSIFICATION_METHOD,
        },
    )
    written_summary = write_parquet_with_provenance(summary, paths["summary"], provenance)
    written_detail = write_parquet_with_provenance(detail, paths["detail"], provenance)
    payload.update(
        {
            "rows": int(len(summary)),
            "coc_count": int(summary["coc_id"].nunique()) if not summary.empty else 0,
            "total_population": float(summary["coc_total_population"].sum())
            if not summary.empty
            else 0.0,
            "urban_population": float(summary["coc_urban_population"].sum())
            if not summary.empty
            else 0.0,
            "min_fraction": _float_or_none(summary["urban_population_fraction"].min()),
            "max_fraction": _float_or_none(summary["urban_population_fraction"].max()),
            "artifact": str(written_summary),
            "detail_artifact": str(written_detail),
            "diagnostics": {
                "missing_denominator_block_count": int(
                    summary["missing_denominator_block_count"].sum()
                )
                if not summary.empty
                else 0,
                "min_population_coverage_ratio": _float_or_none(
                    summary["population_coverage_ratio"].min()
                ),
                "chunk_count": chunk_count,
            },
        }
    )
    _emit_payload(payload, json_output=output_json)


def urban_fraction_paths(
    *,
    boundary: str,
    urban_area_vintage: int,
    block_vintage: int,
    decennial: int,
    block_geometry: Path | None,
) -> dict[str, Path]:
    """Resolve canonical inputs and outputs for urban fraction builds."""
    measures_dir = curated_dir("measures")
    resolved_block_geometry = (
        block_geometry
        if block_geometry is not None
        else curated_dir("tiger") / naming.block_geometry_filename(block_vintage)
    )
    return {
        "coc_boundaries": curated_dir("coc_boundaries") / naming.coc_base_filename(boundary),
        "urban_areas": curated_dir("tiger") / naming.urban_area_filename(urban_area_vintage),
        "pl_blocks": curated_dir("census")
        / naming.pl_block_population_filename(decennial, block_vintage),
        "block_geometry": resolved_block_geometry,
        "summary": measures_dir
        / naming.coc_urban_fraction_filename(
            boundary,
            urban_area_vintage,
            block_vintage,
            decennial,
        ),
        "detail": measures_dir
        / naming.coc_urban_area_detail_filename(
            boundary,
            urban_area_vintage,
            block_vintage,
            decennial,
        ),
    }


def urban_fraction_preflight_payload(
    *,
    boundary: str,
    urban_area_vintage: int,
    block_vintage: int,
    decennial: int,
    paths: dict[str, Path],
    dry_run: bool,
    force: bool,
) -> dict[str, object]:
    """Build a JSON-serializable preflight payload for urban fraction builds."""
    return {
        "status": "ok",
        "action": "dry_run" if dry_run else "build",
        "boundary_vintage": str(boundary),
        "urban_area_vintage": str(urban_area_vintage),
        "block_vintage": str(block_vintage),
        "decennial_vintage": str(decennial),
        "inputs": {
            "coc_boundaries": _path_status(paths["coc_boundaries"]),
            "urban_areas": _path_status(paths["urban_areas"]),
            "pl_blocks": _path_status(paths["pl_blocks"]),
            "block_geometry": _block_geometry_status(paths["pl_blocks"], paths["block_geometry"]),
        },
        "artifact": str(paths["summary"]),
        "detail_artifact": str(paths["detail"]),
        "will_write": not dry_run,
        "force": force,
    }


def _load_block_inputs(pl_blocks_path: Path, block_geometry_path: Path | None) -> gpd.GeoDataFrame:
    """Load all block inputs.

    Kept for tests and small custom inputs. The CLI build path uses
    _load_block_inputs_for_state to avoid materializing national blocks.
    """
    try:
        pl_blocks_geo = gpd.read_parquet(pl_blocks_path)
        if "geometry" in pl_blocks_geo.columns:
            return pl_blocks_geo
    except ValueError as exc:
        message = str(exc.args[0]) if exc.args else ""
        if not message.startswith(MISSING_GEOPARQUET_METADATA_MESSAGE):
            raise

    pl_blocks = pd.read_parquet(pl_blocks_path)
    if block_geometry_path is None:
        raise ValueError(
            "PL block population artifact has no geometry and no canonical block geometry "
            "artifact was resolved. Run `hhplab ingest block-geometry --year <block-vintage>` "
            "or provide --block-geometry with a GeoParquet containing block_geoid and geometry."
        )
    block_geometry = gpd.read_parquet(block_geometry_path)
    missing_columns = sorted({"block_geoid", "geometry"} - set(block_geometry.columns))
    if missing_columns:
        raise ValueError(
            "Block geometry artifact missing required column(s): "
            f"{', '.join(missing_columns)}."
        )
    merged = pl_blocks.merge(
        block_geometry[["block_geoid", "geometry"]],
        on="block_geoid",
        how="left",
        validate="one_to_one",
    )
    missing_geometry = merged.loc[merged["geometry"].isna(), "block_geoid"]
    if not missing_geometry.empty:
        examples = ", ".join(missing_geometry.astype(str).head(5))
        raise ValueError(
            "Block geometry coverage is incomplete for PL block population rows: "
            f"{len(missing_geometry)} block(s) lack geometry. "
            f"Example block_geoid values: {examples}. "
            "Rebuild block geometry for the same block vintage or exclude unsupported coverage "
            "deliberately before running urban-fraction."
        )
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=block_geometry.crs)


def _build_coc_urban_fraction_chunked(
    coc_gdf: gpd.GeoDataFrame,
    urban_area_gdf: gpd.GeoDataFrame,
    *,
    pl_blocks_path: Path,
    block_geometry_path: Path | None,
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    progress: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Build urban fractions one state at a time to bound peak memory."""
    states = _state_chunks_for_coc_boundaries(coc_gdf, pl_blocks_path)
    summary_parts: list[pd.DataFrame] = []
    detail_parts: list[pd.DataFrame] = []
    total_chunks = len(states)

    for index, state_fips in enumerate(states, start=1):
        state_coc = _coc_for_state(coc_gdf, state_fips)
        if state_coc.empty:
            continue
        if progress:
            typer.echo(
                "Processing block chunk "
                f"{index}/{total_chunks}: state_fips={state_fips}, "
                f"cocs={len(state_coc)}"
            )
        block_gdf = _load_block_inputs_for_state(
            pl_blocks_path,
            block_geometry_path,
            state_fips=state_fips,
        )
        if progress:
            typer.echo(f"  Loaded {len(block_gdf):,} block rows.")
        summary, detail = build_coc_urban_fraction(
            state_coc,
            block_gdf,
            urban_area_gdf,
            boundary_vintage=boundary_vintage,
            urban_area_vintage=urban_area_vintage,
            block_vintage=block_vintage,
            decennial_vintage=decennial_vintage,
        )
        summary_parts.append(summary)
        detail_parts.append(detail)
        if progress:
            typer.echo(f"  Completed {len(summary):,} CoC summary row(s).")

    summary = _concat_or_empty(summary_parts, COC_URBAN_FRACTION_COLUMNS)
    detail = _concat_or_empty(detail_parts, COC_URBAN_AREA_DETAIL_COLUMNS)
    return (
        summary.sort_values("coc_id").reset_index(drop=True),
        detail.reset_index(drop=True),
        total_chunks,
    )


def _state_chunks_for_coc_boundaries(
    coc_gdf: gpd.GeoDataFrame,
    pl_blocks_path: Path,
) -> list[str]:
    if "state_abbrev" not in coc_gdf.columns:
        return _available_block_states(pl_blocks_path)
    state_fips = (
        coc_gdf["state_abbrev"]
        .dropna()
        .astype(str)
        .str.upper()
        .map(STATE_ABBREV_TO_BLOCK_FIPS)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    available = set(_available_block_states(pl_blocks_path))
    return [state for state in STATE_FIPS_CODES if state in available and state in set(state_fips)]


def _available_block_states(pl_blocks_path: Path) -> list[str]:
    states = pd.read_parquet(pl_blocks_path, columns=["state_fips"])
    values = set(states["state_fips"].dropna().astype(str).str.zfill(2))
    return [state for state in STATE_FIPS_CODES if state in values]


def _coc_for_state(coc_gdf: gpd.GeoDataFrame, state_fips: str) -> gpd.GeoDataFrame:
    if "state_abbrev" not in coc_gdf.columns:
        return coc_gdf
    fips_by_abbrev = coc_gdf["state_abbrev"].astype(str).str.upper().map(
        STATE_ABBREV_TO_BLOCK_FIPS
    )
    return coc_gdf.loc[fips_by_abbrev == state_fips].copy()


def _load_block_inputs_for_state(
    pl_blocks_path: Path,
    block_geometry_path: Path | None,
    *,
    state_fips: str,
) -> gpd.GeoDataFrame:
    filters = [("state_fips", "==", state_fips)]
    try:
        pl_blocks_geo = gpd.read_parquet(pl_blocks_path, filters=filters)
        if "geometry" in pl_blocks_geo.columns:
            return pl_blocks_geo
    except ValueError as exc:
        message = str(exc.args[0]) if exc.args else ""
        if not message.startswith(MISSING_GEOPARQUET_METADATA_MESSAGE):
            raise

    pl_blocks = pd.read_parquet(pl_blocks_path, filters=filters)
    if block_geometry_path is None:
        raise ValueError(
            "PL block population artifact has no geometry and no canonical block geometry "
            "artifact was resolved. Run `hhplab ingest block-geometry --year <block-vintage>` "
            "or provide --block-geometry with a GeoParquet containing block_geoid and geometry."
        )
    block_geometry = gpd.read_parquet(block_geometry_path, filters=filters)
    missing_columns = sorted({"block_geoid", "geometry"} - set(block_geometry.columns))
    if missing_columns:
        raise ValueError(
            "Block geometry artifact missing required column(s): "
            f"{', '.join(missing_columns)}."
        )
    merged = pl_blocks.merge(
        block_geometry[["block_geoid", "geometry"]],
        on="block_geoid",
        how="left",
        validate="one_to_one",
    )
    missing_geometry = merged.loc[merged["geometry"].isna(), "block_geoid"]
    if not missing_geometry.empty:
        examples = ", ".join(missing_geometry.astype(str).head(5))
        raise ValueError(
            "Block geometry coverage is incomplete for PL block population rows: "
            f"{len(missing_geometry)} block(s) lack geometry for state_fips={state_fips}. "
            f"Example block_geoid values: {examples}. "
            "Rebuild block geometry for the same block vintage or exclude unsupported coverage "
            "deliberately before running urban-fraction."
        )
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=block_geometry.crs)


def _concat_or_empty(parts: list[pd.DataFrame], columns: tuple[str, ...]) -> pd.DataFrame:
    non_empty = [part for part in parts if not part.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return pd.concat(non_empty, ignore_index=True)[list(columns)]


def _missing_input_commands(
    *,
    boundary: str,
    urban_area_vintage: int,
    decennial: int,
    block_vintage: int,
    missing_inputs: list[str],
) -> dict[str, str]:
    commands = {
        "coc_boundaries": (
            f"hhplab ingest boundaries --source hud_exchange --vintage {boundary}"
        ),
        "urban_areas": f"hhplab ingest urban-areas --year {urban_area_vintage}",
        "pl_blocks": f"hhplab ingest pl-blocks --decennial {decennial} --blocks {block_vintage}",
        "block_geometry": (
            f"hhplab ingest block-geometry --year {block_vintage} "
            "or provide --block-geometry <path-to-block-geoparquet>"
        ),
    }
    return {name: command for name, command in commands.items() if name in missing_inputs}


def _path_status(path: Path) -> dict[str, str | bool]:
    return {"path": str(path), "exists": path.exists()}


def _block_geometry_status(
    pl_blocks_path: Path,
    block_geometry_path: Path | None,
) -> dict[str, str | bool]:
    if pl_blocks_path.exists() and _geoparquet_has_geometry(pl_blocks_path):
        return {"path": str(pl_blocks_path), "exists": True, "has_geometry": True}
    if block_geometry_path is not None:
        status = _path_status(block_geometry_path)
        status["has_geometry"] = (
            _geoparquet_has_geometry(block_geometry_path)
            if block_geometry_path.exists()
            else False
        )
        return status
    return {"path": "", "exists": False}


def _geoparquet_has_geometry(path: Path) -> bool:
    try:
        schema = pq.ParquetFile(path).schema_arrow
    except (pa.ArrowInvalid, ValueError, OSError):
        return False
    return "geometry" in schema.names


def _emit_payload(payload: dict[str, object], *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(payload))
        return
    if payload.get("status") == "error":
        error = payload.get("error", "error")
        typer.echo(f"Error: {error}", err=True)
        commands = payload.get("commands")
        if isinstance(commands, dict) and commands:
            typer.echo("Run:", err=True)
            for command in commands.values():
                typer.echo(f"  {command}", err=True)
        return
    if payload.get("action") == "dry_run":
        typer.echo("Urban fraction preflight passed.")
        typer.echo(f"Output: {payload['artifact']}")
        return
    typer.echo("Built CoC urban fraction artifact.")
    typer.echo(f"Output: {payload['artifact']}")
    typer.echo(f"Rows: {payload.get('rows', 0)}")


def _emit_error(message: str, *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps({"status": "error", "error": message}))
    else:
        typer.echo(f"Error: {message}", err=True)


def _float_or_none(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)
