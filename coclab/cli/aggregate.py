"""CLI command group for aggregating datasets to CoC level.

Provides commands for acs, zori, pep, and pit dataset aggregation.
Each command validates inputs, resolves build-scoped parameters, and
delegates to the corresponding pipeline module.

All commands follow the hub-and-spoke model: the CoC boundary year is the
hub, and each build year produces one output file with that year's boundary
as the geographic reference.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from coclab.builds import (
    build_curated_dir,
    get_build_years,
    record_aggregate_run,
    require_build_dir,
    resolve_build_dir,
)
from coclab.year_spec import parse_year_spec

aggregate_app = typer.Typer(
    name="aggregate",
    help="Aggregate datasets to CoC level.",
    no_args_is_help=True,
)

# ---------------------------------------------------------------------------
# Valid alignment modes per dataset
# ---------------------------------------------------------------------------

PEP_ALIGN_MODES = ("as_of_july", "to_calendar_year", "to_pit_year", "lagged")
PIT_ALIGN_MODES = ("point_in_time_jan", "to_calendar_year")
ACS_ALIGN_MODES = ("vintage_end_year", "window_center_year")
ZORI_ALIGN_MODES = ("monthly_native", "pit_january", "calendar_year_average")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _validate_build(build: str) -> Path:
    """Validate that the named build directory exists.

    Returns the build directory path.
    Raises ``typer.Exit(2)`` with a helpful message when the build is missing.
    """
    try:
        return require_build_dir(build)
    except FileNotFoundError:
        build_path = resolve_build_dir(build)
        typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
        typer.echo("Run: coclab build create --name <build>", err=True)
        raise typer.Exit(2) from None


def _validate_align(align: str, valid_modes: tuple[str, ...], dataset: str) -> None:
    """Validate that *align* is one of *valid_modes* for *dataset*."""
    if align not in valid_modes:
        typer.echo(
            f"Error: Invalid alignment mode '{align}' for {dataset}. "
            f"Valid modes: {', '.join(valid_modes)}",
            err=True,
        )
        raise typer.Exit(2)


def _resolve_years(years: str | None, build_dir: Path) -> list[int]:
    """Parse ``--years`` if provided, otherwise use build years from manifest."""
    if years is not None:
        try:
            return parse_year_spec(years)
        except ValueError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(2) from exc

    build_years = get_build_years(build_dir)
    if not build_years:
        typer.echo("Error: Build manifest has no years defined.", err=True)
        raise typer.Exit(2)
    return build_years


def _require_boundary_years(build_dir: Path) -> list[int]:
    """Return sorted boundary years from manifest, or exit with error."""
    from coclab.builds import read_build_manifest

    manifest = read_build_manifest(build_dir)
    base_assets = manifest.get("base_assets", [])
    boundary_years = sorted(
        a["year"] for a in base_assets if a["asset_type"] == "coc_boundary"
    )
    if not boundary_years:
        typer.echo("Error: No coc_boundary base assets found in manifest.", err=True)
        raise typer.Exit(2)
    return boundary_years


# ---------------------------------------------------------------------------
# pep
# ---------------------------------------------------------------------------


@aggregate_app.command("pep")
def aggregate_pep(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: as_of_july, to_calendar_year, to_pit_year, lagged."
            ),
        ),
    ] = "as_of_july",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    lag_years: Annotated[
        int | None,
        typer.Option(
            "--lag-years",
            help="Number of lag years (required when --align=lagged).",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area_share' (default) or 'equal'.",
        ),
    ] = "area_share",
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Minimum coverage ratio for valid CoC-year (default 0.95).",
        ),
    ] = 0.95,
) -> None:
    """Aggregate PEP population estimates to CoC level.

    Produces one file per boundary year (hub). County vintage matches
    boundary year by default.
    """
    build_dir = _validate_build(build)
    _validate_align(align, PEP_ALIGN_MODES, "pep")
    parsed_years = _resolve_years(years, build_dir)

    if align == "lagged" and lag_years is None:
        typer.echo(
            "Error: --lag-years is required when --align=lagged.",
            err=True,
        )
        raise typer.Exit(2)

    _require_boundary_years(build_dir)

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "pep"

    typer.echo(f"Aggregating PEP to CoC (build '{build}', align '{align}')...")

    from coclab.pep.aggregate import aggregate_pep_to_coc

    align_params: dict | None = {"lag_years": lag_years} if lag_years else None
    all_outputs: list[str] = []
    materialized: list[int] = []

    for build_year in parsed_years:
        boundary_vintage = str(build_year)
        county_vintage = str(build_year)

        # Apply alignment adjustments to determine PEP data year
        pep_year = build_year
        if align == "to_pit_year":
            pep_year = build_year - 1
        elif align == "lagged" and lag_years is not None:
            pep_year = build_year - lag_years

        typer.echo(f"  B{build_year}: PEP year {pep_year}, counties {county_vintage}")

        try:
            result_path = aggregate_pep_to_coc(
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                weighting=weighting,
                start_year=pep_year,
                end_year=pep_year,
                min_coverage=min_coverage,
                output_dir=output_dir,
                force=True,
            )

            if result_path.is_relative_to(build_dir):
                rel = result_path.relative_to(build_dir).as_posix()
            else:
                rel = str(result_path)
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {result_path.name}")

        except FileNotFoundError as exc:
            record_aggregate_run(
                build_dir, dataset="pep", alignment=align,
                years_requested=parsed_years, status="failed",
                error=str(exc), alignment_params=align_params,
            )
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure PEP data and crosswalks are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            record_aggregate_run(
                build_dir, dataset="pep", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
                alignment_params=align_params,
            )
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc

    record_aggregate_run(
        build_dir,
        dataset="pep",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        alignment_params=align_params,
        outputs=all_outputs,
    )
    typer.echo(f"PEP aggregation complete ({len(materialized)} years). Output in: {output_dir}")


# ---------------------------------------------------------------------------
# pit
# ---------------------------------------------------------------------------


@aggregate_app.command("pit")
def aggregate_pit(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: point_in_time_jan, to_calendar_year."
            ),
        ),
    ] = "point_in_time_jan",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
) -> None:
    """Aggregate PIT counts to CoC level.

    PIT data already contains coc_id, so this command filters and
    aligns PIT count data to the build's year scope.  Produces one
    output file per boundary year.
    """
    build_dir = _validate_build(build)
    _validate_align(align, PIT_ALIGN_MODES, "pit")
    parsed_years = _resolve_years(years, build_dir)

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "pit"
    output_dir.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Aggregating PIT to CoC (build '{build}', align '{align}')...")
    typer.echo(f"  Years: {parsed_years}")

    import pandas as pd

    from coclab.naming import (
        coc_pit_filename,
        discover_pit_vintages,
        pit_path,
        pit_vintage_path,
    )

    # --- Load all available PIT data for requested years ---
    collected: dict[int, pd.DataFrame] = {}
    missing: list[int] = []

    # Pass 1: try individual year files
    for year in parsed_years:
        src = pit_path(year)
        if not Path(src).exists():
            missing.append(year)
            continue
        df = pd.read_parquet(src)
        if align == "to_calendar_year":
            df = df.copy()
            if "calendar_year" not in df.columns:
                df["calendar_year"] = year
        collected[year] = df

    # Pass 2: fall back to vintage files for any missing years
    if missing:
        vintages = discover_pit_vintages()
        still_missing = set(missing)

        for vintage in vintages:
            if not still_missing:
                break
            vpath = pit_vintage_path(vintage)
            if not vpath.exists():
                continue
            vdf = pd.read_parquet(vpath)
            if "pit_year" not in vdf.columns:
                continue
            available = set(vdf["pit_year"].unique()) & still_missing
            if not available:
                continue

            typer.echo(
                f"  Using vintage P{vintage} for years: "
                f"{sorted(available)}"
            )

            for year in sorted(available):
                ydf = vdf[vdf["pit_year"] == year].copy()
                if align == "to_calendar_year" and "calendar_year" not in ydf.columns:
                    ydf["calendar_year"] = year
                collected[year] = ydf
                still_missing.discard(year)

        missing = sorted(still_missing)

    if missing:
        typer.echo(
            f"Warning: PIT data missing for years: {missing}",
            err=True,
        )

    if not collected:
        typer.echo("Error: No PIT data found for any requested year.", err=True)
        raise typer.Exit(1)

    # --- Write one file per boundary year ---
    all_outputs: list[str] = []
    for year in sorted(collected):
        df = collected[year]
        out_name = coc_pit_filename(year, year)
        out_path = output_dir / out_name
        df.to_parquet(out_path, index=False)
        all_outputs.append(
            out_path.relative_to(build_dir).as_posix()
            if out_path.is_relative_to(build_dir) else str(out_path)
        )

    materialized = sorted(int(k) for k in collected.keys())
    total_records = sum(len(df) for df in collected.values())
    sample_df = next(iter(collected.values()))
    coc_count = sample_df["coc_id"].nunique() if "coc_id" in sample_df.columns else "n/a"
    typer.echo(f"Wrote PIT aggregate: {len(materialized)} files to {output_dir}")
    typer.echo(f"  CoCs: {coc_count}, Records: {total_records:,}")

    record_aggregate_run(
        build_dir,
        dataset="pit",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=all_outputs,
    )


# ---------------------------------------------------------------------------
# acs
# ---------------------------------------------------------------------------


@aggregate_app.command("acs")
def aggregate_acs(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: vintage_end_year, window_center_year."
            ),
        ),
    ] = "vintage_end_year",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area' (default) or 'population'.",
        ),
    ] = "area",
    tracts: Annotated[
        int | None,
        typer.Option(
            "--tracts",
            "-t",
            help=(
                "Census tract vintage for crosswalk. Defaults to most recent "
                "decennial <= ACS end year."
            ),
        ),
    ] = None,
) -> None:
    """Aggregate ACS estimates to CoC level.

    Iterates over build years using each as the boundary vintage (hub).
    For each boundary year, the ACS vintage is derived from the alignment
    mode and a crosswalk is resolved from the global xwalks directory.
    """
    build_dir = _validate_build(build)
    _validate_align(align, ACS_ALIGN_MODES, "acs")
    parsed_years = _resolve_years(years, build_dir)

    if weighting not in ("area", "population"):
        typer.echo(
            f"Error: Invalid weighting '{weighting}'. Use 'area' or 'population'.",
            err=True,
        )
        raise typer.Exit(2)

    _require_boundary_years(build_dir)

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "measures"

    typer.echo(f"Aggregating ACS to CoC (build '{build}', align '{align}')...")

    from coclab.acs.translate import default_tract_vintage_for_acs
    from coclab.measures.acs import build_coc_measures
    from coclab.naming import tract_xwalk_filename

    def decennial_floor(year: int) -> int:
        return year - (year % 10)

    for build_year in parsed_years:
        boundary_vintage = str(build_year)

        # Derive ACS vintage from alignment mode
        if align == "vintage_end_year":
            acs_vintage = f"{build_year - 4}-{build_year}"
        else:  # window_center_year
            acs_vintage = f"{build_year - 2}-{build_year + 2}"

        tract_vintage = (
            tracts if tracts is not None
            else default_tract_vintage_for_acs(acs_vintage)
        )

        # Resolve crosswalk from build-scoped xwalks directory
        xwalk_path = curated_dir / "xwalks" / tract_xwalk_filename(
            boundary_vintage, tract_vintage
        )

        if not xwalk_path.exists():
            typer.echo(
                f"Error: Crosswalk not found: {xwalk_path}",
                err=True,
            )
            if tract_vintage % 10 != 0:
                suggested = decennial_floor(tract_vintage)
                typer.echo(
                    "The requested census tract year wasn't found and isn't on a decennial. "
                    f"Did you mean to request {suggested}?",
                    err=True,
                )
            typer.echo(
                f"Run: coclab generate xwalks --boundary {boundary_vintage} "
                f"--tracts {tract_vintage}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(
            f"  B{build_year}: ACS {acs_vintage} (tracts {tract_vintage})..."
        )
        try:
            build_coc_measures(
                boundary_vintage=boundary_vintage,
                acs_vintage=acs_vintage,
                crosswalk_path=xwalk_path,
                weighting=weighting,
                output_dir=output_dir,
                show_progress=True,
            )
        except Exception as exc:
            record_aggregate_run(
                build_dir, dataset="acs", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error aggregating ACS {acs_vintage}: {exc}", err=True)
            raise typer.Exit(1) from exc

    record_aggregate_run(
        build_dir,
        dataset="acs",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=parsed_years,
    )
    typer.echo(f"ACS aggregation complete. Output in: {output_dir}")


# ---------------------------------------------------------------------------
# acs-population  (cache-only, no Census API calls)
# ---------------------------------------------------------------------------

ACS_POP_ALIGN_MODES = ("vintage_end_year", "window_center_year")


@aggregate_app.command("acs-population")
def aggregate_acs_population(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: vintage_end_year, window_center_year."
            ),
        ),
    ] = "vintage_end_year",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    tracts: Annotated[
        int | None,
        typer.Option(
            "--tracts",
            "-t",
            help=(
                "Census tract vintage for crosswalk. Defaults to most recent "
                "decennial <= ACS end year."
            ),
        ),
    ] = None,
) -> None:
    """Aggregate cached ACS tract population data to CoC level.

    Reads only pre-ingested ACS tract population files from disk and
    aggregates to CoC level using area-weighted crosswalks.  No Census
    API calls are made.  If cached ingest files are missing, the command
    fails with instructions to run ``coclab ingest acs`` first.

    Examples:

        coclab aggregate acs-population --build demo

        coclab aggregate acs-population --build demo --tracts 2020
    """
    build_dir = _validate_build(build)
    _validate_align(align, ACS_POP_ALIGN_MODES, "acs-population")
    parsed_years = _resolve_years(years, build_dir)

    _require_boundary_years(build_dir)

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "acs_population"

    typer.echo(
        f"Aggregating ACS population to CoC (build '{build}', align '{align}')..."
    )

    import pandas as pd

    from coclab.acs.ingest.tract_population import get_output_path
    from coclab.acs.translate import default_tract_vintage_for_acs
    from coclab.naming import tract_xwalk_filename
    from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

    all_outputs: list[str] = []
    materialized: list[int] = []

    for build_year in parsed_years:
        boundary_vintage = str(build_year)

        # Derive ACS vintage from alignment mode
        if align == "vintage_end_year":
            acs_vintage = f"{build_year - 4}-{build_year}"
        else:  # window_center_year
            acs_vintage = f"{build_year - 2}-{build_year + 2}"

        tract_vintage = (
            tracts if tracts is not None
            else default_tract_vintage_for_acs(acs_vintage)
        )

        # --- Resolve cached ACS tract population file (NO API) ---
        acs_cache_path = get_output_path(acs_vintage, str(tract_vintage))
        if not acs_cache_path.exists():
            record_aggregate_run(
                build_dir, dataset="acs_population", alignment=align,
                years_requested=parsed_years, status="failed",
                error=f"ACS cache not found: {acs_cache_path}",
            )
            typer.echo(
                f"Error: Cached ACS tract population file not found: {acs_cache_path}",
                err=True,
            )
            typer.echo(
                f"Run: coclab ingest acs --acs {acs_vintage} --tracts {tract_vintage}",
                err=True,
            )
            raise typer.Exit(1)

        # --- Resolve crosswalk ---
        xwalk_path = curated_dir / "xwalks" / tract_xwalk_filename(
            boundary_vintage, tract_vintage
        )
        if not xwalk_path.exists():
            record_aggregate_run(
                build_dir, dataset="acs_population", alignment=align,
                years_requested=parsed_years, status="failed",
                error=f"Crosswalk not found: {xwalk_path}",
            )
            typer.echo(f"Error: Crosswalk not found: {xwalk_path}", err=True)
            typer.echo(
                f"Run: coclab generate xwalks --build {build} "
                f"--boundary {boundary_vintage} --tracts {tract_vintage}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(
            f"  B{build_year}: ACS {acs_vintage} (tracts {tract_vintage})..."
        )

        try:
            # Load cached data and crosswalk
            acs_df = pd.read_parquet(acs_cache_path)
            xwalk_df = pd.read_parquet(xwalk_path)

            # Standardize GEOID column names
            if "tract_geoid" in acs_df.columns and "GEOID" not in acs_df.columns:
                acs_df = acs_df.rename(columns={"tract_geoid": "GEOID"})
            if "tract_geoid" in xwalk_df.columns and "GEOID" not in xwalk_df.columns:
                xwalk_df = xwalk_df.rename(columns={"tract_geoid": "GEOID"})

            # Join and aggregate population to CoC level
            merged = xwalk_df.merge(acs_df[["GEOID", "total_population"]], on="GEOID", how="left")
            results = []
            for coc_id, group in merged.groupby("coc_id"):
                weighted_pop = (
                    group["total_population"].fillna(0) * group["area_share"].fillna(0)
                ).sum()
                total_area = (
                    group["intersection_area"].sum()
                    if "intersection_area" in group.columns
                    else 0
                )
                has_data = group["total_population"].notna()
                covered_area = (
                    group.loc[has_data, "intersection_area"].sum()
                    if "intersection_area" in group.columns else has_data.mean()
                )
                coverage = covered_area / total_area if total_area > 0 else has_data.mean()
                results.append({
                    "coc_id": coc_id,
                    "boundary_vintage": boundary_vintage,
                    "acs_vintage": acs_vintage,
                    "total_population": weighted_pop,
                    "coverage_ratio": coverage,
                })
            coc_df = pd.DataFrame(results)

            # Write output
            output_dir.mkdir(parents=True, exist_ok=True)
            out_name = f"coc_pop__A{acs_vintage.split('-')[-1]}@B{boundary_vintage}.parquet"
            out_path = output_dir / out_name

            provenance = ProvenanceBlock(
                boundary_vintage=boundary_vintage,
                tract_vintage=str(tract_vintage),
                acs_vintage=acs_vintage,
                weighting="area",
                extra={
                    "dataset_type": "coc_acs_population",
                    "source": "cached_ingest",
                    "crosswalk_path": str(xwalk_path),
                    "acs_cache_path": str(acs_cache_path),
                },
            )
            write_parquet_with_provenance(coc_df, out_path, provenance)

            rel = (
                out_path.relative_to(build_dir).as_posix()
                if out_path.is_relative_to(build_dir) else str(out_path)
            )
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {out_path.name}")

        except Exception as exc:
            record_aggregate_run(
                build_dir, dataset="acs_population", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error aggregating ACS population {acs_vintage}: {exc}", err=True)
            raise typer.Exit(1) from exc

    record_aggregate_run(
        build_dir,
        dataset="acs_population",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=all_outputs,
    )
    typer.echo(
        f"ACS population aggregation complete ({len(materialized)} years). "
        f"Output in: {output_dir}"
    )


# ---------------------------------------------------------------------------
# zori
# ---------------------------------------------------------------------------


@aggregate_app.command("zori")
def aggregate_zori(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build to aggregate against.",
        ),
    ],
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: monthly_native, pit_january, calendar_year_average."
            ),
        ),
    ] = "monthly_native",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec override (e.g. '2018-2024'). Defaults to build years.",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting: renter_households (default), housing_units, population, equal.",
        ),
    ] = "renter_households",
) -> None:
    """Aggregate ZORI rent indices to CoC level.

    Iterates over build years using each as the boundary vintage (hub).
    County vintage and ACS vintage for weights are derived from the
    boundary year.
    """
    build_dir = _validate_build(build)
    _validate_align(align, ZORI_ALIGN_MODES, "zori")
    parsed_years = _resolve_years(years, build_dir)

    _require_boundary_years(build_dir)

    # Map alignment mode to pipeline parameters
    to_yearly = align != "monthly_native"
    yearly_method_map = {
        "pit_january": "pit_january",
        "calendar_year_average": "calendar_mean",
    }
    yearly_method = yearly_method_map.get(align, "pit_january")

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "zori"

    typer.echo(f"Aggregating ZORI to CoC (build '{build}', align '{align}')...")

    from coclab.rents.aggregate import aggregate_zori_to_coc

    all_outputs: list[str] = []
    materialized: list[int] = []

    for build_year in parsed_years:
        boundary_vintage = str(build_year)
        county_vintage = str(build_year)
        acs_vintage = f"{build_year - 4}-{build_year}"

        typer.echo(
            f"  B{build_year}: counties {county_vintage}, "
            f"ACS {acs_vintage}, weight {weighting}"
        )
        if to_yearly:
            typer.echo(f"    Yearly collapse: {yearly_method}")

        try:
            result_path = aggregate_zori_to_coc(
                boundary=boundary_vintage,
                counties=county_vintage,
                acs_vintage=acs_vintage,
                weighting=weighting,
                output_dir=output_dir,
                to_yearly=to_yearly,
                yearly_method=yearly_method,
                force=True,
            )

            if result_path.is_relative_to(build_dir):
                rel = result_path.relative_to(build_dir).as_posix()
            else:
                rel = str(result_path)
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {result_path.name}")

        except FileNotFoundError as exc:
            record_aggregate_run(
                build_dir, dataset="zori", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure ZORI data, crosswalks, and ACS weights are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            record_aggregate_run(
                build_dir, dataset="zori", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc

    record_aggregate_run(
        build_dir,
        dataset="zori",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=all_outputs,
    )
    typer.echo(f"ZORI aggregation complete ({len(materialized)} years). Output in: {output_dir}")
