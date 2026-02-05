"""CLI command group for aggregating datasets to CoC level.

Provides commands for acs, zori, pep, and pit dataset aggregation.
Each command validates inputs, resolves build-scoped parameters, and
delegates to the corresponding pipeline module.
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
ACS_ALIGN_MODES = ("vintage_end_year", "window_center_year", "as_reported")
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
    counties: Annotated[
        str | None,
        typer.Option(
            "--counties",
            "-c",
            help="TIGER county vintage year for crosswalk (e.g., '2024').",
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
    """Aggregate PEP population estimates to CoC level."""
    build_dir = _validate_build(build)
    _validate_align(align, PEP_ALIGN_MODES, "pep")
    parsed_years = _resolve_years(years, build_dir)

    if align == "lagged" and lag_years is None:
        typer.echo(
            "Error: --lag-years is required when --align=lagged.",
            err=True,
        )
        raise typer.Exit(2)

    # Determine boundary vintage from manifest (use first base asset year
    # as default; all share the same geographic vintage in typical builds)
    from coclab.builds import read_build_manifest

    manifest = read_build_manifest(build_dir)
    base_assets = manifest.get("base_assets", [])
    if not base_assets:
        typer.echo("Error: Build has no pinned base assets.", err=True)
        raise typer.Exit(2)

    # Use the latest boundary year as the canonical boundary vintage
    boundary_years = sorted(a["year"] for a in base_assets if a["asset_type"] == "coc_boundary")
    boundary_vintage = str(boundary_years[-1]) if boundary_years else None
    if boundary_vintage is None:
        typer.echo("Error: No coc_boundary base assets found in manifest.", err=True)
        raise typer.Exit(2)

    if counties is None:
        counties = boundary_vintage

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "pep"

    # Determine year range from parsed years
    start_year = min(parsed_years)
    end_year = max(parsed_years)

    # Apply alignment adjustments
    if align == "to_pit_year":
        # PIT counts in January of year N reflect population of year N-1
        start_year -= 1
        end_year -= 1
    elif align == "lagged" and lag_years is not None:
        start_year -= lag_years
        end_year -= lag_years

    typer.echo(f"Aggregating PEP to CoC (build '{build}', align '{align}')...")
    typer.echo(f"  Boundary: {boundary_vintage}, Counties: {counties}")
    typer.echo(f"  Years: {start_year}-{end_year}")

    from coclab.pep.aggregate import aggregate_pep_to_coc

    align_params: dict | None = {"lag_years": lag_years} if lag_years else None

    try:
        result_path = aggregate_pep_to_coc(
            boundary_vintage=boundary_vintage,
            county_vintage=counties,
            weighting=weighting,
            start_year=start_year,
            end_year=end_year,
            min_coverage=min_coverage,
            output_dir=output_dir,
            force=True,
        )

        import pandas as pd

        df = pd.read_parquet(result_path)
        coc_count = df["coc_id"].nunique()
        year_range = f"{df['year'].min()}-{df['year'].max()}"
        materialized = sorted(df["year"].unique().tolist())
        typer.echo(f"Wrote PEP aggregate: {result_path}")
        typer.echo(f"  CoCs: {coc_count}, Years: {year_range}")

        if result_path.is_relative_to(build_dir):
            rel = result_path.relative_to(build_dir).as_posix()
        else:
            rel = str(result_path)
        record_aggregate_run(
            build_dir,
            dataset="pep",
            alignment=align,
            years_requested=parsed_years,
            years_materialized=materialized,
            alignment_params=align_params,
            outputs=[rel],
        )

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
    aligns PIT count data to the build's year scope.
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

    from coclab.naming import discover_pit_vintages, pit_path, pit_vintage_path

    collected: list[pd.DataFrame] = []
    missing: list[int] = []

    # --- Pass 1: try individual year files ---
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
        collected.append(df)

    # --- Pass 2: fall back to vintage files for any missing years ---
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
                collected.append(ydf)
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

    result = pd.concat(collected, ignore_index=True)
    output_path = output_dir / f"pit__P{parsed_years[0]}-{parsed_years[-1]}.parquet"
    result.to_parquet(output_path, index=False)

    materialized = [y for y in parsed_years if y not in missing]
    coc_count = result["coc_id"].nunique() if "coc_id" in result.columns else "n/a"
    typer.echo(f"Wrote PIT aggregate: {output_path}")
    typer.echo(f"  CoCs: {coc_count}, Records: {len(result):,}")

    if output_path.is_relative_to(build_dir):
        rel = output_path.relative_to(build_dir).as_posix()
    else:
        rel = str(output_path)
    record_aggregate_run(
        build_dir,
        dataset="pit",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=[rel],
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
                "One of: vintage_end_year, window_center_year, as_reported."
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
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs-vintage",
            help="ACS 5-year estimate vintage (e.g. '2019-2023'). Required for as_reported.",
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
            help="Census tract vintage for crosswalk. Defaults to most recent decennial <= ACS end year.",
        ),
    ] = None,
) -> None:
    """Aggregate ACS estimates to CoC level."""
    build_dir = _validate_build(build)
    _validate_align(align, ACS_ALIGN_MODES, "acs")
    parsed_years = _resolve_years(years, build_dir)

    if align == "as_reported" and acs_vintage is None:
        typer.echo(
            "Error: --acs-vintage is required when --align=as_reported.",
            err=True,
        )
        raise typer.Exit(2)

    if weighting not in ("area", "population"):
        typer.echo(
            f"Error: Invalid weighting '{weighting}'. Use 'area' or 'population'.",
            err=True,
        )
        raise typer.Exit(2)

    from coclab.builds import read_build_manifest

    manifest = read_build_manifest(build_dir)
    base_assets = manifest.get("base_assets", [])
    boundary_years = sorted(a["year"] for a in base_assets if a["asset_type"] == "coc_boundary")
    if not boundary_years:
        typer.echo("Error: No coc_boundary base assets found in manifest.", err=True)
        raise typer.Exit(2)

    # Use the latest boundary vintage as the canonical one
    boundary_vintage = str(boundary_years[-1])

    curated_dir = build_curated_dir(build_dir)
    output_dir = curated_dir / "measures"

    # Determine ACS vintage based on alignment mode
    if align == "as_reported":
        vintages_to_run = [acs_vintage]
    elif align == "vintage_end_year":
        # ACS end year = build year, so ACS 2019-2023 maps to year 2023
        vintages_to_run = [f"{y - 4}-{y}" for y in parsed_years]
    elif align == "window_center_year":
        # ACS center year = build year, so year 2021 → ACS 2019-2023
        vintages_to_run = [f"{y - 2}-{y + 2}" for y in parsed_years]

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_vintages: list[str] = []
    for v in vintages_to_run:
        if v not in seen:
            seen.add(v)
            unique_vintages.append(v)

    typer.echo(f"Aggregating ACS to CoC (build '{build}', align '{align}')...")
    typer.echo(f"  Boundary: {boundary_vintage}")
    typer.echo(f"  ACS vintages: {unique_vintages}")

    from coclab.measures.acs import build_coc_measures
    from coclab.naming import tract_xwalk_filename

    def decennial_floor(year: int) -> int:
        return year - (year % 10)

    from coclab.acs.translate import default_tract_vintage_for_acs

    for vintage in unique_vintages:
        # Resolve tract vintage from ACS end year
        tract_vintage = tracts if tracts is not None else default_tract_vintage_for_acs(vintage)

        # Find crosswalk
        xwalk_dir = curated_dir / "xwalks"
        xwalk_path = xwalk_dir / tract_xwalk_filename(boundary_vintage, tract_vintage)
        # Fall back to global xwalks
        if not xwalk_path.exists():
            xwalk_path = Path("data/curated/xwalks") / tract_xwalk_filename(
                boundary_vintage, tract_vintage
            )

        if not xwalk_path.exists():
            typer.echo(
                f"Error: Crosswalk not found: {xwalk_path}",
                err=True,
            )
            suggested_tract = None
            if tract_vintage % 10 != 0:
                suggested = decennial_floor(tract_vintage)
                suggested_tract = suggested
                typer.echo(
                    "The requested census tract year wasn't found and isn't on a decennial. "
                    f"Did you mean to request {suggested}?",
                    err=True,
                )
            hint_tract = suggested_tract if suggested_tract is not None else tract_vintage
            typer.echo(
                f"Run: coclab build xwalks --boundary {boundary_vintage} "
                f"--tracts {hint_tract}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"  Running ACS {vintage} (tracts {tract_vintage})...")
        try:
            build_coc_measures(
                boundary_vintage=boundary_vintage,
                acs_vintage=vintage,
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
            typer.echo(f"Error aggregating ACS {vintage}: {exc}", err=True)
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
    counties: Annotated[
        str | None,
        typer.Option(
            "--counties",
            "-c",
            help="TIGER county vintage year for crosswalk (e.g., '2023').",
        ),
    ] = None,
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs-vintage",
            help="ACS 5-year vintage for weights (e.g. '2019-2023').",
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
    """Aggregate ZORI rent indices to CoC level."""
    build_dir = _validate_build(build)
    _validate_align(align, ZORI_ALIGN_MODES, "zori")
    parsed_years = _resolve_years(years, build_dir)

    from coclab.builds import read_build_manifest

    manifest = read_build_manifest(build_dir)
    base_assets = manifest.get("base_assets", [])
    boundary_years = sorted(a["year"] for a in base_assets if a["asset_type"] == "coc_boundary")
    if not boundary_years:
        typer.echo("Error: No coc_boundary base assets found in manifest.", err=True)
        raise typer.Exit(2)

    boundary_vintage = str(boundary_years[-1])

    if counties is None:
        counties = boundary_vintage
    if acs_vintage is None:
        # Default: ACS ending at the boundary vintage year
        bv = int(boundary_vintage)
        acs_vintage = f"{bv - 4}-{bv}"

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
    typer.echo(f"  Boundary: {boundary_vintage}, Counties: {counties}")
    typer.echo(f"  ACS vintage: {acs_vintage}, Weighting: {weighting}")
    if to_yearly:
        typer.echo(f"  Yearly collapse: {yearly_method}")

    from coclab.rents.aggregate import aggregate_zori_to_coc

    try:
        result_path = aggregate_zori_to_coc(
            boundary=boundary_vintage,
            counties=counties,
            acs_vintage=acs_vintage,
            weighting=weighting,
            output_dir=output_dir,
            to_yearly=to_yearly,
            yearly_method=yearly_method,
            force=True,
        )
        typer.echo(f"Wrote ZORI aggregate: {result_path}")

        if result_path.is_relative_to(build_dir):
            rel = result_path.relative_to(build_dir).as_posix()
        else:
            rel = str(result_path)
        record_aggregate_run(
            build_dir,
            dataset="zori",
            alignment=align,
            years_requested=parsed_years,
            years_materialized=parsed_years,
            outputs=[rel],
        )

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
