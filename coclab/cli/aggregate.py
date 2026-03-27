"""CLI command group for aggregating source datasets into standalone CoC artifacts.

Provides commands for ACS, ZORI, PEP, and PIT aggregation. These
commands validate inputs, resolve parameters, and delegate to the
corresponding pipeline module.

Outputs go to ``data/curated/<dataset>/`` by default.  When ``--build``
is provided, outputs go to ``builds/<name>/data/curated/<dataset>/``
and runs are recorded in the build manifest.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Annotated

import pandas as pd
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
    help="Aggregate source datasets into standalone CoC analysis inputs.",
    no_args_is_help=True,
)

# Default output root when no --build is provided
DEFAULT_CURATED_DIR = Path("data/curated")

# ---------------------------------------------------------------------------
# Valid alignment modes per dataset
# ---------------------------------------------------------------------------

PEP_ALIGN_MODES = ("as_of_july", "lagged")
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
        typer.echo(
            "Create the directory manually or omit --build to write to data/curated/.",
            err=True,
        )
        raise typer.Exit(2) from None


def _maybe_record_run(build_dir: Path | None, **kwargs: object) -> None:
    """Record an aggregate run to manifest if a build directory is in use."""
    if build_dir is not None:
        record_aggregate_run(build_dir, **kwargs)


def _validate_align(align: str, valid_modes: tuple[str, ...], dataset: str) -> None:
    """Validate that *align* is one of *valid_modes* for *dataset*."""
    if align not in valid_modes:
        typer.echo(
            f"Error: Invalid alignment mode '{align}' for {dataset}. "
            f"Valid modes: {', '.join(valid_modes)}",
            err=True,
        )
        raise typer.Exit(2)


def _resolve_years(years: str | None, build_dir: Path | None) -> list[int]:
    """Parse ``--years`` if provided, otherwise use build years from manifest."""
    if years is not None:
        try:
            return parse_year_spec(years)
        except ValueError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(2) from exc

    if build_dir is None:
        typer.echo("Error: --years is required when --build is not specified.", err=True)
        raise typer.Exit(2)

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


def _build_lagged_pep_series(pep_df: pd.DataFrame, target_year: int, lag_months: int) -> pd.DataFrame:
    """Build a one-year county PEP series with month-based lag interpolation."""
    if lag_months < 0 or lag_months > 12:
        raise ValueError("--lag-months must be between 0 and 12.")

    current = (
        pep_df.loc[pep_df["year"] == target_year, ["county_fips", "population"]]
        .drop_duplicates(subset=["county_fips"])
        .rename(columns={"population": "population_current"})
    )
    if current.empty:
        raise FileNotFoundError(f"No PEP data found for year {target_year}.")

    weight_prev = lag_months / 12.0
    weight_current = 1.0 - weight_prev

    if lag_months == 0:
        out = current.rename(columns={"population_current": "population"})[
            ["county_fips", "population"]
        ].copy()
        out["year"] = target_year
        return out[["county_fips", "year", "population"]]

    previous = (
        pep_df.loc[pep_df["year"] == target_year - 1, ["county_fips", "population"]]
        .drop_duplicates(subset=["county_fips"])
        .rename(columns={"population": "population_previous"})
    )
    if previous.empty:
        raise FileNotFoundError(
            f"No PEP data found for year {target_year - 1} "
            f"(required for --lag-months={lag_months})."
        )

    merged = current.merge(previous, on="county_fips", how="outer")
    interpolated = (
        weight_current * merged["population_current"].fillna(0.0)
        + weight_prev * merged["population_previous"].fillna(0.0)
    )

    valid = pd.Series(True, index=merged.index)
    if weight_current > 0:
        valid &= merged["population_current"].notna()
    if weight_prev > 0:
        valid &= merged["population_previous"].notna()
    merged["population"] = interpolated.where(valid)

    out = merged[["county_fips", "population"]].copy()
    out["year"] = target_year
    return out[["county_fips", "year", "population"]]


# ---------------------------------------------------------------------------
# pep
# ---------------------------------------------------------------------------


@aggregate_app.command("pep")
def aggregate_pep(
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            "-b",
            help="Named build directory. Omit to write to data/curated/.",
        ),
    ] = None,
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=(
                "Temporal alignment mode. "
                "One of: as_of_july, lagged."
            ),
        ),
    ] = "as_of_july",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec (e.g. '2018-2024'). Required when --build is omitted.",
        ),
    ] = None,
    lag_months: Annotated[
        int,
        typer.Option(
            "--lag-months",
            help=(
                "Lag in months for --align=lagged (0-12). "
                "0 = current year, 12 = previous year."
            ),
        ),
    ] = 0,
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
    """Aggregate PEP population estimates into build-scoped CoC artifacts.

    Produces one file per boundary year (hub). County vintage matches
    boundary year by default. These CoC outputs can then feed CoC panels
    directly or metro workflows that resample county-native sources.
    """
    build_dir: Path | None = _validate_build(build) if build else None
    _validate_align(align, PEP_ALIGN_MODES, "pep")
    parsed_years = _resolve_years(years, build_dir)

    if lag_months < 0 or lag_months > 12:
        typer.echo(
            "Error: --lag-months must be between 0 and 12.",
            err=True,
        )
        raise typer.Exit(2)
    if align != "lagged" and lag_months != 0:
        typer.echo(
            "Error: --lag-months is only valid when --align=lagged.",
            err=True,
        )
        raise typer.Exit(2)

    if build_dir is not None:
        _require_boundary_years(build_dir)
        curated_dir = build_curated_dir(build_dir)
    else:
        curated_dir = DEFAULT_CURATED_DIR
    output_dir = curated_dir / "pep"

    label = f"build '{build}'" if build else "global curated"
    typer.echo(f"Aggregating PEP to CoC ({label}, align '{align}')...")

    from coclab.pep.aggregate import aggregate_pep_to_coc, load_pep_county

    align_params: dict | None = {"lag_months": lag_months} if align == "lagged" else None
    pep_source_df = pd.DataFrame()
    if align == "lagged":
        try:
            pep_source_df = load_pep_county()
        except FileNotFoundError as exc:
            _maybe_record_run(
                build_dir, dataset="pep", alignment=align,
                years_requested=parsed_years, status="failed",
                error=str(exc), alignment_params=align_params,
            )
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure PEP data and crosswalks are available.", err=True)
            raise typer.Exit(1) from exc
    all_outputs: list[str] = []
    materialized: list[int] = []

    for build_year in parsed_years:
        boundary_vintage = str(build_year)
        county_vintage = str(build_year)
        pep_path: Path | None = None
        pep_year = build_year

        try:
            if align == "lagged":
                weight_prev = lag_months / 12.0
                typer.echo(
                    f"  B{build_year}: lag {lag_months} months "
                    f"(w_current={1.0 - weight_prev:.3f}, w_previous={weight_prev:.3f}), "
                    f"counties {county_vintage}"
                )
                if lag_months > 0:
                    lagged_series = _build_lagged_pep_series(
                        pep_df=pep_source_df,
                        target_year=build_year,
                        lag_months=lag_months,
                    )
                    with tempfile.NamedTemporaryFile(
                        prefix=f"pep_lagged_{build_year}_",
                        suffix=".parquet",
                        delete=False,
                    ) as tmp:
                        pep_path = Path(tmp.name)
                    lagged_series.to_parquet(pep_path, index=False)
            else:
                typer.echo(f"  B{build_year}: PEP year {pep_year}, counties {county_vintage}")

            result_path = aggregate_pep_to_coc(
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                weighting=weighting,
                pep_path=pep_path,
                start_year=pep_year,
                end_year=pep_year,
                min_coverage=min_coverage,
                output_dir=output_dir,
                force=True,
            )

            if build_dir and result_path.is_relative_to(build_dir):
                rel = result_path.relative_to(build_dir).as_posix()
            else:
                rel = str(result_path)
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {result_path.name}")

        except FileNotFoundError as exc:
            _maybe_record_run(
                build_dir, dataset="pep", alignment=align,
                years_requested=parsed_years, status="failed",
                error=str(exc), alignment_params=align_params,
            )
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure PEP data and crosswalks are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            _maybe_record_run(
                build_dir, dataset="pep", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
                alignment_params=align_params,
            )
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc
        finally:
            if pep_path is not None and pep_path.exists():
                pep_path.unlink()

    _maybe_record_run(
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
        str | None,
        typer.Option(
            "--build",
            "-b",
            help="Named build directory. Omit to write to data/curated/.",
        ),
    ] = None,
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
            help="Year spec (e.g. '2018-2024'). Required when --build is omitted.",
        ),
    ] = None,
) -> None:
    """Aggregate PIT counts into build-scoped CoC artifacts.

    PIT data already contains coc_id, so this command filters and
    aligns PIT count data to the build's year scope.  Produces one
    output file per boundary year for downstream panel assembly.
    """
    build_dir: Path | None = _validate_build(build) if build else None
    _validate_align(align, PIT_ALIGN_MODES, "pit")
    parsed_years = _resolve_years(years, build_dir)

    curated_dir = build_curated_dir(build_dir) if build_dir else DEFAULT_CURATED_DIR
    output_dir = curated_dir / "pit"
    output_dir.mkdir(parents=True, exist_ok=True)

    label = f"build '{build}'" if build else "global curated"
    typer.echo(f"Aggregating PIT to CoC ({label}, align '{align}')...")
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
            if build_dir and out_path.is_relative_to(build_dir) else str(out_path)
        )

    materialized = sorted(int(k) for k in collected.keys())
    total_records = sum(len(df) for df in collected.values())
    sample_df = next(iter(collected.values()))
    coc_count = sample_df["coc_id"].nunique() if "coc_id" in sample_df.columns else "n/a"
    typer.echo(f"Wrote PIT aggregate: {len(materialized)} files to {output_dir}")
    typer.echo(f"  CoCs: {coc_count}, Records: {total_records:,}")

    _maybe_record_run(
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
        str | None,
        typer.Option(
            "--build",
            "-b",
            help="Named build directory. Omit to write to data/curated/.",
        ),
    ] = None,
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
            help="Year spec (e.g. '2018-2024'). Required when --build is omitted.",
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
    """Aggregate cached ACS tract data into CoC artifacts.

    Reads pre-ingested ACS tract files from disk and aggregates to CoC
    level using crosswalks.  No Census API calls are made.  If cached
    ingest files are missing, the command fails with instructions to
    run ``coclab ingest acs`` first.

    Iterates over years using each as the boundary vintage (hub).
    For each boundary year, the ACS vintage is derived from the alignment
    mode and a crosswalk is resolved from the xwalks directory.
    """
    build_dir: Path | None = _validate_build(build) if build else None
    _validate_align(align, ACS_ALIGN_MODES, "acs")
    parsed_years = _resolve_years(years, build_dir)

    if weighting not in ("area", "population"):
        typer.echo(
            f"Error: Invalid weighting '{weighting}'. Use 'area' or 'population'.",
            err=True,
        )
        raise typer.Exit(2)

    if build_dir is not None:
        _require_boundary_years(build_dir)
        curated_dir = build_curated_dir(build_dir)
    else:
        curated_dir = DEFAULT_CURATED_DIR
    output_dir = curated_dir / "measures"

    label = f"build '{build}'" if build else "global curated"
    typer.echo(f"Aggregating ACS to CoC ({label}, align '{align}')...")

    import pandas as pd

    from coclab.acs.ingest.tract_population import get_output_path
    from coclab.acs.translate import default_tract_vintage_for_acs
    from coclab.measures.acs import (
        _maybe_remap_ct_planning_regions,
        aggregate_to_coc,
    )
    from coclab.naming import measures_filename, tract_xwalk_filename
    from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

    def decennial_floor(year: int) -> int:
        return year - (year % 10)

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

        # --- Resolve cached ACS tract data file (NO API) ---
        acs_cache_path = get_output_path(acs_vintage, str(tract_vintage))
        if not acs_cache_path.exists():
            _maybe_record_run(
                build_dir, dataset="acs", alignment=align,
                years_requested=parsed_years, status="failed",
                error=f"ACS cache not found: {acs_cache_path}",
            )
            typer.echo(
                f"Error: Cached ACS tract file not found: {acs_cache_path}",
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
            _maybe_record_run(
                build_dir, dataset="acs", alignment=align,
                years_requested=parsed_years, status="failed",
                error=f"Crosswalk not found: {xwalk_path}",
            )
            typer.echo(
                f"Error: Crosswalk not found: {xwalk_path}",
                err=True,
            )
            if isinstance(tract_vintage, int) and tract_vintage % 10 != 0:
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
            # Load cached data and crosswalk
            acs_data = pd.read_parquet(acs_cache_path)
            crosswalk = pd.read_parquet(xwalk_path)

            # Rename tract_geoid → GEOID for aggregate_to_coc compatibility
            if "tract_geoid" in acs_data.columns and "GEOID" not in acs_data.columns:
                acs_data = acs_data.rename(columns={"tract_geoid": "GEOID"})

            # Handle CT planning region GEOID remapping
            acs_data = _maybe_remap_ct_planning_regions(
                acs_data, crosswalk, acs_vintage
            )

            # Aggregate to CoC level
            coc_measures = aggregate_to_coc(acs_data, crosswalk, weighting=weighting)

            # Add vintage columns
            coc_measures["boundary_vintage"] = boundary_vintage
            coc_measures["acs_vintage"] = acs_vintage

            # Reorder columns
            col_order = [
                "coc_id", "boundary_vintage", "acs_vintage", "weighting_method",
                "total_population", "adult_population", "population_below_poverty",
                "median_household_income", "median_gross_rent",
                "coverage_ratio", "source",
            ]
            col_order = [c for c in col_order if c in coc_measures.columns]
            coc_measures = coc_measures[col_order]

            # Write output
            output_dir.mkdir(parents=True, exist_ok=True)

            tv_str = str(tract_vintage)
            if "tract_vintage" in crosswalk.columns:
                tv_str = str(crosswalk["tract_vintage"].iloc[0])

            filename = measures_filename(acs_vintage, boundary_vintage, tv_str)
            out_path = output_dir / filename

            provenance = ProvenanceBlock(
                boundary_vintage=boundary_vintage,
                tract_vintage=tv_str,
                acs_vintage=acs_vintage,
                weighting=weighting,
                extra={
                    "dataset_type": "coc_measures",
                    "source": "cached_ingest",
                    "crosswalk_path": str(xwalk_path),
                    "acs_cache_path": str(acs_cache_path),
                },
            )
            write_parquet_with_provenance(coc_measures, out_path, provenance)

            rel = (
                out_path.relative_to(build_dir).as_posix()
                if out_path.is_relative_to(build_dir) else str(out_path)
            )
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {out_path.name}")

        except Exception as exc:
            _maybe_record_run(
                build_dir, dataset="acs", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error aggregating ACS {acs_vintage}: {exc}", err=True)
            raise typer.Exit(1) from exc

    _maybe_record_run(
        build_dir,
        dataset="acs",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=all_outputs,
    )
    typer.echo(
        f"ACS aggregation complete ({len(materialized)} years). "
        f"Output in: {output_dir}"
    )


# ---------------------------------------------------------------------------
# zori
# ---------------------------------------------------------------------------


@aggregate_app.command("zori")
def aggregate_zori(
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            "-b",
            help="Named build directory. Omit to write to data/curated/.",
        ),
    ] = None,
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
            help="Year spec (e.g. '2018-2024'). Required when --build is omitted.",
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
    """Aggregate ZORI rent indices into CoC artifacts.

    Iterates over years using each as the boundary vintage (hub).
    County vintage and ACS vintage for weights are derived from the
    boundary year. Resulting yearly or monthly CoC artifacts can be used
    directly in CoC panels or as curated inputs to metro workflows.
    """
    build_dir: Path | None = _validate_build(build) if build else None
    _validate_align(align, ZORI_ALIGN_MODES, "zori")
    parsed_years = _resolve_years(years, build_dir)

    if build_dir is not None:
        _require_boundary_years(build_dir)

    # Map alignment mode to pipeline parameters
    to_yearly = align != "monthly_native"
    yearly_method_map = {
        "pit_january": "pit_january",
        "calendar_year_average": "calendar_mean",
    }
    yearly_method = yearly_method_map.get(align, "pit_january")

    curated_dir = build_curated_dir(build_dir) if build_dir else DEFAULT_CURATED_DIR
    output_dir = curated_dir / "zori"

    label = f"build '{build}'" if build else "global curated"
    typer.echo(f"Aggregating ZORI to CoC ({label}, align '{align}')...")

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

            if build_dir and result_path.is_relative_to(build_dir):
                rel = result_path.relative_to(build_dir).as_posix()
            else:
                rel = str(result_path)
            all_outputs.append(rel)
            materialized.append(build_year)
            typer.echo(f"    Wrote: {result_path.name}")

        except FileNotFoundError as exc:
            _maybe_record_run(
                build_dir, dataset="zori", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure ZORI data, crosswalks, and ACS weights are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            _maybe_record_run(
                build_dir, dataset="zori", alignment=align,
                years_requested=parsed_years, status="failed", error=str(exc),
            )
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc

    _maybe_record_run(
        build_dir,
        dataset="zori",
        alignment=align,
        years_requested=parsed_years,
        years_materialized=materialized,
        outputs=all_outputs,
    )
    typer.echo(f"ZORI aggregation complete ({len(materialized)} years). Output in: {output_dir}")
