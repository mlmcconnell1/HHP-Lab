"""CLI command group for aggregating source datasets into standalone CoC artifacts.

Provides commands for ACS, ZORI, PEP, and PIT aggregation. These
commands validate inputs, resolve explicit year parameters, and delegate
to the corresponding pipeline module.

Outputs go to ``data/curated/<dataset>/``. For end-to-end orchestration,
prefer ``hhplab build recipe`` which materializes recipe outputs under
the configured recipe output root.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.paths import curated_root
from hhplab.year_spec import parse_year_spec

aggregate_app = typer.Typer(
    name="aggregate",
    help="Aggregate source datasets into standalone CoC analysis inputs.",
    no_args_is_help=True,
)

# ---------------------------------------------------------------------------
# Valid alignment modes per dataset
# ---------------------------------------------------------------------------

PEP_ALIGN_MODES = ("as_of_july", "lagged")
PIT_ALIGN_MODES = ("point_in_time_jan", "to_calendar_year")
ACS_ALIGN_MODES = ("vintage_end_year", "window_center_year")
ZORI_ALIGN_MODES = ("monthly_native", "pit_january", "calendar_year_average")


# ---------------------------------------------------------------------------
def _validate_align(align: str, valid_modes: tuple[str, ...], dataset: str) -> None:
    """Validate that *align* is one of *valid_modes* for *dataset*."""
    if align not in valid_modes:
        typer.echo(
            f"Error: Invalid alignment mode '{align}' for {dataset}. "
            f"Valid modes: {', '.join(valid_modes)}",
            err=True,
        )
        raise typer.Exit(2)


def _resolve_years(years: str | None) -> list[int]:
    """Parse the required ``--years`` spec."""
    if years is not None:
        try:
            return parse_year_spec(years)
        except ValueError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(2) from exc

    typer.echo(
        "Error: --years is required. Use an explicit year spec such as '2018-2024'.",
        err=True,
    )
    raise typer.Exit(2)


def _build_lagged_pep_series(
    pep_df: pd.DataFrame,
    target_year: int,
    lag_months: int,
) -> pd.DataFrame:
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
    interpolated = weight_current * merged["population_current"].fillna(0.0) + weight_prev * merged[
        "population_previous"
    ].fillna(0.0)

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
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=("Temporal alignment mode. One of: as_of_july, lagged."),
        ),
    ] = "as_of_july",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec (e.g. '2018-2024').",
        ),
    ] = None,
    lag_months: Annotated[
        int,
        typer.Option(
            "--lag-months",
            help=("Lag in months for --align=lagged (0-12). 0 = current year, 12 = previous year."),
        ),
    ] = 0,
    weightings: Annotated[
        list[str] | None,
        typer.Option(
            "--weighting",
            "-w",
            help=(
                "Weighting method or crosswalk weight column. Repeat for "
                "side-by-side outputs. Defaults to area_share."
            ),
        ),
    ] = None,
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Minimum coverage ratio for valid CoC-year (default 0.95).",
        ),
    ] = 0.95,
) -> None:
    """Aggregate PEP population estimates into curated CoC artifacts.

    Produces one file per boundary year (hub). County vintage matches
    boundary year by default. These CoC outputs can then feed CoC panels
    directly or metro workflows that resample county-native sources.
    """
    _validate_align(align, PEP_ALIGN_MODES, "pep")
    parsed_years = _resolve_years(years)

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

    output_dir = curated_root() / "pep"
    typer.echo(f"Aggregating PEP to CoC (curated output, align '{align}')...")

    from hhplab.pep.pep_aggregate import aggregate_pep_to_coc_many, load_pep_county

    pep_source_df = pd.DataFrame()
    selected_weightings = weightings or ["area_share"]
    if align == "lagged":
        try:
            pep_source_df = load_pep_county()
        except FileNotFoundError as exc:
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
                    f"counties {county_vintage}, weights {', '.join(selected_weightings)}"
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
                typer.echo(
                    f"  B{build_year}: PEP year {pep_year}, counties {county_vintage}, "
                    f"weights {', '.join(selected_weightings)}"
                )

            result_paths = aggregate_pep_to_coc_many(
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                weightings=selected_weightings,
                pep_path=pep_path,
                start_year=pep_year,
                end_year=pep_year,
                min_coverage=min_coverage,
                output_dir=output_dir,
                force=True,
            )

            for result_path in result_paths.values():
                all_outputs.append(str(result_path))
            materialized.append(build_year)
            for weighting, result_path in result_paths.items():
                typer.echo(f"    Wrote ({weighting}): {result_path.name}")

        except FileNotFoundError as exc:
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure PEP data and crosswalks are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc
        finally:
            if pep_path is not None and pep_path.exists():
                pep_path.unlink()

    typer.echo(f"PEP aggregation complete ({len(materialized)} years). Output in: {output_dir}")


# ---------------------------------------------------------------------------
# pit
# ---------------------------------------------------------------------------


@aggregate_app.command("pit")
def aggregate_pit(
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=("Temporal alignment mode. One of: point_in_time_jan, to_calendar_year."),
        ),
    ] = "point_in_time_jan",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec (e.g. '2018-2024').",
        ),
    ] = None,
    geo_type: Annotated[
        str,
        typer.Option(
            "--geo-type",
            help="Target analysis geography. One of: coc, msa.",
        ),
    ] = "coc",
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            help="MSA definition version to use when --geo-type=msa.",
        ),
    ] = "census_msa_2023",
    counties: Annotated[
        int | None,
        typer.Option(
            "--counties",
            help="County geometry vintage for the CoC-to-MSA crosswalk. Defaults to the PIT year.",
        ),
    ] = None,
) -> None:
    """Aggregate PIT counts into curated CoC or MSA artifacts.

    PIT data already contains coc_id, so this command filters and
    aligns PIT count data to the build's year scope.  Produces one
    output file per year for downstream panel assembly.
    """
    _validate_align(align, PIT_ALIGN_MODES, "pit")
    parsed_years = _resolve_years(years)
    if geo_type not in {"coc", "msa"}:
        typer.echo(
            "Error: --geo-type must be one of: coc, msa",
            err=True,
        )
        raise typer.Exit(2)

    output_dir = curated_root() / "pit"
    output_dir.mkdir(parents=True, exist_ok=True)

    target_label = "CoC" if geo_type == "coc" else "MSA"
    typer.echo(f"Aggregating PIT to {target_label} (curated output, align '{align}')...")
    typer.echo(f"  Years: {parsed_years}")

    from hhplab.msa import read_coc_msa_crosswalk
    from hhplab.naming import (
        coc_pit_filename,
        discover_pit_vintages,
        msa_pit_filename,
        pit_path,
        pit_vintage_path,
    )
    from hhplab.pit import aggregate_pit_to_msa, save_msa_pit

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

            typer.echo(f"  Using vintage P{vintage} for years: {sorted(available)}")

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
        if geo_type == "coc":
            out_name = coc_pit_filename(year, year)
            out_path = output_dir / out_name
            df.to_parquet(out_path, index=False)
        else:
            boundary_vintage = str(year)
            county_vintage = str(counties if counties is not None else year)
            try:
                crosswalk = read_coc_msa_crosswalk(
                    boundary_vintage,
                    definition_version,
                    county_vintage,
                )
            except FileNotFoundError as exc:
                typer.echo(f"Error: {exc}", err=True)
                raise typer.Exit(1) from exc

            try:
                msa_df = aggregate_pit_to_msa(
                    df,
                    crosswalk,
                    definition_version=definition_version,
                    boundary_vintage=boundary_vintage,
                    county_vintage=county_vintage,
                )
            except ValueError as exc:
                typer.echo(f"Error: {exc}", err=True)
                raise typer.Exit(1) from exc

            out_name = msa_pit_filename(
                year,
                definition_version,
                boundary_vintage,
                county_vintage,
            )
            out_path = output_dir / out_name
            save_msa_pit(
                msa_df,
                pit_year=year,
                definition_version=definition_version,
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                output_dir=output_dir,
            )

        all_outputs.append(str(out_path))

    materialized = sorted(int(k) for k in collected.keys())
    total_records = sum(len(df) for df in collected.values())
    sample_df = next(iter(collected.values()))
    source_coc_count = sample_df["coc_id"].nunique() if "coc_id" in sample_df.columns else "n/a"
    typer.echo(f"Wrote PIT aggregate: {len(materialized)} files to {output_dir}")
    if geo_type == "coc":
        typer.echo(f"  CoCs: {source_coc_count}, Records: {total_records:,}")
    else:
        typer.echo(
            f"  Source CoCs: {source_coc_count}, Records: {total_records:,}, "
            f"MSA definition: {definition_version}"
        )


# ---------------------------------------------------------------------------
# acs
# ---------------------------------------------------------------------------


@aggregate_app.command("acs")
def aggregate_acs(
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help=("Temporal alignment mode. One of: vintage_end_year, window_center_year."),
        ),
    ] = "vintage_end_year",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec (e.g. '2018-2024').",
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
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a structured JSON summary instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Aggregate cached ACS tract data into CoC artifacts.

    Reads pre-ingested ACS tract files from disk and aggregates to CoC
    level using crosswalks.  No Census API calls are made.  If cached
    ingest files are missing, the command fails with instructions to
    run ``hhplab ingest acs5-tract`` first.

    Iterates over years using each as the boundary vintage (hub).
    For each boundary year, the ACS vintage is derived from the alignment
    mode and a crosswalk is resolved from the xwalks directory.
    """
    _validate_align(align, ACS_ALIGN_MODES, "acs")
    parsed_years = _resolve_years(years)

    if weighting not in ("area", "population"):
        if output_json:
            import json

            msg = f"Invalid weighting '{weighting}'. Use 'area' or 'population'."
            typer.echo(json.dumps({"status": "error", "message": msg}))
            raise typer.Exit(2)
        typer.echo(
            f"Error: Invalid weighting '{weighting}'. Use 'area' or 'population'.",
            err=True,
        )
        raise typer.Exit(2)

    curated_dir = curated_root()
    output_dir = curated_dir / "measures"
    typer.echo(f"Aggregating ACS to CoC (curated output, align '{align}')...")

    import pandas as pd

    from hhplab.acs.ingest.tract_population import get_output_path
    from hhplab.acs.translate import default_tract_vintage_for_acs
    from hhplab.measures.measures_acs import (
        _maybe_remap_ct_planning_regions,
        aggregate_to_coc,
    )
    from hhplab.naming import measures_filename, tract_xwalk_filename
    from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

    def decennial_floor(year: int) -> int:
        return year - (year % 10)

    all_outputs: list[str] = []
    materialized: list[int] = []
    total_row_count = 0
    total_coc_count = 0

    for build_year in parsed_years:
        boundary_vintage = str(build_year)

        # Derive ACS vintage from alignment mode
        if align == "vintage_end_year":
            acs_vintage = f"{build_year - 4}-{build_year}"
        else:  # window_center_year
            acs_vintage = f"{build_year - 2}-{build_year + 2}"

        tract_vintage = tracts if tracts is not None else default_tract_vintage_for_acs(acs_vintage)

        # --- Resolve cached ACS tract data file (NO API) ---
        acs_cache_path = get_output_path(acs_vintage, str(tract_vintage))
        if not acs_cache_path.exists():
            if output_json:
                import json

                typer.echo(
                    json.dumps(
                        {
                            "status": "error",
                            "message": f"Cached ACS tract file not found: {acs_cache_path}",
                            "boundary_vintage": boundary_vintage,
                            "acs_vintage": acs_vintage,
                            "remedy": (
                                f"hhplab ingest acs5-tract"
                                f" --acs {acs_vintage}"
                                f" --tracts {tract_vintage}"
                            ),
                        }
                    )
                )
                raise typer.Exit(1)
            typer.echo(
                f"Error: Cached ACS tract file not found: {acs_cache_path}",
                err=True,
            )
            typer.echo(
                f"Run: hhplab ingest acs5-tract --acs {acs_vintage} --tracts {tract_vintage}",
                err=True,
            )
            raise typer.Exit(1)

        # --- Resolve crosswalk ---
        xwalk_path = curated_dir / "xwalks" / tract_xwalk_filename(boundary_vintage, tract_vintage)

        if not xwalk_path.exists():
            if output_json:
                import json

                typer.echo(
                    json.dumps(
                        {
                            "status": "error",
                            "message": f"Crosswalk not found: {xwalk_path}",
                            "boundary_vintage": boundary_vintage,
                            "acs_vintage": acs_vintage,
                            "tract_vintage": str(tract_vintage),
                            "remedy": (
                                f"hhplab generate xwalks"
                                f" --boundary {boundary_vintage}"
                                f" --tracts {tract_vintage}"
                            ),
                        }
                    )
                )
                raise typer.Exit(1)
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
                f"Run: hhplab generate xwalks --boundary {boundary_vintage} "
                f"--tracts {tract_vintage}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"  B{build_year}: ACS {acs_vintage} (tracts {tract_vintage})...")
        try:
            # Load cached data and crosswalk
            acs_data = pd.read_parquet(acs_cache_path)
            crosswalk = pd.read_parquet(xwalk_path)

            # Rename tract_geoid → GEOID for aggregate_to_coc compatibility
            if "tract_geoid" in acs_data.columns and "GEOID" not in acs_data.columns:
                acs_data = acs_data.rename(columns={"tract_geoid": "GEOID"})

            # Handle CT planning region GEOID remapping
            acs_data = _maybe_remap_ct_planning_regions(acs_data, crosswalk, acs_vintage)

            # Aggregate to CoC level
            coc_measures = aggregate_to_coc(acs_data, crosswalk, weighting=weighting)

            # Add vintage columns
            coc_measures["boundary_vintage"] = boundary_vintage
            coc_measures["acs_vintage"] = acs_vintage

            # Reorder columns
            col_order = [
                "coc_id",
                "boundary_vintage",
                "acs_vintage",
                "weighting_method",
                "total_population",
                "adult_population",
                "population_below_poverty",
                "median_household_income",
                "median_gross_rent",
                "coverage_ratio",
                "source",
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

            all_outputs.append(str(out_path))
            materialized.append(build_year)
            total_row_count += len(coc_measures)
            if "coc_id" in coc_measures.columns:
                total_coc_count = coc_measures["coc_id"].nunique()
            typer.echo(f"    Wrote: {out_path.name}")

        except Exception as exc:
            if output_json:
                import json

                typer.echo(
                    json.dumps(
                        {
                            "status": "error",
                            "message": f"Error aggregating ACS {acs_vintage}: {exc}",
                            "boundary_vintage": boundary_vintage,
                            "acs_vintage": acs_vintage,
                        }
                    )
                )
                raise typer.Exit(1) from exc
            typer.echo(f"Error aggregating ACS {acs_vintage}: {exc}", err=True)
            raise typer.Exit(1) from exc

    if output_json:
        import json

        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "alignment": align,
                    "weighting": weighting,
                    "years_requested": parsed_years,
                    "years_materialized": materialized,
                    "output_path": str(output_dir),
                    "coc_count": total_coc_count,
                    "row_count": total_row_count,
                    "outputs": all_outputs,
                }
            )
        )
    else:
        typer.echo(f"ACS aggregation complete ({len(materialized)} years). Output in: {output_dir}")


# ---------------------------------------------------------------------------
# zori
# ---------------------------------------------------------------------------


@aggregate_app.command("zori")
def aggregate_zori(
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
            help="Year spec (e.g. '2018-2024').",
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
    _validate_align(align, ZORI_ALIGN_MODES, "zori")
    parsed_years = _resolve_years(years)

    # Map alignment mode to pipeline parameters
    to_yearly = align != "monthly_native"
    yearly_method_map = {
        "pit_january": "pit_january",
        "calendar_year_average": "calendar_mean",
    }
    yearly_method = yearly_method_map.get(align, "pit_january")

    output_dir = curated_root() / "zori"
    typer.echo(f"Aggregating ZORI to CoC (curated output, align '{align}')...")

    from hhplab.rents.zori_aggregate import aggregate_zori_to_coc

    all_outputs: list[str] = []
    materialized: list[int] = []

    for build_year in parsed_years:
        boundary_vintage = str(build_year)
        county_vintage = str(build_year)
        acs_vintage = f"{build_year - 4}-{build_year}"

        typer.echo(
            f"  B{build_year}: counties {county_vintage}, ACS {acs_vintage}, weight {weighting}"
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

            all_outputs.append(str(result_path))
            materialized.append(build_year)
            typer.echo(f"    Wrote: {result_path.name}")

        except FileNotFoundError as exc:
            typer.echo(f"Error: {exc}", err=True)
            typer.echo("Ensure ZORI data, crosswalks, and ACS weights are available.", err=True)
            raise typer.Exit(1) from exc
        except Exception as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc

    typer.echo(f"ZORI aggregation complete ({len(materialized)} years). Output in: {output_dir}")
