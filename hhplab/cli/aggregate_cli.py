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
CDC_OVERDOSE_ALIGN_MODES = ("january_trailing_12_months",)
COVARIATE_ALIGN_MODES = ("native_year",)


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


@aggregate_app.command("covariate")
def aggregate_covariate(
    source: Annotated[
        str,
        typer.Option("--source", help="Covariate source id; see `hhplab list covariates`."),
    ],
    align: Annotated[
        str,
        typer.Option("--align", help="Temporal alignment mode. One of: native_year."),
    ] = "native_year",
    years: Annotated[
        str | None,
        typer.Option("--years", help="Optional year spec such as '2018-2024'."),
    ] = None,
    curated_path: Annotated[
        Path | None,
        typer.Option("--curated-path", help="Curated covariate parquet path."),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Output directory for panel artifact."),
    ] = None,
    target_geo: Annotated[
        str,
        typer.Option(
            "--target-geo",
            help="Target geography for pass-through covariate output: coc, county, or state.",
        ),
    ] = "coc",
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Rebuild even if output already exists."),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option("--json", help="Emit structured JSON."),
    ] = False,
) -> None:
    """Validate and materialize an expanded covariate source as panel-ready parquet."""
    import json

    from hhplab.covariates.aggregate import aggregate_covariate_source

    _validate_align(align, COVARIATE_ALIGN_MODES, "covariate")
    parsed_years = parse_year_spec(years) if years is not None else None
    try:
        result_path = aggregate_covariate_source(
            source,
            curated_path=curated_path,
            output_dir=output_dir,
            years=parsed_years,
            target_geo=target_geo,
            force=force,
        )
        row_count = len(pd.read_parquet(result_path))
    except (FileNotFoundError, KeyError, ValueError) as exc:
        if output_json:
            typer.echo(json.dumps({"status": "error", "message": str(exc)}))
            raise typer.Exit(2) from exc
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc

    payload = {
        "status": "ok",
        "dataset": "covariate",
        "source_id": source,
        "align": align,
        "target_geo": target_geo,
        "years": parsed_years,
        "output_path": str(result_path),
        "row_count": row_count,
    }
    if output_json:
        typer.echo(json.dumps(payload))
        return
    typer.echo(f"Covariate aggregation complete: {result_path}")


# ---------------------------------------------------------------------------
# cdc-overdose
# ---------------------------------------------------------------------------


@aggregate_app.command("cdc-overdose")
def aggregate_cdc_overdose(
    align: Annotated[
        str,
        typer.Option(
            "--align",
            help="Temporal alignment mode. One of: january_trailing_12_months.",
        ),
    ] = "january_trailing_12_months",
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year spec (e.g. '2020-2025'). January rows are used for each year.",
        ),
    ] = None,
    geo_type: Annotated[
        str,
        typer.Option(
            "--geo-type",
            help="Target geography. One of: county, msa.",
        ),
    ] = "msa",
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            help="MSA definition version to use when --geo-type=msa.",
        ),
    ] = "census_msa_2023",
    counties: Annotated[
        str,
        typer.Option(
            "--counties",
            help="County vintage label for output provenance and filenames.",
        ),
    ] = "2023",
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Minimum county-count coverage ratio for valid MSA-year output.",
        ),
    ] = 0.0,
    raw_path: Annotated[
        Path,
        typer.Option(
            "--raw-path",
            help="Path to the CDC VSRR county overdose CSV.",
        ),
    ] = Path("data/raw/cdc/VSRR_Provisional_County-Level_Drug_Overdose_Death_Counts.csv"),
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a structured JSON summary instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Aggregate CDC county overdose counts to MSAs using January annual values."""
    _validate_align(align, CDC_OVERDOSE_ALIGN_MODES, "cdc-overdose")
    parsed_years = _resolve_years(years)
    if geo_type not in {"county", "msa"}:
        msg = "Invalid --geo-type. Use one of: county, msa."
        if output_json:
            import json

            typer.echo(json.dumps({"status": "error", "message": msg}))
            raise typer.Exit(2)
        typer.echo(f"Error: {msg}", err=True)
        raise typer.Exit(2)

    if min_coverage < 0 or min_coverage > 1:
        msg = "--min-coverage must be between 0 and 1."
        if output_json:
            import json

            typer.echo(json.dumps({"status": "error", "message": msg}))
            raise typer.Exit(2)
        typer.echo(f"Error: {msg}", err=True)
        raise typer.Exit(2)

    from hhplab.cdc.overdose import (
        ingest_and_aggregate_overdose_to_msa,
        ingest_county_overdose,
    )

    output_dir = curated_root() / "cdc"
    try:
        if geo_type == "county":
            county_df, county_path = ingest_county_overdose(
                raw_path=raw_path,
                years=parsed_years,
                reference_month=1,
                county_vintage=counties,
                output_dir=output_dir,
            )
            outputs = {"county": str(county_path)}
            row_counts = {"county": len(county_df)}
            msa_count = None
        else:
            county_df, msa_df, county_path, msa_path = ingest_and_aggregate_overdose_to_msa(
                raw_path=raw_path,
                years=parsed_years,
                reference_month=1,
                definition_version=definition_version,
                county_vintage=counties,
                min_coverage=min_coverage,
                output_dir=output_dir,
            )
            outputs = {"county": str(county_path), "msa": str(msa_path)}
            row_counts = {"county": len(county_df), "msa": len(msa_df)}
            msa_count = int(msa_df["msa_id"].nunique())
    except (FileNotFoundError, ValueError) as exc:
        if output_json:
            import json

            typer.echo(json.dumps({"status": "error", "message": str(exc)}))
            raise typer.Exit(1) from exc
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if output_json:
        import json

        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "dataset": "cdc-overdose",
                    "geo_type": geo_type,
                    "years": parsed_years,
                    "align": align,
                    "definition_version": definition_version if geo_type == "msa" else None,
                    "county_vintage": counties,
                    "min_coverage": min_coverage if geo_type == "msa" else None,
                    "row_counts": row_counts,
                    "msa_count": msa_count,
                    "outputs": outputs,
                }
            )
        )
        return

    typer.echo(
        "CDC overdose aggregation complete "
        f"({geo_type}, years {parsed_years[0]}-{parsed_years[-1]})."
    )
    for label, path in outputs.items():
        typer.echo(f"  Wrote {label}: {path}")


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
                "side-by-side outputs. Defaults to area_share for back-compat; "
                "area_share is deprecated for analytical population panels."
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

    from hhplab.pep.pep_aggregate import (
        DIRECT_COUNTY_AREA_DEPRECATION_NOTICE,
        aggregate_pep_to_coc_many,
        build_lagged_pep_series,
        is_deprecated_direct_county_area_weighting,
        load_pep_county,
    )

    pep_source_df = pd.DataFrame()
    selected_weightings = weightings or ["area_share"]
    if any(is_deprecated_direct_county_area_weighting(w) for w in selected_weightings):
        typer.echo(f"Warning: {DIRECT_COUNTY_AREA_DEPRECATION_NOTICE}", err=True)
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
                    lagged_series = build_lagged_pep_series(
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
    target_geo: Annotated[
        str,
        typer.Option(
            "--target-geo",
            help="Target geography. One of: coc, msa.",
        ),
    ] = "coc",
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            help="MSA definition version to use when --target-geo=msa.",
        ),
    ] = "census_msa_2023",
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a structured JSON summary instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Aggregate cached ACS tract data into analysis-geography artifacts.

    Reads pre-ingested ACS tract files from disk and aggregates to CoC or MSA
    level using curated crosswalks/membership.  No Census API calls are made.  If cached
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
    if target_geo not in ("coc", "msa"):
        if output_json:
            import json

            msg = f"Invalid --target-geo '{target_geo}'. Use one of: coc, msa."
            typer.echo(json.dumps({"status": "error", "message": msg}))
            raise typer.Exit(2)
        typer.echo(
            f"Error: Invalid --target-geo '{target_geo}'. Use one of: coc, msa.",
            err=True,
        )
        raise typer.Exit(2)

    curated_dir = curated_root()
    output_dir = curated_dir / "measures"
    geo_label = "MSA" if target_geo == "msa" else "CoC"
    typer.echo(f"Aggregating ACS to {geo_label} (curated output, align '{align}')...")

    import pandas as pd

    from hhplab.acs.acs_aggregate import (
        _maybe_remap_ct_planning_regions,
        aggregate_to_coc,
        aggregate_to_geo,
    )
    from hhplab.acs.ingest.tract_population import get_output_path
    from hhplab.acs.translate import default_tract_vintage_for_acs
    from hhplab.msa.msa_io import read_msa_county_membership
    from hhplab.naming import measures_filename, msa_measures_filename, tract_xwalk_filename
    from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

    def build_msa_tract_crosswalk(
        acs_data: pd.DataFrame,
        membership: pd.DataFrame,
    ) -> pd.DataFrame:
        required = {"msa_id", "county_fips"}
        missing = sorted(required - set(membership.columns))
        if missing:
            raise ValueError(
                "MSA county membership is missing required columns "
                f"{missing}. Available columns: {list(membership.columns)}"
            )
        tracts = acs_data[["GEOID"]].drop_duplicates().copy()
        tracts["GEOID"] = tracts["GEOID"].astype(str).str.zfill(11)
        tracts["county_fips"] = tracts["GEOID"].str[:5]
        selected_membership = membership[["msa_id", "county_fips"]].drop_duplicates().copy()
        selected_membership["msa_id"] = selected_membership["msa_id"].astype(str).str.zfill(5)
        selected_membership["county_fips"] = (
            selected_membership["county_fips"].astype(str).str.zfill(5)
        )
        xwalk = selected_membership.merge(tracts, on="county_fips", how="inner")
        xwalk["area_share"] = 1.0
        if weighting == "population" and "total_population" in acs_data.columns:
            tract_pop = acs_data[["GEOID", "total_population"]].copy()
            tract_pop["GEOID"] = tract_pop["GEOID"].astype(str).str.zfill(11)
            xwalk = xwalk.merge(tract_pop, on="GEOID", how="left")
            totals = xwalk.groupby("msa_id")["total_population"].transform("sum")
            xwalk["pop_share"] = xwalk["total_population"] / totals.where(totals > 0)
            xwalk["pop_share"] = xwalk["pop_share"].fillna(0.0)
            xwalk = xwalk.drop(columns=["total_population"])
        elif weighting == "population":
            raise ValueError(
                "Population weighting requires total_population in the cached ACS tract file."
            )
        return xwalk.rename(columns={"GEOID": "tract_geoid"})[
            ["msa_id", "tract_geoid", "area_share"]
            + (["pop_share"] if weighting == "population" else [])
        ]

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

        # --- Resolve target geography mapping ---
        xwalk_path = curated_dir / "xwalks" / tract_xwalk_filename(boundary_vintage, tract_vintage)
        msa_membership_path = None

        if target_geo == "coc" and not xwalk_path.exists():
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

        if target_geo == "msa":
            from hhplab.naming import msa_county_membership_path

            msa_membership_path = msa_county_membership_path(definition_version)
            if not msa_membership_path.exists():
                if output_json:
                    import json

                    typer.echo(
                        json.dumps(
                            {
                                "status": "error",
                                "message": (
                                    "MSA county membership artifact not found: "
                                    f"{msa_membership_path}"
                                ),
                                "definition_version": definition_version,
                                "remedy": (
                                    "hhplab generate msa"
                                    f" --definition-version {definition_version}"
                                ),
                            }
                        )
                    )
                    raise typer.Exit(1)
                typer.echo(
                    f"Error: MSA county membership artifact not found: {msa_membership_path}",
                    err=True,
                )
                typer.echo(
                    f"Run: hhplab generate msa --definition-version {definition_version}",
                    err=True,
                )
                raise typer.Exit(1)

        typer.echo(f"  B{build_year}: ACS {acs_vintage} (tracts {tract_vintage})...")
        try:
            # Load cached data and crosswalk
            acs_data = pd.read_parquet(acs_cache_path)

            # Rename tract_geoid → GEOID for aggregate_to_coc compatibility
            if "tract_geoid" in acs_data.columns and "GEOID" not in acs_data.columns:
                acs_data = acs_data.rename(columns={"tract_geoid": "GEOID"})
            if "GEOID" in acs_data.columns:
                acs_data["GEOID"] = acs_data["GEOID"].astype(str).str.zfill(11)

            if target_geo == "coc":
                crosswalk = pd.read_parquet(xwalk_path)
                # Handle CT planning region GEOID remapping
                acs_data = _maybe_remap_ct_planning_regions(acs_data, crosswalk, acs_vintage)
                measures = aggregate_to_coc(acs_data, crosswalk, weighting=weighting)
                id_col = "coc_id"
                provenance_extra = {
                    "dataset_type": "coc_measures",
                    "source": "cached_ingest",
                    "crosswalk_path": str(xwalk_path),
                    "acs_cache_path": str(acs_cache_path),
                }
            else:
                membership = read_msa_county_membership(definition_version)
                crosswalk = build_msa_tract_crosswalk(acs_data, membership)
                measures = aggregate_to_geo(
                    acs_data,
                    crosswalk,
                    weighting=weighting,
                    geo_id_col="msa_id",
                )
                measures["definition_version"] = definition_version
                id_col = "msa_id"
                provenance_extra = {
                    "dataset_type": "msa_measures",
                    "source": "cached_ingest",
                    "definition_version": definition_version,
                    "msa_membership_path": str(msa_membership_path),
                    "acs_cache_path": str(acs_cache_path),
                }

            # Add vintage columns
            measures["boundary_vintage"] = boundary_vintage
            measures["acs_vintage"] = acs_vintage

            # Reorder columns
            col_order = [
                id_col,
                "boundary_vintage",
                "acs_vintage",
                "definition_version",
                "weighting_method",
            ]
            col_order = [c for c in col_order if c in measures.columns]
            measures = measures[col_order + [c for c in measures.columns if c not in col_order]]

            # Write output
            output_dir.mkdir(parents=True, exist_ok=True)

            tv_str = str(tract_vintage)
            if "tract_vintage" in crosswalk.columns:
                tv_str = str(crosswalk["tract_vintage"].iloc[0])

            if target_geo == "msa":
                filename = msa_measures_filename(acs_vintage, definition_version, tv_str)
            else:
                filename = measures_filename(acs_vintage, boundary_vintage, tv_str)
            out_path = output_dir / filename

            provenance = ProvenanceBlock(
                boundary_vintage=boundary_vintage,
                tract_vintage=tv_str,
                acs_vintage=acs_vintage,
                weighting=weighting,
                geo_type=target_geo,
                definition_version=definition_version if target_geo == "msa" else None,
                extra=provenance_extra,
            )
            write_parquet_with_provenance(measures, out_path, provenance)

            all_outputs.append(str(out_path))
            materialized.append(build_year)
            total_row_count += len(measures)
            if id_col in measures.columns:
                total_coc_count = measures[id_col].nunique()
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
                    "target_geo": target_geo,
                    "definition_version": definition_version if target_geo == "msa" else None,
                    "years_requested": parsed_years,
                    "years_materialized": materialized,
                    "output_path": str(output_dir),
                    "coc_count": total_coc_count if target_geo == "coc" else None,
                    "geo_count": total_coc_count,
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
