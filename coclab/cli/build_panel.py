"""CLI command for building analysis-geography x year panels."""

from pathlib import Path
from typing import Annotated

import typer

from coclab.builds import build_curated_dir, require_build_dir, resolve_build_dir

# Default ZORI coverage threshold
DEFAULT_ZORI_MIN_COVERAGE = 0.90

# Default ZORI rents directory
DEFAULT_RENTS_DIR = Path("data/curated/zori")
DEFAULT_PANEL_DIR = Path("data/curated/panel")


def _resolve_zori_yearly_path(
    explicit_path: Path | None,
    base_dir: Path | None = None,
    *,
    geo_type: str = "coc",
) -> Path | None:
    """Resolve the path to yearly ZORI data.

    Parameters
    ----------
    explicit_path : Path or None
        User-provided explicit path to yearly ZORI parquet.

    Returns
    -------
    Path or None
        Resolved path if found, None otherwise.

    Notes
    -----
    If explicit_path is provided, validates it exists.
    Otherwise, searches for yearly ZORI files in the default rents directory,
    trying new naming first (zori_yearly__A*.parquet), then legacy
    (coc_zori_yearly__*.parquet), and returns the most recent one.
    """
    if explicit_path is not None:
        if explicit_path.exists():
            return explicit_path
        return None

    # Search for yearly ZORI files in default location
    rents_dir = (base_dir / "data" / "curated" / "zori") if base_dir else DEFAULT_RENTS_DIR
    if not rents_dir.exists():
        return None

    if geo_type == "metro":
        yearly_files = list(rents_dir.glob("zori_yearly__metro__*.parquet"))
        if not yearly_files:
            yearly_files = list(rents_dir.glob("zori__metro__*.parquet"))
    else:
        yearly_files = list(rents_dir.glob("zori_yearly__A*.parquet"))
        if not yearly_files:
            yearly_files = list(rents_dir.glob("coc_zori_yearly__*.parquet"))

    if not yearly_files:
        return None

    # Return the most recently modified file
    yearly_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return yearly_files[0]


def build_panel_cmd(
    start: Annotated[
        int,
        typer.Option(
            "--start",
            "-s",
            help="First PIT year to include in the panel (inclusive).",
        ),
    ],
    end: Annotated[
        int,
        typer.Option(
            "--end",
            "-e",
            help="Last PIT year to include in the panel (inclusive).",
        ),
    ],
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method for ACS measures: 'population' or 'area'.",
        ),
    ] = "population",
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Custom output path for the panel Parquet file.",
        ),
    ] = None,
    build: Annotated[
        str,
        typer.Option(
            "--build",
            help="Named build directory for outputs and build-local artifacts.",
        ),
    ] = ...,
    include_zori: Annotated[
        bool,
        typer.Option(
            "--include-zori/--no-include-zori",
            help="Include ZORI rent data and compute rent_to_income ratio.",
        ),
    ] = False,
    zori_yearly_path: Annotated[
        Path | None,
        typer.Option(
            "--zori-yearly-path",
            help="Explicit path to yearly ZORI parquet. If omitted, searches defaults.",
        ),
    ] = None,
    zori_min_coverage: Annotated[
        float,
        typer.Option(
            "--zori-min-coverage",
            help="Minimum ZORI coverage ratio for eligibility (0.0-1.0).",
        ),
    ] = DEFAULT_ZORI_MIN_COVERAGE,
    strict: Annotated[
        bool,
        typer.Option(
            "--strict/--no-strict",
            help="Treat conformance errors as build failures (exit non-zero).",
        ),
    ] = False,
    skip_conformance: Annotated[
        bool,
        typer.Option(
            "--skip-conformance/--no-skip-conformance",
            help="Skip all post-build conformance checks.",
        ),
    ] = False,
    geo_type: Annotated[
        str | None,
        typer.Option(
            "--geo-type",
            help="Target analysis geography: 'coc' or 'metro'. Defaults to the build manifest or 'coc'.",
        ),
    ] = None,
    definition_version: Annotated[
        str | None,
        typer.Option(
            "--definition-version",
            help="Synthetic geography definition version required for metro panels.",
        ),
    ] = None,
    include_acs1: Annotated[
        bool,
        typer.Option(
            "--include-acs1/--no-include-acs1",
            help="Include ACS 1-year metro measures (e.g., unemployment_rate_acs1). Metro only.",
        ),
    ] = False,
) -> None:
    """Build an analysis-geography x year panel.

    Constructs an analysis-ready panel by joining PIT counts with ACS measures
    for each year in the specified range, using alignment policies to determine
    which boundary and ACS vintages to use. CoC remains the default target.
    Metro builds require a definition version and read metro aggregate outputs.

    ZORI Integration (optional):

    When --include-zori is specified, the panel will include ZORI rent data and
    compute a rent_to_income affordability ratio. This adds four columns:

    - zori_coc: CoC-level ZORI (yearly rent index)
    - zori_coverage_ratio: Coverage of base geography weights (0-1)
    - zori_is_eligible: Boolean eligibility flag based on coverage threshold
    - rent_to_income: ZORI divided by monthly median household income

    A CoC-year is ZORI-eligible if its coverage_ratio >= --zori-min-coverage
    (default 0.90). Ineligible observations will have null zori_coc and
    rent_to_income values.

    Prerequisites for ZORI integration:
    - Run 'coclab ingest zori' to download ZORI data
    - Run 'coclab aggregate zori --build <BUILD>' to create CoC-level ZORI

    Examples:

        coclab build panel --start 2018 --end 2024

        coclab build panel --start 2018 --end 2024 --weighting population

        coclab build panel --start 2018 --end 2024 --weighting area

        coclab build panel --start 2020 --end 2024 --output custom_panel.parquet

        coclab build panel --start 2018 --end 2024 --include-zori

        coclab build panel --start 2018 --end 2024 --include-zori --zori-min-coverage 0.85

        coclab build panel --start 2018 --end 2024 --include-zori \\
            --zori-yearly-path data/curated/zori/coc_zori_yearly.parquet

        coclab build panel --build demo --start 2018 --end 2024
    """
    from coclab.analysis_geo import resolve_geo_col
    from coclab.builds import read_build_manifest
    from coclab.panel import AlignmentPolicy, build_panel, save_panel
    from coclab.panel.conformance import PanelRequest, run_conformance
    from coclab.panel.policies import default_acs_vintage, default_boundary_vintage

    # Validate weighting method
    valid_weighting = {"population", "area"}
    if weighting not in valid_weighting:
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. "
            f"Must be one of: {', '.join(sorted(valid_weighting))}",
            err=True,
        )
        raise typer.Exit(1)

    # Validate year range
    if start > end:
        typer.echo(
            f"Error: Start year ({start}) must be less than or equal to end year ({end}).",
            err=True,
        )
        raise typer.Exit(1)

    # Validate ZORI coverage threshold
    if not 0.0 <= zori_min_coverage <= 1.0:
        typer.echo(
            f"Error: --zori-min-coverage must be between 0.0 and 1.0, got {zori_min_coverage}.",
            err=True,
        )
        raise typer.Exit(1)

    # Resolve build directory
    try:
        build_dir = require_build_dir(build)
    except FileNotFoundError as exc:
        build_path = resolve_build_dir(build)
        typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
        typer.echo("Run: coclab build create --name <build>", err=True)
        raise typer.Exit(2) from exc
    build_curated = build_curated_dir(build_dir)
    manifest = read_build_manifest(build_dir)
    manifest_build = manifest.get("build", {})
    resolved_geo_type = geo_type or manifest_build.get("geo_type") or "coc"
    resolved_definition_version = (
        definition_version or manifest_build.get("definition_version")
    )
    if resolved_geo_type not in {"coc", "metro"}:
        typer.echo(
            f"Error: Unsupported --geo-type '{resolved_geo_type}'. Use 'coc' or 'metro'.",
            err=True,
        )
        raise typer.Exit(2)
    if resolved_geo_type == "metro" and not resolved_definition_version:
        typer.echo(
            "Error: metro panel builds require --definition-version or a build manifest definition_version.",
            err=True,
        )
        raise typer.Exit(2)

    # Validate ZORI data availability if --include-zori is set
    resolved_zori_path: Path | None = None
    if include_zori:
        resolved_zori_path = _resolve_zori_yearly_path(
            zori_yearly_path,
            base_dir=build_dir,
            geo_type=resolved_geo_type,
        )
        if resolved_zori_path is None:
            typer.echo(
                "Error: --include-zori was specified but no ZORI yearly data is available.",
                err=True,
            )
            typer.echo("")
            typer.echo("To generate ZORI yearly data, run:")
            typer.echo("  coclab ingest zori --geography county")
            typer.echo(
                "  coclab aggregate zori --build <BUILD>"
            )
            typer.echo("")
            if zori_yearly_path:
                typer.echo(f"Specified path does not exist: {zori_yearly_path}")
            else:
                if build_dir is not None:
                    build_zori_dir = build_dir / "data" / "curated" / "zori"
                    typer.echo(
                        f"No yearly ZORI file found in build location: {build_zori_dir}/"
                    )
                else:
                    typer.echo("No yearly ZORI file found in default location: data/curated/zori/")
            raise typer.Exit(1)
        typer.echo(f"ZORI yearly data: {resolved_zori_path}")

    typer.echo(
        f"Building {resolved_geo_type} panel for {start}-{end} with {weighting} weighting..."
    )
    if resolved_definition_version is not None:
        typer.echo(f"  Definition version: {resolved_definition_version}")
    if include_zori:
        typer.echo(f"  ZORI integration enabled (min coverage: {zori_min_coverage:.2f})")
    if include_acs1:
        if resolved_geo_type != "metro":
            typer.echo(
                "Warning: --include-acs1 is only supported for metro panels; ignoring.",
                err=True,
            )
            include_acs1 = False
        else:
            typer.echo("  ACS 1-year integration enabled")

    # Create alignment policy
    policy = AlignmentPolicy(
        boundary_vintage_func=default_boundary_vintage,
        acs_vintage_func=default_acs_vintage,
        weighting_method=weighting,  # type: ignore[arg-type]
    )

    # Resolve build-scoped directories for PIT and measures so that
    # `build panel` reads aggregate outputs produced by earlier steps.
    build_pit_dir = build_curated / "pit"
    build_measures_dir = build_curated / "measures"
    build_rents_dir = build_curated / "zori"
    build_acs1_dir = build_curated / "acs"

    # Build the panel
    try:
        panel_df = build_panel(
            start_year=start,
            end_year=end,
            policy=policy,
            pit_dir=build_pit_dir if build_pit_dir.exists() else None,
            measures_dir=build_measures_dir if build_measures_dir.exists() else None,
            include_zori=include_zori,
            zori_yearly_path=resolved_zori_path,
            rents_dir=build_rents_dir if build_rents_dir.exists() else None,
            zori_min_coverage=zori_min_coverage,
            geo_type=resolved_geo_type,
            definition_version=resolved_definition_version,
            include_acs1=include_acs1,
            acs1_dir=build_acs1_dir if build_acs1_dir.exists() else None,
        )
    except Exception as e:
        typer.echo(f"Error building panel: {e}", err=True)
        raise typer.Exit(1) from e

    if panel_df.empty:
        typer.echo("Warning: Panel is empty. No data found for the specified year range.")
        raise typer.Exit(1)

    conformance_report = None
    if not skip_conformance:
        # Derive expected_geo_count from an independent source, not the panel
        # itself.  For metro panels the definition encodes a fixed count; for
        # CoC panels we have no single authoritative count so we leave it to
        # the check to skip.
        independent_geo_count: int | None = None
        if resolved_geo_type == "metro":
            from coclab.metro.definitions import METRO_COUNT

            independent_geo_count = METRO_COUNT

        conformance_request = PanelRequest(
            start_year=start,
            end_year=end,
            include_zori=include_zori,
            weighting_method=weighting,  # type: ignore[arg-type]
            zori_min_coverage=zori_min_coverage,
            geo_type=resolved_geo_type,
            expected_geo_count=independent_geo_count,
        )
        conformance_report = run_conformance(panel_df, conformance_request)
        typer.echo("")
        typer.echo(conformance_report.summary())
        if strict and not conformance_report.passed:
            typer.echo("Error: conformance checks failed under --strict.", err=True)
            raise typer.Exit(1)

    # Save the panel
    try:
        if output:
            # Custom output path
            output_dir = output.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

            provenance = ProvenanceBlock(
                weighting=weighting,
                geo_type=resolved_geo_type,
                definition_version=resolved_definition_version,
                extra={
                    "dataset_type": f"{resolved_geo_type}_panel",
                    "start_year": start,
                    "end_year": end,
                    "row_count": len(panel_df),
                    "geo_count": int(panel_df[resolve_geo_col(panel_df)].nunique()),
                    "year_count": int(panel_df["year"].nunique()),
                    "policy": policy.to_dict(),
                    "geo_type": resolved_geo_type,
                },
            )
            if "coc_id" in panel_df.columns:
                provenance.extra["coc_count"] = int(panel_df["coc_id"].nunique())
            if conformance_report is not None:
                provenance.extra["conformance"] = conformance_report.to_dict()
            write_parquet_with_provenance(panel_df, output, provenance)
            output_path = output
        else:
            # Default output path
            output_path = save_panel(
                df=panel_df,
                start_year=start,
                end_year=end,
                output_dir=(build_curated / "panel") if build_curated else DEFAULT_PANEL_DIR,
                policy=policy,
                conformance_report=conformance_report,
                geo_type=resolved_geo_type,
                definition_version=resolved_definition_version,
            )
        typer.echo(f"Saved panel to: {output_path}")
    except Exception as e:
        typer.echo(f"Error saving panel: {e}", err=True)
        raise typer.Exit(1) from e

    # Display summary
    typer.echo("")
    typer.echo("Panel Summary:")
    typer.echo(f"  Years: {start} - {end} ({panel_df['year'].nunique()} years)")
    geo_col = resolve_geo_col(panel_df)
    geo_label = "CoCs" if resolved_geo_type == "coc" else "Metros"
    typer.echo(f"  {geo_label}: {panel_df[geo_col].nunique()}")
    typer.echo(f"  Total rows: {len(panel_df)}")
    typer.echo(f"  Weighting: {weighting}")

    # Coverage statistics
    if "coverage_ratio" in panel_df.columns:
        coverage = panel_df["coverage_ratio"].dropna()
        if len(coverage) > 0:
            typer.echo("")
            typer.echo("Coverage Statistics:")
            typer.echo(f"  Mean coverage ratio: {coverage.mean():.3f}")
            typer.echo(f"  Min coverage ratio: {coverage.min():.3f}")
            typer.echo(f"  Max coverage ratio: {coverage.max():.3f}")
            low_coverage = (coverage < 0.9).sum()
            typer.echo(f"  Low coverage (<0.9): {low_coverage} observations")

    # Boundary changes
    if "boundary_changed" in panel_df.columns:
        changes = panel_df["boundary_changed"].sum()
        if changes > 0:
            typer.echo("")
            typer.echo(f"Boundary Changes: {int(changes)} observations had boundary changes")

    # ZORI integration summary (if enabled)
    if include_zori:
        typer.echo("")
        typer.echo("ZORI Integration:")
        typer.echo(f"  Source file: {resolved_zori_path}")
        typer.echo(f"  Coverage threshold: {zori_min_coverage:.2f}")

        # Report ZORI statistics if columns are present
        if "zori_coc" in panel_df.columns:
            zori_with_data = panel_df["zori_coc"].notna().sum()
            total_rows = len(panel_df)
            typer.echo(f"  {resolved_geo_type}-years with ZORI data: {zori_with_data} / {total_rows}")

            geos_with_zori = panel_df.loc[panel_df["zori_coc"].notna(), geo_col].nunique()
            total_geos = panel_df[geo_col].nunique()
            typer.echo(f"  {geo_label} with any ZORI data: {geos_with_zori} / {total_geos}")
        else:
            typer.echo(
                "  Note: ZORI columns not present (check ZORI data compatibility with panel years)"
            )

        if "zori_is_eligible" in panel_df.columns:
            eligible_count = panel_df["zori_is_eligible"].sum()
            typer.echo(f"  ZORI-eligible observations: {int(eligible_count)}")

        if "rent_to_income" in panel_df.columns:
            rti_count = panel_df["rent_to_income"].notna().sum()
            typer.echo(f"  Observations with rent_to_income: {rti_count}")

            if rti_count > 0:
                rti_mean = panel_df["rent_to_income"].mean()
                rti_median = panel_df["rent_to_income"].median()
                typer.echo(f"  Mean rent_to_income: {rti_mean:.3f}")
                typer.echo(f"  Median rent_to_income: {rti_median:.3f}")

    typer.echo("")
    typer.echo(f"Output: {output_path}")
