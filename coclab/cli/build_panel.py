"""CLI command for building CoC x year panels."""

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
    explicit_path: Path | None, base_dir: Path | None = None
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

    # Try new naming convention first: zori_yearly__A*.parquet
    yearly_files = list(rents_dir.glob("zori_yearly__A*.parquet"))

    # Fall back to legacy naming: coc_zori_yearly__*.parquet
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
) -> None:
    """Build a CoC x year analysis panel.

    Constructs an analysis-ready panel by joining PIT counts with ACS measures
    for each year in the specified range, using alignment policies to determine
    which boundary and ACS vintages to use.

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
    - Run 'coclab ingest-zori' to download ZORI data
    - Run 'coclab build zori --to-yearly' to create yearly CoC-level ZORI

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
    from coclab.panel import AlignmentPolicy, build_panel, save_panel
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
    except FileNotFoundError:
        build_path = resolve_build_dir(build)
        typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
        typer.echo("Run: coclab build create --name <build>", err=True)
        raise typer.Exit(2)
    build_curated = build_curated_dir(build_dir)

    # Validate ZORI data availability if --include-zori is set
    resolved_zori_path: Path | None = None
    if include_zori:
        resolved_zori_path = _resolve_zori_yearly_path(
            zori_yearly_path, base_dir=build_dir
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
                "  coclab build zori --boundary <VINTAGE> --counties <YEAR> "
                "--acs <ACS_VINTAGE> --to-yearly"
            )
            typer.echo("")
            if zori_yearly_path:
                typer.echo(f"Specified path does not exist: {zori_yearly_path}")
            else:
                if build_dir is not None:
                    typer.echo(
                        f"No yearly ZORI file found in build location: {build_dir / 'data' / 'curated' / 'zori'}/"
                    )
                else:
                    typer.echo("No yearly ZORI file found in default location: data/curated/zori/")
            raise typer.Exit(1)
        typer.echo(f"ZORI yearly data: {resolved_zori_path}")

    typer.echo(f"Building panel for {start}-{end} with {weighting} weighting...")
    if include_zori:
        typer.echo(f"  ZORI integration enabled (min coverage: {zori_min_coverage:.2f})")

    # Create alignment policy
    policy = AlignmentPolicy(
        boundary_vintage_func=default_boundary_vintage,
        acs_vintage_func=default_acs_vintage,
        weighting_method=weighting,  # type: ignore[arg-type]
    )

    # Build the panel
    try:
        panel_df = build_panel(
            start_year=start,
            end_year=end,
            policy=policy,
            include_zori=include_zori,
            zori_yearly_path=resolved_zori_path,
            zori_min_coverage=zori_min_coverage,
        )
    except Exception as e:
        typer.echo(f"Error building panel: {e}", err=True)
        raise typer.Exit(1) from e

    if panel_df.empty:
        typer.echo("Warning: Panel is empty. No data found for the specified year range.")
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
                extra={
                    "dataset_type": "coc_panel",
                    "start_year": start,
                    "end_year": end,
                    "row_count": len(panel_df),
                    "coc_count": int(panel_df["coc_id"].nunique()),
                    "year_count": int(panel_df["year"].nunique()),
                    "policy": policy.to_dict(),
                },
            )
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
            )
        typer.echo(f"Saved panel to: {output_path}")
    except Exception as e:
        typer.echo(f"Error saving panel: {e}", err=True)
        raise typer.Exit(1) from e

    # Display summary
    typer.echo("")
    typer.echo("Panel Summary:")
    typer.echo(f"  Years: {start} - {end} ({panel_df['year'].nunique()} years)")
    typer.echo(f"  CoCs: {panel_df['coc_id'].nunique()}")
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
            typer.echo(f"  CoC-years with ZORI data: {zori_with_data} / {total_rows}")

            # Count by unique CoCs
            cocs_with_zori = panel_df.loc[panel_df["zori_coc"].notna(), "coc_id"].nunique()
            total_cocs = panel_df["coc_id"].nunique()
            typer.echo(f"  CoCs with any ZORI data: {cocs_with_zori} / {total_cocs}")
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
